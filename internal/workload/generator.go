package workload

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"failforge/internal/config"
	"failforge/internal/model"
	"failforge/internal/store"
)

type Generator struct {
	cfg                  *config.Config
	runID                string
	seed                 int64
	store                *store.Store
	onEvent              func(timeMs int64, category, eventType, payloadJSON string)
	startTime            time.Time
	proxyURL             string
	httpClient           *http.Client
	IsBlocked            func(from, to string) bool
	GetDelay             func(from, to string) time.Duration
	coordinationSessions sync.Map
	mu                   sync.RWMutex
	currentLeader        string
}

type OpWeight struct {
	name   string
	weight int
}

func NewGenerator(cfg *config.Config, runID string, seed int64, store *store.Store, onEvent func(timeMs int64, category, eventType, payloadJSON string)) *Generator {
	return &Generator{
		cfg:      cfg,
		runID:    runID,
		seed:     seed,
		store:    store,
		onEvent:  onEvent,
		proxyURL: fmt.Sprintf("http://localhost:%d", cfg.Network.ProxyPort),
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

func (g *Generator) Start(ctx context.Context, startTime time.Time) {
	g.startTime = startTime
	clientsCount := g.cfg.Workload.Clients
	if clientsCount <= 0 {
		clientsCount = 1
	}

	// Start leadership status monitor
	go g.monitorLeadership(ctx)

	// 1. Setup operations weights
	var opWeights []OpWeight
	totalWeight := 0
	for opName, v := range g.cfg.Workload.Operations {
		weight := 0
		switch val := v.(type) {
		case int:
			weight = val
		case float64:
			weight = int(val)
		case map[string]interface{}:
			if w, ok := val["weight"].(int); ok {
				weight = w
			} else if wf, ok := val["weight"].(float64); ok {
				weight = int(wf)
			}
		}
		if weight > 0 {
			opWeights = append(opWeights, OpWeight{name: opName, weight: weight})
			totalWeight += weight
		}
	}

	if len(opWeights) == 0 {
		opWeights = []OpWeight{
			{name: "get", weight: 5},
			{name: "put", weight: 5},
		}
		totalWeight = 10
	}

	// 2. Setup deterministic seeds for client goroutines
	masterRand := rand.New(rand.NewSource(g.seed))
	clientSeeds := make([]int64, clientsCount)
	for i := 0; i < clientsCount; i++ {
		clientSeeds[i] = masterRand.Int63()
	}

	// 3. Spawn client threads
	var wg sync.WaitGroup
	for i := 0; i < clientsCount; i++ {
		wg.Add(1)
		clientID := fmt.Sprintf("client-%d", i+1)
		go g.worker(ctx, &wg, clientID, clientSeeds[i], opWeights, totalWeight)
	}

	wg.Wait()
}

func (g *Generator) worker(ctx context.Context, wg *sync.WaitGroup, clientID string, seed int64, opWeights []OpWeight, totalWeight int) {
	defer wg.Done()

	r := rand.New(rand.NewSource(seed))
	opCounter := 0
	keys := g.cfg.Workload.Keys
	if len(keys) == 0 {
		keys = []string{"x"}
	}

	nodeCount := g.cfg.System.Nodes.Count
	if nodeCount <= 0 {
		nodeCount = 1
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			opCounter++
			opID := fmt.Sprintf("op-%s-%d", clientID, opCounter)

			// Select operation type
			rVal := r.Intn(totalWeight)
			curr := 0
			selectedOp := "get"
			for _, opW := range opWeights {
				curr += opW.weight
				if rVal < curr {
					selectedOp = opW.name
					break
				}
			}

			// Select key, value, and target node deterministically
			key := keys[r.Intn(len(keys))]
			val := fmt.Sprintf("%d", r.Intn(100))
			targetIdx := r.Intn(nodeCount) + 1
			targetNode := fmt.Sprintf("node-%d", targetIdx)

			startMs := time.Since(g.startTime).Milliseconds()
			g.onEvent(startMs, "Operation", "OperationInvoked", fmt.Sprintf(
				`{"op_id":"%s","client_id":"%s","op":"%s","key":"%s","target":"%s"}`,
				opID, clientID, selectedOp, key, targetNode,
			))

			// Execute request
			opStatus, inputJSON, outputJSON := g.executeRequest(ctx, clientID, targetNode, selectedOp, key, val)
			endMs := time.Since(g.startTime).Milliseconds()

			// Log completed operation record to DB
			_ = g.store.CreateOperation(&model.Operation{
				OpID:       opID,
				RunID:      g.runID,
				ClientID:   clientID,
				Operation:  strings.ToUpper(selectedOp),
				InputJSON:  inputJSON,
				OutputJSON: outputJSON,
				StartMs:    startMs,
				EndMs:      endMs,
				Status:     opStatus,
			})

			g.onEvent(endMs, "Operation", "OperationCompleted", fmt.Sprintf(
				`{"op_id":"%s","client_id":"%s","op":"%s","status":"%s","latency_ms":%d}`,
				opID, clientID, selectedOp, opStatus, endMs-startMs,
			))

			// Sleep brief jitter to rate limit requests
			sleepJitter := time.Duration(50+r.Intn(50)) * time.Millisecond
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleepJitter):
			}
		}
	}
}

func (g *Generator) executeRequest(ctx context.Context, clientID string, targetNode string, op string, key string, val string) (string, string, string) {
	if g.cfg.Workload.Type == "coordination-service" {
		return g.executeCoordinationRequest(ctx, clientID, targetNode, op, key, val)
	} else if g.cfg.Workload.Type == "mini-redis-cassandra" {
		return g.executeMiniDBRequest(ctx, clientID, targetNode, op, key, val)
	}

	method := "GET"
	reqURL := fmt.Sprintf("%s/keys/%s", g.proxyURL, key)
	var reqBody io.Reader
	inputJSON := fmt.Sprintf(`{"key":"%s"}`, key)

	if strings.ToLower(op) == "put" {
		method = "PUT"
		reqBody = bytes.NewBufferString(val)
		inputJSON = fmt.Sprintf(`{"key":"%s","value":"%s"}`, key, val)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, reqBody)
	if err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"failed to create request: %v"}`, err)
	}

	req.Header.Set("X-FailForge-From", clientID)
	req.Header.Set("X-FailForge-To", targetNode)
	req.Header.Set("X-FailForge-MsgType", "client_req")
	if method == "PUT" {
		req.Header.Set("Content-Type", "text/plain")
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	bodyStr := strings.TrimSpace(string(respBody))
	outputJSON := fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, resp.StatusCode, strings.ReplaceAll(bodyStr, `"`, `\"`))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "ok", inputJSON, outputJSON
	}

	return "fail", inputJSON, outputJSON
}

func (g *Generator) openCoordinationSession(ctx context.Context, targetNode string) string {
	g.mu.RLock()
	leaderNode := g.currentLeader
	g.mu.RUnlock()
	if leaderNode != "" {
		targetNode = leaderNode
	}

	reqBody := bytes.NewBufferString(`{"timeout_seconds":60}`)
	req, err := http.NewRequestWithContext(ctx, "POST", g.proxyURL+"/api/session/open", reqBody)
	if err != nil {
		return ""
	}
	req.Header.Set("X-FailForge-From", "client")
	req.Header.Set("X-FailForge-To", targetNode)
	req.Header.Set("X-FailForge-MsgType", "client_req")
	req.Header.Set("Content-Type", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}

	var res struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return ""
	}
	return res.SessionID
}

func (g *Generator) executeCoordinationRequest(ctx context.Context, clientID string, targetNode string, op string, key string, val string) (string, string, string) {
	opType := strings.ToLower(op)

	// Route write operations to the active leader
	if opType == "lock" || opType == "unlock" || opType == "put" {
		g.mu.RLock()
		leaderNode := g.currentLeader
		g.mu.RUnlock()
		if leaderNode != "" {
			targetNode = leaderNode
		}
	}

	sessionVal, ok := g.coordinationSessions.Load(clientID)
	var sessionID string
	if !ok {
		sessionID = g.openCoordinationSession(ctx, targetNode)
		if sessionID != "" {
			g.coordinationSessions.Store(clientID, sessionID)
		}
	} else {
		sessionID = sessionVal.(string)
	}

	if opType == "lock" {
		if sessionID == "" {
			sessionID = g.openCoordinationSession(ctx, targetNode)
			if sessionID == "" {
				return "fail", fmt.Sprintf(`{"key":"/locks/%s"}`, key), `{"error":"no session"}`
			}
			g.coordinationSessions.Store(clientID, sessionID)
		}

		path := "/locks/" + key
		inputJSON := fmt.Sprintf(`{"key":"%s","session_id":"%s"}`, path, sessionID)

		reqPayload := map[string]interface{}{
			"path":                 path,
			"session_id":           sessionID,
			"holder":               clientID,
			"lease_ttl_seconds":    10.0,
			"wait_timeout_seconds": 0.0,
		}
		payloadBytes, _ := json.Marshal(reqPayload)
		req, err := http.NewRequestWithContext(ctx, "POST", g.proxyURL+"/api/lease/acquire", bytes.NewBuffer(payloadBytes))
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%v"}`, err)
		}
		req.Header.Set("X-FailForge-From", clientID)
		req.Header.Set("X-FailForge-To", targetNode)
		req.Header.Set("X-FailForge-MsgType", "client_req")
		req.Header.Set("Content-Type", "application/json")

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		outputJSON := fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, resp.StatusCode, strings.TrimSpace(strings.ReplaceAll(string(bodyBytes), `"`, `\"`)))

		if resp.StatusCode == http.StatusOK {
			return "ok", inputJSON, outputJSON
		}
		return "fail", inputJSON, outputJSON

	} else if opType == "unlock" {
		if sessionID == "" {
			return "fail", fmt.Sprintf(`{"key":"/locks/%s"}`, key), `{"error":"no session"}`
		}

		path := "/locks/" + key
		inputJSON := fmt.Sprintf(`{"key":"%s","session_id":"%s"}`, path, sessionID)

		reqPayload := map[string]interface{}{
			"path":       path,
			"session_id": sessionID,
		}
		payloadBytes, _ := json.Marshal(reqPayload)
		req, err := http.NewRequestWithContext(ctx, "POST", g.proxyURL+"/api/lease/release", bytes.NewBuffer(payloadBytes))
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%v"}`, err)
		}
		req.Header.Set("X-FailForge-From", clientID)
		req.Header.Set("X-FailForge-To", targetNode)
		req.Header.Set("X-FailForge-MsgType", "client_req")
		req.Header.Set("Content-Type", "application/json")

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		outputJSON := fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, resp.StatusCode, strings.TrimSpace(strings.ReplaceAll(string(bodyBytes), `"`, `\"`)))

		if resp.StatusCode == http.StatusOK {
			return "ok", inputJSON, outputJSON
		}
		return "fail", inputJSON, outputJSON

	} else if opType == "get" {
		path := "/data/" + key
		inputJSON := fmt.Sprintf(`{"key":"%s"}`, path)

		reqURL := fmt.Sprintf("%s/api/node/get?path=%s", g.proxyURL, url.QueryEscape(path))
		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%v"}`, err)
		}
		req.Header.Set("X-FailForge-From", clientID)
		req.Header.Set("X-FailForge-To", targetNode)
		req.Header.Set("X-FailForge-MsgType", "client_req")

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyStr := strings.TrimSpace(string(bodyBytes))

		if resp.StatusCode == http.StatusOK {
			var res struct {
				Data string `json:"data"`
			}
			if err := json.Unmarshal(bodyBytes, &res); err == nil {
				return "ok", inputJSON, fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, resp.StatusCode, strings.ReplaceAll(res.Data, `"`, `\"`))
			}
		} else if resp.StatusCode == http.StatusNotFound {
			return "ok", inputJSON, `{"status_code":404,"body":"null"}`
		}

		return "fail", inputJSON, fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, resp.StatusCode, strings.ReplaceAll(bodyStr, `"`, `\"`))

	} else if opType == "put" {
		path := "/data/" + key
		inputJSON := fmt.Sprintf(`{"key":"%s","value":"%s"}`, path, val)

		reqPayload := map[string]interface{}{
			"path":       path,
			"data":       val,
			"persistent": true,
		}
		payloadBytes, _ := json.Marshal(reqPayload)
		req, err := http.NewRequestWithContext(ctx, "POST", g.proxyURL+"/api/node/create", bytes.NewBuffer(payloadBytes))
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%v"}`, err)
		}
		req.Header.Set("X-FailForge-From", clientID)
		req.Header.Set("X-FailForge-To", targetNode)
		req.Header.Set("X-FailForge-MsgType", "client_req")
		req.Header.Set("Content-Type", "application/json")

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusOK {
			return "ok", inputJSON, fmt.Sprintf(`{"status_code":200,"body":"%s"}`, val)
		} else if resp.StatusCode == http.StatusConflict || resp.StatusCode == 409 {
			setPayload := map[string]interface{}{
				"path": path,
				"data": val,
			}
			setBytes, _ := json.Marshal(setPayload)
			setReq, err := http.NewRequestWithContext(ctx, "POST", g.proxyURL+"/api/node/set", bytes.NewBuffer(setBytes))
			if err != nil {
				return "fail", inputJSON, fmt.Sprintf(`{"error":"%v"}`, err)
			}
			setReq.Header.Set("X-FailForge-From", clientID)
			setReq.Header.Set("X-FailForge-To", targetNode)
			setReq.Header.Set("X-FailForge-MsgType", "client_req")
			setReq.Header.Set("Content-Type", "application/json")

			setResp, err := g.httpClient.Do(setReq)
			if err != nil {
				return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
			}
			defer setResp.Body.Close()

			setBodyBytes, _ := io.ReadAll(setResp.Body)
			if setResp.StatusCode == http.StatusOK {
				return "ok", inputJSON, fmt.Sprintf(`{"status_code":200,"body":"%s"}`, val)
			}
			return "fail", inputJSON, fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, setResp.StatusCode, strings.TrimSpace(strings.ReplaceAll(string(setBodyBytes), `"`, `\"`)))
		}

		return "fail", inputJSON, fmt.Sprintf(`{"status_code":%d,"body":"%s"}`, resp.StatusCode, strings.TrimSpace(strings.ReplaceAll(string(bodyBytes), `"`, `\"`)))
	}

	return "fail", fmt.Sprintf(`{"key":"%s"}`, key), `{"error":"unknown operation"}`
}

func (g *Generator) executeMiniDBRequest(ctx context.Context, clientID string, targetNode string, op string, key string, val string) (string, string, string) {
	targetIdx := 0
	_, _ = fmt.Sscanf(targetNode, "node-%d", &targetIdx)
	if targetIdx <= 0 {
		return "fail", "", fmt.Sprintf(`{"error":"invalid target node: %s"}`, targetNode)
	}
	startPort := g.cfg.System.Nodes.Ports.Start
	port := startPort + targetIdx - 1

	inputJSON := fmt.Sprintf(`{"key":"%s"}`, key)
	if strings.ToLower(op) == "put" {
		inputJSON = fmt.Sprintf(`{"key":"%s","value":"%s"}`, key, val)
	}

	if g.IsBlocked != nil && g.IsBlocked(clientID, targetNode) {
		return "fail", inputJSON, `{"status_code":504,"body":"","error":"network partition client block"}`
	}
	if g.GetDelay != nil {
		delay := g.GetDelay(clientID, targetNode)
		if delay > 0 {
			select {
			case <-ctx.Done():
				return "fail", inputJSON, `{"error":"context cancelled"}`
			case <-time.After(delay):
			}
		}
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 1*time.Second)
	if err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(err.Error(), `"`, `\"`))
	}
	defer conn.Close()

	var reqMsg struct {
		Type    string                 `json:"type"`
		Sender  string                 `json:"sender"`
		Payload map[string]interface{} `json:"payload"`
	}
	reqMsg.Type = "CMD"
	reqMsg.Sender = clientID
	if strings.ToLower(op) == "put" {
		reqMsg.Payload = map[string]interface{}{
			"cmd":  "SET",
			"args": []string{key, val},
		}
	} else {
		reqMsg.Payload = map[string]interface{}{
			"cmd":  "GET",
			"args": []string{key},
		}
	}

	reqBytes, err := json.Marshal(reqMsg)
	if err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"failed to marshal cmd: %v"}`, err)
	}
	reqStr := fmt.Sprintf("%d:%s\n", len(reqBytes), string(reqBytes))
	_, err = conn.Write([]byte(reqStr))
	if err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"failed to send cmd: %v"}`, err)
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"failed to read response: %v"}`, err)
	}
	line = strings.TrimSpace(line)
	if idx := strings.Index(line, ":"); idx != -1 {
		line = line[idx+1:]
	}

	var respMsg struct {
		Type    string `json:"type"`
		Payload struct {
			Success bool        `json:"success"`
			Data    interface{} `json:"data"`
			Error   string      `json:"error"`
		} `json:"payload"`
	}
	if err := json.Unmarshal([]byte(line), &respMsg); err != nil {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"failed to parse response: %v"}`, err)
	}

	if !respMsg.Payload.Success {
		return "fail", inputJSON, fmt.Sprintf(`{"error":"%s"}`, strings.ReplaceAll(respMsg.Payload.Error, `"`, `\"`))
	}

	var dataStr string
	if respMsg.Payload.Data != nil {
		dataStr = fmt.Sprintf("%v", respMsg.Payload.Data)
	} else {
		dataStr = "null"
	}

	outputJSON := fmt.Sprintf(`{"body":"%s"}`, strings.ReplaceAll(dataStr, `"`, `\"`))
	return "ok", inputJSON, outputJSON
}

func (g *Generator) monitorLeadership(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	lastLeaderInTerm := make(map[int]string)
	lastActiveLeader := ""

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nodeCount := g.cfg.System.Nodes.Count
			startPort := g.cfg.System.Nodes.Ports.Start

			currentLeader := ""
			currentTerm := 0

			for i := 1; i <= nodeCount; i++ {
				nodeID := fmt.Sprintf("node-%d", i)
				port := startPort + i - 1

				if g.cfg.Workload.Type == "coordination-service" {
					if g.IsBlocked != nil && g.IsBlocked("client", nodeID) {
						continue
					}
					url := fmt.Sprintf("http://localhost:%d/api/cluster/status", port)
					req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
					if err != nil {
						continue
					}
					resp, err := g.httpClient.Do(req)
					if err != nil {
						continue
					}
					var status struct {
						NodeID      string `json:"node_id"`
						Role        string `json:"role"`
						LeaderID    string `json:"leader_id"`
						CurrentTerm int    `json:"current_term"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&status); err == nil {
						if status.Role == "leader" {
							currentLeader = status.NodeID
							currentTerm = status.CurrentTerm
						}
					}
					resp.Body.Close()
				} else if g.cfg.Workload.Type == "mini-redis-cassandra" {
					if g.IsBlocked != nil && g.IsBlocked("client", nodeID) {
						continue
					}

					conn, err := net.DialTimeout("tcp", fmt.Sprintf("localhost:%d", port), 100*time.Millisecond)
					if err != nil {
						continue
					}

					reqMsg := struct {
						Type    string                 `json:"type"`
						Sender  string                 `json:"sender"`
						Payload map[string]interface{} `json:"payload"`
					}{
						Type:   "CMD",
						Sender: "client",
						Payload: map[string]interface{}{
							"cmd":  "INFO",
							"args": []string{},
						},
					}
					reqBytes, err := json.Marshal(reqMsg)
					if err != nil {
						conn.Close()
						continue
					}
					reqStr := fmt.Sprintf("%d:%s\n", len(reqBytes), string(reqBytes))
					_, _ = conn.Write([]byte(reqStr))

					reader := bufio.NewReader(conn)
					line, err := reader.ReadString('\n')
					conn.Close()
					if err != nil {
						continue
					}
					line = strings.TrimSpace(line)
					if idx := strings.Index(line, ":"); idx != -1 {
						line = line[idx+1:]
					}

					var respMsg struct {
						Type    string `json:"type"`
						Payload struct {
							Success bool `json:"success"`
							Data    struct {
								NodeID   string `json:"node_id"`
								Role     string `json:"role"`
								LeaderID string `json:"leader_id"`
								Term     int    `json:"term"`
							} `json:"data"`
						} `json:"payload"`
					}
					if err := json.Unmarshal([]byte(line), &respMsg); err == nil {
						if respMsg.Payload.Success && respMsg.Payload.Data.Role == "leader" {
							currentLeader = respMsg.Payload.Data.NodeID
							currentTerm = respMsg.Payload.Data.Term
						}
					}
				}
			}

			// Update generator leader state
			g.mu.Lock()
			g.currentLeader = currentLeader
			g.mu.Unlock()

			if currentLeader != "" {
				prevLeaderInTerm, seenTerm := lastLeaderInTerm[currentTerm]
				if !seenTerm || prevLeaderInTerm != currentLeader {
					timeMs := time.Since(g.startTime).Milliseconds()

					payload := fmt.Sprintf(`{"node_id":"%s","term":%d}`, currentLeader, currentTerm)
					g.onEvent(timeMs, "Node", "LeaderElected", payload)

					lastLeaderInTerm[currentTerm] = currentLeader

					if lastActiveLeader != "" && lastActiveLeader != currentLeader {
						payloadDown := fmt.Sprintf(`{"node_id":"%s"}`, lastActiveLeader)
						g.onEvent(timeMs, "Node", "LeaderStepDown", payloadDown)
					}
					lastActiveLeader = currentLeader
				}
			} else {
				if lastActiveLeader != "" {
					timeMs := time.Since(g.startTime).Milliseconds()
					payloadDown := fmt.Sprintf(`{"node_id":"%s"}`, lastActiveLeader)
					g.onEvent(timeMs, "Node", "LeaderStepDown", payloadDown)
					lastActiveLeader = ""
				}
			}
		}
	}
}
