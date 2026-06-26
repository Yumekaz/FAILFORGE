package workload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"failforge/internal/config"
	"failforge/internal/model"
	"failforge/internal/store"
)

type Generator struct {
	cfg        *config.Config
	runID      string
	seed       int64
	store      *store.Store
	onEvent    func(timeMs int64, category, eventType, payloadJSON string)
	startTime  time.Time
	proxyURL   string
	httpClient *http.Client
}

type OpWeight struct {
	name   string
	weight int
}

func NewGenerator(cfg *config.Config, runID string, seed int64, store *store.Store, onEvent func(timeMs int64, category, eventType, payloadJSON string)) *Generator {
	return &Generator{
		cfg:       cfg,
		runID:     runID,
		seed:      seed,
		store:     store,
		onEvent:   onEvent,
		proxyURL:  fmt.Sprintf("http://localhost:%d", cfg.Network.ProxyPort),
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
