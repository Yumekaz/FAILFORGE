package proxy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"failforge/internal/model"
	"failforge/internal/store"
)

type PortResolver interface {
	GetPort(nodeID string) (int, error)
}

type Proxy struct {
	mu           sync.RWMutex
	port         int
	resolver     PortResolver
	store        *store.Store
	runID        string
	startTime    time.Time
	server       *http.Server
	partitions   map[string]map[string]bool // fromNode -> toNode -> isBlocked
	messageCount int
	onEvent      func(timeMs int64, category, eventType, payloadJSON string)
	dropRules        map[string]map[string]bool
	delayRules       map[string]map[string]time.Duration
	duplicateRules   map[string]map[string]bool
	corruptionRules  map[string]map[string]float64
	asymmetricBlocks map[string]map[string]bool
	clockOffsets     map[string]int64
}

func NewProxy(port int, runID string, resolver PortResolver, store *store.Store, onEvent func(timeMs int64, category, eventType, payloadJSON string)) *Proxy {
	return &Proxy{
		port:       port,
		resolver:   resolver,
		store:      store,
		runID:      runID,
		startTime:  time.Now(),
		partitions: make(map[string]map[string]bool),
		onEvent:    onEvent,
		dropRules:        make(map[string]map[string]bool),
		delayRules:       make(map[string]map[string]time.Duration),
		duplicateRules:   make(map[string]map[string]bool),
		corruptionRules:  make(map[string]map[string]float64),
		asymmetricBlocks: make(map[string]map[string]bool),
		clockOffsets:     make(map[string]int64),
	}
}

func (p *Proxy) getElapsedTimeMs() int64 {
	return time.Since(p.startTime).Milliseconds()
}

func (p *Proxy) generateMsgID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "msg-" + hex.EncodeToString(b)
}

// Start runs the HTTP proxy server.
func (p *Proxy) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", p.handleProxy)

	p.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", p.port),
		Handler: mux,
	}

	log.Printf("[Proxy] Listening on :%d\n", p.port)
	if err := p.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Stop gracefully shuts down the proxy.
func (p *Proxy) Stop() error {
	if p.server != nil {
		return p.server.Close()
	}
	return nil
}

// SetPartition configures network partitions between groups of nodes.
func (p *Proxy) SetPartition(groups [][]string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Clear old partitions
	p.partitions = make(map[string]map[string]bool)

	// Create mapping: nodes that are NOT in the same partition group cannot communicate
	// nodeID -> groupIndex
	nodeGroup := make(map[string]int)
	for groupIdx, group := range groups {
		for _, nodeID := range group {
			nodeGroup[nodeID] = groupIdx
		}
	}

	// For all pairs of nodes, block communication if they are in different groups
	for nodeA, groupA := range nodeGroup {
		for nodeB, groupB := range nodeGroup {
			if groupA != groupB {
				if p.partitions[nodeA] == nil {
					p.partitions[nodeA] = make(map[string]bool)
				}
				p.partitions[nodeA][nodeB] = true
			}
		}
	}
}

// ClearPartitions clears all network partitions.
func (p *Proxy) ClearPartitions() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.partitions = make(map[string]map[string]bool)
}

func (p *Proxy) isPartitioned(from, to string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.partitions[from] == nil {
		return false
	}
	return p.partitions[from][to]
}

// SetDropRule adds or removes a message dropping rule between a pair of nodes.
func (p *Proxy) SetDropRule(from, to string, active bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dropRules[from] == nil {
		p.dropRules[from] = make(map[string]bool)
	}
	p.dropRules[from][to] = active
}

// SetDelayRule configures a latency delay between a pair of nodes.
func (p *Proxy) SetDelayRule(from, to string, delay time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.delayRules[from] == nil {
		p.delayRules[from] = make(map[string]time.Duration)
	}
	p.delayRules[from][to] = delay
}

// ClearFaultRules clears all message drop and delay rules.
func (p *Proxy) ClearFaultRules() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.dropRules = make(map[string]map[string]bool)
	p.delayRules = make(map[string]map[string]time.Duration)
}

func (p *Proxy) isDropped(from, to string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.dropRules[from] == nil {
		return false
	}
	return p.dropRules[from][to]
}

func (p *Proxy) getDelay(from, to string) time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.delayRules[from] == nil {
		return 0
	}
	return p.delayRules[from][to]
}

// SetDuplicateRule configures message duplication between a pair of nodes.
func (p *Proxy) SetDuplicateRule(from, to string, active bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.duplicateRules[from] == nil {
		p.duplicateRules[from] = make(map[string]bool)
	}
	p.duplicateRules[from][to] = active
}

func (p *Proxy) isDuplicated(from, to string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.duplicateRules[from] == nil {
		return false
	}
	return p.duplicateRules[from][to]
}

// SetCorruptionRule configures packet corruption rate between a pair of nodes.
func (p *Proxy) SetCorruptionRule(from, to string, rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.corruptionRules[from] == nil {
		p.corruptionRules[from] = make(map[string]float64)
	}
	p.corruptionRules[from][to] = rate
}

func (p *Proxy) getCorruptionRate(from, to string) float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.corruptionRules[from] == nil {
		return 0
	}
	return p.corruptionRules[from][to]
}

// SetAsymmetricBlock configures one-way blocking from node A to node B.
func (p *Proxy) SetAsymmetricBlock(from, to string, blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.asymmetricBlocks[from] == nil {
		p.asymmetricBlocks[from] = make(map[string]bool)
	}
	p.asymmetricBlocks[from][to] = blocked
}

func (p *Proxy) isAsymmetricBlocked(from, to string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.asymmetricBlocks[from] == nil {
		return false
	}
	return p.asymmetricBlocks[from][to]
}

// SetClockOffset configures a virtual clock offset for a specific node in milliseconds.
func (p *Proxy) SetClockOffset(nodeID string, offsetMs int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clockOffsets[nodeID] = offsetMs
}

func (p *Proxy) getClockOffset(nodeID string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.clockOffsets[nodeID]
}

// ClearAdvancedFaultRules clears all message duplication, corruption, asymmetric blocks, and clock offsets.
func (p *Proxy) ClearAdvancedFaultRules() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.duplicateRules = make(map[string]map[string]bool)
	p.corruptionRules = make(map[string]map[string]float64)
	p.asymmetricBlocks = make(map[string]map[string]bool)
	p.clockOffsets = make(map[string]int64)
}

func (p *Proxy) IsBlocked(from, to string) bool {
	return p.isPartitioned(from, to) || p.isDropped(from, to) || p.isAsymmetricBlocked(from, to)
}

func (p *Proxy) GetDelay(from, to string) time.Duration {
	return p.getDelay(from, to)
}

func (p *Proxy) handleProxy(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/failforge/check" {
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")
		blocked := p.IsBlocked(from, to)
		delay := p.GetDelay(from, to)

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"blocked":  blocked,
			"delay_ms": delay.Milliseconds(),
		})
		return
	}

	fromNode := r.Header.Get("X-FailForge-From")
	if fromNode == "" {
		fromNode = "client"
	}

	var toNode string
	var err error
	var destPort int

	// 1. Resolve target node destination
	toNodeHeader := r.Header.Get("X-FailForge-To")
	pathParts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")

	if toNodeHeader != "" {
		toNode = toNodeHeader
		// Handle optional port in header (e.g. node-2:7001)
		if idx := strings.Index(toNode, ":"); idx != -1 {
			toNode = toNode[:idx]
		}
	} else if len(pathParts) > 0 && strings.HasPrefix(pathParts[0], "node-") {
		toNode = pathParts[0]
	}

	if toNode == "" {
		http.Error(w, "Bad Request: Missing X-FailForge-To header or node path prefix (e.g. /node-1/...)", http.StatusBadRequest)
		return
	}

	// 2. Resolve port for destination node
	destPort, err = p.resolver.GetPort(toNode)
	if err != nil {
		http.Error(w, fmt.Sprintf("Bad Request: Unknown node ID %s", toNode), http.StatusBadRequest)
		return
	}

	// 3. Check for active network partition or drop rule or asymmetric block
	isBlocked := p.isPartitioned(fromNode, toNode) || p.isDropped(fromNode, toNode) || p.isAsymmetricBlocked(fromNode, toNode)
	reason := "partition"
	if p.isDropped(fromNode, toNode) {
		reason = "drop_rule"
	} else if p.isAsymmetricBlocked(fromNode, toNode) {
		reason = "asymmetric_partition"
	}

	if isBlocked {
		msgID := p.generateMsgID()
		sendMs := p.getElapsedTimeMs()
		log.Printf("[Proxy] [BLOCKED] Drop message %s from %s to %s (reason: %s)\n", msgID, fromNode, toNode, reason)

		// Record message dropped in database
		_ = p.store.CreateMessage(&model.Message{
			MessageID:   msgID,
			RunID:       p.runID,
			FromNode:    fromNode,
			ToNode:      toNode,
			MessageType: r.Header.Get("X-FailForge-MsgType"),
			Status:      "dropped",
			SendMs:      sendMs,
		})

		p.onEvent(sendMs, "Message", "MessageDropped", fmt.Sprintf(
			`{"message_id":"%s","from":"%s","to":"%s","reason":"%s"}`,
			msgID, fromNode, toNode, reason,
		))

		http.Error(w, "Gateway Timeout: Network Blocked", http.StatusGatewayTimeout)
		return
	}

	// 4. Set up proxy forwarding
	destURL, err := url.Parse(fmt.Sprintf("http://localhost:%d", destPort))
	if err != nil {
		http.Error(w, "Internal Server Error: failed to parse destination URL", http.StatusInternalServerError)
		return
	}

	// Check for duplication and duplicate request flags
	isDuplicated := p.isDuplicated(fromNode, toNode)
	isDuplicateRequest := r.Header.Get("X-FailForge-Duplicate") == "true"

	var bodyBytes []byte
	if r.Body != nil && (isDuplicated && !isDuplicateRequest || p.getCorruptionRate(fromNode, toNode) > 0) {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		if err == nil {
			r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		}
	}

	// Packet corruption check
	corruptionRate := p.getCorruptionRate(fromNode, toNode)
	if corruptionRate > 0 && mrand.Float64() < corruptionRate && len(bodyBytes) > 0 {
		corruptedBytes := make([]byte, len(bodyBytes))
		copy(corruptedBytes, bodyBytes)

		// Corrupt 1 to 3 random bytes
		rng := mrand.New(mrand.NewSource(time.Now().UnixNano()))
		numCorruptions := rng.Intn(3) + 1
		if numCorruptions > len(corruptedBytes) {
			numCorruptions = len(corruptedBytes)
		}
		for i := 0; i < numCorruptions; i++ {
			pos := rng.Intn(len(corruptedBytes))
			corruptedBytes[pos] ^= 0xFF // flip bits
		}
		r.Body = io.NopCloser(bytes.NewReader(corruptedBytes))

		corruptTimeMs := p.getElapsedTimeMs()
		p.onEvent(corruptTimeMs, "Message", "MessageCorrupted", fmt.Sprintf(
			`{"from":"%s","to":"%s","bytes_corrupted":%d}`,
			fromNode, toNode, numCorruptions,
		))
	}

	msgID := p.generateMsgID()
	sendMs := p.getElapsedTimeMs()

	// Check if there is an active delay rule
	delay := p.getDelay(fromNode, toNode)
	if delay > 0 {
		log.Printf("[Proxy] [DELAY] Delay message %s from %s to %s by %v\n", msgID, fromNode, toNode, delay)
		p.onEvent(sendMs, "Message", "MessageDelayed", fmt.Sprintf(
			`{"message_id":"%s","from":"%s","to":"%s","delay_ms":%d}`,
			msgID, fromNode, toNode, delay.Milliseconds(),
		))

		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}

		sendMs = p.getElapsedTimeMs()
	}

	// Record outgoing message
	msgType := r.Header.Get("X-FailForge-MsgType")
	_ = p.store.CreateMessage(&model.Message{
		MessageID:   msgID,
		RunID:       p.runID,
		FromNode:    fromNode,
		ToNode:      toNode,
		MessageType: msgType,
		Status:      "sent",
		SendMs:      sendMs,
	})

	p.onEvent(sendMs, "Message", "MessageSent", fmt.Sprintf(
		`{"message_id":"%s","from":"%s","to":"%s","msg_type":"%s"}`,
		msgID, fromNode, toNode, msgType,
	))

	reverseProxy := httputil.NewSingleHostReverseProxy(destURL)

	// Customize Director to adjust paths and headers
	originalDirector := reverseProxy.Director
	reverseProxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = destURL.Host

		// Strip path prefix if it was used for routing
		if toNodeHeader == "" && len(pathParts) > 0 && strings.HasPrefix(pathParts[0], "node-") {
			req.URL.Path = "/" + strings.Join(pathParts[1:], "/")
		}

		// Inject clock offset header
		p.mu.RLock()
		offset := p.clockOffsets[toNode]
		p.mu.RUnlock()
		if offset != 0 {
			req.Header.Set("X-FailForge-Clock-Offset", fmt.Sprintf("%d", offset))
		}
	}

	// Handle successful delivery or failures
	reverseProxy.ModifyResponse = func(resp *http.Response) error {
		deliverMs := p.getElapsedTimeMs()
		_ = p.store.UpdateMessage(&model.Message{
			MessageID: msgID,
			RunID:     p.runID,
			Status:    "delivered",
			DeliverMs: deliverMs,
		})

		p.onEvent(deliverMs, "Message", "MessageDelivered", fmt.Sprintf(
			`{"message_id":"%s","from":"%s","to":"%s","latency_ms":%d}`,
			msgID, fromNode, toNode, deliverMs-sendMs,
		))
		return nil
	}

	reverseProxy.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
		deliverMs := p.getElapsedTimeMs()
		log.Printf("[Proxy] Failed to forward from %s to %s: %v\n", fromNode, toNode, err)

		_ = p.store.UpdateMessage(&model.Message{
			MessageID: msgID,
			RunID:     p.runID,
			Status:    "dropped",
		})

		p.onEvent(deliverMs, "Message", "MessageDropped", fmt.Sprintf(
			`{"message_id":"%s","from":"%s","to":"%s","reason":"connection_refused","error":"%s"}`,
			msgID, fromNode, toNode, strings.ReplaceAll(err.Error(), `"`, `\"`),
		))

		http.Error(w, "Bad Gateway: Target unreachable", http.StatusBadGateway)
	}

	reverseProxy.ServeHTTP(w, r)

	// Trigger duplication after serving the main request
	if isDuplicated && !isDuplicateRequest {
		go func(body []byte, headers http.Header, method, path string) {
			time.Sleep(10 * time.Millisecond) // separate the duplicate message slightly
			proxyURL := fmt.Sprintf("http://localhost:%d%s", p.port, path)
			req, err := http.NewRequest(method, proxyURL, bytes.NewReader(body))
			if err != nil {
				return
			}
			for k, vv := range headers {
				for _, v := range vv {
					req.Header.Add(k, v)
				}
			}
			req.Header.Set("X-FailForge-Duplicate", "true")

			origMsgType := req.Header.Get("X-FailForge-MsgType")
			req.Header.Set("X-FailForge-MsgType", origMsgType+" (duplicate)")

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}(bodyBytes, r.Header.Clone(), r.Method, r.URL.Path)
	}
}
