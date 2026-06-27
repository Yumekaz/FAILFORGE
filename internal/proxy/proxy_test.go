package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"failforge/internal/model"
	"failforge/internal/store"
)

type mockPortResolver struct {
	ports map[string]int
}

func (m *mockPortResolver) GetPort(nodeID string) (int, error) {
	port, ok := m.ports[nodeID]
	if !ok {
		return 0, fmt.Errorf("node %s not found", nodeID)
	}
	return port, nil
}

func TestProxyForwardingAndPartitions(t *testing.T) {
	// 1. Create temp SQLite store
	tempDir, err := os.MkdirTemp("", "failforge-proxy-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.sqlite")
	st, err := store.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer st.Close()

	// 2. Set up dummy target HTTP server representing "node-1"
	targetReceived := false
	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetReceived = true
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello from node-1"))
	}))
	defer targetServer.Close()

	// Resolve the dynamic port of the test target server
	var targetPort int
	_, err = fmt.Sscanf(targetServer.URL, "http://127.0.0.1:%d", &targetPort)
	if err != nil {
		// Try parsing without IP if loopback resolves differently
		_, err = fmt.Sscanf(targetServer.URL, "http://localhost:%d", &targetPort)
	}
	if err != nil {
		// Fallback parse port manually from suffix
		parts := strings.Split(targetServer.URL, ":")
		if len(parts) > 0 {
			fmt.Sscanf(parts[len(parts)-1], "%d", &targetPort)
		}
	}

	resolver := &mockPortResolver{
		ports: map[string]int{
			"node-1": targetPort,
		},
	}

	// 3. Initialize Proxy
	p := NewProxy(0, "run-1", resolver, st, func(timeMs int64, category, eventType, payloadJSON string) {
		_ = st.CreateEvent(&model.Event{
			RunID:       "run-1",
			TimeMs:      timeMs,
			Category:    category,
			Type:        eventType,
			PayloadJSON: payloadJSON,
		})
	})

	// Start proxy on a dynamically allocated port using a test server handler wrapper
	proxyMux := http.NewServeMux()
	proxyMux.HandleFunc("/", p.handleProxy)
	proxyServer := httptest.NewServer(proxyMux)
	defer proxyServer.Close()

	// Test case 1: Successful forwarding
	req, err := http.NewRequest("GET", proxyServer.URL+"/hello", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("X-FailForge-From", "client")
	req.Header.Set("X-FailForge-To", "node-1")
	req.Header.Set("X-FailForge-MsgType", "test_ping")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request to proxy failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got status: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello from node-1" {
		t.Errorf("expected 'hello from node-1', got '%s'", string(body))
	}

	if !targetReceived {
		t.Errorf("target node did not receive request")
	}

	// Check messages and events in SQLite
	messages, err := st.GetMessages("run-1")
	if err != nil || len(messages) != 1 {
		t.Errorf("expected 1 message logged, got: %d (err: %v)", len(messages), err)
	} else {
		if messages[0].Status != "delivered" || messages[0].FromNode != "client" || messages[0].ToNode != "node-1" {
			t.Errorf("invalid message log: %+v", messages[0])
		}
	}

	// Test case 2: Network partition drop
	p.SetPartition([][]string{
		{"client"},
		{"node-1"},
	})

	req2, err := http.NewRequest("GET", proxyServer.URL+"/hello-again", nil)
	if err != nil {
		t.Fatalf("failed to create second request: %v", err)
	}
	req2.Header.Set("X-FailForge-From", "client")
	req2.Header.Set("X-FailForge-To", "node-1")

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("second request to proxy failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("expected 504 Gateway Timeout during partition, got status: %d", resp2.StatusCode)
	}

	// Check messages status in SQLite
	messages, err = st.GetMessages("run-1")
	if err != nil || len(messages) != 2 {
		t.Errorf("expected 2 messages logged, got: %d", len(messages))
	} else {
		// find the second message
		var partitionMsg *model.Message
		for _, m := range messages {
			if m.Status == "dropped" {
				partitionMsg = m
			}
		}
		if partitionMsg == nil {
			t.Errorf("expected dropped message log not found")
		}
	}

	// Test case 3: SetDropRule check
	p.ClearPartitions()
	p.SetDropRule("client", "node-1", true)

	req3, err := http.NewRequest("GET", proxyServer.URL+"/hello-drop", nil)
	if err != nil {
		t.Fatalf("failed to create third request: %v", err)
	}
	req3.Header.Set("X-FailForge-From", "client")
	req3.Header.Set("X-FailForge-To", "node-1")

	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("third request to proxy failed: %v", err)
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("expected 504 Gateway Timeout during drop rule, got status: %d", resp3.StatusCode)
	}

	// Test case 4: SetDelayRule check
	p.ClearFaultRules()
	p.SetDelayRule("client", "node-1", 100*time.Millisecond)

	req4, err := http.NewRequest("GET", proxyServer.URL+"/hello-delay", nil)
	if err != nil {
		t.Fatalf("failed to create fourth request: %v", err)
	}
	req4.Header.Set("X-FailForge-From", "client")
	req4.Header.Set("X-FailForge-To", "node-1")

	start := time.Now()
	resp4, err := http.DefaultClient.Do(req4)
	if err != nil {
		t.Fatalf("fourth request to proxy failed: %v", err)
	}
	defer resp4.Body.Close()
	elapsed := time.Since(start)

	if resp4.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK after delay, got status: %d", resp4.StatusCode)
	}
	if elapsed < 100*time.Millisecond {
		t.Errorf("expected request to take at least 100ms due to delay rule, took: %v", elapsed)
	}
}

func TestAdvancedProxyFaults(t *testing.T) {
	// 1. Create temp SQLite store
	tempDir, err := os.MkdirTemp("", "failforge-proxy-adv-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.sqlite")
	st, err := store.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer st.Close()

	// 2. Set up dummy target HTTP server representing "node-1"
	var mu sync.Mutex
	receivedBodies := []string{}
	receivedHeaders := []http.Header{}
	targetReceivedCount := 0

	targetServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		targetReceivedCount++
		bodyBytes, _ := io.ReadAll(r.Body)
		receivedBodies = append(receivedBodies, string(bodyBytes))
		receivedHeaders = append(receivedHeaders, r.Header.Clone())
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer targetServer.Close()

	var targetPort int
	_, err = fmt.Sscanf(targetServer.URL, "http://127.0.0.1:%d", &targetPort)
	if err != nil {
		_, err = fmt.Sscanf(targetServer.URL, "http://localhost:%d", &targetPort)
	}
	if err != nil {
		parts := strings.Split(targetServer.URL, ":")
		if len(parts) > 0 {
			fmt.Sscanf(parts[len(parts)-1], "%d", &targetPort)
		}
	}

	resolver := &mockPortResolver{
		ports: map[string]int{
			"node-1": targetPort,
			"client": 12345, // dummy port for client
		},
	}

	// 3. Initialize Proxy
	p := NewProxy(0, "run-2", resolver, st, func(timeMs int64, category, eventType, payloadJSON string) {
		_ = st.CreateEvent(&model.Event{
			RunID:       "run-2",
			TimeMs:      timeMs,
			Category:    category,
			Type:        eventType,
			PayloadJSON: payloadJSON,
		})
	})

	proxyMux := http.NewServeMux()
	proxyMux.HandleFunc("/", p.handleProxy)
	proxyServer := httptest.NewServer(proxyMux)
	defer proxyServer.Close()

	// Resolve proxy server port to use for duplicate requests
	var proxyPort int
	_, err = fmt.Sscanf(proxyServer.URL, "http://127.0.0.1:%d", &proxyPort)
	if err != nil {
		_, err = fmt.Sscanf(proxyServer.URL, "http://localhost:%d", &proxyPort)
	}
	if err != nil {
		parts := strings.Split(proxyServer.URL, ":")
		if len(parts) > 0 {
			fmt.Sscanf(parts[len(parts)-1], "%d", &proxyPort)
		}
	}
	p.port = proxyPort

	// Test case 1: Asymmetric Partition (client -> node-1 blocked, node-1 -> client open)
	p.SetAsymmetricBlock("client", "node-1", true)

	// client -> node-1 request
	req1, _ := http.NewRequest("POST", proxyServer.URL+"/", strings.NewReader("msg1"))
	req1.Header.Set("X-FailForge-From", "client")
	req1.Header.Set("X-FailForge-To", "node-1")
	resp1, err := http.DefaultClient.Do(req1)
	if err == nil {
		resp1.Body.Close()
		if resp1.StatusCode != http.StatusGatewayTimeout {
			t.Errorf("expected gateway timeout for asymmetric partition client->node-1, got %d", resp1.StatusCode)
		}
	}

	// node-1 -> client request
	req2, _ := http.NewRequest("POST", proxyServer.URL+"/", strings.NewReader("msg2"))
	req2.Header.Set("X-FailForge-From", "node-1")
	req2.Header.Set("X-FailForge-To", "client")
	// Note: client port resolves to 12345, so it might fail to connect. But we check it doesn't get dropped by block.
	// Actually, resolver.GetPort("client") returns 12345. Since nothing is listening on 12345, the proxy will return 502 Bad Gateway.
	// But it will NOT return 504 Gateway Timeout (which is what we return for drops/partitions)!
	resp2, err := http.DefaultClient.Do(req2)
	if err == nil {
		resp2.Body.Close()
		if resp2.StatusCode == http.StatusGatewayTimeout {
			t.Errorf("expected connection failure (502 Bad Gateway) for node-1->client, but got blocked (504)")
		}
	}

	// Test case 2: Message Duplication
	p.ClearAdvancedFaultRules()
	p.SetDuplicateRule("client", "node-1", true)

	mu.Lock()
	targetReceivedCount = 0
	receivedBodies = []string{}
	mu.Unlock()

	req3, _ := http.NewRequest("POST", proxyServer.URL+"/", strings.NewReader("ping"))
	req3.Header.Set("X-FailForge-From", "client")
	req3.Header.Set("X-FailForge-To", "node-1")
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp3.Body.Close()

	// Wait for background duplicate request to complete
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	count := targetReceivedCount
	bodies := make([]string, len(receivedBodies))
	copy(bodies, receivedBodies)
	mu.Unlock()

	if count != 2 {
		t.Errorf("expected target to receive request twice, got: %d", count)
	}
	for i, b := range bodies {
		if b != "ping" {
			t.Errorf("expected body %d to be 'ping', got '%s'", i, b)
		}
	}

	// Test case 3: Packet Corruption
	p.ClearAdvancedFaultRules()
	p.SetCorruptionRule("client", "node-1", 1.0) // 100% corruption rate

	mu.Lock()
	targetReceivedCount = 0
	receivedBodies = []string{}
	mu.Unlock()

	originalMsg := "this is a very long message that is going to be corrupted by the proxy!"
	req4, _ := http.NewRequest("POST", proxyServer.URL+"/", strings.NewReader(originalMsg))
	req4.Header.Set("X-FailForge-From", "client")
	req4.Header.Set("X-FailForge-To", "node-1")
	resp4, err := http.DefaultClient.Do(req4)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp4.Body.Close()

	mu.Lock()
	corruptedBody := receivedBodies[0]
	mu.Unlock()

	if corruptedBody == originalMsg {
		t.Errorf("expected packet body to be corrupted, but it matched original")
	}
	if len(corruptedBody) != len(originalMsg) {
		t.Errorf("expected corrupted body length %d to match original %d", len(corruptedBody), len(originalMsg))
	}

	// Test case 4: Clock Offset Header Injection
	p.ClearAdvancedFaultRules()
	p.SetClockOffset("node-1", 123456)

	mu.Lock()
	receivedHeaders = []http.Header{}
	mu.Unlock()

	req5, _ := http.NewRequest("GET", proxyServer.URL+"/", nil)
	req5.Header.Set("X-FailForge-From", "client")
	req5.Header.Set("X-FailForge-To", "node-1")
	resp5, err := http.DefaultClient.Do(req5)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp5.Body.Close()

	mu.Lock()
	headers := receivedHeaders[0]
	mu.Unlock()

	offsetHeader := headers.Get("X-FailForge-Clock-Offset")
	if offsetHeader != "123456" {
		t.Errorf("expected X-FailForge-Clock-Offset: 123456, got: '%s'", offsetHeader)
	}
}
