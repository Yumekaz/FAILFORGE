package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
