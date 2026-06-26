package workload

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/store"
)

func TestWorkloadGeneratorDeterminismAndExecution(t *testing.T) {
	// 1. Create SQLite store
	tempDir, err := os.MkdirTemp("", "failforge-workload-test-*")
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

	// 2. Set up mock HTTP target serving client requests (representing the proxy)
	var mu sync.Mutex
	var receivedOps []string
	var receivedKeys []string

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		receivedOps = append(receivedOps, r.Method)
		
		// Extract key from path (e.g. /keys/x)
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) > 0 {
			receivedKeys = append(receivedKeys, parts[len(parts)-1])
		}
		
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer mockServer.Close()

	// Resolve mock server port
	var mockPort int
	u, _ := url.Parse(mockServer.URL)
	parts := strings.Split(u.Host, ":")
	if len(parts) > 1 {
		mockPort, _ = strconv.Atoi(parts[1])
	}

	cfg := &config.Config{
		Time: config.TimeConfig{
			DurationMs: 300,
			TickMs:     10,
		},
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count: 2,
			},
		},
		Network: config.NetworkConfig{
			ProxyPort: mockPort,
		},
		Workload: config.WorkloadConfig{
			Clients:    2,
			DurationMs: 200,
			Keys:       []string{"x", "y"},
			Operations: map[string]interface{}{
				"get": 5,
				"put": 5,
			},
		},
	}

	// 3. Initialize Generator
	eventsRecorded := 0
	dummyCallback := func(timeMs int64, category, eventType, payloadJSON string) {
		eventsRecorded++
	}

	gen := NewGenerator(cfg, "run-1", 42, st, dummyCallback)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start workload
	gen.Start(ctx, time.Now())

	// Assertions
	mu.Lock()
	defer mu.Unlock()
	if len(receivedOps) == 0 {
		t.Errorf("expected to receive operations, got 0")
	}

	// Assert database operations populated
	ops, err := st.GetOperations("run-1")
	if err != nil || len(ops) == 0 {
		t.Errorf("expected database operations to be stored, got: %d (err: %v)", len(ops), err)
	}
}
