package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/store"
)

func TestWorkloadGeneratorDeterminismAndExecution(t *testing.T) {
	// 1. Create SQLite store
	dbPath := filepath.Join(t.TempDir(), "test.sqlite")
	st, err := store.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer st.Close()

	// 2. Set up mock HTTP target serving client requests (representing the proxy)
	var mu sync.Mutex
	var receivedOps []string
	var receivedKeys []string

	transport := workloadRoundTripper(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		receivedOps = append(receivedOps, r.Method)

		// Extract key from path (e.g. /keys/x)
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) > 0 {
			receivedKeys = append(receivedKeys, parts[len(parts)-1])
		}

		return workloadResponse(http.StatusOK, "ok"), nil
	})

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
	dummyCallback := func(timeMs int64, category, eventType, payloadJSON string) {
	}

	gen := NewGenerator(cfg, "run-1", 42, st, dummyCallback)
	gen.httpClient.Transport = transport

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

type workloadRoundTripper func(*http.Request) (*http.Response, error)

func (f workloadRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func workloadResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

// Exercise the actual worker, HTTP requests and SQLite history without sockets,
// services, campaigns, clocks driving lease expiry, or manufactured history rows.
func TestCoordinationWorkerRoutesUnlockToOwnedKey(t *testing.T) {
	st, err := store.NewStore(filepath.Join(t.TempDir(), "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := &config.Config{Workload: config.WorkloadConfig{
		Type: "coordination-service", Keys: []string{"lock-a", "lock-b", "data-x"},
	}, System: config.SystemConfig{Nodes: config.NodesConfig{Count: 3}}}
	weights := []OpWeight{{"get", 2}, {"lock", 3}, {"put", 2}, {"unlock", 3}}
	const count = 30
	const seed = 44
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type invocation struct {
		OpID, ClientID, Op, Key, Target string
	}
	var invoked []invocation
	completed := 0
	gen := NewGenerator(cfg, "owned-routing", seed, st, func(_ int64, _, event, payload string) {
		if event == "OperationInvoked" {
			var row struct {
				OpID     string `json:"op_id"`
				ClientID string `json:"client_id"`
				Op       string `json:"op"`
				Key      string `json:"key"`
				Target   string `json:"target"`
			}
			if err := json.Unmarshal([]byte(payload), &row); err != nil {
				t.Fatal(err)
			}
			invoked = append(invoked, invocation{row.OpID, row.ClientID, row.Op, row.Key, row.Target})
		} else if event == "OperationCompleted" {
			completed++
			if completed == count {
				cancel()
			}
		}
	})
	gen.startTime = time.Now()
	owned := map[string]bool{}
	requests, sessions, misrouted, negativeProbes := 0, 0, 0, 0
	gen.httpClient.Transport = workloadRoundTripper(func(req *http.Request) (*http.Response, error) {
		requests++
		if req.Header.Get("X-FailForge-To") != invoked[len(invoked)-1].Target {
			t.Fatalf("target draw changed: %v", req.Header)
		}
		if req.URL.Path == "/api/session/open" {
			sessions++
			return workloadResponse(200, `{"session_id":"session-1"}`), nil
		}
		row := invoked[len(invoked)-1]
		if req.Header.Get("X-FailForge-From") != row.ClientID || req.Header.Get("X-FailForge-MsgType") != "client_req" {
			t.Fatalf("missing actual request routing headers: %v", req.Header)
		}
		var body struct{ Path, SessionID, Holder, Data string }
		if req.Method == "POST" {
			var payload map[string]interface{}
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			body.Path, _ = payload["path"].(string)
			body.Data, _ = payload["data"].(string)
			body.SessionID, _ = payload["session_id"].(string)
			body.Holder, _ = payload["holder"].(string)
		}
		switch req.URL.Path {
		case "/api/lease/acquire":
			if body.Path != "/locks/"+row.Key || body.SessionID != "session-1" || body.Holder != row.ClientID {
				t.Fatalf("acquire differs from invocation: %+v", body)
			}
			owned[body.Path] = true
			return workloadResponse(200, fmt.Sprintf(`{"path":%q,"session_id":"session-1","lease_token":1}`, body.Path)), nil
		case "/api/lease/release":
			if body.Path != "/locks/"+row.Key || body.SessionID != "session-1" {
				t.Fatalf("release differs from invocation: %+v", body)
			}
			if !owned[body.Path] {
				if len(owned) > 0 {
					misrouted++
				} else {
					negativeProbes++
				}
				return workloadResponse(404, `{"error":"not_found"}`), nil
			}
			delete(owned, body.Path)
			return workloadResponse(200, `{"status":"released"}`), nil
		case "/api/node/get":
			if req.URL.Query().Get("path") != "/data/"+row.Key {
				t.Fatal("get key changed")
			}
			return workloadResponse(200, `{"data":"value"}`), nil
		case "/api/node/create":
			if body.Path != "/data" && body.Path != "/data/"+row.Key {
				t.Fatal("put key changed")
			}
			return workloadResponse(200, `{}`), nil
		default:
			t.Fatalf("unexpected implicit request: %s", req.URL)
			return nil, nil
		}
	})
	var wg sync.WaitGroup
	wg.Add(1)
	gen.worker(ctx, &wg, "client-1", seed, weights, 10)
	wg.Wait()
	if completed != count || len(invoked) != count {
		t.Fatalf("completed=%d invoked=%d", completed, len(invoked))
	}
	if misrouted != 0 {
		t.Errorf("%d unlock requests ignored known owned keys", misrouted)
	}
	if negativeProbes == 0 {
		t.Error("no-ownership negative probes were lost")
	}
	ops, err := st.GetOperations("owned-routing")
	if err != nil || len(ops) != count {
		t.Fatalf("history=%d error=%v", len(ops), err)
	}
	byID := make(map[string]int)
	for i, op := range ops {
		byID[op.OpID] = i
	}
	r := rand.New(rand.NewSource(seed))
	expectedRequests := 1 // Existing lazy session-open behavior, once per worker.
	for _, row := range invoked {
		draw := r.Intn(10)
		opName := ""
		for _, weight := range weights {
			draw -= weight.weight
			if draw < 0 {
				opName = weight.name
				break
			}
		}
		key := cfg.Workload.Keys[r.Intn(len(cfg.Workload.Keys))]
		value := fmt.Sprintf("%s-%d", row.OpID, r.Intn(100))
		target := fmt.Sprintf("node-%d", r.Intn(3)+1)
		r.Intn(50) // Existing sleep jitter draw, including the final operation.
		if row.Op != opName || row.Target != target || (row.Op != "unlock" && row.Key != key) {
			t.Fatalf("seeded selection changed: %+v want op=%s key=%s target=%s", row, opName, key, target)
		}
		op := ops[byID[row.OpID]]
		var input struct{ Key, Value string }
		if err := json.Unmarshal([]byte(op.InputJSON), &input); err != nil {
			t.Fatal(err)
		}
		prefix := "/data/"
		if row.Op == "lock" || row.Op == "unlock" {
			prefix = "/locks/"
		}
		if op.Operation != strings.ToUpper(row.Op) || input.Key != prefix+row.Key {
			t.Fatalf("history does not describe actual invocation: %+v", op)
		}
		var output struct {
			StatusCode int `json:"status_code"`
		}
		if err := json.Unmarshal([]byte(op.OutputJSON), &output); err != nil {
			t.Fatal(err)
		}
		if (op.Status == "ok") != (output.StatusCode == 200) {
			t.Fatalf("response status rewritten: %+v", op)
		}
		expectedRequests++
		if row.Op == "put" {
			expectedRequests++ // Existing parent creation, no new hidden requests.
			if input.Value != value {
				t.Fatalf("write value RNG changed: %q want %q", input.Value, value)
			}
		}
	}
	if sessions != 1 || requests != expectedRequests {
		t.Fatalf("sessions=%d requests=%d want=%d", sessions, requests, expectedRequests)
	}
}

func TestCoordinationLeaseHintsLifecycle(t *testing.T) {
	var hints, otherClient coordinationLeaseHints
	if got := hints.unlockKey("negative-probe"); got != "negative-probe" {
		t.Fatalf("no-ownership probe changed to %q", got)
	}
	hints.observe("lock", "failed", "fail", `{"status_code":409}`)
	hints.observe("put", "not-a-lease", "ok", `{"status_code":200}`)
	if len(hints) != 0 {
		t.Fatalf("non-acquires created ownership: %v", hints)
	}
	hints.observe("LOCK", "b", "ok", `{"status_code":200}`)
	hints.observe("lock", "a", "ok", `{"status_code":200}`)
	hints.observe("lock", "b", "ok", `{"status_code":200}`)
	if len(hints) != 2 || hints.unlockKey("random") != "b" || hints.unlockKey("a") != "a" {
		t.Fatalf("duplicate, deterministic ordering, or selected-owned preference broken: %v", hints)
	}
	if otherClient.unlockKey("random") != "random" {
		t.Fatal("ownership leaked across clients")
	}
	for _, output := range []string{`{"status_code":502}`, `{"status_code":409}`, `{"error":"timeout"}`, `not-json`} {
		hints.observe("unlock", "b", "fail", output)
		if len(hints) != 2 || hints.unlockKey("random") != "b" {
			t.Fatalf("uncertain failure discarded known acquisition: %s: %v", output, hints)
		}
	}
	hints.observe("UNLOCK", "b", "ok", `{"status_code":200}`)
	if len(hints) != 1 || hints.unlockKey("random") != "a" {
		t.Fatalf("successful release failed to remove exactly its hint: %v", hints)
	}
	for _, code := range []int{403, 404} {
		hints.observe("lock", "stale", "ok", `{"status_code":200}`)
		hints.observe("unlock", "stale", "fail", fmt.Sprintf(`{"status_code":%d}`, code))
		if len(hints) != 1 || hints.unlockKey("stale") != "a" {
			t.Fatalf("definitive stale-ownership rejection %d retained hint: %v", code, hints)
		}
	}
	hints.observe("unlock", "unknown", "ok", `{"status_code":200}`)
	if len(hints) != 1 {
		t.Fatal("release of an unknown key removed unrelated ownership")
	}
	hints.observe("unlock", "a", "ok", `{"status_code":200}`)
	if len(hints) != 0 || hints.unlockKey("negative-probe") != "negative-probe" {
		t.Fatal("negative probes were not restored after all known releases")
	}
}

func TestCoordinationWorkerRecordsNegativeUnlockFailures(t *testing.T) {
	for _, code := range []int{403, 404, 502, 0} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			st, err := store.NewStore(filepath.Join(t.TempDir(), "history.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			cfg := &config.Config{Workload: config.WorkloadConfig{Type: "coordination-service", Keys: []string{"unowned"}}}
			completed := 0
			gen := NewGenerator(cfg, "negative-probe", 44, st, func(_ int64, _, event, _ string) {
				if event == "OperationCompleted" {
					completed++
					cancel()
				}
			})
			gen.startTime = time.Now()
			gen.coordinationSessions.Store("client-1", "session-1")
			requests := 0
			body := fmt.Sprintf(`{"error":"recorded-server-failure-%d"}`, code)
			gen.httpClient.Transport = workloadRoundTripper(func(req *http.Request) (*http.Response, error) {
				requests++
				if req.URL.Path != "/api/lease/release" {
					t.Fatalf("implicit request: %s", req.URL)
				}
				var payload map[string]string
				if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				if payload["path"] != "/locks/unowned" || payload["session_id"] != "session-1" {
					t.Fatalf("negative probe changed: %v", payload)
				}
				if code == 0 {
					return nil, fmt.Errorf("recorded transport failure")
				}
				return workloadResponse(code, body), nil
			})
			var wg sync.WaitGroup
			wg.Add(1)
			gen.worker(ctx, &wg, "client-1", 44, []OpWeight{{"unlock", 3}}, 3)
			wg.Wait()
			ops, err := st.GetOperations("negative-probe")
			if err != nil || len(ops) != 1 || completed != 1 || requests != 1 {
				t.Fatalf("history=%d completed=%d requests=%d error=%v", len(ops), completed, requests, err)
			}
			if ops[0].Status != "fail" || ops[0].Operation != "UNLOCK" {
				t.Fatalf("negative probe failure suppressed: %+v", ops[0])
			}
			var output struct {
				StatusCode  int `json:"status_code"`
				Body, Error string
			}
			if err := json.Unmarshal([]byte(ops[0].OutputJSON), &output); err != nil {
				t.Fatal(err)
			}
			if code == 0 {
				if !strings.Contains(output.Error, "recorded transport failure") {
					t.Fatalf("transport error lost: %s", ops[0].OutputJSON)
				}
			} else if output.StatusCode != code || output.Body != body {
				t.Fatalf("actual response rewritten: %s", ops[0].OutputJSON)
			}
		})
	}
}
