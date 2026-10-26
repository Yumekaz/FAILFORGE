package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"failforge/internal/checkers"
	"failforge/internal/config"
	"failforge/internal/model"
	"failforge/internal/store"
)

const minimizeTargetChecker = "read_after_acknowledged_write"

func TestMinimizeEngine(t *testing.T) {
	// Every successful GET returns a value that was never written. Unlike node.py,
	// this fixture has no persisted data, replication, or PUT/GET ordering race:
	// the real checker must find the target violation even with one client.
	cfg := newMinimizeFixture(t, "unwritten-value")
	cfg.Time.DurationMs = 1250
	cfg.Workload.DurationMs = 1250
	cfg.Workload.Clients = 2
	// Both faults are real, but unnecessary to the bug, so both must be removed.
	cfg.Faults.Schedule = []config.FaultConfig{
		{Type: "heal", AtMs: 0},
		{Type: "heal", AtMs: 20},
	}
	runDir := runMinimizeFixture(t, cfg)
	originalViolations := requireMinimizeTargetHistory(t, filepath.Join(runDir, "history.sqlite"))
	originalConfig := readMinimizeFile(t, filepath.Join(runDir, "config.json"))
	originalFaults := readMinimizeFile(t, filepath.Join(runDir, "faults.json"))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	res, err := MinimizeRun(ctx, runDir)
	if err != nil {
		t.Fatalf("minimization failed: %v", err)
	}
	if res.OriginalRunDir != runDir || res.OriginalViolations != originalViolations ||
		res.OriginalFaultCount != 2 || res.OriginalClients != 2 || res.OriginalDurationMs != 1250 {
		t.Fatalf("incorrect original statistics: %+v", res)
	}
	if res.MinimizedFaultCount != 0 || len(res.FinalFaults) != 0 || len(res.FaultsRemoved) != 2 {
		t.Errorf("expected both unnecessary faults removed: %+v", res)
	}
	if res.MinimizedClients != 1 || res.MinimizedDurationMs != 1000 {
		t.Errorf("expected actual client and duration reductions to 1 client / 1000ms: %+v", res)
	}
	if res.MinimizedOutputDir != filepath.Join(runDir, "minimized") {
		t.Fatalf("unexpected minimized output directory: %q", res.MinimizedOutputDir)
	}
	finalViolations := requireMinimizeTargetHistory(t, filepath.Join(res.MinimizedOutputDir, "history.sqlite"))
	if res.MinimizedViolations != finalViolations {
		t.Errorf("reported %d minimized violations, history contains %d", res.MinimizedViolations, finalViolations)
	}

	var finalCfg config.Config
	readMinimizeJSON(t, filepath.Join(res.MinimizedOutputDir, "config.json"), &finalCfg)
	if finalCfg.Workload.Clients != res.MinimizedClients || finalCfg.Time.DurationMs != res.MinimizedDurationMs ||
		finalCfg.Workload.DurationMs != 1000 || finalCfg.Faults.Mode != "replay" {
		t.Errorf("saved config does not match accepted reductions: %+v", finalCfg)
	}
	var finalFaults []config.FaultConfig
	readMinimizeJSON(t, filepath.Join(res.MinimizedOutputDir, "faults.json"), &finalFaults)
	if len(finalFaults) != res.MinimizedFaultCount {
		t.Errorf("saved %d faults, reported %d", len(finalFaults), res.MinimizedFaultCount)
	}
	for _, name := range []string{"events.jsonl", "logs/node-1.log", "minimized_report.md"} {
		readMinimizeFile(t, filepath.Join(res.MinimizedOutputDir, name))
	}
	if !bytes.Equal(originalConfig, readMinimizeFile(t, filepath.Join(runDir, "config.json"))) ||
		!bytes.Equal(originalFaults, readMinimizeFile(t, filepath.Join(runDir, "faults.json"))) {
		t.Error("minimization modified the original configuration or fault schedule")
	}
	if _, err := os.Stat(filepath.Join(runDir, "minimizations")); !os.IsNotExist(err) {
		t.Errorf("temporary minimization candidates were not cleaned up: %v", err)
	}

	// Exercise the saved artifact, not just the in-memory reduction result.
	replay, err := ReplayRun(ctx, res.MinimizedOutputDir)
	if err != nil {
		t.Fatalf("replay of minimized artifact failed: %v", err)
	}
	requireMinimizeTargetHistory(t, filepath.Join(replay.ReplayOutputDir, "history.sqlite"))
}

func TestMinimizeRejectsNoViolations(t *testing.T) {
	runDir := runMinimizeFixture(t, newMinimizeFixture(t, "null"))
	_, count, err := getRunViolations(filepath.Join(runDir, "history.sqlite"))
	if err != nil || count != 0 {
		t.Fatalf("healthy fixture should have no violations: count=%d, err=%v", count, err)
	}
	res, err := MinimizeRun(context.Background(), runDir)
	if err == nil || err.Error() != "cannot minimize a run that has 0 violations" || res != nil {
		t.Fatalf("expected zero-violation rejection, got result=%+v, err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "minimizations")); !os.IsNotExist(err) {
		t.Errorf("zero-violation run should be rejected before replay: %v", err)
	}
}

func TestMinimizeRejectsNonReproducingBaseline(t *testing.T) {
	for _, tc := range []struct {
		name              string
		unrelatedCoverage bool
		fabricatedLock    bool
	}{
		{name: "passed-replay"},
		{name: "unrelated-violation", unrelatedCoverage: true},
		{name: "fabricated-lock-history", fabricatedLock: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "unwritten-value"
			target := minimizeTargetChecker
			if tc.fabricatedLock {
				body = "null"
				target = "lock_exclusivity"
			}
			cfg := newMinimizeFixture(t, body)
			runDir := runMinimizeFixture(t, cfg)
			originalDB := filepath.Join(runDir, "history.sqlite")
			if tc.fabricatedLock {
				// A fabricated failure belongs only in this rejection regression,
				// never in the success fixture. Replay must not trust this row.
				runID, count, err := getRunViolations(originalDB)
				if err != nil || count != 0 {
					t.Fatalf("expected healthy history before fabricated violation: count=%d, err=%v", count, err)
				}
				st, err := store.NewStore(originalDB)
				if err != nil {
					t.Fatal(err)
				}
				err = st.CreateViolation(&model.Violation{RunID: runID, CheckerName: target,
					Severity: "ERROR", Description: "deliberately non-reproducing test history", EvidenceJSON: "{}"})
				closeErr := st.Close()
				if err != nil || closeErr != nil {
					t.Fatalf("record fabricated history: write=%v, close=%v", err, closeErr)
				}
			} else {
				requireMinimizeTargetHistory(t, originalDB)
			}

			// Change only the replayed system response, keeping the failed history.
			// An empty register returning null no longer violates the target checker.
			cfg.System.Nodes.Command = minimizeFixtureCommand(t, "null")
			if tc.unrelatedCoverage {
				// Produce a genuine, but different, failure in the baseline replay.
				cfg.Workload.MinimumSuccessfulByOperation = map[string]int{"put": 1}
			}
			writeMinimizeJSON(t, filepath.Join(runDir, "config.json"), cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			res, err := MinimizeRun(ctx, runDir)
			wantErr := "baseline replay did not reproduce target violation(s); minimization aborted"
			if err == nil || err.Error() != wantErr || res != nil {
				t.Fatalf("expected baseline rejection, got result=%+v, err=%v", res, err)
			}
			paths, err := filepath.Glob(filepath.Join(runDir, "minimizations", "baseline", "replays", "replay-*", "history.sqlite"))
			if err != nil || len(paths) != 1 {
				t.Fatalf("expected exactly one baseline history: paths=%v, err=%v", paths, err)
			}
			runID, count, err := getRunViolations(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			if targetCheckerViolated(paths[0], runID, []string{target}) {
				t.Fatal("negative fixture unexpectedly reproduced the target")
			}
			wantCount, wantStatus := 0, "PASSED"
			if tc.unrelatedCoverage {
				wantCount, wantStatus = 1, "FAILED"
				if !targetCheckerViolated(paths[0], runID, []string{"workload_coverage"}) {
					t.Fatal("expected a real unrelated workload coverage violation")
				}
			}
			if count != wantCount {
				t.Errorf("baseline violations=%d, want %d", count, wantCount)
			}
			st, err := store.NewStore(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			run, err := st.GetRun(runID)
			if err != nil || run.Status != wantStatus {
				t.Fatalf("unexpected baseline status: run=%+v, err=%v", run, err)
			}
			ops, err := st.GetOperations(runID)
			if err != nil || len(ops) == 0 {
				t.Fatalf("baseline must execute actual requests: operations=%d, err=%v", len(ops), err)
			}
			candidates, err := os.ReadDir(filepath.Join(runDir, "minimizations"))
			if err != nil || len(candidates) != 1 || candidates[0].Name() != "baseline" {
				t.Fatalf("minimization proceeded after rejected baseline: candidates=%v, err=%v", candidates, err)
			}
			if _, err := os.Stat(filepath.Join(runDir, "minimized")); !os.IsNotExist(err) {
				t.Errorf("rejected baseline published minimized output: %v", err)
			}
		})
	}
}

func newMinimizeFixture(t *testing.T, body string) *config.Config {
	t.Helper()
	tempDir := t.TempDir()
	// Keep both ephemeral ports reserved until both are chosen, so they cannot
	// be the same. Do not use the actual Coordination campaign's fixed ports.
	var ports []int
	for len(ports) < 2 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		if port == 25000 || port == 25090 {
			ln.Close()
			continue
		}
		defer ln.Close()
		ports = append(ports, port)
	}
	return &config.Config{
		Name: "test-minimize", Seed: 42,
		Time: config.TimeConfig{DurationMs: 200, TickMs: 10},
		System: config.SystemConfig{Type: "process_cluster", Nodes: config.NodesConfig{
			Count: 1, Command: minimizeFixtureCommand(t, body),
			Ports: config.PortsConfig{Start: ports[0]}, DataDir: filepath.Join(tempDir, "data", "{node_id}"),
		}},
		Network: config.NetworkConfig{Mode: "controlled_proxy", ProxyPort: ports[1]},
		Workload: config.WorkloadConfig{Type: "kv-register", Clients: 1, DurationMs: 200,
			Keys: []string{"x"}, Operations: map[string]interface{}{"get": 1},
			MinimumSuccessfulOperations: 1},
		Faults:   config.FaultsConfig{Mode: "replay"},
		Checkers: []config.CheckerConfig{{Name: minimizeTargetChecker}},
		Output:   config.OutputConfig{Dir: filepath.Join(tempDir, "run-{seed}")},
	}
}

func minimizeFixtureCommand(t *testing.T, body string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
	return fmt.Sprintf("FAILFORGE_MINIMIZE_FIXTURE=1 exec %s -test.run='^TestMinimizeFixtureProcess$' -- {port} %s", quote(exe), quote(body))
}

// TestMinimizeFixtureProcess is a node subprocess, using the same test binary so
// this integration test does not depend on Python or the shared node.py store.
func TestMinimizeFixtureProcess(t *testing.T) {
	if os.Getenv("FAILFORGE_MINIMIZE_FIXTURE") != "1" {
		return
	}
	args := os.Args
	if len(args) < 4 || args[len(args)-3] != "--" {
		t.Fatal("fixture requires -- <port> <response-body>")
	}
	port, body := args[len(args)-2], args[len(args)-1]
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/keys/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, body)
	})
	if err := http.ListenAndServe(net.JoinHostPort("127.0.0.1", port), handler); err != nil {
		t.Fatal(err)
	}
}

func runMinimizeFixture(t *testing.T, cfg *config.Config) string {
	t.Helper()
	rn, err := NewRunner(cfg, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	defer rn.store.Close()
	writeMinimizeJSON(t, filepath.Join(rn.GetOutputDir(), "faults.json"), cfg.Faults.Schedule)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := rn.Run(ctx); err != nil {
		t.Fatalf("initial fixture execution failed: %v", err)
	}
	return rn.GetOutputDir()
}

func requireMinimizeTargetHistory(t *testing.T, dbPath string) int {
	t.Helper()
	runID, count, err := getRunViolations(dbPath)
	if err != nil || count == 0 {
		t.Fatalf("expected actual violations in %s: count=%d, err=%v", dbPath, count, err)
	}
	targets, err := getTargetCheckers(dbPath, runID)
	if err != nil || len(targets) != 1 || targets[0] != minimizeTargetChecker {
		t.Fatalf("expected only the target correctness violation: targets=%v, err=%v", targets, err)
	}
	st, err := store.NewStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// Recheck recorded operations, rather than trusting violation rows alone.
	violations, err := (&checkers.ReadAfterWriteChecker{}).Check(runID, st)
	if err != nil || len(violations) != count {
		t.Fatalf("raw operation history does not support reported violations: actual=%d, recorded=%d, err=%v", len(violations), count, err)
	}
	return count
}

func readMinimizeFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readMinimizeJSON(t *testing.T, path string, dest interface{}) {
	t.Helper()
	if err := json.Unmarshal(readMinimizeFile(t, path), dest); err != nil {
		t.Fatal(err)
	}
}

func writeMinimizeJSON(t *testing.T, path string, value interface{}) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}
