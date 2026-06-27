package runner

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"failforge/internal/config"
)

func TestMinimizeEngine(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failforge-minimize-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a minimal config for a quick test run
	cfg := &config.Config{
		Name: "test-minimize",
		Seed: 42,
		Time: config.TimeConfig{
			DurationMs: 2500,
			TickMs:     10,
		},
		System: config.SystemConfig{
			Type: "process_cluster",
			Nodes: config.NodesConfig{
				Count:   2,
				Command: "python3 ../../node.py --id {node_id} --port {port} --proxy {proxy_url}",
				Ports: config.PortsConfig{
					Start: 28000,
				},
				DataDir: filepath.Join(tempDir, "data", "{node_id}"),
			},
		},
		Network: config.NetworkConfig{
			Mode:      "controlled_proxy",
			ProxyPort: 29000,
		},
		Workload: config.WorkloadConfig{
			Type:       "kv-register",
			Clients:    2,
			DurationMs: 2000,
			Keys:       []string{"x"},
			Operations: map[string]interface{}{
				"put": 5,
				"get": 5,
			},
		},
		Faults: config.FaultsConfig{
			Mode: "seeded_random",
			Profile: map[string]interface{}{
				"max_faults": 3,
				"kill_node":  1,
				"partition":  2,
			},
		},
		Checkers: []config.CheckerConfig{
			{Name: "read_after_acknowledged_write"},
		},
		Output: config.OutputConfig{
			Dir: filepath.Join(tempDir, "run-{seed}"),
		},
	}

	// 1. Execute initial campaign run
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rn, err := NewRunner(cfg, nil, "")
	if err != nil {
		t.Fatalf("failed to initialize runner: %v", err)
	}

	runDir := rn.GetOutputDir()

	// Wait to execute the initial run and close its store
	_, runErr := rn.RunAndReport(ctx)
	if runErr != nil {
		t.Fatalf("initial run failed: %v", runErr)
	}

	// Ensure config.json exists
	configJsonPath := filepath.Join(runDir, "config.json")
	if _, err := os.Stat(configJsonPath); os.IsNotExist(err) {
		t.Fatalf("config.json not found in output directory")
	}

	// Ensure faults.json exists
	faultsJsonPath := filepath.Join(runDir, "faults.json")
	if _, err := os.Stat(faultsJsonPath); os.IsNotExist(err) {
		t.Fatalf("faults.json not found in output directory")
	}

	// Read initial violations count
	_, initialViolations, err := getRunViolations(filepath.Join(runDir, "history.sqlite"))
	if err != nil {
		t.Fatalf("failed to read original violations: %v", err)
	}

	if initialViolations == 0 {
		// If 0 violations, we cannot minimize, but we can verify the error behavior
		_, err := MinimizeRun(ctx, runDir)
		if err == nil {
			t.Fatalf("expected error when minimizing run with 0 violations, got nil")
		}
		t.Logf("Successfully verified error on 0 violations: %v", err)
		return
	}

	// 2. Perform Minimization
	res, err := MinimizeRun(ctx, runDir)
	if err != nil {
		t.Fatalf("minimization failed: %v", err)
	}

	// 3. Verify Minimization Results
	if res.MinimizedOutputDir == "" {
		t.Errorf("expected non-empty minimized output dir")
	}
	if res.MinimizedViolations == 0 {
		t.Errorf("expected minimized run to still have violations")
	}
	if res.MinimizedDurationMs > res.OriginalDurationMs {
		t.Errorf("expected minimized duration (%d) to be <= original duration (%d)",
			res.MinimizedDurationMs, res.OriginalDurationMs)
	}
	if res.MinimizedClients > res.OriginalClients {
		t.Errorf("expected minimized clients (%d) to be <= original clients (%d)",
			res.MinimizedClients, res.OriginalClients)
	}

	// Check final faults files and config files were written
	minimizedConfigPath := filepath.Join(res.MinimizedOutputDir, "config.json")
	if _, err := os.Stat(minimizedConfigPath); os.IsNotExist(err) {
		t.Errorf("minimized config.json not found")
	}

	minimizedFaultsPath := filepath.Join(res.MinimizedOutputDir, "faults.json")
	if _, err := os.Stat(minimizedFaultsPath); os.IsNotExist(err) {
		t.Errorf("minimized faults.json not found")
	}

	// Parse minimized faults to ensure they are valid
	var minimizedFaults []config.FaultConfig
	minFaultsBytes, err := os.ReadFile(minimizedFaultsPath)
	if err == nil {
		if err := json.Unmarshal(minFaultsBytes, &minimizedFaults); err != nil {
			t.Errorf("failed to parse minimized faults: %v", err)
		}
	}

	t.Logf("Minimization succeeded! Original faults: %d, Minimized faults: %d",
		res.OriginalFaultCount, res.MinimizedFaultCount)
}
