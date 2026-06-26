package runner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"failforge/internal/config"
)

func TestReplayEngine(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failforge-replay-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a minimal config for a quick test run
	cfg := &config.Config{
		Name: "test-replay",
		Seed: 42,
		Time: config.TimeConfig{
			DurationMs: 500,
			TickMs:     10,
		},
		System: config.SystemConfig{
			Type: "process_cluster",
			Nodes: config.NodesConfig{
				Count:   1,
				Command: "python3 ../../node.py --id {node_id} --port {port} --proxy {proxy_url}",
				Ports: config.PortsConfig{
					Start: 18000,
				},
				DataDir: filepath.Join(tempDir, "data", "{node_id}"),
			},
		},
		Network: config.NetworkConfig{
			Mode:      "controlled_proxy",
			ProxyPort: 19000,
		},
		Workload: config.WorkloadConfig{
			Type:       "kv-register",
			Clients:    1,
			DurationMs: 300,
			Keys:       []string{"x"},
			Operations: map[string]interface{}{
				"put": 5,
				"get": 5,
			},
		},
		Faults: config.FaultsConfig{
			Mode: "seeded_random",
			Profile: map[string]interface{}{
				"max_faults": 1,
				"kill_node":  1,
			},
		},
		Checkers: []config.CheckerConfig{
			{Name: "read_after_acknowledged_write"},
		},
		Output: config.OutputConfig{
			Dir: filepath.Join(tempDir, "run-{seed}"),
		},
	}

	// 1. Run the initial campaign
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rn, err := NewRunner(cfg, nil, "")
	if err != nil {
		t.Fatalf("failed to initialize runner: %v", err)
	}

	runDir := rn.GetOutputDir()

	if err := rn.Run(ctx); err != nil {
		t.Fatalf("initial run failed: %v", err)
	}

	// Verify original config.json was written
	configJsonPath := filepath.Join(runDir, "config.json")
	if _, err := os.Stat(configJsonPath); os.IsNotExist(err) {
		t.Fatalf("config.json not found in output directory")
	}

	// Verify faults.json was written
	faultsJsonPath := filepath.Join(runDir, "faults.json")
	if _, err := os.Stat(faultsJsonPath); os.IsNotExist(err) {
		t.Fatalf("faults.json not found in output directory")
	}

	// 2. Execute replay
	res, err := ReplayRun(ctx, runDir)
	if err != nil {
		t.Fatalf("replay run failed: %v", err)
	}

	if res.Seed != cfg.Seed {
		t.Errorf("expected seed %d, got %d", cfg.Seed, res.Seed)
	}

	if res.OriginalRunID == "" || res.ReplayRunID == "" {
		t.Errorf("expected non-empty run IDs, got orig: '%s', replay: '%s'", res.OriginalRunID, res.ReplayRunID)
	}

	if res.OriginalRunID == res.ReplayRunID {
		t.Errorf("original and replay run IDs should be different, got identical: %s", res.OriginalRunID)
	}

	// Verify replay directory was created
	if _, err := os.Stat(res.ReplayOutputDir); os.IsNotExist(err) {
		t.Errorf("replay output directory was not created: %s", res.ReplayOutputDir)
	}

	// Verify replay history.sqlite was created
	replayDbPath := filepath.Join(res.ReplayOutputDir, "history.sqlite")
	if _, err := os.Stat(replayDbPath); os.IsNotExist(err) {
		t.Errorf("replay database was not created: %s", replayDbPath)
	}
}
