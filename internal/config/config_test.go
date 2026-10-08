package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tempFile, err := os.CreateTemp("", "failforge-config-*.yml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())

	content := `
name: test-cluster
seed: 12345
time:
  duration_ms: 1000
  tick_ms: 10
system:
  type: process_cluster
  nodes:
    count: 2
    command: "node --id {node_id} --port {port}"
    ports:
      start: 8000
    data_dir: "/tmp/data/{node_id}"
network:
  mode: controlled_proxy
  proxy_port: 9090
workload:
  type: dummy
  clients: 1
  duration_ms: 500
  operations:
    read:
      weight: 10
faults:
  mode: none
output:
  dir: "/tmp/runs"
`
	if _, err := tempFile.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write config content: %v", err)
	}
	tempFile.Close()

	cfg, err := LoadConfig(tempFile.Name())
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Name != "test-cluster" {
		t.Errorf("expected Name to be 'test-cluster', got '%s'", cfg.Name)
	}
	if cfg.Seed != 12345 {
		t.Errorf("expected Seed to be 12345, got %d", cfg.Seed)
	}
	if cfg.System.Nodes.Count != 2 {
		t.Errorf("expected node count to be 2, got %d", cfg.System.Nodes.Count)
	}
	if cfg.System.Nodes.Ports.Start != 8000 {
		t.Errorf("expected start port to be 8000, got %d", cfg.System.Nodes.Ports.Start)
	}
	if cfg.Network.ProxyPort != 9090 {
		t.Errorf("expected proxy port to be 9090, got %d", cfg.Network.ProxyPort)
	}
}

func TestLoadConfigCoverageAndWarmupSettings(t *testing.T) {
	content := `
name: coverage
time:
  duration_ms: 10000
  warmup_ms: 4000
workload:
  type: test
  minimum_successful_operations: 20
  minimum_successful_by_operation:
    get: 5
faults:
  mode: seeded_random
  start_after_ms: 2000
  minimum_successful_injections: 1
`
	file, err := os.CreateTemp(t.TempDir(), "coverage-*.yml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Time.WarmupMs != 4000 {
		t.Fatalf("warmup_ms = %d, want 4000", cfg.Time.WarmupMs)
	}
	if cfg.Workload.MinimumSuccessfulOperations != 20 || cfg.Workload.MinimumSuccessfulByOperation["get"] != 5 {
		t.Fatalf("workload coverage settings not decoded: %+v", cfg.Workload)
	}
	if cfg.Faults.MinimumSuccessfulInjections != 1 {
		t.Fatalf("minimum successful injections = %d, want 1", cfg.Faults.MinimumSuccessfulInjections)
	}
	if cfg.Faults.StartAfterMs != 2000 {
		t.Fatalf("fault start delay = %d, want 2000", cfg.Faults.StartAfterMs)
	}
}
