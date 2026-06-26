package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"failforge/internal/config"
)

func TestNodeManagerLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failforge-node-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count:   2,
				Command: "sleep 10",
				Ports: config.PortsConfig{
					Start: 8080,
				},
				DataDir: filepath.Join(tempDir, "data-{node_id}"),
			},
		},
		Network: config.NetworkConfig{
			ProxyPort: 9000,
		},
	}

	events := make(map[string]string)
	callback := func(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {
		events[nodeID+"-"+eventType] = eventType
	}

	nm := NewNodeManager(cfg, "run-1", tempDir, callback)

	// Verify initialization
	port, err := nm.GetPort("node-1")
	if err != nil || port != 8080 {
		t.Errorf("expected node-1 port 8080, got %d (err: %v)", port, err)
	}

	port, err = nm.GetPort("node-2")
	if err != nil || port != 8081 {
		t.Errorf("expected node-2 port 8081, got %d (err: %v)", port, err)
	}

	// Start all nodes
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := nm.StartAll(ctx); err != nil {
		t.Fatalf("failed to start all nodes: %v", err)
	}

	// Verify states
	status := nm.GetNodesStatus()
	if status["node-1"] != StateRunning || status["node-2"] != StateRunning {
		t.Errorf("expected nodes running, got status: %+v", status)
	}

	// Pause node-1
	if err := nm.PauseNode("node-1"); err != nil {
		t.Fatalf("failed to pause node-1: %v", err)
	}
	if nm.GetNodesStatus()["node-1"] != StatePaused {
		t.Errorf("expected node-1 paused, got: %s", nm.GetNodesStatus()["node-1"])
	}

	// Resume node-1
	if err := nm.ResumeNode("node-1"); err != nil {
		t.Fatalf("failed to resume node-1: %v", err)
	}
	if nm.GetNodesStatus()["node-1"] != StateRunning {
		t.Errorf("expected node-1 running, got: %s", nm.GetNodesStatus()["node-1"])
	}

	// Kill node-1
	if err := nm.KillNode("node-1"); err != nil {
		t.Fatalf("failed to kill node-1: %v", err)
	}
	if nm.GetNodesStatus()["node-1"] != StateKilled {
		t.Errorf("expected node-1 killed, got: %s", nm.GetNodesStatus()["node-1"])
	}

	// Stop all
	if err := nm.StopAll(); err != nil {
		t.Fatalf("failed to stop all: %v", err)
	}
}
