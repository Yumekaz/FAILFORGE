package faults

import (
	"context"
	"sync"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/node"
)

func TestCpuPauseFault(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count:   1,
				Ports:   config.PortsConfig{Start: 8080},
				DataDir: tempDir + "/data-{node_id}",
			},
		},
	}

	var mu sync.Mutex
	var events []string
	onEvent := func(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, eventType)
	}

	nm := node.NewNodeManager(cfg, "run-1", tempDir, onEvent)
	// Force the node to appear running so we don't need to start a process
	status := nm.GetNodesStatus()
	if len(status) == 0 {
		t.Fatalf("expected initialized nodes")
	}

	// PauseNode/ResumeNode requires node to be in RUNNING/PAUSED states
	// Let's call startNodeUnlocked mock, or just manipulate states directly using internal access if we can.
	// But nm.nodes is private. nm.StartNode(ctx, "node-1") actually starts a real process.
	// Wait, we can define a command that runs a sleep command, e.g. "sleep 10" in the command config!
	cfg.System.Nodes.Command = "sleep 10"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := nm.StartNode(ctx, "node-1")
	if err != nil {
		t.Fatalf("failed to start mock node: %v", err)
	}
	defer nm.StopAll()

	fctx := &FaultContext{
		Config: &config.FaultConfig{
			Type: "cpu_pause",
			Params: map[string]interface{}{
				"node":        "node-1",
				"duration_ms": 100,
			},
		},
		Manager:   nm,
		StartTime: time.Now(),
	}

	fault := &CpuPauseFault{}
	if err := fault.Validate(fctx.Config); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	err = fault.Inject(ctx, fctx)
	if err != nil {
		t.Fatalf("injection failed: %v", err)
	}

	// Wait for CPU pause to complete (100ms duration)
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	hasPaused := false
	hasResumed := false
	for _, ev := range events {
		if ev == "NodePaused" {
			hasPaused = true
		}
		if ev == "NodeResumed" {
			hasResumed = true
		}
	}

	if !hasPaused || !hasResumed {
		t.Errorf("expected CPU pause to transition through NodePaused and NodeResumed. Events: %v", events)
	}
}

func TestSlowDiskFault(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count:   1,
				Ports:   config.PortsConfig{Start: 8080},
				DataDir: tempDir + "/data-{node_id}",
			},
		},
	}

	var mu sync.Mutex
	var events []string
	onEvent := func(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, eventType)
	}

	nm := node.NewNodeManager(cfg, "run-1", tempDir, onEvent)
	cfg.System.Nodes.Command = "sleep 10"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := nm.StartNode(ctx, "node-1")
	if err != nil {
		t.Fatalf("failed to start mock node: %v", err)
	}
	defer nm.StopAll()

	fctx := &FaultContext{
		Config: &config.FaultConfig{
			Type: "slow_disk",
			Params: map[string]interface{}{
				"node":        "node-1",
				"duration_ms": 300,
				"stall_ms":    20,
				"interval_ms": 50,
			},
		},
		Manager:   nm,
		StartTime: time.Now(),
	}

	fault := &SlowDiskFault{}
	err = fault.Inject(ctx, fctx)
	if err != nil {
		t.Fatalf("injection failed: %v", err)
	}

	// Wait for slow disk fault loop to complete (300ms duration)
	time.Sleep(450 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	pausedCount := 0
	resumedCount := 0
	for _, ev := range events {
		if ev == "NodePaused" {
			pausedCount++
		}
		if ev == "NodeResumed" {
			resumedCount++
		}
	}

	// We expect multiple micro-pauses within 300ms (stall 20ms + interval 50ms = ~70ms cycle time. 300ms / 70ms = ~4 cycles)
	if pausedCount < 2 || resumedCount < 2 {
		t.Errorf("expected multiple micro-pauses, got paused: %d, resumed: %d. Events: %v", pausedCount, resumedCount, events)
	}
}
