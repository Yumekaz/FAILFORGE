package faults

import (
	"testing"
	"failforge/internal/config"
)

func TestRegistryRegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	reg.Register(&KillNodeFault{})

	f, ok := reg.Get("kill_node")
	if !ok {
		t.Fatalf("expected to find kill_node fault")
	}
	if f.Type() != "kill_node" {
		t.Errorf("expected Type() 'kill_node', got '%s'", f.Type())
	}

	// case insensitivity test
	_, ok2 := reg.Get("KILL_NODE")
	if !ok2 {
		t.Errorf("expected case-insensitive match for KILL_NODE")
	}
}

func TestDefaultRegistryContainsAll(t *testing.T) {
	reg := DefaultRegistry()
	expectedFaults := []string{
		"kill_node", "restart_node", "partition", "heal", "delay_messages", "drop_messages",
		"asymmetric_partition", "duplicate_messages", "corrupt_messages",
		"cpu_pause", "slow_disk", "disk_write_loss", "partial_persistence",
		"stale_snapshot_restart", "clock_skew",
	}

	for _, name := range expectedFaults {
		if _, ok := reg.Get(name); !ok {
			t.Errorf("default registry is missing fault: %s", name)
		}
	}
}

func TestFaultValidation(t *testing.T) {
	reg := DefaultRegistry()

	// Test validation of kill_node without node
	kill, _ := reg.Get("kill_node")
	cfg1 := &config.FaultConfig{Type: "kill_node"}
	if err := kill.Validate(cfg1); err == nil {
		t.Errorf("expected validation to fail for kill_node without node")
	}

	cfg2 := &config.FaultConfig{
		Type: "kill_node",
		Params: map[string]interface{}{
			"node": "node-1",
		},
	}
	if err := kill.Validate(cfg2); err != nil {
		t.Errorf("expected validation to pass for kill_node with params.node: %v", err)
	}
}
