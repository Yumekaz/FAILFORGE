package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"failforge/internal/model"
)

func TestStoreOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failforge-db-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.sqlite")
	s, err := NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer s.Close()

	runID := "run-123"
	now := time.Now().Round(time.Second) // round for comparison stability in string formatting

	// 1. Test Run CRUD
	run := &model.Run{
		ID:         runID,
		Seed:       999,
		StartedAt:  now,
		Status:     "RUNNING",
		ConfigHash: "hash123",
	}

	if err := s.CreateRun(run); err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	retrievedRun, err := s.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run: %v", err)
	}

	if retrievedRun.ID != run.ID || retrievedRun.Seed != run.Seed || retrievedRun.Status != run.Status {
		t.Errorf("retrieved run mismatch: %+v vs %+v", retrievedRun, run)
	}

	ended := now.Add(time.Second)
	run.EndedAt = &ended
	run.Status = "PASSED"
	if err := s.UpdateRun(run); err != nil {
		t.Fatalf("failed to update run: %v", err)
	}

	retrievedRun, err = s.GetRun(runID)
	if err != nil {
		t.Fatalf("failed to get run second time: %v", err)
	}
	if retrievedRun.Status != "PASSED" || retrievedRun.EndedAt == nil || !retrievedRun.EndedAt.Equal(ended) {
		t.Errorf("updated run mismatch")
	}

	// 2. Test Node CRUD
	nodeObj := &model.Node{
		RunID:   runID,
		NodeID:  "node-1",
		Status:  "RUNNING",
		PID:     1234,
		Port:    8081,
		DataDir: "/tmp/data",
	}

	if err := s.CreateNode(nodeObj); err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	nodes, err := s.GetNodes(runID)
	if err != nil {
		t.Fatalf("failed to get nodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].NodeID != "node-1" || nodes[0].PID != 1234 {
		t.Errorf("nodes retrieval mismatch: %+v", nodes)
	}

	nodeObj.Status = "KILLED"
	if err := s.UpdateNode(nodeObj); err != nil {
		t.Fatalf("failed to update node: %v", err)
	}

	nodes, err = s.GetNodes(runID)
	if err != nil {
		t.Fatalf("failed to get nodes: %v", err)
	}
	if nodes[0].Status != "KILLED" {
		t.Errorf("updated node status mismatch: %s", nodes[0].Status)
	}

	// 3. Test Event CRUD
	eventObj := &model.Event{
		RunID:       runID,
		TimeMs:      150,
		Category:    "Node",
		Type:        "NodeKilled",
		PayloadJSON: `{"node_id":"node-1"}`,
	}

	if err := s.CreateEvent(eventObj); err != nil {
		t.Fatalf("failed to create event: %v", err)
	}

	events, err := s.GetEvents(runID)
	if err != nil {
		t.Fatalf("failed to get events: %v", err)
	}
	if len(events) != 1 || events[0].Type != "NodeKilled" || events[0].TimeMs != 150 {
		t.Errorf("events retrieval mismatch: %+v", events)
	}
}
