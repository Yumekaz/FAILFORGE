package runner

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"failforge/internal/model"
	"failforge/internal/store"
)

func TestTimelineVisualizer(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "failforge-timeline-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "history.sqlite")
	st, err := store.NewStore(dbPath)
	if err != nil {
		t.Fatalf("failed to initialize store: %v", err)
	}
	defer st.Close()

	// Seed some mock data in SQLite
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite connection: %v", err)
	}
	defer db.Close()

	runID := "test-run-123"
	_, _ = db.Exec("INSERT INTO runs (id, seed, started_at, status, config_hash) VALUES (?, ?, ?, ?, ?)",
		runID, 999, time.Now().Format(time.RFC3339), "FAILED", "mock-hash")

	// Insert mock events
	mockEvents := []model.Event{
		{
			RunID:       runID,
			TimeMs:      0,
			Category:    "Node",
			Type:        "NodeStarted",
			PayloadJSON: `{"node_id":"node-1","pid":101,"port":8081}`,
		},
		{
			RunID:       runID,
			TimeMs:      50,
			Category:    "Node",
			Type:        "NodeStarted",
			PayloadJSON: `{"node_id":"node-2","pid":102,"port":8082}`,
		},
		{
			RunID:       runID,
			TimeMs:      100,
			Category:    "Operation",
			Type:        "OperationInvoked",
			PayloadJSON: `{"client_id":"client-1","op":"PUT","key":"x","value":"1","target":"node-1","op_id":"op-1"}`,
		},
		{
			RunID:       runID,
			TimeMs:      150,
			Category:    "Fault",
			Type:        "partition",
			PayloadJSON: `{"groups":[["node-1"],["node-2"]]}`,
		},
		{
			RunID:       runID,
			TimeMs:      200,
			Category:    "Operation",
			Type:        "OperationCompleted",
			PayloadJSON: `{"client_id":"client-1","op":"PUT","key":"x","status":"ok","latency_ms":100,"op_id":"op-1","target":"node-1"}`,
		},
		{
			RunID:       runID,
			TimeMs:      250,
			Category:    "Run",
			Type:        "Violation",
			PayloadJSON: `{"checker_name":"read_after_acknowledged_write","description":"Stale read on key x"}`,
		},
	}

	for _, e := range mockEvents {
		_, err = db.Exec("INSERT INTO events (run_id, time_ms, category, type, payload_json) VALUES (?, ?, ?, ?, ?)",
			e.RunID, e.TimeMs, e.Category, e.Type, e.PayloadJSON)
		if err != nil {
			t.Fatalf("failed to insert mock event: %v", err)
		}
	}

	// 1. Test Terminal Timeline
	termTimeline, err := GenerateTerminalTimeline(runID, st)
	if err != nil {
		t.Fatalf("GenerateTerminalTimeline failed: %v", err)
	}

	if !strings.Contains(termTimeline, "CHRONOLOGICAL EVENT TIMELINE") {
		t.Errorf("expected timeline header, got: %s", termTimeline)
	}
	if !strings.Contains(termTimeline, "Node node-1") {
		t.Errorf("expected node-1 start description, got: %s", termTimeline)
	}
	if !strings.Contains(termTimeline, "Fault Injected: partition") {
		t.Errorf("expected fault injection text, got: %s", termTimeline)
	}
	if !strings.Contains(termTimeline, "Invariant Violation") {
		t.Errorf("expected violation in timeline, got: %s", termTimeline)
	}

	// 2. Test Mermaid Sequence
	mermaidSeq, err := GenerateMermaidSequence(runID, st)
	if err != nil {
		t.Fatalf("GenerateMermaidSequence failed: %v", err)
	}

	if !strings.Contains(mermaidSeq, "sequenceDiagram") {
		t.Errorf("expected sequenceDiagram declaration, got: %s", mermaidSeq)
	}
	if !strings.Contains(mermaidSeq, "actor client-1") {
		t.Errorf("expected client-1 participant, got: %s", mermaidSeq)
	}
	if !strings.Contains(mermaidSeq, "participant node-1") {
		t.Errorf("expected node-1 participant, got: %s", mermaidSeq)
	}
	if !strings.Contains(mermaidSeq, "client-1->>+node-1: PUT(x=1)") {
		t.Errorf("expected client PUT invocation arrow, got: %s", mermaidSeq)
	}
	if !strings.Contains(mermaidSeq, "node-1-->>-client-1: ok") {
		t.Errorf("expected client PUT response arrow, got: %s", mermaidSeq)
	}

	// 3. Test HTML timeline generation
	err = GenerateHTMLTimeline(runID, st, tempDir)
	if err != nil {
		t.Fatalf("GenerateHTMLTimeline failed: %v", err)
	}

	htmlPath := filepath.Join(tempDir, "timeline.html")
	if _, err := os.Stat(htmlPath); os.IsNotExist(err) {
		t.Fatalf("timeline.html not generated")
	}

	htmlBytes, err := os.ReadFile(htmlPath)
	if err != nil {
		t.Fatalf("failed to read generated timeline.html: %v", err)
	}
	htmlStr := string(htmlBytes)

	if !strings.Contains(htmlStr, "FailForge Visual Timeline") {
		t.Errorf("expected visual title inside HTML")
	}
	if !strings.Contains(htmlStr, "mermaid.initialize") {
		t.Errorf("expected mermaid initialization script inside HTML")
	}
	if !strings.Contains(htmlStr, "rawEvents =") {
		t.Errorf("expected events JSON payload inside HTML")
	}
	if !strings.Contains(htmlStr, "test-run-123") {
		t.Errorf("expected run ID inside HTML")
	}
}
