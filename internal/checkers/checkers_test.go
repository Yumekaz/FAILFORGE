package checkers

import (
	"os"
	"path/filepath"
	"testing"

	"failforge/internal/model"
	"failforge/internal/store"
)

func setupTestStore(t *testing.T) (*store.Store, string, func()) {
	tempDir, err := os.MkdirTemp("", "failforge-checker-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tempDir, "test.sqlite")
	s, err := store.NewStore(dbPath)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("failed to create store: %v", err)
	}

	cleanup := func() {
		s.Close()
		os.RemoveAll(tempDir)
	}

	return s, "run-test", cleanup
}

func TestReadAfterWriteChecker(t *testing.T) {
	t.Run("Valid History", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// PUT key "x" = "val1" completing at 10ms
		err := st.CreateOperation(&model.Operation{
			OpID:       "op-1",
			RunID:      runID,
			ClientID:   "client-1",
			Operation:  "PUT",
			InputJSON:  `{"key":"x","value":"val1"}`,
			OutputJSON: `{"status_code":200,"body":"ok"}`,
			StartMs:    0,
			EndMs:      10,
			Status:     "ok",
		})
		if err != nil {
			t.Fatalf("failed to write op: %v", err)
		}

		// GET key "x" starting at 15ms, ending at 20ms, returning "val1"
		err = st.CreateOperation(&model.Operation{
			OpID:       "op-2",
			RunID:      runID,
			ClientID:   "client-2",
			Operation:  "GET",
			InputJSON:  `{"key":"x"}`,
			OutputJSON: `{"status_code":200,"body":"val1"}`,
			StartMs:    15,
			EndMs:      20,
			Status:     "ok",
		})
		if err != nil {
			t.Fatalf("failed to write op: %v", err)
		}

		checker := &ReadAfterWriteChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 0 {
			t.Errorf("expected 0 violations, got %d: %+v", len(violations), violations)
		}
	})

	t.Run("Stale Read Violation", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// PUT key "x" = "val1" completing at 10ms
		_ = st.CreateOperation(&model.Operation{
			OpID:       "op-1",
			RunID:      runID,
			ClientID:   "client-1",
			Operation:  "PUT",
			InputJSON:  `{"key":"x","value":"val1"}`,
			OutputJSON: `{"status_code":200,"body":"ok"}`,
			StartMs:    0,
			EndMs:      10,
			Status:     "ok",
		})

		// PUT key "x" = "val2" completing at 20ms
		_ = st.CreateOperation(&model.Operation{
			OpID:       "op-2",
			RunID:      runID,
			ClientID:   "client-1",
			Operation:  "PUT",
			InputJSON:  `{"key":"x","value":"val2"}`,
			OutputJSON: `{"status_code":200,"body":"ok"}`,
			StartMs:    12,
			EndMs:      20,
			Status:     "ok",
		})

		// GET key "x" starting at 25ms returning stale "val1"
		_ = st.CreateOperation(&model.Operation{
			OpID:       "op-3",
			RunID:      runID,
			ClientID:   "client-2",
			Operation:  "GET",
			InputJSON:  `{"key":"x"}`,
			OutputJSON: `{"status_code":200,"body":"val1"}`,
			StartMs:    25,
			EndMs:      30,
			Status:     "ok",
		})

		checker := &ReadAfterWriteChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 1 {
			t.Fatalf("expected 1 violation, got %d", len(violations))
		}
		if violations[0].CheckerName != checker.Name() {
			t.Errorf("expected checker name %s, got %s", checker.Name(), violations[0].CheckerName)
		}
	})

	t.Run("Corrupt Read Violation", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// GET key "x" returning "unwritten"
		_ = st.CreateOperation(&model.Operation{
			OpID:       "op-1",
			RunID:      runID,
			ClientID:   "client-2",
			Operation:  "GET",
			InputJSON:  `{"key":"x"}`,
			OutputJSON: `{"status_code":200,"body":"unwritten"}`,
			StartMs:    10,
			EndMs:      20,
			Status:     "ok",
		})

		checker := &ReadAfterWriteChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 1 {
			t.Fatalf("expected 1 violation, got %d", len(violations))
		}
		if violations[0].CheckerName != checker.Name() {
			t.Errorf("expected checker name %s, got %s", checker.Name(), violations[0].CheckerName)
		}
	})
}

func TestLockExclusivityChecker(t *testing.T) {
	t.Run("Valid Lock Exclusivity", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// Client-1 locks key "l1" at [10, 20]ms
		_ = st.CreateOperation(&model.Operation{
			OpID:      "lock-1",
			RunID:     runID,
			ClientID:  "client-1",
			Operation: "LOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   5,
			EndMs:     10,
			Status:    "ok",
		})
		_ = st.CreateOperation(&model.Operation{
			OpID:      "unlock-1",
			RunID:     runID,
			ClientID:  "client-1",
			Operation: "UNLOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   15,
			EndMs:     20,
			Status:    "ok",
		})

		// Client-2 locks key "l1" at [30, 40]ms
		_ = st.CreateOperation(&model.Operation{
			OpID:      "lock-2",
			RunID:     runID,
			ClientID:  "client-2",
			Operation: "LOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   25,
			EndMs:     30,
			Status:    "ok",
		})
		_ = st.CreateOperation(&model.Operation{
			OpID:      "unlock-2",
			RunID:     runID,
			ClientID:  "client-2",
			Operation: "UNLOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   35,
			EndMs:     40,
			Status:    "ok",
		})

		checker := &LockExclusivityChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 0 {
			t.Errorf("expected 0 violations, got %d", len(violations))
		}
	})

	t.Run("Overlap Lock Exclusivity Violation", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// Client-1 locks key "l1" at [10, 40]ms
		_ = st.CreateOperation(&model.Operation{
			OpID:      "lock-1",
			RunID:     runID,
			ClientID:  "client-1",
			Operation: "LOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   5,
			EndMs:     10,
			Status:    "ok",
		})
		_ = st.CreateOperation(&model.Operation{
			OpID:      "unlock-1",
			RunID:     runID,
			ClientID:  "client-1",
			Operation: "UNLOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   35,
			EndMs:     40,
			Status:    "ok",
		})

		// Client-2 locks key "l1" at [20, 50]ms (overlapping client-1)
		_ = st.CreateOperation(&model.Operation{
			OpID:      "lock-2",
			RunID:     runID,
			ClientID:  "client-2",
			Operation: "LOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   15,
			EndMs:     20,
			Status:    "ok",
		})
		_ = st.CreateOperation(&model.Operation{
			OpID:      "unlock-2",
			RunID:     runID,
			ClientID:  "client-2",
			Operation: "UNLOCK",
			InputJSON: `{"key":"l1"}`,
			StartMs:   45,
			EndMs:     50,
			Status:    "ok",
		})

		checker := &LockExclusivityChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 1 {
			t.Fatalf("expected 1 violation, got %d", len(violations))
		}
		if violations[0].CheckerName != checker.Name() {
			t.Errorf("expected checker name %s, got %s", checker.Name(), violations[0].CheckerName)
		}
	})
}

func TestLeaderUniquenessChecker(t *testing.T) {
	t.Run("Valid Term Elections", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// Leader node-1 elected for term 1
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      10,
			Category:    "Consensus",
			Type:        "LeaderElected",
			PayloadJSON: `{"node_id":"node-1","term":1}`,
		})

		// node-1 steps down
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      50,
			Category:    "Consensus",
			Type:        "LeaderStepDown",
			PayloadJSON: `{"node_id":"node-1"}`,
		})

		// Leader node-2 elected for term 2
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      60,
			Category:    "Consensus",
			Type:        "LeaderElected",
			PayloadJSON: `{"node_id":"node-2","term":2}`,
		})

		checker := &LeaderUniquenessChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 0 {
			t.Errorf("expected 0 violations, got %d", len(violations))
		}
	})

	t.Run("Term Leader Uniqueness Violation", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// node-1 elected for term 1
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      10,
			Category:    "Consensus",
			Type:        "LeaderElected",
			PayloadJSON: `{"node_id":"node-1","term":1}`,
		})

		// node-2 ALSO elected for term 1 (conflict!)
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      20,
			Category:    "Consensus",
			Type:        "LeaderElected",
			PayloadJSON: `{"node_id":"node-2","term":1}`,
		})

		checker := &LeaderUniquenessChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		// We expect 2 violations: 1 for Term conflict, and 1 for Real-Time overlap (since node-1 didn't step down yet)
		if len(violations) < 1 {
			t.Fatalf("expected at least 1 violation, got %d", len(violations))
		}
	})

	t.Run("Real-Time Leader Uniqueness Violation (no term)", func(t *testing.T) {
		st, runID, cleanup := setupTestStore(t)
		defer cleanup()

		// node-1 elected
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      10,
			Category:    "Consensus",
			Type:        "LeaderElected",
			PayloadJSON: `{"node_id":"node-1"}`,
		})

		// node-2 elected concurrently without term
		_ = st.CreateEvent(&model.Event{
			RunID:       runID,
			TimeMs:      20,
			Category:    "Consensus",
			Type:        "LeaderElected",
			PayloadJSON: `{"node_id":"node-2"}`,
		})

		checker := &LeaderUniquenessChecker{}
		violations, err := checker.Check(runID, st)
		if err != nil {
			t.Fatalf("checker error: %v", err)
		}

		if len(violations) != 1 {
			t.Fatalf("expected exactly 1 violation, got %d", len(violations))
		}
		if violations[0].CheckerName != checker.Name() {
			t.Errorf("expected checker name %s, got %s", checker.Name(), violations[0].CheckerName)
		}
	})
}
