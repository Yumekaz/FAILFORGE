package checkers

import (
	"path/filepath"
	"testing"

	"failforge/internal/model"
	"failforge/internal/store"
)

func newCoverageTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.NewStore(filepath.Join(t.TempDir(), "coverage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestWorkloadCoverageRejectsVacuousAndUndercoveredRuns(t *testing.T) {
	st := newCoverageTestStore(t)
	for i := 0; i < 4; i++ {
		if err := st.CreateOperation(&model.Operation{
			OpID:      string(rune('a' + i)),
			RunID:     "coverage-run",
			Operation: "get",
			Status:    "fail",
		}); err != nil {
			t.Fatal(err)
		}
	}

	checker := &WorkloadCoverageChecker{
		MinimumSuccessfulOperations:  3,
		MinimumSuccessfulByOperation: map[string]int{"get": 2, "put": 1},
	}
	violations, err := checker.Check("coverage-run", st)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 3 {
		t.Fatalf("got %d coverage violations, want total plus two operation gaps: %+v", len(violations), violations)
	}

	for i, operation := range []string{"GET", "get", "put", "put"} {
		if err := st.CreateOperation(&model.Operation{
			OpID:      string(rune('k' + i)),
			RunID:     "covered-run",
			Operation: operation,
			Status:    "ok",
		}); err != nil {
			t.Fatal(err)
		}
	}
	checker.MinimumSuccessfulOperations = 3
	checker.MinimumSuccessfulByOperation = map[string]int{"get": 2, "put": 2}
	violations, err = checker.Check("covered-run", st)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("fully covered run produced violations: %+v", violations)
	}
}

func TestFaultInjectionCoverageCountsOnlyCompletedInjections(t *testing.T) {
	st := newCoverageTestStore(t)
	checker := &FaultInjectionCoverageChecker{MinimumSuccessfulInjections: 1}
	if err := st.CreateEvent(&model.Event{RunID: "fault-run", Category: "Fault", Type: "FaultAttempted"}); err != nil {
		t.Fatal(err)
	}
	violations, err := checker.Check("fault-run", st)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 1 {
		t.Fatalf("attempt without effect should fail coverage, got %+v", violations)
	}

	if err := st.CreateEvent(&model.Event{RunID: "fault-run", Category: "Fault", Type: "FaultInjected", PayloadJSON: `{"type":"partition"}`}); err != nil {
		t.Fatal(err)
	}
	violations, err = checker.Check("fault-run", st)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("completed injection should satisfy coverage, got %+v", violations)
	}
}
