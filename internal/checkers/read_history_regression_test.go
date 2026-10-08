package checkers

import (
	"failforge/internal/model"
	"testing"
)

func TestReadHistoryRespectsRealTimeAndAmbiguousWrites(t *testing.T) {
	for _, scenario := range []struct {
		name                 string
		firstStatus          string
		firstStart, firstEnd int64
		want                 int
	}{
		{"overlapping writes may commit in either order", "ok", 0, 25, 0},
		{"nonoverlapping stale acknowledged write", "ok", 0, 5, 1},
		{"failed write may have partially applied", "error", 0, 25, 0},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			st, runID, cleanup := setupTestStore(t)
			defer cleanup()
			for _, op := range []*model.Operation{
				{OpID: "a", RunID: runID, Operation: "PUT", InputJSON: `{"key":"x","value":"a"}`, Status: scenario.firstStatus, StartMs: scenario.firstStart, EndMs: scenario.firstEnd},
				{OpID: "b", RunID: runID, Operation: "PUT", InputJSON: `{"key":"x","value":"b"}`, Status: "ok", StartMs: 10, EndMs: 20},
				{OpID: "read", RunID: runID, Operation: "GET", InputJSON: `{"key":"x"}`, OutputJSON: `{"body":"a"}`, Status: "ok", StartMs: 30, EndMs: 35},
			} {
				if err := st.CreateOperation(op); err != nil {
					t.Fatal(err)
				}
			}
			violations, err := (&ReadAfterWriteChecker{}).Check(runID, st)
			if err != nil || len(violations) != scenario.want {
				t.Fatalf("got %v, %v; want %d violations", violations, err, scenario.want)
			}
		})
	}
}
