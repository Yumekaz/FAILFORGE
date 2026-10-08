package faults

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/model"
	"failforge/internal/store"
)

func TestSchedulerDeterministicGeneration(t *testing.T) {
	cfg := &config.Config{
		Time: config.TimeConfig{
			DurationMs: 10000,
		},
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count: 3,
			},
		},
		Faults: config.FaultsConfig{
			Mode: "seeded_random",
			Profile: map[string]interface{}{
				"max_faults":             12,
				"kill_node":              2,
				"restart_node":           2,
				"partition":              3,
				"heal":                   3,
				"asymmetric_partition":   2,
				"duplicate_messages":     2,
				"corrupt_messages":       2,
				"cpu_pause":              2,
				"slow_disk":              2,
				"disk_write_loss":        2,
				"partial_persistence":    2,
				"stale_snapshot_restart": 2,
				"clock_skew":             2,
			},
		},
	}

	s1 := NewScheduler(cfg, "run-1", 42, "", nil, nil, nil, nil, nil)
	sched1 := s1.generateRandomSchedule()

	s2 := NewScheduler(cfg, "run-2", 42, "", nil, nil, nil, nil, nil)
	sched2 := s2.generateRandomSchedule()

	// 1. Assert identical seeds yield identical schedules
	if len(sched1) != len(sched2) {
		t.Fatalf("expected identical length, got %d vs %d", len(sched1), len(sched2))
	}

	for i := 0; i < len(sched1); i++ {
		if sched1[i].AtMs != sched2[i].AtMs || sched1[i].Type != sched2[i].Type || sched1[i].Node != sched2[i].Node {
			t.Errorf("mismatch at index %d: %+v vs %+v", i, sched1[i], sched2[i])
		}
	}

	// 2. Assert different seeds yield different schedules
	s3 := NewScheduler(cfg, "run-3", 43, "", nil, nil, nil, nil, nil)
	sched3 := s3.generateRandomSchedule()

	different := false
	if len(sched1) != len(sched3) {
		different = true
	} else {
		for i := 0; i < len(sched1); i++ {
			if sched1[i].AtMs != sched3[i].AtMs || sched1[i].Type != sched3[i].Type {
				different = true
				break
			}
		}
	}

	if !different {
		t.Errorf("expected different seeds to yield different schedules, but they were identical")
	}
}

func TestSchedulerNestedWeights(t *testing.T) {
	cfg := &config.Config{
		Time: config.TimeConfig{
			DurationMs: 10000,
		},
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count: 3,
			},
		},
		Faults: config.FaultsConfig{
			Mode: "seeded_random",
			Profile: map[string]interface{}{
				"max_faults":   3,
				"kill_node":    map[string]interface{}{"weight": 2},
				"restart_node": map[string]interface{}{"weight": 2},
				"partition":    map[string]interface{}{"weight": 3},
				"heal":         map[string]interface{}{"weight": 3},
			},
		},
	}

	s1 := NewScheduler(cfg, "run-1", 42, "", nil, nil, nil, nil, nil)
	sched1 := s1.generateRandomSchedule()

	if len(sched1) == 0 {
		t.Errorf("expected schedule to be generated, got 0 items")
	}
	for _, f := range sched1 {
		if f.Type == "" {
			t.Errorf("expected fault type to be populated, got empty")
		}
	}
}

func TestSchedulerWarmupOffsetsFaultSchedule(t *testing.T) {
	s := NewScheduler(&config.Config{
		Time:   config.TimeConfig{WarmupMs: 2500},
		Faults: config.FaultsConfig{StartAfterMs: 1000},
	}, "run", 42, "", nil, nil, nil, nil, nil)
	s.schedule = []config.FaultConfig{{AtMs: 500}, {AtMs: 3000}}
	s.shiftScheduleForWarmup()
	if s.schedule[0].AtMs != 4000 || s.schedule[1].AtMs != 6500 {
		t.Fatalf("warmup-adjusted schedule = %+v", s.schedule)
	}
}

type schedulerTestFault struct{ injectErr error }

func (f *schedulerTestFault) Type() string                                { return "scheduler_test" }
func (f *schedulerTestFault) Validate(*config.FaultConfig) error          { return nil }
func (f *schedulerTestFault) Inject(context.Context, *FaultContext) error { return f.injectErr }

func TestSchedulerRecordsAttemptAndOnlyConfirmsSuccessfulInjection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		injectErr  error
		wantEvents []string
		wantWarn   int
	}{
		{name: "success", wantEvents: []string{"FaultAttempted", "FaultInjected"}},
		{name: "resume failure", injectErr: errors.New("resume failed"), wantEvents: []string{"FaultAttempted", "FaultInjectionFailed"}, wantWarn: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.NewStore(filepath.Join(t.TempDir(), "scheduler.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			var types []string
			onEvent := func(ms int64, category, eventType, payload string) {
				types = append(types, eventType)
				if err := st.CreateEvent(&model.Event{RunID: "scheduler-run", TimeMs: ms, Category: category, Type: eventType, PayloadJSON: payload}); err != nil {
					t.Errorf("store event: %v", err)
				}
			}
			registry := NewRegistry()
			registry.Register(&schedulerTestFault{injectErr: tc.injectErr})
			s := NewScheduler(&config.Config{}, "scheduler-run", 1, t.TempDir(), nil, nil, st, onEvent, registry)
			s.startTime = time.Now()
			s.injectFault(context.Background(), config.FaultConfig{Type: "scheduler_test"})

			if len(types) != len(tc.wantEvents) {
				t.Fatalf("event types = %v, want %v", types, tc.wantEvents)
			}
			for i := range types {
				if types[i] != tc.wantEvents[i] {
					t.Fatalf("event types = %v, want %v", types, tc.wantEvents)
				}
			}
			violations, err := st.GetViolations("scheduler-run")
			if err != nil {
				t.Fatal(err)
			}
			warnCount := 0
			for _, violation := range violations {
				if violation.Severity == "warning" {
					warnCount++
				}
			}
			if warnCount != tc.wantWarn {
				t.Fatalf("warning count = %d, want %d: %+v", warnCount, tc.wantWarn, violations)
			}
		})
	}
}
