package faults

import (
	"testing"

	"failforge/internal/config"
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
