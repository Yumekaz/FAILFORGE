package faults

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/node"
	"failforge/internal/store"
)

func TestSchedulerSeed44And45ProcessTargets(t *testing.T) {
	// The frozen minidb profile selected three kills for both seeds. Keep all
	// three attempts and their times, but never kill an already killed node.
	for _, tc := range []struct {
		seed  int64
		times []int64
	}{
		{44, []int64{5347, 9915, 22337}},
		{45, []int64{835, 14550, 23108}},
	} {
		t.Run(fmt.Sprintf("seed-%d", tc.seed), func(t *testing.T) {
			cfg := &config.Config{
				Time:   config.TimeConfig{DurationMs: 30000, WarmupMs: 5000},
				System: config.SystemConfig{Nodes: config.NodesConfig{Count: 3}},
				Faults: config.FaultsConfig{
					Mode:                        "seeded_random",
					StartAfterMs:                5000,
					MinimumSuccessfulInjections: 1,
					Profile: map[string]interface{}{
						"max_faults":   3,
						"kill_node":    map[string]interface{}{"weight": 1},
						"restart_node": map[string]interface{}{"weight": 1},
						"partition":    map[string]interface{}{"weight": 2},
					},
				},
			}
			s := NewScheduler(cfg, "regression", tc.seed, "", nil, nil, nil, nil, nil)
			generated, err := s.generateRandomSchedule()
			if err != nil {
				t.Fatal(err)
			}
			if len(generated) != 3 {
				t.Fatalf("generated %d faults, want all 3 attempts: %+v", len(generated), generated)
			}
			killed := make(map[string]bool)
			for i, f := range generated {
				if f.Type != "kill_node" || f.AtMs != tc.times[i] {
					t.Fatalf("attempt %d = %+v, want kill_node at %dms", i, f, tc.times[i])
				}
				if killed[f.Node] {
					t.Fatalf("attempt %d kills %s again without restart: %+v", i, f.Node, generated)
				}
				killed[f.Node] = true
			}
			again, err := s.generateRandomSchedule()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(generated, again) {
				t.Fatalf("same seed changed schedule: %+v vs %+v", generated, again)
			}
			t.Logf("seed %d repaired schedule: %+v", tc.seed, generated)
		})
	}
}

func TestGeneratedProcessTargetsFollowPlannedState(t *testing.T) {
	parameterFault := func(at int64, kind string, duration int) config.FaultConfig {
		return config.FaultConfig{AtMs: at, Type: kind, Params: map[string]interface{}{
			"node": "node-1", "duration_ms": duration, "stall_ms": 30,
		}}
	}
	for _, tc := range []struct {
		name     string
		nodes    int
		schedule []config.FaultConfig
		want     []string
		wantErr  bool
	}{
		{
			name: "restart recovers a killed node before restarting a live one", nodes: 2,
			schedule: []config.FaultConfig{
				{Type: "kill_node", Node: "node-1"},
				{AtMs: 1, Type: "restart_node", Node: "node-2"},
				{AtMs: 2, Type: "kill_node", Node: "node-1"},
			},
			want: []string{"node-1", "node-1", "node-1"},
		},
		{
			name: "restarting an initially live process remains valid", nodes: 2,
			schedule: []config.FaultConfig{{Type: "restart_node", Node: "node-2"}},
			want:     []string{"node-2"},
		},
		{
			name: "process faults never target a planned killed node", nodes: 2,
			schedule: []config.FaultConfig{
				{Type: "kill_node", Node: "node-1"},
				parameterFault(1, "cpu_pause", 100),
				parameterFault(102, "slow_disk", 100),
				parameterFault(233, "disk_write_loss", 0),
				parameterFault(234, "partial_persistence", 0),
				parameterFault(235, "stale_snapshot_restart", 0),
				{AtMs: 236, Type: "kill_node", Node: "node-1"},
			},
			want: []string{"node-1", "node-2", "node-2", "node-2", "node-2", "node-2", "node-2"},
		},
		{
			name: "do not interrupt asynchronous pause recovery", nodes: 2,
			schedule: []config.FaultConfig{
				parameterFault(0, "cpu_pause", 100),
				{AtMs: 1, Type: "kill_node", Node: "node-1"},
				{AtMs: 2, Type: "restart_node", Node: "node-1"},
				parameterFault(3, "slow_disk", 100),
				{AtMs: 134, Type: "kill_node", Node: "node-1"},
			},
			want: []string{"node-1", "node-2", "node-2", "node-2", "node-1"},
		},
		{
			name: "pause becomes available again after recovery", nodes: 1,
			schedule: []config.FaultConfig{
				parameterFault(0, "cpu_pause", 100),
				{AtMs: 101, Type: "kill_node", Node: "node-1"},
			},
			want: []string{"node-1", "node-1"},
		},
		{
			name: "kill-only exhaustion is an error not an omitted attempt", nodes: 1,
			schedule: []config.FaultConfig{
				{Type: "kill_node", Node: "node-1"},
				{AtMs: 1, Type: "kill_node", Node: "node-1"},
			},
			wantErr: true,
		},
		{
			name: "overlapping pauses cannot share one process", nodes: 1,
			schedule: []config.FaultConfig{parameterFault(0, "cpu_pause", 100), parameterFault(1, "cpu_pause", 100)},
			wantErr:  true,
		},
		{
			name: "last slow-disk stall is reserved too", nodes: 1,
			schedule: []config.FaultConfig{
				parameterFault(0, "slow_disk", 100),
				{AtMs: 101, Type: "restart_node", Node: "node-1"},
			},
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := assignGeneratedProcessTargets(tc.schedule, tc.nodes)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "no eligible process target") {
					t.Fatalf("expected explicit impossible-plan error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for i, f := range tc.schedule {
				target := f.Node
				if target == "" {
					target = f.GetParamString("node", "")
				}
				if target != tc.want[i] {
					t.Fatalf("attempt %d targeted %q, want %q: %+v", i, target, tc.want[i], tc.schedule)
				}
			}
		})
	}
}

func TestSchedulerRejectsImpossibleSeededProfilesBeforeStarting(t *testing.T) {
	for _, tc := range []struct {
		name, kind, wantErr                  string
		nodes, maximum, duration, startAfter int
	}{
		{name: "exhausted live nodes", kind: "kill_node", nodes: 1, maximum: 2, duration: 10000, wantErr: "no eligible process target"},
		{name: "no configured nodes", kind: "kill_node", maximum: 1, duration: 10000, wantErr: "no configured nodes"},
		{name: "one-node partition", kind: "partition", nodes: 1, maximum: 1, duration: 10000, wantErr: "at least two nodes"},
		{name: "one-node asymmetric partition", kind: "asymmetric_partition", nodes: 1, maximum: 1, duration: 10000, wantErr: "at least two nodes"},
		{name: "too many attempts for window", kind: "restart_node", nodes: 1, maximum: 101, duration: 1, wantErr: "distinct attempts"},
		{name: "no window after start delay", kind: "restart_node", nodes: 1, maximum: 1, duration: 10, startAfter: 10, wantErr: "no fault window"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Time:   config.TimeConfig{DurationMs: tc.duration},
				System: config.SystemConfig{Nodes: config.NodesConfig{Count: tc.nodes}},
				Faults: config.FaultsConfig{Mode: "seeded_random", StartAfterMs: tc.startAfter, Profile: map[string]interface{}{
					"max_faults": tc.maximum, tc.kind: 1,
				}},
			}
			dir := t.TempDir()
			events := 0
			s := NewScheduler(cfg, "impossible", 44, dir, nil, nil, nil, func(int64, string, string, string) { events++ }, nil)
			err := s.Start(context.Background(), time.Now())
			defer s.Stop()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) || !strings.Contains(err.Error(), "seed 44") {
				t.Fatalf("Start error = %v, want seed and %q", err, tc.wantErr)
			}
			if events != 0 || s.cancelFunc != nil || len(s.schedule) != 0 {
				t.Fatalf("impossible plan partially started: events=%d schedule=%+v", events, s.schedule)
			}
			if _, err := os.Stat(filepath.Join(dir, "faults.json")); !os.IsNotExist(err) {
				t.Fatalf("impossible plan wrote faults.json: %v", err)
			}
		})
	}
}

func TestSchedulerBoundedGeneration(t *testing.T) {
	// A finite sweep includes the former one-node rejection loops, exhausted
	// process targets, nested YAML/JSON weights, and narrow scheduling windows.
	for nodes := 1; nodes <= 4; nodes++ {
		for _, kind := range []string{"kill_node", "restart_node", "cpu_pause", "slow_disk", "partition", "asymmetric_partition", "duplicate_messages", "corrupt_messages", "mixed"} {
			t.Run(fmt.Sprintf("%d-nodes/%s", nodes, kind), func(t *testing.T) {
				successes, rejections := 0, 0
				for seed := int64(0); seed < 128; seed++ {
					maximum := nodes
					profile := map[string]interface{}{"max_faults": maximum, kind: 1}
					if kind == "mixed" {
						maximum = 12
						profile = map[string]interface{}{
							"max_faults":   float64(maximum),
							"kill_node":    map[string]interface{}{"weight": 3.0},
							"restart_node": map[interface{}]interface{}{"weight": 3},
							"cpu_pause":    1, "slow_disk": 1, "disk_write_loss": 1,
							"partial_persistence": 1, "stale_snapshot_restart": 1,
							"partition": 1, "asymmetric_partition": 1,
							"duplicate_messages": 1, "corrupt_messages": 1,
						}
					}
					cfg := &config.Config{
						Time:   config.TimeConfig{DurationMs: 30000},
						System: config.SystemConfig{Nodes: config.NodesConfig{Count: nodes}},
						Faults: config.FaultsConfig{StartAfterMs: 5000, Profile: profile},
					}
					s := NewScheduler(cfg, "bounded", seed, "", nil, nil, nil, nil, nil)
					generated, err := s.generateRandomSchedule()
					again, againErr := s.generateRandomSchedule()
					if fmt.Sprint(err) != fmt.Sprint(againErr) || !reflect.DeepEqual(generated, again) {
						t.Fatalf("seed %d is not deterministic: %+v/%v vs %+v/%v", seed, generated, err, again, againErr)
					}
					if err != nil {
						rejections++
						if generated != nil {
							t.Fatalf("seed %d returned partial schedule with error: %+v / %v", seed, generated, err)
						}
						continue
					}
					successes++
					if len(generated) != maximum && !(len(generated) == maximum+1 && generated[maximum].Type == "heal") {
						t.Fatalf("seed %d dropped/replaced required attempts: %+v", seed, generated)
					}
					if kind != "mixed" {
						for _, f := range generated[:maximum] {
							if f.Type != kind {
								t.Fatalf("seed %d substituted %s for required %s", seed, f.Type, kind)
							}
						}
					}
					assertValidGeneratedPlan(t, generated, nodes, 25000)
				}
				if nodes == 1 && (kind == "partition" || kind == "asymmetric_partition") {
					if rejections != 128 {
						t.Fatalf("impossible profile accepted %d seeds", successes)
					}
				} else if kind != "mixed" && successes != 128 {
					t.Fatalf("valid profile rejected %d seeds", rejections)
				} else if kind == "mixed" && nodes > 1 && successes == 0 {
					t.Fatal("all mixed schedules were rejected")
				}
				t.Logf("128 seeds: %d complete plans, %d explicit rejections", successes, rejections)
			})
		}
	}
	for duration := 1; duration <= 10; duration++ {
		for maximum := 1; maximum <= 12; maximum++ {
			cfg := &config.Config{
				Time:   config.TimeConfig{DurationMs: duration},
				System: config.SystemConfig{Nodes: config.NodesConfig{Count: 1}},
				Faults: config.FaultsConfig{Profile: map[string]interface{}{"max_faults": maximum, "restart_node": 1}},
			}
			s := NewScheduler(cfg, "short", 44, "", nil, nil, nil, nil, nil)
			generated, err := s.generateRandomSchedule()
			if maximum > duration {
				if err == nil || generated != nil {
					t.Fatalf("%d attempts in %dms were not explicitly rejected", maximum, duration)
				}
			} else {
				if err != nil || len(generated) != maximum {
					t.Fatalf("%d attempts in %dms: %+v / %v", maximum, duration, generated, err)
				}
				assertValidGeneratedPlan(t, generated, 1, int64(duration))
			}
		}
	}
}

func assertValidGeneratedPlan(t *testing.T, schedule []config.FaultConfig, nodes int, window int64) {
	t.Helper()
	live := make(map[string]bool)
	reserved := make(map[string]int64)
	for n := 1; n <= nodes; n++ {
		live[fmt.Sprintf("node-%d", n)] = true
	}
	for i, f := range schedule {
		if f.AtMs < 0 || f.AtMs >= window || (i > 0 && f.AtMs <= schedule[i-1].AtMs) {
			t.Fatalf("schedule is not strictly ordered within %dms: %+v", window, schedule)
		}
		fault, ok := DefaultRegistry().Get(f.Type)
		if !ok || fault.Validate(&f) != nil {
			t.Fatalf("invalid generated fault: %+v", f)
		}
		target := f.Node
		if target == "" {
			target = f.GetParamString("node", "")
		}
		switch f.Type {
		case "kill_node", "restart_node", "cpu_pause", "slow_disk", "disk_write_loss", "partial_persistence", "stale_snapshot_restart":
			running, exists := live[target]
			if !exists || (f.Type != "restart_node" && !running) || reserved[target] > f.AtMs {
				t.Fatalf("invalid process target at attempt %d: %+v; live=%v reserved=%v", i, f, live, reserved)
			}
		}
		switch f.Type {
		case "kill_node":
			live[target] = false
		case "restart_node":
			live[target] = true
		case "cpu_pause":
			reserved[target] = f.AtMs + int64(f.GetParamInt("duration_ms", 0))
		case "slow_disk":
			reserved[target] = f.AtMs + int64(f.GetParamInt("duration_ms", 0)+f.GetParamInt("stall_ms", 0))
		case "duplicate_messages", "corrupt_messages", "asymmetric_partition":
			from, to := f.GetParamString("from", ""), f.GetParamString("to", "")
			if from == to || (from != "client" && !nodeExists(from, nodes)) || !nodeExists(to, nodes) {
				t.Fatalf("invalid directed pair: %+v", f)
			}
		}
	}
}

func nodeExists(id string, count int) bool {
	for n := 1; n <= count; n++ {
		if id == fmt.Sprintf("node-%d", n) {
			return true
		}
	}
	return false
}

func TestSchedulerPreservesScriptedAndReplayPlans(t *testing.T) {
	// Deliberately invalid duplicates must stay invalid, not be retargeted,
	// retried, or removed by the seeded-only planner.
	original := []config.FaultConfig{
		{AtMs: 1000000, Type: "kill_node", Node: "node-1"},
		{AtMs: 1000001, Type: "kill_node", Node: "node-1"},
	}
	for _, mode := range []string{"scripted", "replay"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			data, err := json.Marshal(original)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "faults.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{
				Time:   config.TimeConfig{WarmupMs: 7},
				System: config.SystemConfig{Nodes: config.NodesConfig{Count: 1}},
				Faults: config.FaultsConfig{Mode: mode, StartAfterMs: 13, Schedule: original},
			}
			s := NewScheduler(cfg, "explicit", 44, dir, nil, nil, nil, nil, nil)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := s.Start(ctx, time.Now()); err != nil {
				t.Fatal(err)
			}
			s.Stop()
			for i, f := range s.schedule {
				if f.Type != original[i].Type || f.Node != original[i].Node || f.AtMs != original[i].AtMs+20 {
					t.Fatalf("explicit schedule rewritten: %+v", s.schedule)
				}
			}
			if len(s.schedule) != len(original) || cfg.Faults.Schedule[0].AtMs != 1000000 {
				t.Fatalf("explicit schedule dropped attempts or mutated config: %+v", s.schedule)
			}
			persisted, err := os.ReadFile(filepath.Join(dir, "faults.json"))
			if err != nil || string(persisted) != string(data) {
				t.Fatalf("explicit persisted schedule changed: %s / %v", persisted, err)
			}
		})
	}
}

func TestSchedulerSeededTargetsPersistAndReplayWithoutReplanning(t *testing.T) {
	dir := t.TempDir()
	cfg := &config.Config{
		Time:   config.TimeConfig{DurationMs: 30000, WarmupMs: 5000},
		System: config.SystemConfig{Nodes: config.NodesConfig{Count: 3}},
		Faults: config.FaultsConfig{Mode: "seeded_random", StartAfterMs: 5000, Profile: map[string]interface{}{
			"max_faults": 3, "kill_node": 1, "restart_node": 1, "partition": 2,
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	seeded := NewScheduler(cfg, "persisted", 44, dir, nil, nil, nil, nil, nil)
	if err := seeded.Start(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	seeded.Stop()
	data, err := os.ReadFile(filepath.Join(dir, "faults.json"))
	if err != nil {
		t.Fatal(err)
	}
	var relative []config.FaultConfig
	if err := json.Unmarshal(data, &relative); err != nil {
		t.Fatal(err)
	}
	want := []config.FaultConfig{
		{AtMs: 5347, Type: "kill_node", Node: "node-3"},
		{AtMs: 9915, Type: "kill_node", Node: "node-1"},
		{AtMs: 22337, Type: "kill_node", Node: "node-2"},
	}
	if !reflect.DeepEqual(relative, want) {
		t.Fatalf("persisted targets/times = %+v, want %+v", relative, want)
	}
	cfg.Faults.Mode = "replay"
	// The saved plan, not a new seed or an incompatible new profile, is the
	// source of truth in replay mode.
	cfg.Faults.Profile = map[string]interface{}{"max_faults": 1000, "kill_node": 1}
	replay := NewScheduler(cfg, "replay", 99, dir, nil, nil, nil, nil, nil)
	if err := replay.Start(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	replay.Stop()
	if !reflect.DeepEqual(seeded.schedule, replay.schedule) {
		t.Fatalf("replay changed the plan: %+v vs %+v", seeded.schedule, replay.schedule)
	}
	for i := range relative {
		if seeded.schedule[i].AtMs != relative[i].AtMs+10000 {
			t.Fatalf("warmup/start-after offsets not applied exactly once: %+v", seeded.schedule)
		}
	}
}

func TestSchedulerSeed44And45ConfirmEveryKillOnOwnedProcesses(t *testing.T) {
	for _, seed := range []int64{44, 45} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			dir := t.TempDir()
			cfg := &config.Config{
				Time:   config.TimeConfig{DurationMs: 30000},
				System: config.SystemConfig{Nodes: config.NodesConfig{Count: 3, Command: "exec sleep 10", DataDir: filepath.Join(dir, "{node_id}")}},
				Faults: config.FaultsConfig{StartAfterMs: 5000, Profile: map[string]interface{}{
					"max_faults": 3, "kill_node": 1, "restart_node": 1, "partition": 2,
				}},
			}
			st, err := store.NewStore(filepath.Join(dir, "history.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			nm := node.NewNodeManager(cfg, "owned", dir, func(int64, string, string, map[string]interface{}) {})
			if err := nm.StartAll(ctx); err != nil {
				t.Fatal(err)
			}
			defer nm.StopAll()
			counts := make(map[string]int)
			s := NewScheduler(cfg, "owned", seed, dir, nm, nil, st, func(_ int64, _, kind, _ string) { counts[kind]++ }, nil)
			s.startTime = time.Now()
			generated, err := s.generateRandomSchedule()
			if err != nil {
				t.Fatal(err)
			}
			if len(generated) != 3 {
				t.Fatalf("expected all three kills, got %+v", generated)
			}
			// Execute the selected targets immediately; this is a tiny owned
			// process fixture, not a workload or hardening campaign.
			for _, f := range generated {
				if f.Type != "kill_node" {
					t.Fatalf("regression seed changed selected fault type: %+v", f)
				}
				s.injectFault(ctx, f)
			}
			if counts["FaultAttempted"] != 3 || counts["FaultInjected"] != 3 || counts["FaultInjectionFailed"] != 0 {
				t.Fatalf("fault outcomes = %v, want three attempted and confirmed kills with no failure", counts)
			}
			for id, state := range nm.GetNodesStatus() {
				if state != node.StateKilled {
					t.Fatalf("%s ended in %s, want KILLED", id, state)
				}
			}
			t.Logf("seed %d: attempted=%d injected=%d failed=%d", seed, counts["FaultAttempted"], counts["FaultInjected"], counts["FaultInjectionFailed"])
		})
	}
}
