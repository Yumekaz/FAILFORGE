package hardening

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/model"
	"failforge/internal/store"
)

func testOptions(t *testing.T) Options {
	t.Helper()
	root := t.TempDir()
	profile := `name: fixture
system:
  nodes:
    data_dir: .failforge/data/{run_id}/{node_id}
workload:
  minimum_successful_operations: 1
faults:
  minimum_successful_injections: 1
checkers:
  - name: read_after_acknowledged_write
`
	if err := os.WriteFile(filepath.Join(root, "failforge_minidb.yml"), []byte(profile), 0644); err != nil {
		t.Fatal(err)
	}
	return Options{RepoRoot: root, OutputDir: filepath.Join(root, "validation-runs", "test"), Profiles: []string{"minidb"}, SeedStart: 42, SeedEnd: 43, Repetitions: 2, MaxRuns: 10, MaxRuntime: time.Minute, RunTimeout: time.Second, MaxBytes: 128 << 20, MaxRunBytes: 32 << 20, MaxFileBytes: 1 << 20, MiniDBRoot: root, CoordRoot: root}
}

func TestMatrixBoundsAndOverflow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Options)
	}{
		{"reversed", func(o *Options) { o.SeedEnd = 41 }},
		{"overflow", func(o *Options) { o.SeedStart = math.MinInt64; o.SeedEnd = math.MaxInt64 }},
		{"too many", func(o *Options) { o.MaxRuns = 3 }},
		{"repetitions", func(o *Options) { o.Repetitions = 0 }},
		{"runtime", func(o *Options) { o.MaxRuntime = 0 }},
		{"storage", func(o *Options) { o.MaxRunBytes = o.MaxBytes + 1 }},
		{"file", func(o *Options) { o.MaxFileBytes = 1 }},
		{"unknown", func(o *Options) { o.Profiles = []string{"other"} }},
		{"duplicate", func(o *Options) { o.Profiles = []string{"minidb", "minidb"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := testOptions(t)
			tc.change(&o)
			if err := o.Validate(); err == nil {
				t.Fatal("invalid matrix accepted")
			}
		})
	}
	o := testOptions(t)
	o.SeedStart = math.MaxInt64
	o.SeedEnd = math.MaxInt64
	if err := o.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestMatrixIndependentRepetitionsAndImmutableEvidence(t *testing.T) {
	o := testOptions(t)
	original := executeAttempt
	t.Cleanup(func() { executeAttempt = original })
	var seen []string
	executeAttempt = func(_ context.Context, _ Options, _ string, _ profile, a *Attempt) error {
		seen = append(seen, filepath.Base(a.Dir))
		a.Status = "PASSED"
		code := 0
		a.ExitCode = &code
		return nil
	}
	s, err := Run(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 4 || s.Status != "PASSED" {
		t.Fatalf("matrix=%+v seen=%v", s, seen)
	}
	for i, a := range s.Attempts {
		if a.Seed != int64(42+i%2) || a.Repetition != 1+i/2 {
			t.Fatalf("wrong matrix order: %+v", a)
		}
		info, err := os.Stat(filepath.Join(a.Dir, "attempt.json"))
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0222 != 0 {
			t.Fatal("completed evidence is writable")
		}
		var inventory map[string]string
		data, err := os.ReadFile(filepath.Join(a.Dir, "inventory.json"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &inventory); err != nil {
			t.Fatal(err)
		}
		hash, err := fileHash(filepath.Join(a.Dir, "attempt.json"))
		if err != nil {
			t.Fatal(err)
		}
		if inventory["attempt.json"] != hash {
			t.Fatal("inventory did not hash final attempt")
		}
	}
	if _, err := Run(context.Background(), o); err == nil {
		t.Fatal("existing campaign was reused")
	}
	if len(seen) != 4 {
		t.Fatal("overwrite rejection retried a campaign")
	}
	// Restore directory write permission only to allow testing.T's cleanup.
	t.Cleanup(func() {
		for _, a := range s.Attempts {
			_ = os.Chmod(a.Dir, 0755)
		}
	})
}

func TestMatrixStopsWithoutRetryAndKeepGoingIsExplicit(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(strconv.FormatBool(keep), func(t *testing.T) {
			o := testOptions(t)
			o.KeepGoing = keep
			original := executeAttempt
			t.Cleanup(func() { executeAttempt = original })
			calls := 0
			executeAttempt = func(_ context.Context, _ Options, _ string, _ profile, a *Attempt) error {
				calls++
				a.Status = "FAILED"
				return errors.New("preserved failure")
			}
			s, err := Run(context.Background(), o)
			if err == nil || s.Status == "PASSED" {
				t.Fatal("failed campaign passed")
			}
			want := 1
			if keep {
				want = 4
			}
			if calls != want {
				t.Fatalf("calls=%d want=%d", calls, want)
			}
			if !keep && s.Attempts[1].Status != "NOT_RUN" {
				t.Fatal("unstarted seed was hidden")
			}
			if s.Attempts[0].Error != "preserved failure" {
				t.Fatal("failure error lost")
			}
			t.Cleanup(func() {
				for _, a := range s.Attempts {
					_ = os.Chmod(a.Dir, 0755)
				}
			})
		})
	}
}

func TestCancelledCampaignHasNoPassedAttempts(t *testing.T) {
	o := testOptions(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, err := Run(ctx, o)
	if err == nil || s.Status != "INTERRUPTED" {
		t.Fatalf("status=%s err=%v", s.Status, err)
	}
	for _, a := range s.Attempts {
		if a.Status != "NOT_RUN" || a.ExitCode != nil {
			t.Fatalf("unexpected attempted run: %+v", a)
		}
	}
}

func TestCoverageNeverPassesMissingOrFailedEvidence(t *testing.T) {
	valid := Coverage{RunStatus: "PASSED", Ended: true, FaultEvents: map[string]int{}, Violations: map[string]int{}}
	for _, tc := range []struct {
		name   string
		mutate func(*Coverage)
		exit   int
		err    error
		want   string
	}{
		{name: "covered", want: "PASSED"},
		{name: "missing database", err: errors.New("absent"), want: "CRASHED"},
		{name: "nonzero exit", exit: 7, want: "CRASHED"},
		{name: "unknown", mutate: func(c *Coverage) { c.RunStatus = "UNKNOWN" }, want: "CRASHED"},
		{name: "interrupted", mutate: func(c *Coverage) { c.Ended = false }, want: "INTERRUPTED"},
		{name: "no coverage", mutate: func(c *Coverage) { c.Missing = []string{"no confirmed faults"} }, want: "NO_COVERAGE"},
		{name: "recovery", mutate: func(c *Coverage) { c.FaultEvents = map[string]int{"FaultInjected": 1, "FaultRecoveryFailed": 1} }, want: "FAILED"},
		{name: "invariant", mutate: func(c *Coverage) { c.Violations = map[string]int{"lock_exclusivity": 1} }, want: "FAILED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			if tc.mutate != nil {
				tc.mutate(&c)
			}
			if got := classify(tc.exit, c, tc.err); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func TestCoverageReadsConfirmedTypesAndPreservesRequirements(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewStore(filepath.Join(dir, "history.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	rec := &model.Run{ID: "run", Seed: 42, Status: "PASSED", StartedAt: now, EndedAt: &now}
	if err := st.CreateRun(rec); err != nil {
		t.Fatal(err)
	}
	for i, kind := range []string{"put", "get"} {
		if err := st.CreateOperation(&model.Operation{RunID: "run", OpID: strconv.Itoa(i), Operation: kind, Status: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"FaultAttempted", "FaultInjected", "FaultRecoveryFailed"} {
		if err := st.CreateEvent(&model.Event{RunID: "run", Category: "Fault", Type: kind, PayloadJSON: `{"type":"cpu_pause"}`}); err != nil {
			t.Fatal(err)
		}
	}
	st.Close()
	cfg := &config.Config{Workload: config.WorkloadConfig{MinimumSuccessfulOperations: 3, MinimumSuccessfulByOperation: map[string]int{"put": 2}}, Faults: config.FaultsConfig{MinimumSuccessfulInjections: 2}}
	c, err := readCoverage(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if c.ConfirmedByType["cpu_pause"] != 1 || len(c.Missing) != 3 || c.SuccessfulOperations != 2 {
		t.Fatalf("bad coverage: %+v", c)
	}
}

func TestBusyPortsNeverPermitLaunch(t *testing.T) {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := checkPorts([]int{l.Addr().(*net.TCPAddr).Port}); err == nil {
		t.Fatal("busy port accepted")
	}
}

func TestSupervisorRetainsExitAndTimeoutKillsSeparateGroups(t *testing.T) {
	dir := t.TempDir()
	code, reason, err := execute(context.Background(), exec.Command("sh", "-c", "exit 7"), dir, dir, time.Second, 1<<20, 1<<20)
	if code != 7 || reason != "" || err == nil {
		t.Fatalf("exit=%d reason=%s err=%v", code, reason, err)
	}
	pidPath := filepath.Join(dir, "child.pid")
	cmd := exec.Command("sh", "-c", `trap '' TERM; setsid sh -c 'trap "" TERM; sleep 30' & echo $! > "$1"; wait`, "fixture", pidPath)
	start := time.Now()
	code, reason, _ = execute(context.Background(), cmd, dir, dir, 200*time.Millisecond, 1<<20, 1<<20)
	if reason != "TIMED_OUT" || time.Since(start) > 5*time.Second {
		t.Fatalf("timeout: code=%d reason=%s elapsed=%s", code, reason, time.Since(start))
	}
	data, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
		if os.IsNotExist(err) || strings.Contains(string(b), ") Z ") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	t.Fatal("separate target process group survived timeout cleanup")
}

func TestSupervisorStorageLimitDoesNotDeleteEvidence(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command("sh", "-c", `head -c 2097152 /dev/zero > "$1"; sleep 30`, "fixture", filepath.Join(dir, "large.log"))
	_, reason, _ := execute(context.Background(), cmd, dir, dir, time.Second, 1<<20, 1<<20)
	if reason != "STORAGE_LIMIT" {
		t.Fatalf("got %s", reason)
	}
	info, err := os.Stat(filepath.Join(dir, "large.log"))
	if err != nil || info.Size() != 2097152 {
		t.Fatal("storage stop removed or truncated failure evidence")
	}
}
