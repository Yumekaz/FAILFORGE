// Package hardening runs an append-only, bounded first-party profile matrix.
package hardening

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"failforge/internal/config"

	"gopkg.in/yaml.v3"
)

type Options struct {
	RepoRoot     string
	OutputDir    string
	Profiles     []string
	SeedStart    int64
	SeedEnd      int64
	Repetitions  int
	MaxRuns      int
	MaxRuntime   time.Duration
	RunTimeout   time.Duration
	MaxBytes     int64
	MaxRunBytes  int64
	MaxFileBytes uint64
	Duration     time.Duration // optional workload duration override; checkers/minima unchanged
	KeepGoing    bool
	MiniDBRoot   string
	CoordRoot    string
}

type Attempt struct {
	Profile          string              `json:"profile"`
	Seed             int64               `json:"seed"`
	Repetition       int                 `json:"repetition"`
	Dir              string              `json:"dir"`
	Status           string              `json:"status"`
	ExitCode         *int                `json:"exit_code"`
	Command          []string            `json:"command"`
	WorkingDirectory string              `json:"working_directory"`
	Environment      map[string]string   `json:"environment_overrides"`
	StartedAt        time.Time           `json:"started_at"`
	Elapsed          time.Duration       `json:"elapsed_ns"`
	Error            string              `json:"error,omitempty"`
	Before           map[string]Revision `json:"source_before"`
	After            map[string]Revision `json:"source_after"`
	Coverage         Coverage            `json:"coverage"`
}

type Summary struct {
	Schema              int        `json:"schema"`
	Options             Options    `json:"options"`
	Invocation          []string   `json:"invocation"`
	StartedAt           time.Time  `json:"started_at"`
	EndedAt             *time.Time `json:"ended_at"`
	Status              string     `json:"status"`
	StopReason          string     `json:"stop_reason,omitempty"`
	BinarySHA256        string     `json:"binary_sha256"`
	BuildInfo           string     `json:"build_info"`
	Platform            string     `json:"platform"`
	ExcludedSourcePaths []string   `json:"excluded_source_paths"`
	Attempts            []Attempt  `json:"attempts"`
}

type profile struct {
	cfg                          *config.Config
	path, targetName, targetRoot string
	ports                        []int
}

var executeAttempt = runAttempt

func (o Options) Validate() error {
	if o.SeedStart > o.SeedEnd {
		return fmt.Errorf("seed start exceeds end")
	}
	if o.Repetitions < 1 || o.MaxRuns < 1 || o.MaxRuns > 10000 || len(o.Profiles) == 0 {
		return fmt.Errorf("require positive repetitions, 1..10000 max-runs, and profiles")
	}
	// Unsigned subtraction avoids signed overflow for extreme ranges.
	span := uint64(o.SeedEnd) - uint64(o.SeedStart)
	if span >= uint64(o.MaxRuns) || uint64(len(o.Profiles)) > uint64(o.MaxRuns)/uint64(o.Repetitions) || span+1 > uint64(o.MaxRuns)/uint64(o.Repetitions)/uint64(len(o.Profiles)) {
		return fmt.Errorf("matrix exceeds max-runs=%d", o.MaxRuns)
	}
	if o.MaxRuntime <= 0 || o.RunTimeout <= 0 || o.MaxRuntime > 24*time.Hour || o.RunTimeout > time.Hour {
		return fmt.Errorf("require max-runtime in (0,24h], run-timeout in (0,1h]")
	}
	if o.MaxBytes < 1<<20 || o.MaxRunBytes < 1<<20 || o.MaxRunBytes > o.MaxBytes || o.MaxFileBytes < 65536 || o.MaxFileBytes > uint64(o.MaxRunBytes) {
		return fmt.Errorf("require storage >=1MiB and 64KiB <= file limit <= run limit <= total limit")
	}
	if o.Duration < 0 || o.Duration > time.Hour {
		return fmt.Errorf("duration must be 0..1h")
	}
	seen := map[string]bool{}
	for _, name := range o.Profiles {
		if seen[name] {
			return fmt.Errorf("duplicate profile %q", name)
		}
		seen[name] = true
		if name != "minidb" && name != "coordination" && name != "coordination-baseline" {
			return fmt.Errorf("unsupported first-party profile %q", name)
		}
	}
	return nil
}

func writeJSON(path string, value interface{}) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func checkpoint(root string, s *Summary) error {
	path := filepath.Join(root, "summary.next.json")
	if err := writeJSON(path, s); err != nil {
		return err
	}
	return os.Rename(path, filepath.Join(root, "summary.json"))
}

func loadProfiles(o Options) (map[string]profile, error) {
	profiles := map[string]profile{}
	for _, name := range o.Profiles {
		file := "failforge_" + strings.ReplaceAll(name, "-", "_") + ".yml"
		path := filepath.Join(o.RepoRoot, file)
		cfg, err := config.LoadConfig(path)
		if err != nil {
			return nil, err
		}
		if o.Duration > 0 {
			if o.Duration < time.Millisecond {
				return nil, fmt.Errorf("duration must be at least 1ms")
			}
			cfg.Time.DurationMs = int(o.Duration.Milliseconds())
			cfg.Workload.DurationMs = cfg.Time.DurationMs
		}
		dataDir := filepath.Clean(cfg.System.Nodes.DataDir)
		if filepath.IsAbs(dataDir) || dataDir == ".." || strings.HasPrefix(dataDir, "../") || !strings.Contains(dataDir, "{run_id}") {
			return nil, fmt.Errorf("profile %s must use a relative, run-private data directory", name)
		}
		if cfg.Workload.MinimumSuccessfulOperations < 1 || len(cfg.Checkers) == 0 {
			return nil, fmt.Errorf("profile %s lacks meaningful workload checks", name)
		}
		if name != "coordination-baseline" && cfg.Faults.MinimumSuccessfulInjections < 1 {
			return nil, fmt.Errorf("profile %s lacks confirmed-fault coverage", name)
		}
		p := profile{cfg: cfg, path: path, targetName: "Coordination", targetRoot: o.CoordRoot, ports: []int{cfg.Network.ProxyPort}}
		if name == "minidb" {
			p.targetName = "MiniDB"
			p.targetRoot = o.MiniDBRoot
		}
		for i := 0; i < cfg.System.Nodes.Count; i++ {
			port := cfg.System.Nodes.Ports.Start + i
			p.ports = append(p.ports, port)
			if name == "minidb" {
				p.ports = append(p.ports, port+1000)
			}
		}
		profiles[name] = p
	}
	return profiles, nil
}

func Run(ctx context.Context, o Options) (result *Summary, resultErr error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("hardening supervisor requires Linux /proc and process groups")
	}
	if err := o.Validate(); err != nil {
		return nil, err
	}
	var err error
	for _, p := range []*string{&o.RepoRoot, &o.OutputDir, &o.MiniDBRoot, &o.CoordRoot} {
		*p, err = filepath.Abs(*p)
		if err != nil {
			return nil, err
		}
	}
	// Evidence must be durable and remain inside this tool checkout.
	rel, err := filepath.Rel(filepath.Join(o.RepoRoot, "validation-runs"), o.OutputDir)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil, fmt.Errorf("output must be a new subdirectory of %s/validation-runs", o.RepoRoot)
	}
	profiles, err := loadProfiles(o)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(o.OutputDir), 0755); err != nil {
		return nil, err
	}
	if err := os.Mkdir(o.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("output is single-use; refusing retry/overwrite: %w", err)
	}
	s := &Summary{Schema: 1, Options: o, Invocation: os.Args, StartedAt: time.Now().UTC(), Status: "RUNNING", Platform: runtime.GOOS + "/" + runtime.GOARCH, ExcludedSourcePaths: []string{"validation-runs/**", "verification/**", "git-ignored files"}, Attempts: []Attempt{}}
	defer func() {
		if resultErr != nil && s.EndedAt == nil {
			s.Status = "CRASHED"
			s.StopReason = resultErr.Error()
			ended := time.Now().UTC()
			s.EndedAt = &ended
			if err := checkpoint(o.OutputDir, s); err != nil {
				resultErr = fmt.Errorf("%w; final checkpoint failed: %v", resultErr, err)
			}
		}
	}()
	for rep := 1; rep <= o.Repetitions; rep++ {
		for _, name := range o.Profiles {
			for seed := o.SeedStart; ; seed++ {
				dir := filepath.Join(o.OutputDir, fmt.Sprintf("rep-%03d", rep), fmt.Sprintf("%s-seed-%d", name, seed))
				s.Attempts = append(s.Attempts, Attempt{Profile: name, Seed: seed, Repetition: rep, Dir: dir, Status: "NOT_RUN"})
				if seed == o.SeedEnd {
					break
				}
			}
		}
	}
	if err := checkpoint(o.OutputDir, s); err != nil {
		return s, err
	}
	executable, err := os.Executable()
	if err != nil {
		return s, err
	}
	if info, err := buildinfo.ReadFile(executable); err == nil {
		s.BuildInfo = info.String()
	}
	binary := filepath.Join(o.OutputDir, "failforge")
	in, err := os.Open(executable)
	if err != nil {
		return s, err
	}
	out, err := os.OpenFile(binary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0555)
	if err != nil {
		in.Close()
		return s, err
	}
	n, copyErr := io.Copy(out, io.LimitReader(in, o.MaxBytes+1))
	in.Close()
	closeErr := out.Close()
	if copyErr != nil {
		return s, copyErr
	}
	if closeErr != nil {
		return s, closeErr
	}
	if n > o.MaxBytes {
		return s, fmt.Errorf("binary exceeds campaign storage budget")
	}
	s.BinarySHA256, err = fileHash(binary)
	if err != nil {
		return s, err
	}
	ctx, cancel := context.WithTimeout(ctx, o.MaxRuntime)
	defer cancel()
	allPassed := true
	for i := range s.Attempts {
		if ctx.Err() != nil {
			allPassed = false
			s.StopReason = ctx.Err().Error()
			break
		}
		if n, err := treeBytes(o.OutputDir); err != nil || n >= o.MaxBytes {
			allPassed = false
			s.StopReason = "campaign storage threshold reached"
			break
		}
		a := &s.Attempts[i]
		p := profiles[a.Profile]
		if err := os.MkdirAll(filepath.Dir(a.Dir), 0755); err != nil {
			return s, err
		}
		if err := os.Mkdir(a.Dir, 0755); err != nil {
			return s, err
		}
		a.StartedAt = time.Now().UTC()
		a.Status = "RUNNING"
		if err := checkpoint(o.OutputDir, s); err != nil {
			return s, err
		}
		err := executeAttempt(ctx, o, binary, p, a)
		a.Elapsed = time.Since(a.StartedAt)
		if err != nil {
			a.Error = err.Error()
			if a.Status == "RUNNING" {
				a.Status = "CRASHED"
			}
		}
		if err := writeJSON(filepath.Join(a.Dir, "attempt.json"), a); err != nil {
			return s, err
		}
		if err := seal(a.Dir); err != nil {
			return s, err
		}
		fmt.Printf("%s repetition=%d seed=%d: %s (%s)\n", a.Profile, a.Repetition, a.Seed, a.Status, a.Dir)
		if err := checkpoint(o.OutputDir, s); err != nil {
			return s, err
		}
		if a.Status != "PASSED" {
			allPassed = false
			if !o.KeepGoing {
				s.StopReason = "stopped after " + a.Status + "; no retry"
				break
			}
		}
	}
	s.Status = "FAILED"
	if allPassed {
		s.Status = "PASSED"
	} else if ctx.Err() != nil {
		s.Status = "INTERRUPTED"
		if ctx.Err() == context.DeadlineExceeded {
			s.Status = "TIMED_OUT"
		}
	} else if s.StopReason == "campaign storage threshold reached" {
		s.Status = "STORAGE_LIMIT"
	}
	ended := time.Now().UTC()
	s.EndedAt = &ended
	if err := checkpoint(o.OutputDir, s); err != nil {
		return s, err
	}
	if !allPassed {
		return s, fmt.Errorf("hardening campaign %s; failures and unstarted attempts retained in %s", s.Status, o.OutputDir)
	}
	return s, nil
}

func runAttempt(ctx context.Context, o Options, binary string, p profile, a *Attempt) error {
	a.Before = map[string]Revision{}
	a.After = map[string]Revision{}
	roots := map[string]string{"FailForge": o.RepoRoot, p.targetName: p.targetRoot}
	for name, root := range roots {
		r, err := revision(root, filepath.Join(a.Dir, "provenance", name))
		if err != nil {
			return err
		}
		a.Before[name] = r
	}
	data, err := yaml.Marshal(p.cfg)
	if err != nil {
		return err
	}
	configPath := filepath.Join(a.Dir, "profile.yml")
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return err
	}
	original, err := os.ReadFile(p.path)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(a.Dir, "original-profile.yml"), original, 0644); err != nil {
		return err
	}
	a.Command = []string{binary, "run", configPath, "--seed", fmt.Sprint(a.Seed), "--output-dir", filepath.Join(a.Dir, "artifacts"), "--max-file-bytes", fmt.Sprint(o.MaxFileBytes)}
	a.WorkingDirectory = a.Dir
	a.Environment = map[string]string{"MINIDB_ROOT": o.MiniDBRoot, "COORD_ROOT": o.CoordRoot, "PYTHONDONTWRITEBYTECODE": "1"}
	if err := writeJSON(filepath.Join(a.Dir, "command.json"), map[string]interface{}{"argv": a.Command, "cwd": a.Dir, "env_overrides": a.Environment, "ports": p.ports}); err != nil {
		return err
	}
	if err := checkPorts(p.ports); err != nil {
		a.Status = "PORTS_BUSY"
		return err
	}
	log, err := os.OpenFile(filepath.Join(a.Dir, "console.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	cmd := exec.Command(a.Command[0], a.Command[1:]...)
	cmd.Dir = a.Dir
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Env = os.Environ()
	for key, value := range a.Environment {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	code, reason, runErr := execute(ctx, cmd, o.OutputDir, a.Dir, o.RunTimeout, o.MaxBytes, o.MaxRunBytes)
	closeErr := log.Close()
	a.ExitCode = &code
	for name, root := range roots {
		r, err := revision(root, "")
		if err != nil {
			a.Status = "PROVENANCE_CHANGED"
			return err
		}
		a.After[name] = r
		if r.Fingerprint != a.Before[name].Fingerprint {
			a.Status = "PROVENANCE_CHANGED"
		}
	}
	c, evidenceErr := readCoverage(filepath.Join(a.Dir, "artifacts"), p.cfg)
	a.Coverage = c
	if a.Status != "PROVENANCE_CHANGED" {
		a.Status = classify(code, c, evidenceErr)
	}
	if reason != "" {
		a.Status = reason
	}
	if runErr != nil {
		return runErr
	}
	if closeErr != nil {
		return closeErr
	}
	return evidenceErr
}
