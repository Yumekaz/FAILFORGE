package cli

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"failforge/internal/hardening"

	"github.com/spf13/cobra"
)

func init() {
	var o hardening.Options
	var seeds string
	cmd := &cobra.Command{Use: "hardening", Short: "Run a bounded, append-only first-party hardening matrix", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
		var err error
		o.SeedStart, o.SeedEnd, err = parseSeeds(seeds)
		if err != nil {
			return err
		}
		if o.RepoRoot == "" {
			o.RepoRoot, err = os.Getwd()
			if err != nil {
				return err
			}
		}
		if o.MiniDBRoot == "" {
			o.MiniDBRoot = os.Getenv("MINIDB_ROOT")
			if o.MiniDBRoot == "" {
				o.MiniDBRoot = filepath.Join(o.RepoRoot, "..", "Mini-Redis-Cassandra")
			}
		}
		if o.CoordRoot == "" {
			o.CoordRoot = os.Getenv("COORD_ROOT")
			if o.CoordRoot == "" {
				o.CoordRoot = filepath.Join(o.RepoRoot, "..", "Coordination-service")
			}
		}
		if o.OutputDir == "" {
			return fmt.Errorf("--output-dir is required")
		}
		ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		_, err = hardening.Run(ctx, o)
		return err
	}}
	f := cmd.Flags()
	f.StringVar(&o.RepoRoot, "repo-root", "", "FailForge checkout root (default current directory)")
	f.StringVar(&o.OutputDir, "output-dir", "", "NEW directory under <repo-root>/validation-runs; never reused")
	f.StringSliceVar(&o.Profiles, "profiles", []string{"minidb", "coordination"}, "first-party profiles: minidb,coordination,coordination-baseline")
	f.StringVar(&seeds, "seeds", "42..43", "inclusive deterministic seed range")
	f.IntVar(&o.Repetitions, "repetitions", 2, "independent attempts per profile and seed; not retries")
	f.IntVar(&o.MaxRuns, "max-runs", 100, "reject matrices larger than this before starting")
	f.DurationVar(&o.MaxRuntime, "max-runtime", 10*time.Minute, "overall execution budget plus bounded cleanup")
	f.DurationVar(&o.RunTimeout, "run-timeout", 60*time.Second, "per-attempt wall timeout plus 3s cleanup grace")
	f.Int64Var(&o.MaxBytes, "max-bytes", 512<<20, "sampled total campaign storage stop threshold")
	f.Int64Var(&o.MaxRunBytes, "max-run-bytes", 32<<20, "sampled per-attempt storage stop threshold")
	f.Uint64Var(&o.MaxFileBytes, "max-file-bytes", 8<<20, "inherited hard per-file RLIMIT_FSIZE")
	f.DurationVar(&o.Duration, "duration", 0, "override workload duration only (0 keeps profile); coverage/checkers unchanged")
	f.BoolVar(&o.KeepGoing, "keep-going", false, "explicitly continue matrix after a non-passing attempt; never retry")
	f.StringVar(&o.MiniDBRoot, "minidb-root", "", "target checkout (default MINIDB_ROOT or sibling)")
	f.StringVar(&o.CoordRoot, "coord-root", "", "target checkout (default COORD_ROOT or sibling)")
	RootCmd.AddCommand(cmd)
}
