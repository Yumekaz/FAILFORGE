package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"failforge/internal/config"
	"failforge/internal/runner"

	"github.com/spf13/cobra"
)

var (
	seedFlag       int64
	runOutputDir   string
	fileLimitBytes uint64
)

var runCmd = &cobra.Command{
	Use:   "run [config file]",
	Short: "Run a test campaign or a single test seed",
	Long:  `Parses the provided yaml configuration, initializes a SQLite history database, boots the multi-node processes and HTTP proxy, and runs test execution.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if fileLimitBytes > 0 {
			limit := &syscall.Rlimit{Cur: fileLimitBytes, Max: fileLimitBytes}
			if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, limit); err != nil {
				return err
			}
		}
		configPath := args[0]
		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		var overrideSeed *int64
		if cmd.Flags().Changed("seed") {
			overrideSeed = &seedFlag
		}

		if runOutputDir != "" {
			// Explicit output directories are single-use; never append to old evidence.
			if err := os.Mkdir(runOutputDir, 0755); err != nil {
				return fmt.Errorf("reserve output directory: %w", err)
			}
		}
		rn, err := runner.NewRunner(cfg, overrideSeed, runOutputDir)
		if err != nil {
			return fmt.Errorf("failed to initialize runner: %w", err)
		}

		// Handle OS interrupts gracefully (Ctrl+C)
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		fmt.Printf("Starting FailForge simulation run...\n")
		fmt.Printf("Run ID:     %s\n", rn.GetRunID())
		fmt.Printf("Output Dir: %s\n", rn.GetOutputDir())

		result, err := rn.RunAndReport(ctx)
		if err != nil {
			return fmt.Errorf("run failed: %w", err)
		}
		if result == nil || result.Status != "PASSED" {
			if result == nil {
				return fmt.Errorf("run produced no result")
			}
			return fmt.Errorf("run status is %s; configured checks failed or coverage was insufficient (see %s)", result.Status, result.OutputDir)
		}

		fmt.Println("FailForge run completed successfully.")
		return nil
	},
}

func init() {
	runCmd.Flags().Int64Var(&seedFlag, "seed", 0, "override the seed for deterministic random schedules")
	runCmd.Flags().StringVar(&runOutputDir, "output-dir", "", "new, single-use output directory (parent must exist)")
	runCmd.Flags().Uint64Var(&fileLimitBytes, "max-file-bytes", 0, "per-file RLIMIT_FSIZE inherited by target processes (0 disables)")
	RootCmd.AddCommand(runCmd)
}
