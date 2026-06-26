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
	seedFlag int64
)

var runCmd = &cobra.Command{
	Use:   "run [config file]",
	Short: "Run a test campaign or a single test seed",
	Long:  `Parses the provided yaml configuration, initializes a SQLite history database, boots the multi-node processes and HTTP proxy, and runs test execution.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath := args[0]
		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		var overrideSeed *int64
		if cmd.Flags().Changed("seed") {
			overrideSeed = &seedFlag
		}

		rn, err := runner.NewRunner(cfg, overrideSeed)
		if err != nil {
			return fmt.Errorf("failed to initialize runner: %w", err)
		}

		// Handle OS interrupts gracefully (Ctrl+C)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("\nReceived interrupt signal. Gracefully shutting down...")
			cancel()
		}()

		fmt.Printf("Starting FailForge simulation run...\n")
		fmt.Printf("Run ID:     %s\n", rn.GetRunID())
		fmt.Printf("Output Dir: %s\n", rn.GetOutputDir())

		if err := rn.Run(ctx); err != nil {
			return fmt.Errorf("run failed: %w", err)
		}

		fmt.Println("FailForge run completed successfully.")
		return nil
	},
}

func init() {
	runCmd.Flags().Int64Var(&seedFlag, "seed", 0, "override the seed for deterministic random schedules")
	RootCmd.AddCommand(runCmd)
}
