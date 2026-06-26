package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"failforge/internal/runner"

	"github.com/spf13/cobra"
)

var replayCmd = &cobra.Command{
	Use:   "replay [run directory]",
	Short: "Replay a previous simulation run deterministically using its seed and fault schedule",
	Long:  `Reads the config, seed, and generated fault schedule from the target run directory, spins up a replay run under a sub-folder, and verifies if the violations were successfully reproduced.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		runDir := args[0]
		if _, err := os.Stat(runDir); os.IsNotExist(err) {
			return fmt.Errorf("run directory not found: %s", runDir)
		}

		// Handle OS interrupts gracefully (Ctrl+C)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("\nReceived interrupt. Cancelling replay campaign...")
			cancel()
		}()

		fmt.Printf("Initializing FailForge Replay Engine for: %s\n", runDir)

		res, err := runner.ReplayRun(ctx, runDir)
		if err != nil {
			return fmt.Errorf("replay failed: %w", err)
		}

		fmt.Println("\n==================================================")
		fmt.Printf("REPLAY CAMPAIGN COMPLETE\n")
		fmt.Printf("Replay Output Dir: %s\n", res.ReplayOutputDir)
		fmt.Printf("Original Run ID:   %s\n", res.OriginalRunID)
		fmt.Printf("Replay Run ID:     %s\n", res.ReplayRunID)
		fmt.Printf("Seed Replayed:     %d\n", res.Seed)
		fmt.Printf("Original Violations: %d\n", res.OriginalViolations)
		fmt.Printf("Replay Violations:   %d\n", res.ReplayViolations)

		if res.ViolationReproduced {
			// Green bold SUCCESS banner
			fmt.Println("\033[1;32m[REPLAY SUCCESS] Invariant violation successfully reproduced!\033[0m")
		} else {
			if res.OriginalViolations == 0 {
				fmt.Println("\033[1;36m[REPLAY OK] No violations found in original run, and none found in replay.\033[0m")
			} else {
				// Red/Yellow bold WARNING banner
				fmt.Println("\033[1;31m[REPLAY WARNING] Failed to reproduce the original invariant violations.\033[0m")
				fmt.Println("This could indicate timing non-determinism in the target system process execution.")
			}
		}
		fmt.Println("==================================================")

		return nil
	},
}

func init() {
	RootCmd.AddCommand(replayCmd)
}
