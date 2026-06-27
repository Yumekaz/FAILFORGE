package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"failforge/internal/runner"

	"github.com/spf13/cobra"
)

var minimizeCmd = &cobra.Command{
	Use:   "minimize [run directory]",
	Short: "Minimize a failing run's faults schedule and workload",
	Long:  `Automatically reduces a failing run by removing redundant faults, decreasing concurrent clients, and shortening duration while ensuring the invariant violations still reproduce.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		runDir := args[0]

		// Check if run directory exists
		if _, err := os.Stat(runDir); os.IsNotExist(err) {
			return fmt.Errorf("run directory %s does not exist", runDir)
		}

		// Handle OS interrupts gracefully (Ctrl+C)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("\nReceived interrupt. Cancelling minimization...")
			cancel()
		}()

		fmt.Printf("Starting FailForge Minimizer for: %s\n", runDir)

		res, err := runner.MinimizeRun(ctx, runDir)
		if err != nil {
			return fmt.Errorf("minimization failed: %w", err)
		}

		// Print summary comparison table to CLI
		fmt.Println("\n==================================================")
		fmt.Printf("MINIMIZATION COMPLETE (in %v)\n", res.Elapsed.Round(time.Millisecond))
		fmt.Printf("Output Dir: %s\n\n", res.MinimizedOutputDir)
		
		fmt.Printf("| Metric         | Original | Minimized |\n")
		fmt.Printf("|----------------|----------|-----------|\n")
		fmt.Printf("| Duration       | %6dms | %7dms |\n", res.OriginalDurationMs, res.MinimizedDurationMs)
		fmt.Printf("| Clients        | %8d | %9d |\n", res.OriginalClients, res.MinimizedClients)
		fmt.Printf("| Fault Count    | %8d | %9d |\n", res.OriginalFaultCount, res.MinimizedFaultCount)
		fmt.Printf("| Violations     | %8d | %9d |\n", res.OriginalViolations, res.MinimizedViolations)
		
		fmt.Println("==================================================")
		
		if len(res.FaultsRemoved) > 0 {
			fmt.Printf("\033[1;32m[REDUCTIONS] Removed %d redundant fault(s):\033[0m\n", len(res.FaultsRemoved))
			for _, fr := range res.FaultsRemoved {
				fmt.Printf("  - %s\n", fr)
			}
			fmt.Println()
		}

		fmt.Printf("Minimized report created: %s/minimized_report.md\n", res.MinimizedOutputDir)
		fmt.Printf("\033[1;32mReplay the minimized version using:\033[0m\n")
		fmt.Printf("  failforge replay %s\n\n", res.MinimizedOutputDir)

		return nil
	},
}

func init() {
	RootCmd.AddCommand(minimizeCmd)
}
