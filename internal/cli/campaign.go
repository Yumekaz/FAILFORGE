package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"failforge/internal/campaign"
	"failforge/internal/config"

	"github.com/spf13/cobra"
)

var (
	seedsFlag  string
	stopOnFail bool
)

var campaignCmd = &cobra.Command{
	Use:   "campaign [config file]",
	Short: "Run a range of test seeds automatically as a campaign",
	Long:  `Loops through the specified seed range, runs a complete FailForge simulation for each seed, collects pass/fail statuses and any invariant violations, and generates a markdown summary report.`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		configPath := args[0]
		cfg, err := config.LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("failed to load config: %w", err)
		}

		start, end, err := parseSeeds(seedsFlag)
		if err != nil {
			return fmt.Errorf("failed to parse seeds: %w", err)
		}

		// Handle OS interrupts gracefully (Ctrl+C)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigChan
			fmt.Println("\nReceived interrupt. Stopping campaign...")
			cancel()
		}()

		fmt.Printf("Starting FailForge campaign mode...\n")
		fmt.Printf("Config File: %s\n", configPath)
		fmt.Printf("Seed Range:  %d..%d (%d seeds)\n", start, end, end-start+1)

		res, err := campaign.RunCampaign(ctx, cfg, configPath, start, end, stopOnFail)
		if err != nil {
			return fmt.Errorf("campaign execution failed: %w", err)
		}

		// Generate the markdown report
		if err := campaign.GenerateCampaignReport(res); err != nil {
			return fmt.Errorf("failed to generate campaign report: %w", err)
		}

		// Print summary table to CLI
		fmt.Println("\n==================================================")
		fmt.Printf("CAMPAIGN RUN COMPLETE\n")
		fmt.Printf("Output Dir: %s\n", res.OutputDir)
		fmt.Printf("Total Seeds: %d\n", res.TotalSeeds)
		fmt.Printf("Passed:      %d\n", res.Passed)
		fmt.Printf("Failed:      %d\n", res.Failed)
		fmt.Printf("Crashed:     %d\n", res.Crashed)
		fmt.Printf("Aborted:     %d\n", res.Aborted)
		fmt.Printf("Report File: %s/campaign_report.md\n", res.OutputDir)
		fmt.Println("==================================================")

		if res.Failed > 0 {
			// Red warning for failed campaign
			fmt.Printf("\033[1;31m[CAMPAIGN FAILED] %d seeds failed! Check the report for replay commands.\033[0m\n", res.Failed)
		} else {
			// Green success for passed campaign
			fmt.Println("\033[1;32m[CAMPAIGN PASSED] All seeds completed successfully!\033[0m")
		}

		return nil
	},
}

func parseSeeds(seedsStr string) (int64, int64, error) {
	parts := strings.Split(seedsStr, "..")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid seeds format, expected START..END (e.g. 1..100)")
	}
	var start, end int64
	_, err1 := fmt.Sscanf(parts[0], "%d", &start)
	_, err2 := fmt.Sscanf(parts[1], "%d", &end)
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("invalid seeds format, expected integers")
	}
	if start > end {
		return 0, 0, fmt.Errorf("invalid seeds format, START must be <= END")
	}
	return start, end, nil
}

func init() {
	campaignCmd.Flags().StringVar(&seedsFlag, "seeds", "", "range of seeds to run (e.g. 1..100)")
	_ = campaignCmd.MarkFlagRequired("seeds")
	campaignCmd.Flags().BoolVar(&stopOnFail, "stop-on-failure", false, "stop the campaign on the first failed seed")
	RootCmd.AddCommand(campaignCmd)
}
