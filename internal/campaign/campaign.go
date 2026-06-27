package campaign

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"failforge/internal/config"
	"failforge/internal/runner"
	"failforge/internal/store"
)

type ViolationSummary struct {
	CheckerName string `json:"checker_name"`
	Severity    string `json:"severity"`
	Description string `json:"description"`
}

type SeedResult struct {
	Seed           int64              `json:"seed"`
	RunID          string             `json:"run_id"`
	Status         string             `json:"status"`
	ViolationCount int                `json:"violation_count"`
	Violations     []ViolationSummary `json:"violations"`
	OutputDir      string             `json:"output_dir"`
	Elapsed        time.Duration      `json:"elapsed"`
}

type CampaignResult struct {
	ConfigPath    string             `json:"config_path"`
	SeedStart     int64              `json:"seed_start"`
	SeedEnd       int64              `json:"seed_end"`
	TotalSeeds    int                `json:"total_seeds"`
	Passed        int                `json:"passed"`
	Failed        int                `json:"failed"`
	Crashed       int                `json:"crashed"`
	Aborted       int                `json:"aborted"`
	StoppedEarly  bool               `json:"stopped_early"`
	SeedResults   []SeedResult       `json:"seed_results"`
	FailureGroups map[string][]int64 `json:"failure_groups"`
	OutputDir     string             `json:"output_dir"`
	Elapsed       time.Duration      `json:"elapsed"`
}

func RunCampaign(ctx context.Context, cfg *config.Config, configPath string, seedStart, seedEnd int64, stopOnFailure bool) (*CampaignResult, error) {
	startTime := time.Now()

	// 1. Create a campaign output directory
	timestamp := time.Now().Format("20060102-150405")
	campaignDir := filepath.Join("runs", fmt.Sprintf("campaign-%s", timestamp))
	if err := os.MkdirAll(campaignDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create campaign output directory: %w", err)
	}

	res := &CampaignResult{
		ConfigPath:    configPath,
		SeedStart:     seedStart,
		SeedEnd:       seedEnd,
		TotalSeeds:    int(seedEnd - seedStart + 1),
		SeedResults:   []SeedResult{},
		FailureGroups: make(map[string][]int64),
		OutputDir:     campaignDir,
	}

	// 2. Sequentially execute each seed in the range
	for seed := seedStart; seed <= seedEnd; seed++ {
		select {
		case <-ctx.Done():
			res.StoppedEarly = true
			res.Aborted++
			break
		default:
		}

		seedOutputDir := filepath.Join(campaignDir, fmt.Sprintf("seed-%d", seed))
		seedStartTime := time.Now()

		fmt.Printf("Campaign executing seed %d/%d (%d)...\n", seed-seedStart+1, res.TotalSeeds, seed)

		// Create a separate runner instance for this seed
		sOverride := seed
		rn, err := runner.NewRunner(cfg, &sOverride, seedOutputDir)
		if err != nil {
			res.SeedResults = append(res.SeedResults, SeedResult{
				Seed:      seed,
				Status:    "CRASHED",
				OutputDir: seedOutputDir,
				Elapsed:   time.Since(seedStartTime),
			})
			res.Crashed++
			continue
		}

		// Run the simulation run
		runRes, runErr := rn.RunAndReport(ctx)
		elapsed := time.Since(seedStartTime)

		status := "PASSED"
		violationCount := 0
		violationSummaries := []ViolationSummary{}

		if runRes != nil {
			status = runRes.Status
			violationCount = runRes.ViolationCount
		} else if runErr != nil {
			status = "CRASHED"
		}

		// Query violations details if there were any
		if violationCount > 0 && runRes != nil {
			dbPath := filepath.Join(seedOutputDir, "history.sqlite")
			if st, err := store.NewStore(dbPath); err == nil {
				if viols, err := st.GetViolations(runRes.RunID); err == nil {
					for _, v := range viols {
						if strings.ToUpper(v.Severity) != "INFO" {
							violationSummaries = append(violationSummaries, ViolationSummary{
								CheckerName: v.CheckerName,
								Severity:    v.Severity,
								Description: v.Description,
							})
						}
					}
				}
				st.Close()
			}
		}

		seedRes := SeedResult{
			Seed:           seed,
			RunID:          runRes.RunID,
			Status:         status,
			ViolationCount: violationCount,
			Violations:     violationSummaries,
			OutputDir:      seedOutputDir,
			Elapsed:        elapsed,
		}

		res.SeedResults = append(res.SeedResults, seedRes)

		// Update campaign statistics
		switch status {
		case "PASSED":
			res.Passed++
		case "FAILED":
			res.Failed++
		case "CRASHED":
			res.Crashed++
		case "ABORTED":
			res.Aborted++
		default:
			res.Passed++
		}

		if violationCount > 0 && stopOnFailure {
			res.StoppedEarly = true
			fmt.Printf("Stop on failure enabled. Stopping campaign after failure in seed %d.\n", seed)
			break
		}
	}

	// 3. Group failures by checker name
	res.FailureGroups = GroupFailures(res.SeedResults)
	res.Elapsed = time.Since(startTime)

	// Update latest campaign symlink/latest file
	_ = os.WriteFile(filepath.Join("runs", "latest_campaign.txt"), []byte(campaignDir), 0644)

	return res, nil
}

func GroupFailures(results []SeedResult) map[string][]int64 {
	groups := make(map[string][]int64)
	for _, r := range results {
		if r.ViolationCount > 0 {
			// Find unique checker names for this seed
			seenCheckers := make(map[string]bool)
			for _, v := range r.Violations {
				seenCheckers[v.CheckerName] = true
			}
			for chk := range seenCheckers {
				groups[chk] = append(groups[chk], r.Seed)
			}
		}
	}
	return groups
}
