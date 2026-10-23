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
	Error          string             `json:"error,omitempty"`
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

type campaignRunner interface {
	RunAndReport(context.Context) (*runner.RunResult, error)
}

var newCampaignRunner = func(cfg *config.Config, seed *int64, outputDir string) (campaignRunner, error) {
	return runner.NewRunner(cfg, seed, outputDir)
}

func RunCampaign(ctx context.Context, cfg *config.Config, configPath string, seedStart, seedEnd int64, stopOnFailure bool) (*CampaignResult, error) {
	startTime := time.Now()
	if seedStart > seedEnd || uint64(seedEnd)-uint64(seedStart) >= 1000000 {
		return nil, fmt.Errorf("invalid or excessive seed range")
	}

	// 1. Create a campaign output directory
	if err := os.MkdirAll("runs", 0755); err != nil {
		return nil, fmt.Errorf("failed to create campaign output directory: %w", err)
	}
	campaignDir, err := os.MkdirTemp("runs", "campaign-"+time.Now().Format("20060102-150405")+"-")
	if err != nil {
		return nil, err
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
campaignLoop:
	for index := 0; index < res.TotalSeeds; index++ {
		seed := seedStart + int64(index)
		select {
		case <-ctx.Done():
			res.StoppedEarly = true
			res.Aborted++
			break campaignLoop
		default:
		}

		seedOutputDir := filepath.Join(campaignDir, fmt.Sprintf("seed-%d", seed))
		seedStartTime := time.Now()

		fmt.Printf("Campaign executing seed %d/%d (%d)...\n", seed-seedStart+1, res.TotalSeeds, seed)

		// Create a separate runner instance for this seed
		sOverride := seed
		rn, err := newCampaignRunner(cfg, &sOverride, seedOutputDir)
		if err != nil {
			res.SeedResults = append(res.SeedResults, SeedResult{
				Seed:      seed,
				Status:    "CRASHED",
				OutputDir: seedOutputDir,
				Elapsed:   time.Since(seedStartTime),
				Error:     err.Error(),
			})
			res.Crashed++
			if stopOnFailure {
				res.StoppedEarly = true
				break campaignLoop
			}
			continue
		}

		// Run the simulation run
		runRes, runErr := rn.RunAndReport(ctx)
		elapsed := time.Since(seedStartTime)

		status := "CRASHED"
		violationCount := 0
		violationSummaries := []ViolationSummary{}
		runID := ""

		if runRes != nil {
			runID = runRes.RunID
			status = runRes.Status
			violationCount = runRes.ViolationCount
		} else if runErr != nil {
			// A nil result cannot provide a run ID or status. Keep the seed
			// represented in the campaign as a clean crash instead of panicking.
			status = "CRASHED"
		}
		if runErr != nil && status == "PASSED" {
			status = "CRASHED"
		}
		if status != "PASSED" && status != "FAILED" && status != "CRASHED" && status != "ABORTED" {
			status = "CRASHED"
		}
		if status == "PASSED" && violationCount > 0 {
			status = "FAILED"
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
			RunID:          runID,
			Status:         status,
			ViolationCount: violationCount,
			Violations:     violationSummaries,
			OutputDir:      seedOutputDir,
			Elapsed:        elapsed,
		}
		if runErr != nil {
			seedRes.Error = runErr.Error()
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
			res.Crashed++
		}

		if status != "PASSED" && stopOnFailure {
			res.StoppedEarly = true
			fmt.Printf("Stop on failure enabled. Stopping campaign after failure in seed %d.\n", seed)
			break campaignLoop
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
