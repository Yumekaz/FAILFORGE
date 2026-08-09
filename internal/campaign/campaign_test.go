package campaign

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/runner"
)

type stubCampaignRunner struct {
	result *runner.RunResult
	err    error
}

func (s stubCampaignRunner) RunAndReport(context.Context) (*runner.RunResult, error) {
	return s.result, s.err
}

func TestRunCampaignRecordsCrashedSeedWhenRunnerReturnsNil(t *testing.T) {
	t.Chdir(t.TempDir())
	originalFactory := newCampaignRunner
	t.Cleanup(func() { newCampaignRunner = originalFactory })
	newCampaignRunner = func(*config.Config, *int64, string) (campaignRunner, error) {
		return stubCampaignRunner{err: errors.New("simulated runner crash")}, nil
	}

	result, err := RunCampaign(context.Background(), &config.Config{}, "config.yml", 42, 42, false)
	if err != nil {
		t.Fatalf("RunCampaign returned unexpected error: %v", err)
	}
	if result.Crashed != 1 || len(result.SeedResults) != 1 {
		t.Fatalf("expected one crashed seed, got crashed=%d results=%d", result.Crashed, len(result.SeedResults))
	}
	seedResult := result.SeedResults[0]
	if seedResult.Status != "CRASHED" || seedResult.RunID != "" || seedResult.Seed != 42 {
		t.Fatalf("unexpected crashed seed result: %+v", seedResult)
	}
}

func TestRunCampaignStopsSeedLoopWhenContextIsCancelled(t *testing.T) {
	t.Chdir(t.TempDir())
	originalFactory := newCampaignRunner
	t.Cleanup(func() { newCampaignRunner = originalFactory })
	factoryCalls := 0
	newCampaignRunner = func(*config.Config, *int64, string) (campaignRunner, error) {
		factoryCalls++
		return stubCampaignRunner{result: &runner.RunResult{RunID: "unexpected", Status: "PASSED"}}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := RunCampaign(ctx, &config.Config{}, "config.yml", 1, 3, false)
	if err != nil {
		t.Fatalf("RunCampaign returned unexpected error: %v", err)
	}
	if !result.StoppedEarly || result.Aborted != 1 || len(result.SeedResults) != 0 || factoryCalls != 0 {
		t.Fatalf("expected cancelled campaign to stop before first seed: result=%+v factoryCalls=%d", result, factoryCalls)
	}
}

func TestGroupFailures(t *testing.T) {
	results := []SeedResult{
		{
			Seed:           1,
			Status:         "FAILED",
			ViolationCount: 2,
			Violations: []ViolationSummary{
				{CheckerName: "read_after_acknowledged_write"},
				{CheckerName: "no_two_leaders"},
			},
		},
		{
			Seed:           2,
			Status:         "PASSED",
			ViolationCount: 0,
		},
		{
			Seed:           3,
			Status:         "FAILED",
			ViolationCount: 1,
			Violations: []ViolationSummary{
				{CheckerName: "read_after_acknowledged_write"},
			},
		},
	}

	groups := GroupFailures(results)

	if len(groups) != 2 {
		t.Fatalf("expected 2 failure groups, got %d", len(groups))
	}

	rawSeeds := groups["read_after_acknowledged_write"]
	if len(rawSeeds) != 2 || rawSeeds[0] != 1 || rawSeeds[1] != 3 {
		t.Errorf("unexpected seeds for read_after_acknowledged_write: %v", rawSeeds)
	}

	leaderSeeds := groups["no_two_leaders"]
	if len(leaderSeeds) != 1 || leaderSeeds[0] != 1 {
		t.Errorf("unexpected seeds for no_two_leaders: %v", leaderSeeds)
	}
}

func TestGenerateCampaignReport(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "campaign-report-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	res := &CampaignResult{
		ConfigPath: "test_config.yml",
		SeedStart:  1,
		SeedEnd:    3,
		TotalSeeds: 3,
		Passed:     2,
		Failed:     1,
		OutputDir:  tmpDir,
		SeedResults: []SeedResult{
			{Seed: 1, Status: "PASSED", Elapsed: 10 * time.Millisecond},
			{Seed: 2, Status: "PASSED", Elapsed: 15 * time.Millisecond},
			{Seed: 3, Status: "FAILED", ViolationCount: 1, Violations: []ViolationSummary{{CheckerName: "no_two_leaders"}}, Elapsed: 20 * time.Millisecond},
		},
		FailureGroups: map[string][]int64{
			"no_two_leaders": {3},
		},
	}

	err = GenerateCampaignReport(res)
	if err != nil {
		t.Fatalf("failed to generate report: %v", err)
	}

	reportPath := filepath.Join(tmpDir, "campaign_report.md")
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatalf("failed to read report: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "# FailForge Campaign Report") {
		t.Errorf("missing report title")
	}
	if !strings.Contains(content, "- **Passed**: 2") {
		t.Errorf("incorrect passed count")
	}
	if !strings.Contains(content, "- **Failed**: 1") {
		t.Errorf("incorrect failed count")
	}
	if !strings.Contains(content, "no_two_leaders") {
		t.Errorf("missing checker group name")
	}
}
