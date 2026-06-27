package campaign

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func GenerateCampaignReport(res *CampaignResult) error {
	var sb strings.Builder

	sb.WriteString("# FailForge Campaign Report\n\n")

	sb.WriteString("## Summary\n")
	sb.WriteString(fmt.Sprintf("- **Config**: `%s`\n", filepath.Base(res.ConfigPath)))
	sb.WriteString(fmt.Sprintf("- **Seed Range**: `%d..%d`\n", res.SeedStart, res.SeedEnd))
	sb.WriteString(fmt.Sprintf("- **Total Runs**: %d\n", res.TotalSeeds))
	sb.WriteString(fmt.Sprintf("- **Passed**: %d\n", res.Passed))
	sb.WriteString(fmt.Sprintf("- **Failed**: %d\n", res.Failed))
	sb.WriteString(fmt.Sprintf("- **Crashed**: %d\n", res.Crashed))
	sb.WriteString(fmt.Sprintf("- **Aborted**: %d\n", res.Aborted))
	if res.StoppedEarly {
		sb.WriteString("- **Stopped Early**: Yes (due to stop-on-failure)\n")
	} else {
		sb.WriteString("- **Stopped Early**: No\n")
	}
	sb.WriteString(fmt.Sprintf("- **Total Duration**: %v\n\n", res.Elapsed.Round(time.Millisecond)))

	sb.WriteString("## Failure Groups\n\n")
	if len(res.FailureGroups) == 0 {
		sb.WriteString("No failures/invariant violations detected during this campaign!\n\n")
	} else {
		// Sort failure groups by checker name for consistency
		keys := make([]string, 0, len(res.FailureGroups))
		for k := range res.FailureGroups {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, checker := range keys {
			seeds := res.FailureGroups[checker]
			sb.WriteString(fmt.Sprintf("### %s (%d seeds)\n", checker, len(seeds)))

			// Format seeds as string list
			seedStrs := make([]string, len(seeds))
			for i, s := range seeds {
				seedStrs[i] = fmt.Sprintf("%d", s)
			}
			sb.WriteString(fmt.Sprintf("- **Seeds**: %s\n", strings.Join(seedStrs, ", ")))
			if len(seeds) > 0 {
				sb.WriteString(fmt.Sprintf("- **Example Replay**: `failforge replay %s/seed-%d`\n", res.OutputDir, seeds[0]))
			}
			sb.WriteString("\n")
		}
	}

	sb.WriteString("## Per-Seed Results\n\n")
	sb.WriteString("| Seed | Status | Violations | Duration | Replay Command |\n")
	sb.WriteString("|---|---|---|---|---|\n")

	for _, r := range res.SeedResults {
		replayCmd := "—"
		if r.ViolationCount > 0 || r.Status == "CRASHED" || r.Status == "FAILED" {
			replayCmd = fmt.Sprintf("`failforge replay %s/seed-%d`", res.OutputDir, r.Seed)
		}
		sb.WriteString(fmt.Sprintf("| %d | **%s** | %d | %v | %s |\n",
			r.Seed, r.Status, r.ViolationCount, r.Elapsed.Round(time.Millisecond), replayCmd))
	}

	reportPath := filepath.Join(res.OutputDir, "campaign_report.md")
	return os.WriteFile(reportPath, []byte(sb.String()), 0644)
}
