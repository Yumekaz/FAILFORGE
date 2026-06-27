package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"failforge/internal/config"
	"failforge/internal/store"
)

type MinimizationResult struct {
	OriginalRunDir      string                 `json:"original_run_dir"`
	OriginalViolations  int                    `json:"original_violations"`
	OriginalFaultCount  int                    `json:"original_fault_count"`
	OriginalDurationMs  int                    `json:"original_duration_ms"`
	OriginalClients     int                    `json:"original_clients"`

	MinimizedOutputDir  string                 `json:"minimized_output_dir"`
	MinimizedViolations int                    `json:"minimized_violations"`
	MinimizedFaultCount int                    `json:"minimized_fault_count"`
	MinimizedDurationMs int                    `json:"minimized_duration_ms"`
	MinimizedClients    int                    `json:"minimized_clients"`

	FaultsRemoved       []string               `json:"faults_removed"`
	FinalFaults         []config.FaultConfig   `json:"final_faults"`
	Elapsed             time.Duration          `json:"elapsed"`
}

// MinimizeRun attempts to reduce the faults, clients, and duration of a failing run.
func MinimizeRun(ctx context.Context, runDir string) (*MinimizationResult, error) {
	startTime := time.Now()

	// 1. Read config.json
	configPath := filepath.Join(runDir, "config.json")
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config.json from %s: %w", runDir, err)
	}

	var baseCfg config.Config
	if err := json.Unmarshal(configData, &baseCfg); err != nil {
		return nil, fmt.Errorf("failed to parse config.json: %w", err)
	}

	// 2. Read faults.json (original faults schedule)
	origFaultsPath := filepath.Join(runDir, "faults.json")
	var origFaults []config.FaultConfig
	if _, err := os.Stat(origFaultsPath); err == nil {
		data, err := os.ReadFile(origFaultsPath)
		if err == nil {
			_ = json.Unmarshal(data, &origFaults)
		}
	}

	// 3. Read original run stats
	origDbPath := filepath.Join(runDir, "history.sqlite")
	origRunID, origViolations, err := getRunViolations(origDbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read original violations from SQLite: %w", err)
	}
	if origViolations == 0 {
		return nil, fmt.Errorf("cannot minimize a run that has 0 violations")
	}

	// Find the checker names that failed in the original run
	targetCheckers, err := getTargetCheckers(origDbPath, origRunID)
	if err != nil || len(targetCheckers) == 0 {
		return nil, fmt.Errorf("failed to identify target checkers: %w", err)
	}

	log.Printf("[Minimizer] Starting minimization for run directory: %s\n", runDir)
	log.Printf("[Minimizer] Original stats - Violations: %d, Faults: %d, Clients: %d, Duration: %dms\n",
		origViolations, len(origFaults), baseCfg.Workload.Clients, baseCfg.Time.DurationMs)
	log.Printf("[Minimizer] Target checkers to reproduce: %v\n", targetCheckers)

	// 4. Baseline verification: verify full replay still fails
	stepCounter := 0
	baselineName := "baseline"
	reproduced, _, _, err := testSchedule(ctx, &baseCfg, origFaults, runDir, baselineName, targetCheckers)
	if err != nil {
		return nil, fmt.Errorf("baseline replay check failed: %w", err)
	}
	if !reproduced {
		return nil, fmt.Errorf("baseline replay did not reproduce target violation(s); minimization aborted")
	}
	log.Printf("[Minimizer] Baseline replay verified successfully. Proceeding with reduction...\n")

	// 5. Fault Minimization (Delta Debugging style loop)
	minimizedFaults := append([]config.FaultConfig{}, origFaults...)
	var faultsRemoved []string
	reduced := true

	for reduced {
		reduced = false
		for i := 0; i < len(minimizedFaults); i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			// Try removing fault at index i
			candFaults := append([]config.FaultConfig{}, minimizedFaults[:i]...)
			candFaults = append(candFaults, minimizedFaults[i+1:]...)

			stepCounter++
			stepName := fmt.Sprintf("fault-step-%d", stepCounter)
			ok, _, _, err := testSchedule(ctx, &baseCfg, candFaults, runDir, stepName, targetCheckers)
			if err == nil && ok {
				// Violation still reproduced! Keep the reduction
				removedFault := minimizedFaults[i]
				faultsRemoved = append(faultsRemoved, fmt.Sprintf("%s at %dms", removedFault.Type, removedFault.AtMs))
				log.Printf("[Minimizer] Successfully removed fault: %s at %dms\n", removedFault.Type, removedFault.AtMs)
				minimizedFaults = candFaults
				reduced = true
				break // Restart the scan on the reduced list
			}
		}
	}

	// 6. Client Count Minimization
	minimizedCfg := baseCfg
	minimizedCfg.Faults.Mode = "replay"
	for minimizedCfg.Workload.Clients > 1 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		candClients := minimizedCfg.Workload.Clients - 1
		candCfg := minimizedCfg
		candCfg.Workload.Clients = candClients

		stepCounter++
		stepName := fmt.Sprintf("client-step-%d", stepCounter)
		ok, _, _, err := testSchedule(ctx, &candCfg, minimizedFaults, runDir, stepName, targetCheckers)
		if err == nil && ok {
			// Violation still reproduced! Keep the reduced client count
			log.Printf("[Minimizer] Successfully reduced client count to: %d\n", candClients)
			minimizedCfg.Workload.Clients = candClients
		} else {
			// Cannot reduce clients further
			break
		}
	}

	// 7. Workload Duration Minimization (20% step reduction)
	origDuration := minimizedCfg.Time.DurationMs
	currentDuration := origDuration
	for currentDuration > 1000 { // Don't minimize below 1 second to avoid setup overhead timing out
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		candDuration := int(float64(currentDuration) * 0.8)
		candCfg := minimizedCfg
		candCfg.Time.DurationMs = candDuration
		candCfg.Workload.DurationMs = int(float64(minimizedCfg.Workload.DurationMs) * 0.8)

		stepCounter++
		stepName := fmt.Sprintf("duration-step-%d", stepCounter)
		ok, _, _, err := testSchedule(ctx, &candCfg, minimizedFaults, runDir, stepName, targetCheckers)
		if err == nil && ok {
			// Violation still reproduced! Keep the reduced duration
			log.Printf("[Minimizer] Successfully reduced duration to: %dms\n", candDuration)
			minimizedCfg.Time.DurationMs = candDuration
			minimizedCfg.Workload.DurationMs = candCfg.Workload.DurationMs
			currentDuration = candDuration
		} else {
			// Cannot reduce duration further
			break
		}
	}

	// 8. Generate and save final minimized outputs
	minimizedOutputDir := filepath.Join(runDir, "minimized")
	_ = os.RemoveAll(minimizedOutputDir)
	if err := os.MkdirAll(minimizedOutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create minimized output directory: %w", err)
	}

	// Save final config.json and faults.json
	minimizedCfgBytes, _ := json.MarshalIndent(&minimizedCfg, "", "  ")
	_ = os.WriteFile(filepath.Join(minimizedOutputDir, "config.json"), minimizedCfgBytes, 0644)

	minimizedFaultsBytes, _ := json.MarshalIndent(minimizedFaults, "", "  ")
	_ = os.WriteFile(filepath.Join(minimizedOutputDir, "faults.json"), minimizedFaultsBytes, 0644)

	// Run the final minimized version to populate sqlite database and events.jsonl
	finalStepName := "final-run"
	_, _, finalOutputDir, err := testSchedule(ctx, &minimizedCfg, minimizedFaults, runDir, finalStepName, targetCheckers)
	if err != nil {
		return nil, fmt.Errorf("final minimized execution failed: %w", err)
	}

	// Copy final history database and events.jsonl to minimized/
	_ = copyFile(filepath.Join(finalOutputDir, "history.sqlite"), filepath.Join(minimizedOutputDir, "history.sqlite"))
	_ = copyFile(filepath.Join(finalOutputDir, "events.jsonl"), filepath.Join(minimizedOutputDir, "events.jsonl"))

	// Copy final log directory if present
	finalLogsSrc := filepath.Join(finalOutputDir, "logs")
	finalLogsDest := filepath.Join(minimizedOutputDir, "logs")
	_ = copyDir(finalLogsSrc, finalLogsDest)

	// Get final violation count
	_, finalViolations, _ := getRunViolations(filepath.Join(minimizedOutputDir, "history.sqlite"))

	// 9. Generate markdown minimized report
	var reportSb strings.Builder
	reportSb.WriteString("# FailForge Minimized Bug Report\n\n")
	reportSb.WriteString("## Summary\n")
	reportSb.WriteString("The original failing run has been minimized by delta debugging.\n\n")
	reportSb.WriteString("| Metric | Original | Minimized |\n")
	reportSb.WriteString("|---|---|---|\n")
	reportSb.WriteString(fmt.Sprintf("| **Duration** | %dms | %dms |\n", origDuration, minimizedCfg.Time.DurationMs))
	reportSb.WriteString(fmt.Sprintf("| **Clients** | %d | %d |\n", baseCfg.Workload.Clients, minimizedCfg.Workload.Clients))
	reportSb.WriteString(fmt.Sprintf("| **Fault Count** | %d | %d |\n", len(origFaults), len(minimizedFaults)))
	reportSb.WriteString(fmt.Sprintf("| **Violations** | %d | %d |\n\n", origViolations, finalViolations))

	if len(faultsRemoved) > 0 {
		reportSb.WriteString("## Faults Removed\n")
		for _, fr := range faultsRemoved {
			reportSb.WriteString(fmt.Sprintf("- Removed %s\n", fr))
		}
		reportSb.WriteString("\n")
	}

	reportSb.WriteString("## Final Fault Schedule\n")
	if len(minimizedFaults) == 0 {
		reportSb.WriteString("No network or process faults required! The system violates the invariant under happy path workload.\n\n")
	} else {
		for _, f := range minimizedFaults {
			reportSb.WriteString(fmt.Sprintf("- **%s** on `%s` at **%dms**\n", f.Type, f.Node, f.AtMs))
		}
		reportSb.WriteString("\n")
	}

	reportSb.WriteString("## Replay Command\n")
	reportSb.WriteString(fmt.Sprintf("```bash\nfailforge replay %s\n```\n", filepath.Join(runDir, "minimized")))

	_ = os.WriteFile(filepath.Join(minimizedOutputDir, "minimized_report.md"), []byte(reportSb.String()), 0644)

	// 10. Clean up temporary minimization folders to save disk space
	_ = os.RemoveAll(filepath.Join(runDir, "minimizations"))

	elapsed := time.Since(startTime)
	log.Printf("[Minimizer] Minimization complete in %v. Output stored at: %s\n", elapsed.Round(time.Millisecond), minimizedOutputDir)

	return &MinimizationResult{
		OriginalRunDir:      runDir,
		OriginalViolations:  origViolations,
		OriginalFaultCount:  len(origFaults),
		OriginalDurationMs:  origDuration,
		OriginalClients:     baseCfg.Workload.Clients,
		MinimizedOutputDir:  minimizedOutputDir,
		MinimizedViolations: finalViolations,
		MinimizedFaultCount: len(minimizedFaults),
		MinimizedDurationMs: minimizedCfg.Time.DurationMs,
		MinimizedClients:    minimizedCfg.Workload.Clients,
		FaultsRemoved:       faultsRemoved,
		FinalFaults:         minimizedFaults,
		Elapsed:             elapsed,
	}, nil
}

// testSchedule executes a replay run with candidate configs and faults, and checks if target checkers are violated.
func testSchedule(ctx context.Context, baseCfg *config.Config, faults []config.FaultConfig, runDir string, stepName string, targetCheckers []string) (bool, string, string, error) {
	candDir := filepath.Join(runDir, "minimizations", stepName)
	_ = os.RemoveAll(candDir)
	if err := os.MkdirAll(candDir, 0755); err != nil {
		return false, "", "", err
	}

	// 1. Write config.json
	cfgBytes, err := json.Marshal(baseCfg)
	if err != nil {
		return false, "", "", err
	}
	if err := os.WriteFile(filepath.Join(candDir, "config.json"), cfgBytes, 0644); err != nil {
		return false, "", "", err
	}

	// 2. Write faults.json
	faultsBytes, err := json.Marshal(faults)
	if err != nil {
		return false, "", "", err
	}
	if err := os.WriteFile(filepath.Join(candDir, "faults.json"), faultsBytes, 0644); err != nil {
		return false, "", "", err
	}

	// 3. Execute replay
	res, err := ReplayRun(ctx, candDir)
	if err != nil {
		return false, "", "", err
	}

	// 4. Verify if any of the target checkers produced a violation
	candDbPath := filepath.Join(res.ReplayOutputDir, "history.sqlite")
	violated := targetCheckerViolated(candDbPath, res.ReplayRunID, targetCheckers)

	return violated, res.ReplayRunID, res.ReplayOutputDir, nil
}

func getTargetCheckers(dbPath string, runID string) ([]string, error) {
	st, err := store.NewStore(dbPath)
	if err != nil {
		return nil, err
	}
	defer st.Close()

	violations, err := st.GetViolations(runID)
	if err != nil {
		return nil, err
	}

	var checkersList []string
	seen := make(map[string]bool)
	for _, v := range violations {
		if v.Severity != "info" && !seen[v.CheckerName] {
			seen[v.CheckerName] = true
			checkersList = append(checkersList, v.CheckerName)
		}
	}
	return checkersList, nil
}

func targetCheckerViolated(candDbPath string, candRunID string, targetCheckers []string) bool {
	st, err := store.NewStore(candDbPath)
	if err != nil {
		return false
	}
	defer st.Close()

	violations, err := st.GetViolations(candRunID)
	if err != nil {
		return false
	}

	targetMap := make(map[string]bool)
	for _, tc := range targetCheckers {
		targetMap[tc] = true
	}

	for _, v := range violations {
		if v.Severity != "info" && targetMap[v.CheckerName] {
			return true
		}
	}

	return false
}

func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}
