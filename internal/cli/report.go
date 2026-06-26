package cli

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"failforge/internal/store"

	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

var reportCmd = &cobra.Command{
	Use:   "report [run directory]",
	Short: "Generate a markdown bug report for a test run",
	Long:  `Queries SQLite event history inside the target run directory and produces a structured markdown report.md detailing nodes, operations, and any invariant violations.`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		runDir := ""
		if len(args) == 1 {
			runDir = args[0]
		} else {
			data, err := os.ReadFile("runs/latest.txt")
			if err != nil {
				linkInfo, errLink := os.Readlink("runs/latest")
				if errLink == nil {
					runDir = filepath.Join("runs", linkInfo)
				} else {
					return fmt.Errorf("no run directory specified, and runs/latest.txt could not be read: %w", err)
				}
			} else {
				runDir = strings.TrimSpace(string(data))
			}
		}

		dbPath := filepath.Join(runDir, "history.sqlite")
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			return fmt.Errorf("SQLite database not found at: %s", dbPath)
		}

		st, err := store.NewStore(dbPath)
		if err != nil {
			return fmt.Errorf("failed to open store: %w", err)
		}
		defer st.Close()

		return generateReport(st, dbPath, runDir)
	},
}

func generateReport(st *store.Store, dbPath string, runDir string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	// 1. Fetch Run Metadata
	var runID, startedAtStr, endedAtStr, status, configHash string
	var seed int64
	var endedAt sql.NullString
	err = db.QueryRow("SELECT id, seed, started_at, ended_at, status, config_hash FROM runs ORDER BY started_at DESC LIMIT 1").
		Scan(&runID, &seed, &startedAtStr, &endedAt, &status, &configHash)
	if err != nil {
		return fmt.Errorf("failed to fetch run metadata: %w", err)
	}
	if endedAt.Valid {
		endedAtStr = endedAt.String
	}

	// 2. Fetch Nodes Status
	nodes, err := st.GetNodes(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch nodes: %w", err)
	}

	// 3. Fetch Operations Statistics
	ops, err := st.GetOperations(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch operations: %w", err)
	}
	totalOps := len(ops)
	successOps := 0
	failedOps := 0
	opCounts := make(map[string]int)
	opSuccess := make(map[string]int)

	for _, op := range ops {
		opCounts[op.Operation]++
		if op.Status == "ok" {
			successOps++
			opSuccess[op.Operation]++
		} else {
			failedOps++
		}
	}

	// 4. Fetch Message Statistics
	messages, err := st.GetMessages(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch messages: %w", err)
	}
	totalMsgs := len(messages)
	sentMsgs := 0
	deliveredMsgs := 0
	droppedMsgs := 0
	for _, m := range messages {
		switch m.Status {
		case "sent":
			sentMsgs++
		case "delivered":
			deliveredMsgs++
		case "dropped":
			droppedMsgs++
		}
	}

	// 5. Fetch Violations
	violations, err := st.GetViolations(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch violations: %w", err)
	}

	// Build Markdown Report
	var sb strings.Builder
	sb.WriteString("# FailForge Simulation Run Report\n\n")
	sb.WriteString(fmt.Sprintf("## Summary\n"))
	sb.WriteString(fmt.Sprintf("- **Run ID**: `%s`\n", runID))
	sb.WriteString(fmt.Sprintf("- **Seed**: `%d`\n", seed))
	sb.WriteString(fmt.Sprintf("- **Config Hash**: `%s`\n", configHash))
	sb.WriteString(fmt.Sprintf("- **Status**: **%s**\n", status))
	sb.WriteString(fmt.Sprintf("- **Started At**: `%s`\n", startedAtStr))
	if endedAtStr != "" {
		sb.WriteString(fmt.Sprintf("- **Ended At**: `%s`\n", endedAtStr))
	}
	sb.WriteString("\n")

	// Node Details Table
	sb.WriteString("## Cluster Node Lifecycle Status\n\n")
	sb.WriteString("| Node ID | Port | PID | Status | Data Directory |\n")
	sb.WriteString("|---|---|---|---|---|\n")
	for _, n := range nodes {
		sb.WriteString(fmt.Sprintf("| %s | %d | %d | %s | `%s` |\n", n.NodeID, n.Port, n.PID, n.Status, n.DataDir))
	}
	sb.WriteString("\n")

	// Client Operations Table
	sb.WriteString("## Client Operations Statistics\n\n")
	sb.WriteString(fmt.Sprintf("- **Total Operations**: %d\n", totalOps))
	sb.WriteString(fmt.Sprintf("- **Successful Operations**: %d\n", successOps))
	sb.WriteString(fmt.Sprintf("- **Failed Operations**: %d\n\n", failedOps))

	if totalOps > 0 {
		sb.WriteString("| Operation Type | Total Invoked | Successful | Failed |\n")
		sb.WriteString("|---|---|---|---|\n")
		for opName, count := range opCounts {
			ok := opSuccess[opName]
			fail := count - ok
			sb.WriteString(fmt.Sprintf("| %s | %d | %d | %d |\n", opName, count, ok, fail))
		}
		sb.WriteString("\n")
	}

	// Network Intercept Statistics
	sb.WriteString("## Proxy Network Messages\n\n")
	sb.WriteString(fmt.Sprintf("- **Total Intercepted Messages**: %d\n", totalMsgs))
	sb.WriteString(fmt.Sprintf("- **Delivered**: %d\n", deliveredMsgs))
	sb.WriteString(fmt.Sprintf("- **Dropped/Partitioned**: %d\n", droppedMsgs))
	sb.WriteString(fmt.Sprintf("- **In-Flight/Lost**: %d\n\n", sentMsgs))

	// Invariant Violations
	sb.WriteString("## Invariant Violations\n\n")
	if len(violations) == 0 {
		sb.WriteString("✓ No correctness invariant violations detected.\n\n")
	} else {
		sb.WriteString("| ID | Checker Name | Severity | Description | Evidence |\n")
		sb.WriteString("|---|---|---|---|---|\n")
		for _, v := range violations {
			sb.WriteString(fmt.Sprintf("| %d | %s | **%s** | %s | `%s` |\n", v.ID, v.CheckerName, v.Severity, v.Description, v.EvidenceJSON))
		}
		sb.WriteString("\n")
	}

	// Replay Instructions
	sb.WriteString("## Replay Campaign Instructions\n")
	sb.WriteString("To reproduce this exact execution timeline, execute the run command with the seed:\n")
	sb.WriteString(fmt.Sprintf("```bash\nfailforge run failforge.yml --seed %d\n```\n", seed))

	reportPath := filepath.Join(runDir, "report.md")
	if err := os.WriteFile(reportPath, []byte(sb.String()), 0644); err != nil {
		return fmt.Errorf("failed to write report.md: %w", err)
	}

	// Print summary to console
	fmt.Printf("Generated simulation run report in: %s\n", reportPath)
	fmt.Printf("Summary:\n")
	fmt.Printf("  Status:     %s\n", status)
	fmt.Printf("  Seed:       %d\n", seed)
	fmt.Printf("  Nodes:      %d running / crashed\n", len(nodes))
	fmt.Printf("  Operations: %d total (%d success, %d fail)\n", totalOps, successOps, failedOps)
	fmt.Printf("  Messages:   %d intercepted (%d dropped)\n", totalMsgs, droppedMsgs)
	if len(violations) > 0 {
		fmt.Printf("  Violations: %d invariant violations found!\n", len(violations))
	} else {
		fmt.Printf("  Violations: None\n")
	}

	return nil
}

func init() {
	RootCmd.AddCommand(reportCmd)
}
