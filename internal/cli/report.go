package cli

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"failforge/internal/model"
	"failforge/internal/runner"
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

	// 6. Fetch Timeline Events
	events, err := st.GetEvents(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch timeline events: %w", err)
	}

	absRunDir, errAbs := filepath.Abs(runDir)
	if errAbs != nil {
		absRunDir = runDir
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

	// Chronological Event Timeline
	sb.WriteString("## Chronological Timeline of Events\n\n")
	if len(events) == 0 {
		sb.WriteString("No timeline events logged.\n\n")
	} else {
		for _, e := range events {
			desc := formatEventDescription(e)
			sb.WriteString(fmt.Sprintf("- `[%dms]` %s\n", e.TimeMs, desc))
		}
		sb.WriteString("\n")
	}

	// Replay Instructions
	sb.WriteString("## Replay Campaign Instructions\n\n")
	sb.WriteString("To reproduce this exact execution timeline, execute the replay command:\n")
	sb.WriteString(fmt.Sprintf("```bash\nfailforge replay %s\n```\n\n", runDir))

	// Automatically generate visual HTML timeline
	_ = runner.GenerateHTMLTimeline(runID, st, runDir)

	// Relevant Logs and Artifacts
	sb.WriteString("## Relevant Logs and Artifacts\n\n")
	sb.WriteString(fmt.Sprintf("- **Interactive Visual Timeline**: [timeline.html](file://%s/timeline.html)\n", filepath.ToSlash(absRunDir)))
	sb.WriteString(fmt.Sprintf("- **SQLite Database**: [history.sqlite](file://%s/history.sqlite)\n", filepath.ToSlash(absRunDir)))
	sb.WriteString(fmt.Sprintf("- **JSONL Event Stream**: [events.jsonl](file://%s/events.jsonl)\n", filepath.ToSlash(absRunDir)))
	sb.WriteString(fmt.Sprintf("- **Fault Schedule**: [faults.json](file://%s/faults.json)\n", filepath.ToSlash(absRunDir)))
	sb.WriteString("- **Node Logs**:\n")
	for _, n := range nodes {
		sb.WriteString(fmt.Sprintf("  - Node %s log: [%s.log](file://%s/logs/%s.log)\n", n.NodeID, n.NodeID, filepath.ToSlash(absRunDir), n.NodeID))
	}
	sb.WriteString("\n")

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

func formatEventDescription(e *model.Event) string {
	var payload map[string]interface{}
	_ = json.Unmarshal([]byte(e.PayloadJSON), &payload)

	switch e.Category {
	case "Run":
		if e.Type == "Violation" {
			desc, _ := payload["description"].(string)
			return fmt.Sprintf("⚠️ **Invariant Violation** (%s): %s", payload["checker_name"], desc)
		}
		return fmt.Sprintf("**Run Status**: %s", e.Type)
	case "Node":
		nodeID, _ := payload["node_id"].(string)
		if nodeID == "" {
			nodeID = e.Type
		}
		details := ""
		if pid, ok := payload["pid"]; ok {
			details = fmt.Sprintf(" (PID: %v, Port: %v)", pid, payload["port"])
		}
		return fmt.Sprintf("🖥️ **Node %s**: %s%s", nodeID, e.Type, details)
	case "Fault":
		nodeID, _ := payload["node"].(string)
		nodeStr := ""
		if nodeID != "" {
			nodeStr = fmt.Sprintf(" on node %s", nodeID)
		}
		return fmt.Sprintf("💥 **Fault Injected**: %s%s - Payload: `%s`", e.Type, nodeStr, e.PayloadJSON)
	case "Operation":
		opID, _ := payload["op_id"].(string)
		clientID, _ := payload["client_id"].(string)
		op, _ := payload["op"].(string)
		key, _ := payload["key"].(string)
		if e.Type == "OperationInvoked" {
			target, _ := payload["target"].(string)
			return fmt.Sprintf("📥 **%s** by %s: %s key '%s' targeting %s (ID: %s)", strings.ToUpper(e.Type), clientID, strings.ToUpper(op), key, target, opID)
		} else if e.Type == "OperationCompleted" {
			status, _ := payload["status"].(string)
			latency, _ := payload["latency_ms"].(float64)
			return fmt.Sprintf("📤 **%s** by %s: %s -> %s (latency: %gms, ID: %s)", strings.ToUpper(e.Type), clientID, strings.ToUpper(op), status, latency, opID)
		}
		return fmt.Sprintf("⚙️ **Operation %s**: %s", e.Type, e.PayloadJSON)
	default:
		return fmt.Sprintf("**%s**: %s", e.Type, e.PayloadJSON)
	}
}

func init() {
	RootCmd.AddCommand(reportCmd)
}
