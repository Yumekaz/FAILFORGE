package cli

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"failforge/internal/store"

	"github.com/spf13/cobra"
	_ "modernc.org/sqlite"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31;1m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
)

var historyCmd = &cobra.Command{
	Use:   "history [run directory]",
	Short: "Print a chronological timeline of events for a test run",
	Long:  `Reads the SQLite event logs from the specified run directory (or runs/latest if omitted) and outputs a detailed ASCII colored timeline.`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		runDir := ""
		if len(args) == 1 {
			runDir = args[0]
		} else {
			// Resolve latest run directory
			data, err := os.ReadFile("runs/latest.txt")
			if err != nil {
				// Try reading as a symlink directly
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

		return renderTimeline(st, dbPath)
	},
}

func formatTime(ms int64) string {
	mins := ms / 60000
	secs := (ms % 60000) / 1000
	milli := ms % 1000
	return fmt.Sprintf("[%02d:%02d.%03d]", mins, secs, milli)
}

func renderTimeline(st *store.Store, dbPath string) error {
	// Query the first run ID from runs table
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database directly: %w", err)
	}
	defer db.Close()

	var runID string
	var seed int64
	var status string
	err = db.QueryRow("SELECT id, seed, status FROM runs ORDER BY started_at DESC LIMIT 1").Scan(&runID, &seed, &status)
	if err != nil {
		return fmt.Errorf("failed to scan run record: %w", err)
	}

	events, err := st.GetEvents(runID)
	if err != nil {
		return fmt.Errorf("failed to fetch events: %w", err)
	}

	fmt.Printf("%s=== Timeline for Run ID: %s (Seed: %d, Final Status: %s) ===%s\n", colorPurple, runID, seed, status, colorReset)

	for _, e := range events {
		timeStr := formatTime(e.TimeMs)

		var payload map[string]interface{}
		_ = json.Unmarshal([]byte(e.PayloadJSON), &payload)

		categoryColor := colorGray
		typeColor := colorReset
		details := ""

		switch e.Category {
		case "Run":
			categoryColor = colorPurple
			typeColor = colorPurple
			if e.Type == "RunStarted" {
				details = fmt.Sprintf("seed=%v config_hash=%v", payload["seed"], payload["config_hash"])
			} else if e.Type == "RunCompleted" {
				details = fmt.Sprintf("status=%v", payload["status"])
			}
		case "Node":
			if e.Type == "NodeCrashed" || e.Type == "NodeKilled" {
				categoryColor = colorRed
				typeColor = colorRed
				details = fmt.Sprintf("node=%s error=%v", payload["node_id"], payload["error"])
				if e.Type == "NodeKilled" {
					details = fmt.Sprintf("node=%s signal=%v", payload["node_id"], payload["signal"])
				}
			} else {
				categoryColor = colorBlue
				typeColor = colorGreen
				details = fmt.Sprintf("node=%s port=%v pid=%v", payload["node_id"], payload["port"], payload["pid"])
			}
		case "Message":
			if e.Type == "MessageDropped" {
				categoryColor = colorYellow
				typeColor = colorYellow
				details = fmt.Sprintf("msg_id=%s from=%s to=%s reason=%v", payload["message_id"], payload["from"], payload["to"], payload["reason"])
			} else {
				categoryColor = colorCyan
				typeColor = colorGray
				details = fmt.Sprintf("msg_id=%s from=%s to=%s", payload["message_id"], payload["from"], payload["to"])
				if e.Type == "MessageDelivered" {
					details += fmt.Sprintf(" latency=%vms", payload["latency_ms"])
				}
			}
		case "Violation":
			categoryColor = colorRed
			typeColor = colorRed
			details = fmt.Sprintf("checker=%s desc=%s", payload["checker_name"], payload["description"])
		}

		// Clean up payload details printing
		if details == "" {
			details = e.PayloadJSON
		}

		fmt.Printf("%s%s %s[%-7s] %s%-18s %s%s\n",
			colorGray, timeStr,
			categoryColor, strings.ToUpper(e.Category),
			typeColor, e.Type,
			colorReset, details,
		)
	}

	return nil
}

func init() {
	RootCmd.AddCommand(historyCmd)
}
