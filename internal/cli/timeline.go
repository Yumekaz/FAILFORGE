package cli

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"failforge/internal/runner"
	"failforge/internal/store"

	"github.com/spf13/cobra"
)

var (
	htmlFlag    bool
	mermaidFlag bool
)

var timelineCmd = &cobra.Command{
	Use:   "timeline [run directory]",
	Short: "Visualize chronological timeline of events for a run",
	Long:  `Displays a detailed colorized terminal timeline, generates an interactive HTML dashboard, or extracts a Mermaid sequence diagram for any simulation run.`,
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

		// Fetch Run ID
		db, err := sql.Open("sqlite", dbPath)
		if err != nil {
			return err
		}
		defer db.Close()

		var runID string
		err = db.QueryRow("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1").Scan(&runID)
		if err != nil {
			return fmt.Errorf("failed to fetch run ID: %w", err)
		}

		if htmlFlag {
			err := runner.GenerateHTMLTimeline(runID, st, runDir)
			if err != nil {
				return fmt.Errorf("failed to generate HTML timeline: %w", err)
			}
			fmt.Printf("\033[1;32mInteractive HTML timeline generated successfully:\033[0m %s/timeline.html\n", runDir)
			return nil
		}

		if mermaidFlag {
			mermaidText, err := runner.GenerateMermaidSequence(runID, st)
			if err != nil {
				return fmt.Errorf("failed to generate Mermaid sequence diagram: %w", err)
			}
			fmt.Println(mermaidText)
			return nil
		}

		// Default: print colorized terminal timeline
		terminalText, err := runner.GenerateTerminalTimeline(runID, st)
		if err != nil {
			return fmt.Errorf("failed to generate terminal timeline: %w", err)
		}
		fmt.Println(terminalText)

		return nil
	},
}

func init() {
	timelineCmd.Flags().BoolVar(&htmlFlag, "html", false, "Generate interactive HTML timeline report")
	timelineCmd.Flags().BoolVar(&mermaidFlag, "mermaid", false, "Print Mermaid sequence diagram script")
	RootCmd.AddCommand(timelineCmd)
}
