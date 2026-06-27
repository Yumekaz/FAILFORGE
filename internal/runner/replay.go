package runner

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"failforge/internal/config"
	"failforge/internal/store"
)

type ReplayResult struct {
	OriginalRunID       string
	ReplayRunID         string
	Seed                int64
	OriginalViolations  int
	ReplayViolations    int
	ViolationReproduced bool
	ReplayOutputDir     string
}

// ReplayRun executes a replay campaign of a previous run.
func ReplayRun(ctx context.Context, runDir string) (*ReplayResult, error) {
	// 1. Read config.json from original run directory
	configPath := filepath.Join(runDir, "config.json")
	configData, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config.json from %s: %w", runDir, err)
	}

	var cfg config.Config
	if err := json.Unmarshal(configData, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config.json: %w", err)
	}

	// 2. Set faults mode to replay
	cfg.Faults.Mode = "replay"

	// 3. Setup replay output directory
	timestamp := time.Now().Unix()
	replayOutputDir := filepath.Join(runDir, "replays", fmt.Sprintf("replay-%d", timestamp))
	if err := os.MkdirAll(replayOutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create replay output directory: %w", err)
	}

	// 4. Copy faults.json from original run directory if it exists
	origFaultsPath := filepath.Join(runDir, "faults.json")
	destFaultsPath := filepath.Join(replayOutputDir, "faults.json")
	if _, err := os.Stat(origFaultsPath); err == nil {
		if err := copyFile(origFaultsPath, destFaultsPath); err != nil {
			return nil, fmt.Errorf("failed to copy faults.json: %w", err)
		}
	}

	// 5. Initialize the Replay Runner
	rn, err := NewRunner(&cfg, &cfg.Seed, replayOutputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize replay runner: %w", err)
	}
	defer rn.store.Close()

	// 6. Execute the Replay Campaign
	if err := rn.Run(ctx); err != nil {
		return nil, fmt.Errorf("replay execution failed: %w", err)
	}

	// 7. Retrieve statistics and compare results
	origDbPath := filepath.Join(runDir, "history.sqlite")
	replayDbPath := filepath.Join(replayOutputDir, "history.sqlite")

	origRunID, origViolations, err := getRunViolations(origDbPath)
	if err != nil {
		// Log error but do not fail replay completion
		origViolations = 0
	}

	replayRunID, replayViolations, err := getRunViolations(replayDbPath)
	if err != nil {
		replayViolations = 0
	}

	reproduced := replayViolations > 0

	return &ReplayResult{
		OriginalRunID:       origRunID,
		ReplayRunID:         replayRunID,
		Seed:                cfg.Seed,
		OriginalViolations:  origViolations,
		ReplayViolations:    replayViolations,
		ViolationReproduced: reproduced,
		ReplayOutputDir:     replayOutputDir,
	}, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func getRunViolations(dbPath string) (string, int, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return "", 0, err
	}
	defer db.Close()

	var runID string
	err = db.QueryRow("SELECT id FROM runs ORDER BY started_at DESC LIMIT 1").Scan(&runID)
	if err != nil {
		return "", 0, err
	}

	st, err := store.NewStore(dbPath)
	if err != nil {
		return runID, 0, err
	}
	defer st.Close()

	violations, err := st.GetViolations(runID)
	if err != nil {
		return runID, 0, err
	}

	// Filter out info level events like fault injections from actual correctness violations if severity != "info"
	actualViolationsCount := 0
	for _, v := range violations {
		if v.Severity != "info" {
			actualViolationsCount++
		}
	}

	return runID, actualViolationsCount, nil
}
