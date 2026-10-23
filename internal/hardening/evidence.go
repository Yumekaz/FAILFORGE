package hardening

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"failforge/internal/config"
	_ "modernc.org/sqlite"
)

type Coverage struct {
	RunID                string         `json:"run_id"`
	RunStatus            string         `json:"run_status"`
	Ended                bool           `json:"ended"`
	SuccessfulOperations int            `json:"successful_operations"`
	ByOperation          map[string]int `json:"successful_by_operation"`
	FaultEvents          map[string]int `json:"fault_events"`
	ConfirmedByType      map[string]int `json:"confirmed_by_type"`
	Violations           map[string]int `json:"non_info_violations_by_checker"`
	Missing              []string       `json:"missing_coverage"`
}

func readCoverage(dir string, cfg *config.Config) (Coverage, error) {
	c := Coverage{ByOperation: map[string]int{}, FaultEvents: map[string]int{}, ConfirmedByType: map[string]int{}, Violations: map[string]int{}}
	path := filepath.Join(dir, "history.sqlite")
	if _, err := os.Stat(path); err != nil {
		return c, err
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return c, err
	}
	defer db.Close()
	var ended sql.NullString
	var count int
	if err := db.QueryRow("SELECT count(*) FROM runs").Scan(&count); err != nil {
		return c, err
	}
	if count != 1 {
		return c, fmt.Errorf("expected exactly one run record, got %d", count)
	}
	if err := db.QueryRow("SELECT id,status,ended_at FROM runs").Scan(&c.RunID, &c.RunStatus, &ended); err != nil {
		return c, err
	}
	c.Ended = ended.Valid
	rows, err := db.Query("SELECT lower(operation),count(*) FROM operations WHERE run_id=? AND lower(status)='ok' GROUP BY lower(operation)", c.RunID)
	if err != nil {
		return c, err
	}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			rows.Close()
			return c, err
		}
		c.ByOperation[name] = n
		c.SuccessfulOperations += n
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return c, err
	}
	rows.Close()
	rows, err = db.Query("SELECT type,payload_json FROM events WHERE run_id=? AND category='Fault'", c.RunID)
	if err != nil {
		return c, err
	}
	for rows.Next() {
		var kind, payload string
		if err := rows.Scan(&kind, &payload); err != nil {
			rows.Close()
			return c, err
		}
		c.FaultEvents[kind]++
		if kind == "FaultInjected" {
			var f config.FaultConfig
			if err := json.Unmarshal([]byte(payload), &f); err != nil || f.Type == "" {
				rows.Close()
				return c, fmt.Errorf("malformed fault confirmation: %s", payload)
			}
			c.ConfirmedByType[f.Type]++
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return c, err
	}
	rows.Close()
	rows, err = db.Query("SELECT checker_name,count(*) FROM violations WHERE run_id=? AND upper(severity)!='INFO' GROUP BY checker_name", c.RunID)
	if err != nil {
		return c, err
	}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			rows.Close()
			return c, err
		}
		c.Violations[name] = n
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return c, err
	}
	rows.Close()
	minimum := cfg.Workload.MinimumSuccessfulOperations
	if minimum < 1 {
		minimum = 1
	}
	if c.SuccessfulOperations < minimum {
		c.Missing = append(c.Missing, fmt.Sprintf("successful operations %d < %d", c.SuccessfulOperations, minimum))
	}
	for op, n := range cfg.Workload.MinimumSuccessfulByOperation {
		if c.ByOperation[strings.ToLower(op)] < n {
			c.Missing = append(c.Missing, fmt.Sprintf("%s operations %d < %d", op, c.ByOperation[strings.ToLower(op)], n))
		}
	}
	if c.FaultEvents["FaultInjected"] < cfg.Faults.MinimumSuccessfulInjections {
		c.Missing = append(c.Missing, "insufficient confirmed fault injections")
	}
	return c, nil
}

func classify(exit int, c Coverage, evidenceErr error) string {
	if evidenceErr != nil {
		return "CRASHED"
	}
	if !c.Ended || c.RunStatus == "ABORTED" || c.RunStatus == "RUNNING" {
		return "INTERRUPTED"
	}
	if c.FaultEvents["FaultInjectionFailed"] > 0 || c.FaultEvents["FaultRecoveryFailed"] > 0 {
		return "FAILED"
	}
	for checker, n := range c.Violations {
		if n > 0 && checker != "workload_coverage" && checker != "fault_coverage" {
			return "FAILED"
		}
	}
	if len(c.Missing) > 0 {
		return "NO_COVERAGE"
	}
	if c.RunStatus == "FAILED" {
		return "FAILED"
	}
	if exit != 0 || c.RunStatus != "PASSED" {
		return "CRASHED"
	}
	if len(c.Violations) != 0 {
		return "FAILED"
	}
	return "PASSED"
}
