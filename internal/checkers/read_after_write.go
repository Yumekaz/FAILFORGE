package checkers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"failforge/internal/model"
	"failforge/internal/store"
)

type ReadAfterWriteChecker struct{}

func (c *ReadAfterWriteChecker) Name() string {
	return "read_after_acknowledged_write"
}

func (c *ReadAfterWriteChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	ops, err := st.GetOperations(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch operations: %w", err)
	}

	// Group operations by key
	putOpsByKey := make(map[string][]*model.Operation)
	getOpsByKey := make(map[string][]*model.Operation)

	for _, op := range ops {
		if op.Status != "ok" {
			continue
		}

		opType := strings.ToLower(op.Operation)
		if opType == "put" {
			key := getKeyFromInput(op.InputJSON)
			if key != "" {
				putOpsByKey[key] = append(putOpsByKey[key], op)
			}
		} else if opType == "get" {
			key := getKeyFromInput(op.InputJSON)
			if key != "" {
				getOpsByKey[key] = append(getOpsByKey[key], op)
			}
		}
	}

	// Get union of all keys that have operations
	allKeys := make(map[string]bool)
	for k := range putOpsByKey {
		allKeys[k] = true
	}
	for k := range getOpsByKey {
		allKeys[k] = true
	}

	var violations []model.Violation

	// Check each key's history
	for key := range allKeys {
		puts := putOpsByKey[key]
		gets := getOpsByKey[key]
		if len(gets) == 0 {
			continue
		}

		// Sort puts by EndMs (completion time)
		sort.Slice(puts, func(i, j int) bool {
			return puts[i].EndMs < puts[j].EndMs
		})

		// Map of value to its index in the puts chronological sequence
		valueIdx := make(map[string]int)
		for idx, put := range puts {
			val := getValueFromInput(put.InputJSON)
			valueIdx[val] = idx
		}

		for _, get := range gets {
			getStart := get.StartMs
			getVal := getBodyFromOutput(get.OutputJSON)

			// 1. If the read value is not null/empty, check if it was ever written
			if getVal != "null" && getVal != "" {
				if _, exists := valueIdx[getVal]; !exists {
					violations = append(violations, model.Violation{
						RunID:        runID,
						CheckerName:  c.Name(),
						Severity:     "ERROR",
						Description:  fmt.Sprintf("Corrupt read on key '%s': GET returned value '%s' which was never successfully written", key, getVal),
						EvidenceJSON: fmt.Sprintf(`{"get_op_id":"%s","actual_value":"%s"}`, get.OpID, getVal),
					})
					continue
				}
			}

			// Find the latest PUT that completed before this GET started
			var latestPutBeforeGet *model.Operation
			latestPutIdx := -1
			for idx, put := range puts {
				if put.EndMs <= getStart {
					latestPutBeforeGet = put
					latestPutIdx = idx
				}
			}

			// If no PUT completed before GET started, GET can return anything (e.g. null, concurrent PUT value, or initial state)
			if latestPutBeforeGet == nil {
				continue
			}

			expectedVal := getValueFromInput(latestPutBeforeGet.InputJSON)

			// If the GET returned null, but a PUT already completed, it's a violation!
			if getVal == "null" || getVal == "" {
				violations = append(violations, model.Violation{
					RunID:       runID,
					CheckerName: c.Name(),
					Severity:    "ERROR",
					Description: fmt.Sprintf("Stale read on key '%s': GET started at %dms returned '%s' after PUT value '%s' completed at %dms",
						key, getStart, getVal, expectedVal, latestPutBeforeGet.EndMs),
					EvidenceJSON: fmt.Sprintf(`{"get_op_id":"%s","get_start_ms":%d,"expected_put_op_id":"%s","expected_value":"%s","actual_value":"%s"}`,
						get.OpID, getStart, latestPutBeforeGet.OpID, expectedVal, getVal),
				})
				continue
			}

			// Linearizability condition: read index must be equal or newer than the latest completed write index
			readIdx := valueIdx[getVal]
			if readIdx < latestPutIdx {
				violations = append(violations, model.Violation{
					RunID:       runID,
					CheckerName: c.Name(),
					Severity:    "ERROR",
					Description: fmt.Sprintf("Read-After-Write violation on key '%s': GET returned stale value '%s' (write #%d) but write '%s' (write #%d) already completed at %dms",
						key, getVal, readIdx+1, expectedVal, latestPutIdx+1, latestPutBeforeGet.EndMs),
					EvidenceJSON: fmt.Sprintf(`{"get_op_id":"%s","get_start_ms":%d,"actual_value":"%s","actual_write_index":%d,"expected_value":"%s","expected_write_index":%d}`,
						get.OpID, getStart, getVal, readIdx+1, expectedVal, latestPutIdx+1),
				})
			}
		}
	}

	return violations, nil
}

func getKeyFromInput(inputJSON string) string {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputJSON), &input); err == nil {
		if k, ok := input["key"].(string); ok {
			return k
		}
	}
	return ""
}

func getValueFromInput(inputJSON string) string {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputJSON), &input); err == nil {
		if v, ok := input["value"].(string); ok {
			return v
		}
		// support integer values from tests
		if v, ok := input["value"].(float64); ok {
			return fmt.Sprintf("%g", v)
		}
	}
	return ""
}

func getBodyFromOutput(outputJSON string) string {
	var output map[string]interface{}
	if err := json.Unmarshal([]byte(outputJSON), &output); err == nil {
		if b, ok := output["body"].(string); ok {
			return b
		}
	}
	return ""
}
