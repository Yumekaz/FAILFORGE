package checkers

import (
	"encoding/json"
	"fmt"
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
		opType := strings.ToLower(op.Operation)
		if opType == "put" {
			key := getKeyFromInput(op.InputJSON)
			if key != "" {
				putOpsByKey[key] = append(putOpsByKey[key], op)
			}
		} else if opType == "get" && op.Status == "ok" {
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

		for _, get := range gets {
			getStart := get.StartMs
			getVal := getBodyFromOutput(get.OutputJSON)
			var acknowledgedBefore []*model.Operation
			var candidates []*model.Operation
			for _, put := range puts {
				if put.Status == "ok" && put.EndMs <= getStart {
					acknowledgedBefore = append(acknowledgedBefore, put)
				}
				if put.StartMs <= get.EndMs && getValueFromInput(put.InputJSON) == getVal {
					candidates = append(candidates, put)
				}
			}
			nullRead := getVal == "null" || getVal == ""
			reason := ""
			if nullRead && len(acknowledgedBefore) != 0 {
				reason = "returned no value after an acknowledged write"
			} else if !nullRead && len(candidates) == 0 {
				reason = "returned a value with no preceding or overlapping write attempt"
			} else if !nullRead {
				validCandidate := false
				for _, candidate := range candidates {
					// A failed write can partially apply or complete after timeout.
					// Its effect is indeterminate, not proof of corrupted data.
					if candidate.Status != "ok" {
						validCandidate = true
						break
					}
					orderedBeforeNewerWrite := false
					for _, newer := range acknowledgedBefore {
						if candidate.EndMs < newer.StartMs {
							orderedBeforeNewerWrite = true
							break
						}
					}
					if !orderedBeforeNewerWrite {
						validCandidate = true
						break
					}
				}
				if !validCandidate {
					reason = "returned an older write superseded by a non-overlapping acknowledged write"
				}
			}
			if reason != "" {
				evidence, _ := json.Marshal(map[string]interface{}{"get_op_id": get.OpID, "get_start_ms": getStart, "actual_value": getVal})
				violations = append(violations, model.Violation{RunID: runID, CheckerName: c.Name(), Severity: "ERROR",
					Description: fmt.Sprintf("Read-After-Write violation on key %q: GET %s (%q)", key, reason, getVal), EvidenceJSON: string(evidence)})
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
