package checkers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"failforge/internal/model"
	"failforge/internal/store"
)

// FailedDeployProtectionChecker asserts that a failed or interrupted deploy
// never takes down or leaves a previously healthy service in a broken state.
type FailedDeployProtectionChecker struct{}

func (c *FailedDeployProtectionChecker) Name() string {
	return "failed_deploy_protection"
}

func (c *FailedDeployProtectionChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	ops, err := st.GetOperations(runID)
	if err != nil {
		return nil, err
	}

	var violations []model.Violation
	hasBeenHealthy := make(map[string]bool)

	// Sort chronologically by StartMs
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].StartMs < ops[j].StartMs
	})

	for _, op := range ops {
		opType := strings.ToLower(op.Operation)
		if opType == "deploy" {
			serviceName := getServiceNameFromDeployInput(op.InputJSON)
			if op.Status == "ok" && serviceName != "" {
				hasBeenHealthy[serviceName] = true
			}
		} else if opType == "status" {
			serviceName := getServiceNameFromStatusInput(op.InputJSON)
			if serviceName != "" && hasBeenHealthy[serviceName] && op.Status != "ok" {
				violations = append(violations, model.Violation{
					RunID:        runID,
					CheckerName:  c.Name(),
					Severity:     "ERROR",
					Description:  fmt.Sprintf("Failed deploy protection violation: Service '%s' became unhealthy (status returned fail) after a failed/interrupted deploy", serviceName),
					EvidenceJSON: fmt.Sprintf(`{"status_op_id":"%s","service_name":"%s"}`, op.OpID, serviceName),
				})
			}
		}
	}

	return violations, nil
}

// NoLostActiveVolumeChecker asserts that a volume restore operation does not
// result in service health degradation or broken states.
type NoLostActiveVolumeChecker struct{}

func (c *NoLostActiveVolumeChecker) Name() string {
	return "no_lost_active_volume"
}

func (c *NoLostActiveVolumeChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	ops, err := st.GetOperations(runID)
	if err != nil {
		return nil, err
	}

	var violations []model.Violation
	lastRestoreTime := int64(0)

	sort.Slice(ops, func(i, j int) bool {
		return ops[i].StartMs < ops[j].StartMs
	})

	for _, op := range ops {
		opType := strings.ToLower(op.Operation)
		if opType == "restore" && op.Status == "ok" {
			lastRestoreTime = op.EndMs
		} else if opType == "status" {
			if lastRestoreTime > 0 && op.StartMs > lastRestoreTime && op.Status != "ok" {
				violations = append(violations, model.Violation{
					RunID:        runID,
					CheckerName:  c.Name(),
					Severity:     "ERROR",
					Description:  "No lost active volume violation: Service status check returned fail after a successful volume restore",
					EvidenceJSON: fmt.Sprintf(`{"status_op_id":"%s","restore_end_ms":%d}`, op.OpID, lastRestoreTime),
				})
			}
		}
	}

	return violations, nil
}

func getServiceNameFromDeployInput(inputJSON string) string {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputJSON), &input); err == nil {
		if name, ok := input["name"].(string); ok {
			return name
		}
	}
	return ""
}

func getServiceNameFromStatusInput(inputJSON string) string {
	var input map[string]interface{}
	if err := json.Unmarshal([]byte(inputJSON), &input); err == nil {
		if name, ok := input["service_name"].(string); ok {
			return name
		}
	}
	return ""
}
