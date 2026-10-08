package checkers

import (
	"encoding/json"
	"fmt"
	"strings"

	"failforge/internal/model"
	"failforge/internal/store"
)

// WorkloadCoverageChecker prevents invariant checkers from passing vacuously
// when the campaign never exercised enough successful client operations.
type WorkloadCoverageChecker struct {
	MinimumSuccessfulOperations  int
	MinimumSuccessfulByOperation map[string]int
}

func (c *WorkloadCoverageChecker) Name() string { return "workload_coverage" }

func (c *WorkloadCoverageChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	ops, err := st.GetOperations(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch operations: %w", err)
	}

	counts := make(map[string]int)
	total := 0
	for _, op := range ops {
		if strings.EqualFold(op.Status, "ok") {
			total++
			counts[strings.ToLower(op.Operation)]++
		}
	}

	var violations []model.Violation
	if c.MinimumSuccessfulOperations > 0 && total < c.MinimumSuccessfulOperations {
		violations = append(violations, coverageViolation(runID, c.Name(),
			fmt.Sprintf("Only %d client operations succeeded; configured minimum is %d", total, c.MinimumSuccessfulOperations),
			map[string]interface{}{"successful": total, "required": c.MinimumSuccessfulOperations, "by_operation": counts}))
	}

	for operation, required := range c.MinimumSuccessfulByOperation {
		actual := counts[strings.ToLower(operation)]
		if required > 0 && actual < required {
			violations = append(violations, coverageViolation(runID, c.Name(),
				fmt.Sprintf("Only %d %s operations succeeded; configured minimum is %d", actual, operation, required),
				map[string]interface{}{"operation": operation, "successful": actual, "required": required, "by_operation": counts}))
		}
	}
	return violations, nil
}

// FaultInjectionCoverageChecker makes a seeded/scripted fault campaign fail
// when all scheduled fault attempts fail validation or injection.
type FaultInjectionCoverageChecker struct {
	MinimumSuccessfulInjections int
}

func (c *FaultInjectionCoverageChecker) Name() string { return "fault_coverage" }

func (c *FaultInjectionCoverageChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	events, err := st.GetEvents(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch fault events: %w", err)
	}
	count := 0
	var injectedTypes []string
	for _, event := range events {
		if event.Category != "Fault" || event.Type != "FaultInjected" {
			continue
		}
		count++
		var payload map[string]interface{}
		if json.Unmarshal([]byte(event.PayloadJSON), &payload) == nil {
			if faultType, ok := payload["type"].(string); ok {
				injectedTypes = append(injectedTypes, faultType)
			}
		}
	}

	if count >= c.MinimumSuccessfulInjections {
		return nil, nil
	}
	return []model.Violation{coverageViolation(runID, c.Name(),
		fmt.Sprintf("Only %d fault injections completed successfully; configured minimum is %d", count, c.MinimumSuccessfulInjections),
		map[string]interface{}{"successful_injections": count, "required": c.MinimumSuccessfulInjections, "fault_types": injectedTypes})}, nil
}

func coverageViolation(runID, checkerName, description string, evidence map[string]interface{}) model.Violation {
	encoded, _ := json.Marshal(evidence)
	return model.Violation{
		RunID:        runID,
		CheckerName:  checkerName,
		Severity:     "ERROR",
		Description:  description,
		EvidenceJSON: string(encoded),
	}
}
