package checkers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"failforge/internal/model"
	"failforge/internal/store"
)

type LeaderUniquenessChecker struct{}

func (c *LeaderUniquenessChecker) Name() string {
	return "no_two_leaders"
}

func (c *LeaderUniquenessChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	events, err := st.GetEvents(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch timeline events: %w", err)
	}

	var violations []model.Violation

	termLeaders := make(map[int]string)        // term -> node_id
	activeLeaders := make(map[string]int64)     // node_id -> start_time_ms

	for _, e := range events {
		eventType := e.Type
		category := strings.ToLower(e.Category)

		if eventType == "LeaderElected" {
			nodeID, term, hasTerm := parseLeaderElectedPayload(e.PayloadJSON)
			if nodeID == "" {
				continue
			}

			// 1. Check term-based leader uniqueness
			if hasTerm {
				if existingLeader, ok := termLeaders[term]; ok && existingLeader != nodeID {
					evidenceMap := map[string]interface{}{
						"term":          term,
						"first_leader":  existingLeader,
						"second_leader": nodeID,
						"time_ms":       e.TimeMs,
						"event_id":      e.ID,
					}
					evidenceBytes, _ := json.Marshal(evidenceMap)

					violations = append(violations, model.Violation{
						RunID:        runID,
						CheckerName:  c.Name(),
						Severity:     "ERROR",
						Description:  fmt.Sprintf("Term leader uniqueness violation in term %d: both '%s' and '%s' claimed leadership", term, existingLeader, nodeID),
						EvidenceJSON: string(evidenceBytes),
					})
				}
				termLeaders[term] = nodeID
			}

			// 2. Check real-time leader uniqueness
			activeLeaders[nodeID] = e.TimeMs

			if len(activeLeaders) > 1 {
				var activeList []string
				for nid := range activeLeaders {
					activeList = append(activeList, nid)
				}
				sort.Strings(activeList)

				evidenceMap := map[string]interface{}{
					"active_leaders": activeList,
					"time_ms":        e.TimeMs,
					"event_id":       e.ID,
				}
				evidenceBytes, _ := json.Marshal(evidenceMap)

				violations = append(violations, model.Violation{
					RunID:        runID,
					CheckerName:  c.Name(),
					Severity:     "ERROR",
					Description:  fmt.Sprintf("Leader uniqueness violation: multiple active leaders %v at %dms", activeList, e.TimeMs),
					EvidenceJSON: string(evidenceBytes),
				})
			}
		} else if eventType == "LeaderStepDown" || eventType == "StepDown" {
			nodeID := parseNodeIDFromPayload(e.PayloadJSON)
			if nodeID != "" {
				delete(activeLeaders, nodeID)
			}
		} else if category == "node" && (eventType == "NodeKilled" || eventType == "NodeStopped" || eventType == "NodeCrashed") {
			nodeID := parseNodeIDFromPayload(e.PayloadJSON)
			if nodeID != "" {
				delete(activeLeaders, nodeID)
			}
		}
	}

	return violations, nil
}

func parseLeaderElectedPayload(payloadJSON string) (string, int, bool) {
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		return "", 0, false
	}
	nodeID, _ := payload["node_id"].(string)
	if nodeID == "" {
		nodeID, _ = payload["node"].(string)
	}

	termVal, hasTerm := payload["term"]
	if !hasTerm {
		return nodeID, 0, false
	}

	switch t := termVal.(type) {
	case float64:
		return nodeID, int(t), true
	case int:
		return nodeID, t, true
	}
	return nodeID, 0, false
}

func parseNodeIDFromPayload(payloadJSON string) string {
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err == nil {
		if nodeID, ok := payload["node_id"].(string); ok {
			return nodeID
		}
		if nodeID, ok := payload["node"].(string); ok {
			return nodeID
		}
	}
	return ""
}
