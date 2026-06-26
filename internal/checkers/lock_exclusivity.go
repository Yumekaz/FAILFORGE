package checkers

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"failforge/internal/model"
	"failforge/internal/store"
)

type LockExclusivityChecker struct{}

func (c *LockExclusivityChecker) Name() string {
	return "lock_exclusivity"
}

type lockInterval struct {
	clientID   string
	startMs    int64
	endMs      int64
	lockOpID   string
	unlockOpID string
}

func (c *LockExclusivityChecker) Check(runID string, st *store.Store) ([]model.Violation, error) {
	ops, err := st.GetOperations(runID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch operations: %w", err)
	}

	var maxTime int64
	for _, op := range ops {
		if op.EndMs > maxTime {
			maxTime = op.EndMs
		}
	}

	// Filter and group LOCK/UNLOCK operations by key
	opsByKey := make(map[string][]*model.Operation)
	for _, op := range ops {
		if op.Status != "ok" {
			continue
		}
		opType := strings.ToUpper(op.Operation)
		if opType == "LOCK" || opType == "UNLOCK" {
			key := getKeyFromInput(op.InputJSON)
			if key != "" {
				opsByKey[key] = append(opsByKey[key], op)
			}
		}
	}

	var violations []model.Violation

	// For each lock key, reconstruct and check exclusivity intervals
	for key, keyOps := range opsByKey {
		// Sort key operations chronologically by EndMs
		sort.Slice(keyOps, func(i, j int) bool {
			if keyOps[i].EndMs != keyOps[j].EndMs {
				return keyOps[i].EndMs < keyOps[j].EndMs
			}
			if keyOps[i].StartMs != keyOps[j].StartMs {
				return keyOps[i].StartMs < keyOps[j].StartMs
			}
			return keyOps[i].OpID < keyOps[j].OpID
		})

		activeLocks := make(map[string]*model.Operation) // clientID -> LOCK op
		var intervals []lockInterval

		for _, op := range keyOps {
			opType := strings.ToUpper(op.Operation)
			clientID := op.ClientID

			if opType == "LOCK" {
				activeLocks[clientID] = op
			} else if opType == "UNLOCK" {
				if lockOp, ok := activeLocks[clientID]; ok {
					intervals = append(intervals, lockInterval{
						clientID:   clientID,
						startMs:    lockOp.EndMs,
						endMs:      op.EndMs,
						lockOpID:   lockOp.OpID,
						unlockOpID: op.OpID,
					})
					delete(activeLocks, clientID)
				}
			}
		}

		// Any remaining active locks at the end of simulation are held until maxTime
		for clientID, lockOp := range activeLocks {
			intervals = append(intervals, lockInterval{
				clientID:   clientID,
				startMs:    lockOp.EndMs,
				endMs:      maxTime,
				lockOpID:   lockOp.OpID,
				unlockOpID: "",
			})
		}

		// Check all pairs of intervals for overlap
		for i := 0; i < len(intervals); i++ {
			for j := i + 1; j < len(intervals); j++ {
				intI := intervals[i]
				intJ := intervals[j]

				// Only check exclusivity violations between different clients
				if intI.clientID == intJ.clientID {
					continue
				}

				// Check overlap: max(start1, start2) < min(end1, end2)
				overlapStart := intI.startMs
				if intJ.startMs > overlapStart {
					overlapStart = intJ.startMs
				}
				overlapEnd := intI.endMs
				if intJ.endMs < overlapEnd {
					overlapEnd = intJ.endMs
				}

				if overlapStart < overlapEnd {
					// Exclusivity violation found
					evidenceMap := map[string]interface{}{
						"key":            key,
						"client1":        intI.clientID,
						"lock1_op_id":    intI.lockOpID,
						"unlock1_op_id":  intI.unlockOpID,
						"start1":         intI.startMs,
						"end1":           intI.endMs,
						"client2":        intJ.clientID,
						"lock2_op_id":    intJ.lockOpID,
						"unlock2_op_id":  intJ.unlockOpID,
						"start2":         intJ.startMs,
						"end2":           intJ.endMs,
						"overlap_start":  overlapStart,
						"overlap_end":    overlapEnd,
					}
					evidenceBytes, _ := json.Marshal(evidenceMap)

					violations = append(violations, model.Violation{
						RunID:        runID,
						CheckerName:  c.Name(),
						Severity:     "ERROR",
						Description:  fmt.Sprintf("Lock exclusivity violation on key '%s': client '%s' held lock during [%d, %d]ms overlapping with client '%s' holding lock during [%d, %d]ms", key, intI.clientID, intI.startMs, intI.endMs, intJ.clientID, intJ.startMs, intJ.endMs),
						EvidenceJSON: string(evidenceBytes),
					})
				}
			}
		}
	}

	return violations, nil
}
