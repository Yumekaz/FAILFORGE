package checkers

import (
	"encoding/json"
	"fmt"
	"math"
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
	sessionID  string
	leaseToken int64
	endReason  string
}

type lockOwner struct {
	clientID  string
	sessionID string
}

// lockLeaseMetadata decodes the existing workload's HTTP response envelope. An
// absent TTL is NOT permission to assume expiry. Malformed coordination lease
// evidence fails the check instead of silently discarding the lock.
func lockLeaseMetadata(op *model.Operation, sessionID string) (int64, *float64, error) {
	if sessionID == "" {
		return 0, nil, nil // Generic, non-leased LOCK history.
	}
	var output struct {
		StatusCode int    `json:"status_code"`
		Body       string `json:"body"`
	}
	if err := json.Unmarshal([]byte(op.OutputJSON), &output); err != nil {
		return 0, nil, err
	}
	var lease struct {
		Path       string   `json:"path"`
		SessionID  string   `json:"session_id"`
		Token      int64    `json:"lease_token"`
		AcquiredAt *float64 `json:"acquired_at"`
		ExpiresAt  *float64 `json:"expires_at"`
		TTL        *float64 `json:"lease_ttl_seconds"`
	}
	if err := json.Unmarshal([]byte(output.Body), &lease); err != nil {
		return 0, nil, err
	}
	if output.StatusCode != 200 || lease.Path != getKeyFromInput(op.InputJSON) || lease.SessionID != sessionID || lease.Token <= 0 {
		return 0, nil, fmt.Errorf("successful lease response has inconsistent grant identity")
	}
	if lease.TTL == nil {
		return lease.Token, nil, nil
	}
	if lease.AcquiredAt == nil || lease.ExpiresAt == nil || *lease.TTL <= 0 {
		return 0, nil, fmt.Errorf("TTL lease response is missing its lifetime")
	}
	// Use a duration from ONE response, never subtract server wall time from
	// client monotonic time. TTL assignment precedes the create timestamp, and
	// may be shortened by session expiry. This is a conservative lower bound on
	// the grant lifetime, not proof of ownership after its earliest expiry.
	lifetime := math.Max(0, math.Min(*lease.TTL, *lease.ExpiresAt-*lease.AcquiredAt))
	if math.IsNaN(lifetime) || math.IsInf(lifetime, 0) || lifetime > float64(math.MaxInt64)/1000 {
		return 0, nil, fmt.Errorf("invalid lease lifetime")
	}
	return lease.Token, &lifetime, nil
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

		activeLocks := make(map[lockOwner]*lockInterval)
		var intervals []*lockInterval

		for _, op := range keyOps {
			opType := strings.ToUpper(op.Operation)
			var input struct {
				SessionID string `json:"session_id"`
			}
			if err := json.Unmarshal([]byte(op.InputJSON), &input); err != nil {
				return nil, fmt.Errorf("invalid lock input %s: %w", op.OpID, err)
			}
			owner := lockOwner{clientID: op.ClientID}
			if input.SessionID != "" {
				owner = lockOwner{sessionID: input.SessionID}
			}

			if opType == "LOCK" {
				token, lifetime, err := lockLeaseMetadata(op, input.SessionID)
				if err != nil {
					return nil, fmt.Errorf("invalid lock grant %s: %w", op.OpID, err)
				}
				if previous := activeLocks[owner]; previous != nil && previous.leaseToken == token {
					// Same-session acquire is idempotent, not a renewal. Retain the
					// ORIGINAL grant and deadline; replacing it hid early overlaps
					// and falsely extended a 10s lease until the history's end.
					continue
				}
				interval := &lockInterval{clientID: op.ClientID, sessionID: input.SessionID,
					startMs: op.EndMs, endMs: maxTime, lockOpID: op.OpID,
					leaseToken: token, endReason: "history_end"}
				if lifetime != nil {
					// The grant happened within this call. Use its invocation as
					// the earliest possible TTL origin, not its response (network
					// and quorum delays must not extend a lease).
					deadline := op.StartMs + int64(math.Floor(*lifetime*1000))
					if deadline <= interval.endMs {
						interval.endMs, interval.endReason = deadline, "lease_expiry_bound"
					}
				}
				intervals = append(intervals, interval)
				activeLocks[owner] = interval
			} else if opType == "UNLOCK" {
				if interval := activeLocks[owner]; interval != nil {
					// Release linearizes somewhere in its call. Only ownership
					// BEFORE invocation is certain; using the reply falsely flags
					// a successor while a successful release response is in flight.
					if op.StartMs <= interval.endMs {
						interval.endMs, interval.endReason = op.StartMs, "unlock_invoked"
					}
					interval.unlockOpID = op.OpID
					delete(activeLocks, owner)
				}
			}
		}

		// Check all pairs of intervals for overlap
		for i := 0; i < len(intervals); i++ {
			for j := i + 1; j < len(intervals); j++ {
				intI := intervals[i]
				intJ := intervals[j]

				// A coordination lease belongs to a session, even if that session
				// is used by multiple clients. Generic locks belong to clients.
				sameSession := intI.sessionID != "" && intI.sessionID == intJ.sessionID
				sameGenericClient := intI.sessionID == "" && intJ.sessionID == "" && intI.clientID == intJ.clientID
				if sameSession || sameGenericClient {
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

				// History end is inclusive: a terminal successful grant is an
				// observation, not an empty interval to drop. Expiry/release
				// bounds are exclusive, including at millisecond ties.
				endObserved := func(interval *lockInterval) bool {
					return overlapEnd < interval.endMs || interval.endReason == "history_end"
				}
				if overlapStart < overlapEnd || (overlapStart == overlapEnd && endObserved(intI) && endObserved(intJ)) {
					// Exclusivity violation found
					evidenceMap := map[string]interface{}{
						"key":           key,
						"client1":       intI.clientID,
						"lock1_op_id":   intI.lockOpID,
						"unlock1_op_id": intI.unlockOpID,
						"start1":        intI.startMs,
						"end1":          intI.endMs,
						"client2":       intJ.clientID,
						"lock2_op_id":   intJ.lockOpID,
						"unlock2_op_id": intJ.unlockOpID,
						"start2":        intJ.startMs,
						"end2":          intJ.endMs,
						"overlap_start": overlapStart,
						"overlap_end":   overlapEnd,
						"session1":      intI.sessionID,
						"session2":      intJ.sessionID,
						"lease_token1":  intI.leaseToken,
						"lease_token2":  intJ.leaseToken,
						"end_reason1":   intI.endReason,
						"end_reason2":   intJ.endReason,
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
