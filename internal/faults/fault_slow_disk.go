package faults

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"failforge/internal/config"
)

type SlowDiskFault struct{}

func (f *SlowDiskFault) Type() string {
	return "slow_disk"
}

func (f *SlowDiskFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.GetParamString("node", "")
	if node == "" {
		return fmt.Errorf("slow_disk: node parameter is required")
	}
	return nil
}

func (f *SlowDiskFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.GetParamString("node", "")
	durationMs := fctx.Config.GetParamInt("duration_ms", 3000)
	stallMs := fctx.Config.GetParamInt("stall_ms", 30)
	intervalMs := fctx.Config.GetParamInt("interval_ms", 100)
	if durationMs <= 0 || stallMs <= 0 || intervalMs < 0 {
		return fmt.Errorf("slow_disk: duration_ms and stall_ms must be positive; interval_ms cannot be negative")
	}

	// Confirm the first pause before reporting a scheduled fault as injected.
	if err := fctx.Manager.PauseNode(node); err != nil {
		return fmt.Errorf("slow_disk: %w", err)
	}

	go func() {
		endTime := time.Now().Add(time.Duration(durationMs) * time.Millisecond)
		paused := true
		completed := false
		defer func() {
			if paused {
				f.resumeOrRecord(fctx, node)
			}
			if completed {
				emitSlowDiskOutcome(fctx, "FaultCompleted", node, nil)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Wait stall duration
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(stallMs) * time.Millisecond):
			}

			if !f.resumeOrRecord(fctx, node) {
				return
			}
			paused = false
			if !time.Now().Before(endTime) {
				completed = true
				return
			}

			// Wait interval duration
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(intervalMs) * time.Millisecond):
			}

			if !time.Now().Before(endTime) {
				completed = true
				return
			}
			if err := fctx.Manager.PauseNode(node); err != nil {
				emitSlowDiskOutcome(fctx, "FaultRecoveryFailed", node, err)
				return
			}
			paused = true
		}
	}()

	return nil
}

func (f *SlowDiskFault) resumeOrRecord(fctx *FaultContext, node string) bool {
	if err := fctx.Manager.ResumeNode(node); err != nil {
		emitSlowDiskOutcome(fctx, "FaultRecoveryFailed", node, err)
		return false
	}
	return true
}

func emitSlowDiskOutcome(fctx *FaultContext, eventType, node string, err error) {
	if fctx.LogEvent == nil {
		return
	}
	payload := map[string]interface{}{"fault": "slow_disk", "node": node}
	if err != nil {
		payload["error"] = err.Error()
	}
	encoded, _ := json.Marshal(payload)
	fctx.LogEvent(time.Since(fctx.StartTime).Milliseconds(), "Fault", eventType, string(encoded))
}
