package faults

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"failforge/internal/config"
)

type CpuPauseFault struct{}

func (f *CpuPauseFault) Type() string {
	return "cpu_pause"
}

func (f *CpuPauseFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.GetParamString("node", "")
	if node == "" {
		return fmt.Errorf("cpu_pause: node parameter is required")
	}
	return nil
}

func (f *CpuPauseFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.GetParamString("node", "")
	durationMs := fctx.Config.GetParamInt("duration_ms", 500)
	if durationMs <= 0 {
		return fmt.Errorf("cpu_pause: duration_ms must be positive")
	}

	// The initial pause is synchronous. A scheduled fault is only considered
	// injected once the target process is actually stopped.
	if err := fctx.Manager.PauseNode(node); err != nil {
		return fmt.Errorf("cpu_pause: %w", err)
	}

	// Resume later without blocking the scheduler loop. Recovery failures are
	// explicitly recorded rather than being silently ignored.
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(durationMs) * time.Millisecond):
		}

		if err := fctx.Manager.ResumeNode(node); err != nil {
			emitFaultOutcome(fctx, "FaultRecoveryFailed", node, err)
			return
		}
		emitFaultOutcome(fctx, "FaultCompleted", node, nil)
	}()

	return nil
}

func emitFaultOutcome(fctx *FaultContext, eventType, node string, err error) {
	if fctx.LogEvent == nil {
		return
	}
	payload := map[string]interface{}{"fault": "cpu_pause", "node": node}
	if err != nil {
		payload["error"] = err.Error()
	}
	encoded, _ := json.Marshal(payload)
	fctx.LogEvent(time.Since(fctx.StartTime).Milliseconds(), "Fault", eventType, string(encoded))
}
