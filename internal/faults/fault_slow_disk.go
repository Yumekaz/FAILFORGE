package faults

import (
	"context"
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

	go func() {
		endTime := time.Now().Add(time.Duration(durationMs) * time.Millisecond)
		for time.Now().Before(endTime) {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Pause the node
			_ = fctx.Manager.PauseNode(node)

			// Wait stall duration
			select {
			case <-ctx.Done():
				_ = fctx.Manager.ResumeNode(node)
				return
			case <-time.After(time.Duration(stallMs) * time.Millisecond):
			}

			// Resume the node
			_ = fctx.Manager.ResumeNode(node)

			// Wait interval duration
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(intervalMs) * time.Millisecond):
			}
		}
	}()

	return nil
}
