package faults

import (
	"context"
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

	// Inject asynchronously to avoid blocking the scheduler execution loop
	go func() {
		_ = fctx.Manager.PauseNode(node)
		
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(durationMs) * time.Millisecond):
		}

		_ = fctx.Manager.ResumeNode(node)
	}()

	return nil
}
