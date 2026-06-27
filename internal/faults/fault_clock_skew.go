package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type ClockSkewFault struct{}

func (f *ClockSkewFault) Type() string {
	return "clock_skew"
}

func (f *ClockSkewFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.GetParamString("node", "")
	if node == "" {
		return fmt.Errorf("clock_skew: node parameter is required")
	}
	return nil
}

func (f *ClockSkewFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.GetParamString("node", "")
	offsetMs := fctx.Config.GetParamInt("offset_ms", 5000)
	fctx.Proxy.SetClockOffset(node, int64(offsetMs))
	return nil
}
