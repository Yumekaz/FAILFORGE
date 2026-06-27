package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type CorruptMessagesFault struct{}

func (f *CorruptMessagesFault) Type() string {
	return "corrupt_messages"
}

func (f *CorruptMessagesFault) Validate(cfg *config.FaultConfig) error {
	from := cfg.GetParamString("from", "")
	to := cfg.GetParamString("to", "")
	if from == "" || to == "" {
		return fmt.Errorf("corrupt_messages: from and to parameters are required")
	}
	return nil
}

func (f *CorruptMessagesFault) Inject(ctx context.Context, fctx *FaultContext) error {
	from := fctx.Config.GetParamString("from", "")
	to := fctx.Config.GetParamString("to", "")
	rate := fctx.Config.GetParamFloat64("corruption_rate", 0.1)
	fctx.Proxy.SetCorruptionRule(from, to, rate)
	return nil
}
