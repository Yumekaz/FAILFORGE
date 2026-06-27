package faults

import (
	"context"
	"fmt"
	"time"
	"failforge/internal/config"
)

type DelayMessagesFault struct{}

func (f *DelayMessagesFault) Type() string {
	return "delay_messages"
}

func (f *DelayMessagesFault) Validate(cfg *config.FaultConfig) error {
	from := cfg.From
	if from == "" {
		from = cfg.GetParamString("from", "")
	}
	to := cfg.To
	if to == "" {
		to = cfg.GetParamString("to", "")
	}
	delayMs := cfg.DelayMs
	if delayMs == 0 {
		delayMs = cfg.GetParamInt("delay_ms", 0)
	}
	if from == "" || to == "" || delayMs <= 0 {
		return fmt.Errorf("delay_messages: from, to, and delay_ms parameters are required and delay_ms must be positive")
	}
	return nil
}

func (f *DelayMessagesFault) Inject(ctx context.Context, fctx *FaultContext) error {
	from := fctx.Config.From
	if from == "" {
		from = fctx.Config.GetParamString("from", "")
	}
	to := fctx.Config.To
	if to == "" {
		to = fctx.Config.GetParamString("to", "")
	}
	delayMs := fctx.Config.DelayMs
	if delayMs == 0 {
		delayMs = fctx.Config.GetParamInt("delay_ms", 0)
	}
	
	fctx.Proxy.SetDelayRule(from, to, time.Duration(delayMs)*time.Millisecond)
	return nil
}
