package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type DropMessagesFault struct{}

func (f *DropMessagesFault) Type() string {
	return "drop_messages"
}

func (f *DropMessagesFault) Validate(cfg *config.FaultConfig) error {
	from := cfg.From
	if from == "" {
		from = cfg.GetParamString("from", "")
	}
	to := cfg.To
	if to == "" {
		to = cfg.GetParamString("to", "")
	}
	if from == "" || to == "" {
		return fmt.Errorf("drop_messages: from and to parameters are required")
	}
	return nil
}

func (f *DropMessagesFault) Inject(ctx context.Context, fctx *FaultContext) error {
	from := fctx.Config.From
	if from == "" {
		from = fctx.Config.GetParamString("from", "")
	}
	to := fctx.Config.To
	if to == "" {
		to = fctx.Config.GetParamString("to", "")
	}
	
	fctx.Proxy.SetDropRule(from, to, true)
	return nil
}
