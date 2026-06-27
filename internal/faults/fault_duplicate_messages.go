package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type DuplicateMessagesFault struct{}

func (f *DuplicateMessagesFault) Type() string {
	return "duplicate_messages"
}

func (f *DuplicateMessagesFault) Validate(cfg *config.FaultConfig) error {
	from := cfg.GetParamString("from", "")
	to := cfg.GetParamString("to", "")
	if from == "" || to == "" {
		return fmt.Errorf("duplicate_messages: from and to parameters are required")
	}
	return nil
}

func (f *DuplicateMessagesFault) Inject(ctx context.Context, fctx *FaultContext) error {
	from := fctx.Config.GetParamString("from", "")
	to := fctx.Config.GetParamString("to", "")
	fctx.Proxy.SetDuplicateRule(from, to, true)
	return nil
}
