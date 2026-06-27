package faults

import (
	"context"
	"failforge/internal/config"
)

type HealFault struct{}

func (f *HealFault) Type() string {
	return "heal"
}

func (f *HealFault) Validate(cfg *config.FaultConfig) error {
	return nil
}

func (f *HealFault) Inject(ctx context.Context, fctx *FaultContext) error {
	fctx.Proxy.ClearPartitions()
	fctx.Proxy.ClearFaultRules()
	
	// Dynamically check if the proxy has helper to clear advanced rules
	type advancedHealer interface {
		ClearAdvancedFaultRules()
	}
	if ah, ok := interface{}(fctx.Proxy).(advancedHealer); ok {
		ah.ClearAdvancedFaultRules()
	}
	return nil
}
