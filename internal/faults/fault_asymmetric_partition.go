package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type AsymmetricPartitionFault struct{}

func (f *AsymmetricPartitionFault) Type() string {
	return "asymmetric_partition"
}

func (f *AsymmetricPartitionFault) Validate(cfg *config.FaultConfig) error {
	from := cfg.GetParamString("from", "")
	to := cfg.GetParamString("to", "")
	if from == "" || to == "" {
		return fmt.Errorf("asymmetric_partition: from and to parameters are required")
	}
	return nil
}

func (f *AsymmetricPartitionFault) Inject(ctx context.Context, fctx *FaultContext) error {
	from := fctx.Config.GetParamString("from", "")
	to := fctx.Config.GetParamString("to", "")
	fctx.Proxy.SetAsymmetricBlock(from, to, true)
	return nil
}
