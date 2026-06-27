package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type KillNodeFault struct{}

func (f *KillNodeFault) Type() string {
	return "kill_node"
}

func (f *KillNodeFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.Node
	if node == "" {
		node = cfg.GetParamString("node", "")
	}
	if node == "" {
		return fmt.Errorf("kill_node: node parameter is required")
	}
	return nil
}

func (f *KillNodeFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.Node
	if node == "" {
		node = fctx.Config.GetParamString("node", "")
	}
	return fctx.Manager.KillNode(node)
}
