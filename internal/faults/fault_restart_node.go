package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type RestartNodeFault struct{}

func (f *RestartNodeFault) Type() string {
	return "restart_node"
}

func (f *RestartNodeFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.Node
	if node == "" {
		node = cfg.GetParamString("node", "")
	}
	if node == "" {
		return fmt.Errorf("restart_node: node parameter is required")
	}
	return nil
}

func (f *RestartNodeFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.Node
	if node == "" {
		node = fctx.Config.GetParamString("node", "")
	}
	return fctx.Manager.RestartNode(ctx, node)
}
