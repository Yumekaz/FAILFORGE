package faults

import (
	"context"
	"fmt"
	"failforge/internal/config"
)

type PartitionFault struct{}

func (f *PartitionFault) Type() string {
	return "partition"
}

func (f *PartitionFault) Validate(cfg *config.FaultConfig) error {
	groups := parseGroups(cfg)
	if len(groups) == 0 {
		return fmt.Errorf("partition: groups parameter is required and must contain at least one group")
	}
	return nil
}

func (f *PartitionFault) Inject(ctx context.Context, fctx *FaultContext) error {
	groups := parseGroups(fctx.Config)
	if len(groups) > 0 {
		fctx.Proxy.SetPartition(groups)
	}
	return nil
}

func parseGroups(cfg *config.FaultConfig) [][]string {
	if len(cfg.Groups) > 0 {
		return cfg.Groups
	}
	if cfg.Params == nil {
		return nil
	}
	if val, ok := cfg.Params["groups"]; ok {
		if rawGroups, ok := val.([]interface{}); ok {
			var groups [][]string
			for _, rg := range rawGroups {
				if gList, ok := rg.([]interface{}); ok {
					var group []string
					for _, item := range gList {
						if nodeID, ok := item.(string); ok {
							group = append(group, nodeID)
						}
					}
					if len(group) > 0 {
						groups = append(groups, group)
					}
				}
			}
			return groups
		}
	}
	return nil
}
