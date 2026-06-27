package faults

import (
	"context"
	"fmt"
	"time"
	"failforge/internal/config"
)

type StaleSnapshotRestartFault struct{}

func (f *StaleSnapshotRestartFault) Type() string {
	return "stale_snapshot_restart"
}

func (f *StaleSnapshotRestartFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.GetParamString("node", "")
	if node == "" {
		return fmt.Errorf("stale_snapshot_restart: node parameter is required")
	}
	return nil
}

func (f *StaleSnapshotRestartFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.GetParamString("node", "")
	snapshotIndex := fctx.Config.GetParamInt("snapshot_index", 0)

	// 1. Kill node
	_ = fctx.Manager.KillNode(node)

	// Wait briefly for process cleanup
	time.Sleep(500 * time.Millisecond)

	// 2. List snapshots
	snapshots, err := fctx.Manager.ListSnapshots(node)
	if err != nil || len(snapshots) == 0 {
		// If no snapshot exists, we just start the node back up
		return fctx.Manager.StartNode(ctx, node)
	}

	// Make sure snapshotIndex is in bounds
	if snapshotIndex < 0 {
		snapshotIndex = 0
	}
	if snapshotIndex >= len(snapshots) {
		snapshotIndex = len(snapshots) - 1
	}

	selectedSnapshot := snapshots[snapshotIndex]

	// 3. Restore snapshot
	if err := fctx.Manager.RestoreSnapshot(node, selectedSnapshot); err != nil {
		return err
	}

	// 4. Start node back up
	return fctx.Manager.StartNode(ctx, node)
}
