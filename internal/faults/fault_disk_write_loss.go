package faults

import (
	"context"
	"failforge/internal/config"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type DiskWriteLossFault struct{}

func (f *DiskWriteLossFault) Type() string {
	return "disk_write_loss"
}

func (f *DiskWriteLossFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.GetParamString("node", "")
	if node == "" {
		return fmt.Errorf("disk_write_loss: node parameter is required")
	}
	return nil
}

func (f *DiskWriteLossFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.GetParamString("node", "")
	lossWindowS := fctx.Config.GetParamInt("loss_window_s", 2)

	// 1. Kill node
	if err := fctx.Manager.KillNode(node); err != nil {
		return fmt.Errorf("disk_write_loss: kill %s: %w", node, err)
	}

	// Wait briefly for process cleanup to release files
	time.Sleep(500 * time.Millisecond)

	// 2. Scan and truncate recently modified files
	dataDir, err := fctx.Manager.GetDataDir(node)
	if err != nil {
		return fmt.Errorf("disk_write_loss: locate data for %s: %w", node, err)
	}
	if dataDir == "" {
		return fmt.Errorf("disk_write_loss: empty data directory for %s", node)
	}
	window := time.Duration(lossWindowS) * time.Second
	now := time.Now()
	truncated := false
	err = filepath.Walk(dataDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || now.Sub(info.ModTime()) > window {
			return nil
		}
		if err := os.Truncate(path, 0); err != nil {
			return err
		}
		truncated = true
		return nil
	})
	if err != nil {
		return fmt.Errorf("disk_write_loss: truncate recent data for %s: %w", node, err)
	}
	if !truncated {
		return fmt.Errorf("disk_write_loss: no recently modified file was truncated for %s", node)
	}

	// 3. Start node back up
	return fctx.Manager.StartNode(ctx, node)
}
