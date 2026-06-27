package faults

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"failforge/internal/config"
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
	_ = fctx.Manager.KillNode(node)

	// Wait briefly for process cleanup to release files
	time.Sleep(500 * time.Millisecond)

	// 2. Scan and truncate recently modified files
	dataDir, err := fctx.Manager.GetDataDir(node)
	if err == nil && dataDir != "" {
		window := time.Duration(lossWindowS) * time.Second
		now := time.Now()
		_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if now.Sub(info.ModTime()) <= window {
				_ = os.Truncate(path, 0)
			}
			return nil
		})
	}

	// 3. Start node back up
	return fctx.Manager.StartNode(ctx, node)
}
