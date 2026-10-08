package faults

import (
	"context"
	"failforge/internal/config"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

type PartialPersistenceFault struct{}

func (f *PartialPersistenceFault) Type() string {
	return "partial_persistence"
}

func (f *PartialPersistenceFault) Validate(cfg *config.FaultConfig) error {
	node := cfg.GetParamString("node", "")
	if node == "" {
		return fmt.Errorf("partial_persistence: node parameter is required")
	}
	return nil
}

func (f *PartialPersistenceFault) Inject(ctx context.Context, fctx *FaultContext) error {
	node := fctx.Config.GetParamString("node", "")

	// 1. Kill node
	if err := fctx.Manager.KillNode(node); err != nil {
		return fmt.Errorf("partial_persistence: kill %s: %w", node, err)
	}

	// Wait briefly for process cleanup
	time.Sleep(500 * time.Millisecond)

	// 2. Find most recently modified file in dataDir
	dataDir, err := fctx.Manager.GetDataDir(node)
	if err != nil {
		return fmt.Errorf("partial_persistence: locate data for %s: %w", node, err)
	}
	if dataDir == "" {
		return fmt.Errorf("partial_persistence: empty data directory for %s", node)
	}
	var newestFile string
	var newestTime time.Time
	var newestSize int64
	if err := filepath.Walk(dataDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newestFile = path
			newestSize = info.Size()
		}
		return nil
	}); err != nil {
		return fmt.Errorf("partial_persistence: scan data for %s: %w", node, err)
	}
	if newestFile == "" || newestSize == 0 {
		return fmt.Errorf("partial_persistence: no non-empty persistence file found for %s", node)
	}

	// Use the campaign seed and node ID so the selected corruption is reproducible.
	rng := rand.New(rand.NewSource(fctx.Seed + int64(crc32.ChecksumIEEE([]byte(node)))))
	pct := 0.5 + rng.Float64()*0.4 // 0.5 to 0.9
	newSize := int64(float64(newestSize) * pct)
	if err := os.Truncate(newestFile, newSize); err != nil {
		return fmt.Errorf("partial_persistence: truncate %s: %w", newestFile, err)
	}

	// 4. Start node back up
	return fctx.Manager.StartNode(ctx, node)
}
