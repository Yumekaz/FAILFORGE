package faults

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
	"failforge/internal/config"
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
	_ = fctx.Manager.KillNode(node)

	// Wait briefly for process cleanup
	time.Sleep(500 * time.Millisecond)

	// 2. Find most recently modified file in dataDir
	dataDir, err := fctx.Manager.GetDataDir(node)
	if err == nil && dataDir != "" {
		var newestFile string
		var newestTime time.Time
		var newestSize int64

		_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
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
		})

		// 3. Truncate most recently modified file to a random percentage (50% to 90%)
		if newestFile != "" && newestSize > 0 {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			pct := 0.5 + rng.Float64()*0.4 // 0.5 to 0.9
			newSize := int64(float64(newestSize) * pct)
			_ = os.Truncate(newestFile, newSize)
		}
	}

	// 4. Start node back up
	return fctx.Manager.StartNode(ctx, node)
}
