package faults

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"failforge/internal/config"
	"failforge/internal/node"
)

func TestDiskWriteLossFault(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count:   1,
				Ports:   config.PortsConfig{Start: 8080},
				DataDir: tempDir + "/data-{node_id}",
			},
		},
	}

	nm := node.NewNodeManager(cfg, "run-1", tempDir, func(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {})
	cfg.System.Nodes.Command = "sleep 10"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := nm.StartNode(ctx, "node-1")
	if err != nil {
		t.Fatalf("failed to start mock node: %v", err)
	}
	defer nm.StopAll()

	dataDir, err := nm.GetDataDir("node-1")
	if err != nil {
		t.Fatalf("failed to get data dir: %v", err)
	}

	// Create some files: one modified recently, one modified older
	recentFile := filepath.Join(dataDir, "recent.txt")
	err = os.WriteFile(recentFile, []byte("recent content data here"), 0644)
	if err != nil {
		t.Fatalf("failed to write recent file: %v", err)
	}

	oldFile := filepath.Join(dataDir, "old.txt")
	err = os.WriteFile(oldFile, []byte("old content data here"), 0644)
	if err != nil {
		t.Fatalf("failed to write old file: %v", err)
	}
	// Backdate old file modification time
	oldTime := time.Now().Add(-10 * time.Second)
	err = os.Chtimes(oldFile, oldTime, oldTime)
	if err != nil {
		t.Fatalf("failed to backdate old file: %v", err)
	}

	fctx := &FaultContext{
		Config: &config.FaultConfig{
			Type: "disk_write_loss",
			Params: map[string]interface{}{
				"node":          "node-1",
				"loss_window_s": 5,
			},
		},
		Manager:   nm,
		StartTime: time.Now(),
	}

	fault := &DiskWriteLossFault{}
	err = fault.Inject(ctx, fctx)
	if err != nil {
		t.Fatalf("injection failed: %v", err)
	}

	// Verify recent file is truncated to 0
	recentInfo, err := os.Stat(recentFile)
	if err != nil {
		t.Fatalf("failed to stat recent file: %v", err)
	}
	if recentInfo.Size() != 0 {
		t.Errorf("expected recent file size to be 0, got %d", recentInfo.Size())
	}

	// Verify old file is intact
	oldInfo, err := os.Stat(oldFile)
	if err != nil {
		t.Fatalf("failed to stat old file: %v", err)
	}
	if oldInfo.Size() == 0 {
		t.Errorf("expected old file size to be non-zero, got 0")
	}
}

func TestPartialPersistenceFault(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count:   1,
				Ports:   config.PortsConfig{Start: 8080},
				DataDir: tempDir + "/data-{node_id}",
			},
		},
	}

	nm := node.NewNodeManager(cfg, "run-1", tempDir, func(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {})
	cfg.System.Nodes.Command = "sleep 10"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := nm.StartNode(ctx, "node-1")
	if err != nil {
		t.Fatalf("failed to start mock node: %v", err)
	}
	defer nm.StopAll()

	dataDir, err := nm.GetDataDir("node-1")
	if err != nil {
		t.Fatalf("failed to get data dir: %v", err)
	}

	// Create a file with a known size
	testFile := filepath.Join(dataDir, "test.txt")
	content := []byte("this is some content that is exactly 40 bytes long!") // 51 bytes
	err = os.WriteFile(testFile, content, 0644)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	originalSize := int64(len(content))

	fctx := &FaultContext{
		Config: &config.FaultConfig{
			Type: "partial_persistence",
			Params: map[string]interface{}{
				"node": "node-1",
			},
		},
		Manager:   nm,
		StartTime: time.Now(),
	}

	fault := &PartialPersistenceFault{}
	err = fault.Inject(ctx, fctx)
	if err != nil {
		t.Fatalf("injection failed: %v", err)
	}

	// Verify file is truncated to a size between 50% and 90% of original
	info, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}

	minExpected := int64(float64(originalSize) * 0.5)
	maxExpected := int64(float64(originalSize) * 0.9)

	if info.Size() < minExpected || info.Size() > maxExpected {
		t.Errorf("expected file size to be between %d and %d, got %d", minExpected, maxExpected, info.Size())
	}
}

func TestStaleSnapshotRestartFault(t *testing.T) {
	tempDir := t.TempDir()
	cfg := &config.Config{
		System: config.SystemConfig{
			Nodes: config.NodesConfig{
				Count:   1,
				Ports:   config.PortsConfig{Start: 8080},
				DataDir: tempDir + "/data-{node_id}",
			},
		},
	}

	nm := node.NewNodeManager(cfg, "run-1", tempDir, func(timeMs int64, nodeID string, eventType string, payload map[string]interface{}) {})
	cfg.System.Nodes.Command = "sleep 10"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start node, snapshot automatically takes place
	err := nm.StartNode(ctx, "node-1")
	if err != nil {
		t.Fatalf("failed to start mock node: %v", err)
	}
	defer nm.StopAll()

	dataDir, err := nm.GetDataDir("node-1")
	if err != nil {
		t.Fatalf("failed to get data dir: %v", err)
	}

	// Create snapshot 1 file
	snapFile1 := filepath.Join(dataDir, "snap1.txt")
	_ = os.WriteFile(snapFile1, []byte("snap 1 state"), 0644)

	// Explicitly take snapshot 1
	snap1Path, err := nm.TakeSnapshot("node-1")
	if err != nil {
		t.Fatalf("failed to take snapshot: %v", err)
	}

	// Write new data (which will not be in snap1)
	snapFile2 := filepath.Join(dataDir, "snap2.txt")
	_ = os.WriteFile(snapFile2, []byte("snap 2 state"), 0644)

	// 2. Inject stale snapshot restart pointing to snap1
	// Wait a moment so directory timestamps are different
	time.Sleep(10 * time.Millisecond)

	fctx := &FaultContext{
		Config: &config.FaultConfig{
			Type: "stale_snapshot_restart",
			Params: map[string]interface{}{
				"node":           "node-1",
				"snapshot_index": 1, // index 1 is the snapshot containing snap1.txt
			},
		},
		Manager:   nm,
		StartTime: time.Now(),
	}

	fault := &StaleSnapshotRestartFault{}
	// Make sure we stop node-1 before restoring to avoid conflicts (the fault handles it internally)
	err = fault.Inject(ctx, fctx)
	if err != nil {
		t.Fatalf("injection failed: %v", err)
	}

	// Verify that snapFile2 is gone (because we restored snapshot 1)
	if _, err := os.Stat(snapFile2); !os.IsNotExist(err) {
		t.Errorf("expected snap2.txt to be removed after restoring snapshot 1")
	}

	// Verify snapFile1 still exists (or rather, the data dir was restored to snapshot 1 state)
	// Wait, is snapFile1 in snap1Path? Yes, because we wrote snapFile1 before calling TakeSnapshot.
	// But wait! TakeSnapshot copies the contents of np.DataDir to a folder inside snapshots.
	// So yes, snapFile1 should be there. Let's check it.
	restoredFile := filepath.Join(dataDir, filepath.Base(snap1Path), "snap1.txt")
	// Wait! TakeSnapshot copies `np.DataDir` to `snapshots/node-1/timestamp/`.
	// E.g., it copies the directory `/tmp/data-node-1` to `/tmp/snapshots/node-1/timestamp`.
	// So `/tmp/snapshots/node-1/timestamp` has the files direct, e.g. `/tmp/snapshots/node-1/timestamp/snap1.txt`.
	// So when we restore, we copy `/tmp/snapshots/node-1/timestamp` back to `/tmp/data-node-1`.
	// Meaning `/tmp/data-node-1/snap1.txt` is recreated!
	restoredFile = filepath.Join(dataDir, "snap1.txt")
	if _, err := os.Stat(restoredFile); err != nil {
		t.Errorf("expected snap1.txt to exist after restoring snapshot, got error: %v", err)
	}
}
