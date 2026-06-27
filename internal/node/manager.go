package node

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"failforge/internal/config"
)

type NodeState string

const (
	StateDeclared   NodeState = "DECLARED"
	StateStarting   NodeState = "STARTING"
	StateRunning    NodeState = "RUNNING"
	StatePaused     NodeState = "PAUSED"
	StateKilled     NodeState = "KILLED"
	StateRestarting NodeState = "RESTARTING"
	StateStopped    NodeState = "STOPPED"
	StateCrashed    NodeState = "CRASHED"
)

type EventCallback func(timeMs int64, nodeID string, eventType string, payload map[string]interface{})

type NodeProcess struct {
	ID         string
	Port       int
	DataDir    string
	CmdStr     string
	State      NodeState
	Cmd        *exec.Cmd
	CancelFunc context.CancelFunc
	StdoutFile *os.File
}

type NodeManager struct {
	mu        sync.RWMutex
	cfg       *config.Config
	runID     string
	nodes     map[string]*NodeProcess
	onEvent   EventCallback
	runsDir   string
	startTime time.Time
	wg        sync.WaitGroup
}

func NewNodeManager(cfg *config.Config, runID string, runsDir string, onEvent EventCallback) *NodeManager {
	nm := &NodeManager{
		cfg:       cfg,
		runID:     runID,
		nodes:     make(map[string]*NodeProcess),
		onEvent:   onEvent,
		runsDir:   runsDir,
		startTime: time.Now(),
	}

	// Initialize all node metadata
	count := cfg.System.Nodes.Count
	startPort := cfg.System.Nodes.Ports.Start
	proxyURL := fmt.Sprintf("http://localhost:%d", cfg.Network.ProxyPort)

	for i := 1; i <= count; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		port := startPort + (i - 1)

		// Replace placeholders in command and data directory
		r := strings.NewReplacer(
			"{node_id}", nodeID,
			"{port}", fmt.Sprintf("%d", port),
			"{proxy_url}", proxyURL,
			"{run_id}", runID,
		)

		cmdStr := r.Replace(cfg.System.Nodes.Command)
		dataDir := r.Replace(cfg.System.Nodes.DataDir)

		nm.nodes[nodeID] = &NodeProcess{
			ID:      nodeID,
			Port:    port,
			DataDir: dataDir,
			CmdStr:  cmdStr,
			State:   StateDeclared,
		}
	}

	return nm
}

func (nm *NodeManager) getElapsedTimeMs() int64 {
	return time.Since(nm.startTime).Milliseconds()
}

// StartAll starts all configured nodes.
func (nm *NodeManager) StartAll(ctx context.Context) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	for _, np := range nm.nodes {
		if err := nm.startNodeUnlocked(ctx, np, StateStarting); err != nil {
			return err
		}
	}
	return nil
}

// StopAll stops all node processes and waits for them to exit.
func (nm *NodeManager) StopAll() error {
	nm.mu.Lock()
	for _, np := range nm.nodes {
		nm.stopNodeUnlocked(np)
	}
	nm.mu.Unlock()

	nm.wg.Wait()
	return nil
}

func (nm *NodeManager) startNodeUnlocked(ctx context.Context, np *NodeProcess, triggerState NodeState) error {
	// Ensure directories exist
	if err := os.MkdirAll(np.DataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data dir: %w", err)
	}

	var logDir string
	if nm.runsDir == "runs" {
		logDir = filepath.Join(nm.runsDir, nm.runID, "logs")
	} else {
		logDir = filepath.Join(nm.runsDir, "logs")
	}
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log dir: %w", err)
	}

	logFilePath := filepath.Join(logDir, fmt.Sprintf("%s.log", np.ID))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(nodeCtx, "/bin/sh", "-c", np.CmdStr)

	// Create a pipe for stderr to redirect it to the same file
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	// Set pgid so we can kill subprocess trees if needed
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		cancel()
		return fmt.Errorf("failed to start node %s: %w", np.ID, err)
	}

	np.Cmd = cmd
	np.CancelFunc = cancel
	np.StdoutFile = logFile
	np.State = StateRunning

	nm.onEvent(nm.getElapsedTimeMs(), np.ID, string(triggerState)+"_COMPLETED", map[string]interface{}{
		"pid":      cmd.Process.Pid,
		"port":     np.Port,
		"command":  np.CmdStr,
		"data_dir": np.DataDir,
	})

	// Take a snapshot of the starting/restarting state
	_, _ = nm.takeSnapshotUnlocked(np)

	// Monitor process completion in the background
	nm.wg.Add(1)
	go nm.monitorProcess(np)

	return nil
}

func (nm *NodeManager) monitorProcess(np *NodeProcess) {
	defer nm.wg.Done()
	err := np.Cmd.Wait()

	nm.mu.Lock()
	defer nm.mu.Unlock()

	// If we closed the logfile or context, it was intentional
	if np.StdoutFile != nil {
		np.StdoutFile.Close()
		np.StdoutFile = nil
	}
	if np.CancelFunc != nil {
		np.CancelFunc()
		np.CancelFunc = nil
	}

	// Determine if exit was unexpected
	if np.State == StateRunning {
		np.State = StateCrashed
		exitCode := -1
		if np.Cmd.ProcessState != nil {
			exitCode = np.Cmd.ProcessState.ExitCode()
		}
		nm.onEvent(nm.getElapsedTimeMs(), np.ID, "NodeCrashed", map[string]interface{}{
			"exit_code": exitCode,
			"error":     fmt.Sprintf("%v", err),
		})
	}
}

func (nm *NodeManager) stopNodeUnlocked(np *NodeProcess) {
	if np.State == StateRunning || np.State == StatePaused {
		np.State = StateStopped
		if np.Cmd != nil && np.Cmd.Process != nil {
			// Kill the process group to clean up subprocesses
			syscall.Kill(-np.Cmd.Process.Pid, syscall.SIGTERM)

			// Wait a bit for graceful stop, otherwise kill
			time.AfterFunc(1*time.Second, func() {
				if np.Cmd != nil && np.Cmd.Process != nil {
					syscall.Kill(-np.Cmd.Process.Pid, syscall.SIGKILL)
				}
			})
		}
		if np.CancelFunc != nil {
			np.CancelFunc()
		}
		nm.onEvent(nm.getElapsedTimeMs(), np.ID, "NodeStopped", nil)
	}
}

// StartNode starts a specific node if not already running.
func (nm *NodeManager) StartNode(ctx context.Context, nodeID string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if np.State == StateRunning {
		return nil
	}

	return nm.startNodeUnlocked(ctx, np, StateStarting)
}

// KillNode immediately kills a node using SIGKILL.
func (nm *NodeManager) KillNode(nodeID string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if np.State != StateRunning && np.State != StatePaused {
		return fmt.Errorf("node %s is not running (state: %s)", nodeID, np.State)
	}

	np.State = StateKilled
	if np.Cmd != nil && np.Cmd.Process != nil {
		// Send SIGKILL to the process group
		syscall.Kill(-np.Cmd.Process.Pid, syscall.SIGKILL)
	}

	if np.CancelFunc != nil {
		np.CancelFunc()
	}

	nm.onEvent(nm.getElapsedTimeMs(), np.ID, "NodeKilled", map[string]interface{}{
		"signal": "SIGKILL",
	})

	return nil
}

// RestartNode kills and restarts a node.
func (nm *NodeManager) RestartNode(ctx context.Context, nodeID string) error {
	np, ok := func() (*NodeProcess, bool) {
		nm.mu.Lock()
		defer nm.mu.Unlock()
		np, ok := nm.nodes[nodeID]
		return np, ok
	}()

	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	// 1. Kill the node if running
	_ = nm.KillNode(nodeID)

	// Wait briefly for process cleanup to release ports
	time.Sleep(500 * time.Millisecond)

	// 2. Restart the node
	nm.mu.Lock()
	defer nm.mu.Unlock()

	return nm.startNodeUnlocked(ctx, np, StateRestarting)
}

// PauseNode pauses the node process using SIGSTOP.
func (nm *NodeManager) PauseNode(nodeID string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if np.State != StateRunning {
		return fmt.Errorf("node %s is not running (state: %s)", nodeID, np.State)
	}

	if np.Cmd != nil && np.Cmd.Process != nil {
		syscall.Kill(-np.Cmd.Process.Pid, syscall.SIGSTOP)
	}
	np.State = StatePaused

	nm.onEvent(nm.getElapsedTimeMs(), np.ID, "NodePaused", nil)
	return nil
}

// ResumeNode resumes the node process using SIGCONT.
func (nm *NodeManager) ResumeNode(nodeID string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if np.State != StatePaused {
		return fmt.Errorf("node %s is not paused (state: %s)", nodeID, np.State)
	}

	if np.Cmd != nil && np.Cmd.Process != nil {
		syscall.Kill(-np.Cmd.Process.Pid, syscall.SIGCONT)
	}
	np.State = StateRunning

	nm.onEvent(nm.getElapsedTimeMs(), np.ID, "NodeResumed", nil)
	return nil
}

// GetNodesStatus returns a copy of node states.
func (nm *NodeManager) GetNodesStatus() map[string]NodeState {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	status := make(map[string]NodeState)
	for id, np := range nm.nodes {
		status[id] = np.State
	}
	return status
}

// GetPort returns the port assigned to a node.
func (nm *NodeManager) GetPort(nodeID string) (int, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return 0, fmt.Errorf("node %s not found", nodeID)
	}
	return np.Port, nil
}

func (nm *NodeManager) TakeSnapshot(nodeID string) (string, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return "", fmt.Errorf("node %s not found", nodeID)
	}
	return nm.takeSnapshotUnlocked(np)
}

func (nm *NodeManager) takeSnapshotUnlocked(np *NodeProcess) (string, error) {
	// Create a unique timestamped path: runs/<runID>/snapshots/<nodeID>/<timestamp>
	timestamp := time.Now().Format("20060102150405.000")
	var snapshotDir string
	if nm.runsDir == "runs" {
		snapshotDir = filepath.Join(nm.runsDir, nm.runID, "snapshots", np.ID, timestamp)
	} else {
		snapshotDir = filepath.Join(nm.runsDir, "snapshots", np.ID, timestamp)
	}

	if err := os.MkdirAll(filepath.Dir(snapshotDir), 0755); err != nil {
		return "", err
	}

	if _, err := os.Stat(np.DataDir); os.IsNotExist(err) {
		return "", nil
	}

	cmd := exec.Command("cp", "-a", np.DataDir, snapshotDir)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to copy data dir to snapshot: %w", err)
	}

	// Prune older snapshots if we exceed 3
	parentDir := filepath.Dir(snapshotDir)
	files, err := os.ReadDir(parentDir)
	if err == nil {
		var dirs []string
		for _, f := range files {
			if f.IsDir() {
				dirs = append(dirs, f.Name())
			}
		}
		if len(dirs) > 3 {
			for i := 0; i < len(dirs)-3; i++ {
				os.RemoveAll(filepath.Join(parentDir, dirs[i]))
			}
		}
	}

	return snapshotDir, nil
}

func (nm *NodeManager) RestoreSnapshot(nodeID string, snapshotPath string) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if np.State == StateRunning || np.State == StatePaused {
		return fmt.Errorf("cannot restore snapshot while node %s is active (state: %s)", nodeID, np.State)
	}

	// Delete current data dir contents
	if err := os.RemoveAll(np.DataDir); err != nil {
		return fmt.Errorf("failed to clear data dir: %w", err)
	}

	// Copy snapshotPath back to np.DataDir
	cmd := exec.Command("cp", "-a", snapshotPath, np.DataDir)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	return nil
}

func (nm *NodeManager) ListSnapshots(nodeID string) ([]string, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var parentDir string
	if nm.runsDir == "runs" {
		parentDir = filepath.Join(nm.runsDir, nm.runID, "snapshots", nodeID)
	} else {
		parentDir = filepath.Join(nm.runsDir, "snapshots", nodeID)
	}

	files, err := os.ReadDir(parentDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var snapshots []string
	for _, f := range files {
		if f.IsDir() {
			snapshots = append(snapshots, filepath.Join(parentDir, f.Name()))
		}
	}
	return snapshots, nil
}

func (nm *NodeManager) GetDataDir(nodeID string) (string, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	np, ok := nm.nodes[nodeID]
	if !ok {
		return "", fmt.Errorf("node %s not found", nodeID)
	}
	return np.DataDir, nil
}
