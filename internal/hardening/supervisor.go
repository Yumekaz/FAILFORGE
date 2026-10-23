package hardening

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type processIdentity struct {
	PID   int
	Start string
}

func identity(pid int) (processIdentity, int, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return processIdentity{}, 0, err
	}
	// comm may contain spaces or parentheses. Fields after its final ')' start
	// with state, ppid; starttime is field 22 of proc(5).
	i := strings.LastIndexByte(string(b), ')')
	if i < 0 {
		return processIdentity{}, 0, fmt.Errorf("malformed proc stat")
	}
	f := strings.Fields(string(b)[i+1:])
	if len(f) < 20 {
		return processIdentity{}, 0, fmt.Errorf("short proc stat")
	}
	ppid, err := strconv.Atoi(f[1])
	return processIdentity{PID: pid, Start: f[19]}, ppid, err
}

func trackDescendants(parent processIdentity, owned map[int]processIdentity) {
	current, _, err := identity(parent.PID)
	if err != nil || current != parent {
		return
	}
	// Go can fork from any OS thread, not only the thread-group leader.
	tasks, err := os.ReadDir(fmt.Sprintf("/proc/%d/task", parent.PID))
	if err != nil {
		return
	}
	for _, task := range tasks {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%s/children", parent.PID, task.Name()))
		if err != nil {
			continue
		}
		for _, field := range strings.Fields(string(b)) {
			pid, err := strconv.Atoi(field)
			if err != nil {
				continue
			}
			child, ppid, err := identity(pid)
			if err == nil && ppid == parent.PID {
				owned[pid] = child
				trackDescendants(child, owned)
			}
		}
	}
}

func signalOwned(p processIdentity, sig syscall.Signal) {
	if current, _, err := identity(p.PID); err == nil && current == p {
		// NodeManager creates separate process groups. Kill the group only when
		// this exact owned process is still its leader; never use broad pkill.
		if pgid, err := syscall.Getpgid(p.PID); err == nil && pgid == p.PID {
			_ = syscall.Kill(-pgid, sig)
		} else {
			_ = syscall.Kill(p.PID, sig)
		}
	}
}

func treeBytes(root string) (int64, error) {
	var size int64
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		count++
		if count > 20000 {
			return fmt.Errorf("evidence file-count limit exceeded")
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func checkPorts(ports []int) error {
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	for _, port := range ports {
		l, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", port))
		if err != nil {
			return fmt.Errorf("required port %d unavailable (nothing launched): %w", port, err)
		}
		listeners = append(listeners, l)
		l6, err := net.Listen("tcp6", fmt.Sprintf("[::1]:%d", port))
		if err == nil {
			listeners = append(listeners, l6)
		} else if !strings.Contains(err.Error(), "address family not supported") && !strings.Contains(err.Error(), "cannot assign requested address") {
			return fmt.Errorf("IPv6 port %d unavailable: %w", port, err)
		}
	}
	return nil
}

// execute allows the normal CLI to clean up on TERM. If it hangs, kill only
// identity-checked descendants, including node process groups, before reaping.
// Storage thresholds are sampled, not a filesystem quota (see docs).
func execute(ctx context.Context, cmd *exec.Cmd, root, runDir string, timeout time.Duration, maxBytes, maxRunBytes int64) (int, string, error) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return -1, "CRASHED", err
	}
	parent, _, err := identity(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return -1, "CRASHED", err
	}
	owned := map[int]processIdentity{}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var reason string
	var runErr error
loop:
	for {
		trackDescendants(parent, owned)
		// Visit already-owned groups too, even if an intermediate shell exited.
		for _, p := range owned {
			trackDescendants(p, owned)
		}
		select {
		case runErr = <-done:
			break loop
		case <-ctx.Done():
			reason = "INTERRUPTED"
			break loop
		case <-timer.C:
			reason = "TIMED_OUT"
			break loop
		case <-ticker.C:
			n, err := treeBytes(runDir)
			if err != nil || n >= maxRunBytes {
				reason = "STORAGE_LIMIT"
				break loop
			}
			n, err = treeBytes(root)
			if err != nil || n >= maxBytes {
				reason = "STORAGE_LIMIT"
				break loop
			}
		}
	}
	if reason != "" {
		trackDescendants(parent, owned)
		signalOwned(parent, syscall.SIGTERM)
		select {
		case runErr = <-done:
		case <-time.After(3 * time.Second):
			for _, p := range owned {
				trackDescendants(p, owned)
				signalOwned(p, syscall.SIGKILL)
			}
			signalOwned(parent, syscall.SIGKILL)
			runErr = <-done
		}
	}
	for _, p := range owned {
		signalOwned(p, syscall.SIGKILL)
	}
	code := cmd.ProcessState.ExitCode()
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		code = 128 + int(status.Signal())
	}
	if n, err := treeBytes(runDir); reason == "" && (err != nil || n >= maxRunBytes) {
		reason = "STORAGE_LIMIT"
	}
	if n, err := treeBytes(root); reason == "" && (err != nil || n >= maxBytes) {
		reason = "STORAGE_LIMIT"
	}
	return code, reason, runErr
}
