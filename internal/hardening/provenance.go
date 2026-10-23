package hardening

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const metadataLimit = 8 << 20

// Generated evidence is reported in raw status, but excluded from the source
// fingerprint. Otherwise saving provenance would change its own fingerprint.
var sourcePaths = []string{".", ":(exclude)validation-runs/**", ":(exclude)verification/**"}

type Revision struct {
	Root         string            `json:"root"`
	Commit       string            `json:"commit"`
	Dirty        bool              `json:"dirty"`
	Status       string            `json:"status"`
	SourceStatus string            `json:"source_status"`
	Diff         string            `json:"diff_binary_head"`
	Untracked    map[string]string `json:"untracked_sha256"`
	Fingerprint  string            `json:"fingerprint"`
}

type cappedBuffer struct{ data []byte }

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if len(b.data)+len(p) > metadataLimit {
		return 0, fmt.Errorf("provenance exceeds %d bytes", metadataLimit)
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

func git(root string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...)
	var out cappedBuffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %v in %s: %w", args, root, err)
	}
	return string(out.data), nil
}

func revision(root, snapshotDir string) (Revision, error) {
	r := Revision{Root: root, Untracked: map[string]string{}}
	var err error
	if r.Commit, err = git(root, "rev-parse", "HEAD"); err != nil {
		return r, err
	}
	r.Commit = strings.TrimSpace(r.Commit)
	if r.Status, err = git(root, "status", "--porcelain=v1", "--untracked-files=all"); err != nil {
		return r, err
	}
	r.Dirty = r.Status != ""
	if r.SourceStatus, err = git(root, append([]string{"status", "--porcelain=v1", "--untracked-files=all", "--"}, sourcePaths...)...); err != nil {
		return r, err
	}
	if r.Diff, err = git(root, append([]string{"diff", "--binary", "HEAD", "--"}, sourcePaths...)...); err != nil {
		return r, err
	}
	untracked, err := git(root, append([]string{"ls-files", "--others", "--exclude-standard", "-z", "--"}, sourcePaths...)...)
	if err != nil {
		return r, err
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s", r.Commit, r.SourceStatus, r.Diff)
	remaining := int64(metadataLimit - len(r.Diff))
	for _, name := range strings.Split(untracked, "\x00") {
		if name == "" {
			continue
		}
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if err != nil {
			return r, err
		}
		if !info.Mode().IsRegular() {
			return r, fmt.Errorf("untracked source is not a regular file: %s", path)
		}
		if info.Size() > remaining {
			return r, fmt.Errorf("untracked source exceeds provenance budget: %s", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return r, err
		}
		remaining -= int64(len(data))
		sum := sha256.Sum256(data)
		r.Untracked[name] = hex.EncodeToString(sum[:])
		fmt.Fprintf(h, "\x00%s:%o:%s", name, info.Mode().Perm(), r.Untracked[name])
		if snapshotDir != "" {
			dest := filepath.Join(snapshotDir, "untracked", name)
			if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
				return r, err
			}
			if err := os.WriteFile(dest, data, info.Mode().Perm()); err != nil {
				return r, err
			}
		}
	}
	r.Fingerprint = hex.EncodeToString(h.Sum(nil))
	return r, nil
}

func fileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// seal creates a tamper-evident inventory, then removes write bits. It is not a
// privileged filesystem immutable flag; the owner can explicitly unseal it.
func seal(dir string) error {
	entries := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if len(entries) >= 20000 {
			return fmt.Errorf("evidence file-count limit exceeded")
		}
		rel, _ := filepath.Rel(dir, path)
		if d.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			entries[rel] = "symlink:" + target
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unsupported evidence file: %s", path)
		}
		hash, err := fileHash(path)
		entries[rel] = hash
		return err
	})
	if err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(dir, "inventory.json"), entries); err != nil {
		return err
	}
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.Chmod(path, info.Mode().Perm()&^0222)
	})
}
