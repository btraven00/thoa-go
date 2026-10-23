//go:build unix

package thoa

import (
	"path/filepath"
	"syscall"
)

// freeBytes reports space available to unprivileged users on the filesystem
// holding path (or its nearest existing parent).
func freeBytes(path string) (int64, bool) {
	path, _ = filepath.Abs(path)
	for {
		var st syscall.Statfs_t
		if syscall.Statfs(path, &st) == nil {
			return int64(st.Bavail) * int64(st.Bsize), true
		}
		parent := filepath.Dir(path)
		if parent == path {
			return 0, false
		}
		path = parent
	}
}
