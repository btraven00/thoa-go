//go:build !unix

package thoa

// freeBytes is unknown off unix; the disk space check is skipped.
func freeBytes(string) (int64, bool) { return 0, false }
