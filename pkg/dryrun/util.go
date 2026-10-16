package dryrun

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

// cleanPath resolves a path to an absolute, cleaned form.
func cleanPath(p string) string {
	if p == "" {
		return ""
	}
	abs, err := filepath.Abs(filepath.Clean(p))
	if err != nil {
		return filepath.Clean(p)
	}
	return abs
}

// withinRoot reports whether abs is root itself or located below root. Both
// arguments must be cleaned absolute paths.
func withinRoot(root, abs string) bool {
	if abs == root {
		return true
	}
	rel, err := filepath.Rel(root, abs)
	if err != nil || rel == "." {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) &&
		!filepath.IsAbs(rel)
}

// relativeTo returns the root-relative path using forward slashes so that
// reports and plan IDs are stable across platforms.
func relativeTo(root, abs string) string {
	rel, err := filepath.Rel(root, abs)
	if err != nil {
		return filepath.ToSlash(abs)
	}
	return filepath.ToSlash(rel)
}

// sortStrings sorts in place.
func sortStrings(s []string) {
	sort.Strings(s)
}

// hashBytes returns the hex-encoded sha256 digest of b.
func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// hashFile streams the file content through sha256.
func hashFile(f *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// perm returns the permission bits of mode in a platform-neutral form.
func perm(mode os.FileMode) uint32 {
	return uint32(mode.Perm())
}

// modeSatisfied reports whether on-disk permission bits satisfy the requested
// mode. POSIX compares exact permission bits; Windows only persists the
// read-only flag, so owner writability is all that can be verified there.
func modeSatisfied(want, got os.FileMode) bool {
	if runtime.GOOS != "windows" {
		return perm(want) == perm(got)
	}
	wantReadOnly := want&0o200 == 0
	gotReadOnly := got&0o200 == 0
	return wantReadOnly == gotReadOnly
}
