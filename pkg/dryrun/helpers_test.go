package dryrun

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// treeEntry is one observed filesystem entry for before/after comparisons.
type treeEntry struct {
	rel   string
	isDir bool
	mode  uint32
	mod   time.Time
	size  int64
	sum   string
}

// time2020 is a fixed, distant mtime used so metadata changes are unambiguous.
var time2020 = time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)

// walkState records every entry under root, sorted by relative path.
func walkState(t *testing.T, root string) []treeEntry {
	t.Helper()
	out := []treeEntry{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		e := treeEntry{
			rel:   filepath.ToSlash(rel),
			isDir: d.IsDir(),
			mode:  perm(info.Mode()),
			mod:   info.ModTime(),
			size:  info.Size(),
		}
		if !d.IsDir() {
			f, oerr := os.Open(path)
			if oerr != nil {
				return oerr
			}
			defer f.Close()
			sum, herr := hashFile(f)
			if herr != nil {
				return herr
			}
			e.sum = sum
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s failed: %v", root, err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rel < out[j].rel })
	return out
}

// assertNoTempFiles fails if internal temp files remain under root.
func assertNoTempFiles(t *testing.T, root string) {
	t.Helper()
	bad := []string{}
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".k3s-dryrun-tmp-") || d.Name() == ".k3s-dryrun-journal" {
			bad = append(bad, path)
		}
		return nil
	})
	if len(bad) > 0 {
		t.Fatalf("temporary files leaked: %v", bad)
	}
}

// recoverySpecs builds a deterministic set spanning multiple directories.
func recoverySpecs(root string, when time.Time) []FileSpec {
	return []FileSpec{
		{Path: filepath.Join(root, "tls", "server-ca.crt"), Content: []byte("server-ca"), ModTime: when},
		{Path: filepath.Join(root, "tls", "server-ca.key"), Content: []byte("server-key"), ModTime: when},
		{Path: filepath.Join(root, "tls", "etcd", "peer-ca.crt"), Content: []byte("peer-ca"), ModTime: when},
		{Path: filepath.Join(root, "cred", "passwd"), Content: []byte("passwd-entry\n"), ModTime: when},
	}
}

func assertSpecsOnDisk(t *testing.T, specs []FileSpec) {
	t.Helper()
	for _, s := range specs {
		b, err := os.ReadFile(s.Path)
		if err != nil {
			t.Fatalf("expected %s on disk: %v", s.Path, err)
		}
		if string(b) != string(s.Content) {
			t.Fatalf("%s content = %q, want %q", s.Path, b, s.Content)
		}
		info, err := os.Stat(s.Path)
		if err != nil {
			t.Fatal(err)
		}
		if !s.ModTime.IsZero() && info.ModTime().Unix() != s.ModTime.Unix() {
			t.Fatalf("%s mtime = %v, want %v", s.Path, info.ModTime(), s.ModTime)
		}
		assertFileMode(t, s.Path, 0o600)
	}
}

// assertFileMode checks permission bits on platforms that enforce them.
func assertFileMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		// Windows does not persist POSIX permission bits; content/mtime are
		// still asserted by the caller.
		return
	}
	if perm(info.Mode()) != perm(want) {
		t.Fatalf("%s mode = %03o, want %03o", path, perm(info.Mode()), perm(want))
	}
}
