package dryrun

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func mustSnap(t *testing.T, root string, paths []string) Snapshot {
	t.Helper()
	snap, err := capture(context.Background(), root, paths, 0)
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}
	return snap
}

func writeTestFile(t *testing.T, path, content string, mod time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if !mod.IsZero() {
		if err := os.Chtimes(path, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBuildPlanBoundaries(t *testing.T) {
	root := t.TempDir()
	other := t.TempDir()

	// Parent occupied by a regular file.
	writeTestFile(t, filepath.Join(root, "blocker"), "x", time.Time{})
	// Existing directory target.
	if err := os.MkdirAll(filepath.Join(root, "adir"), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(other, "evil")
	escape := filepath.Join(root, "..", "evil")

	tests := []struct {
		name string
		spec FileSpec
		want *Error
	}{
		{"empty path", FileSpec{Path: ""}, ErrEmptyPath},
		{"relative path", FileSpec{Path: "tls/cert.crt"}, ErrRelativePath},
		{"parent traversal", FileSpec{Path: escape}, ErrPathOutsideRoot},
		{"absolute outside root", FileSpec{Path: outside}, ErrPathOutsideRoot},
		{"root itself", FileSpec{Path: root, Content: []byte("x")}, ErrPathOutsideRoot},
		{"duplicate target", FileSpec{Path: filepath.Join(root, "dup")}, ErrDuplicateTarget},
		{"parent is regular file", FileSpec{Path: filepath.Join(root, "blocker", "child")}, ErrParentNotDir},
		{"target is directory", FileSpec{Path: filepath.Join(root, "adir")}, ErrTargetIsDir},
		{"content too large", FileSpec{Path: filepath.Join(root, "big"), Content: make([]byte, 10)}, ErrFileTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			specs := []FileSpec{tt.spec}
			if tt.name == "duplicate target" {
				specs = append(specs, FileSpec{Path: tt.spec.Path})
			}
			cfg := PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, specPaths(specs))}
			if tt.name == "content too large" {
				cfg.MaxFileSize = 5
			}
			_, err := BuildPlan(cfg)
			var ce *Error
			if !errors.As(err, &ce) {
				t.Fatalf("expected classified error, got %v", err)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want code %s", err, tt.want.Code)
			}
			if ce.Phase != PhasePlan {
				t.Fatalf("phase = %s, want plan", ce.Phase)
			}
		})
	}
}

func specPaths(specs []FileSpec) []string {
	out := make([]string, 0, len(specs))
	for _, s := range specs {
		if s.Path != "" && filepath.IsAbs(filepath.Clean(s.Path)) {
			out = append(out, s.Path)
		}
	}
	return out
}

func TestBuildPlanActions(t *testing.T) {
	root := t.TempDir()
	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	newTime := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	create := filepath.Join(root, "tls", "new.crt")
	update := filepath.Join(root, "tls", "etcd", "old.crt")
	metadata := filepath.Join(root, "cred", "passwd")
	unchanged := filepath.Join(root, "tls", "same.key")
	empty := filepath.Join(root, "tls", "empty.file")
	unicode := filepath.Join(root, "tls", "caf\u00e9.crt")

	writeTestFile(t, update, "old-bytes", oldTime)
	writeTestFile(t, metadata, "same-bytes", oldTime)
	writeTestFile(t, unchanged, "forever", newTime)

	specs := []FileSpec{
		{Path: create, Content: []byte("new"), ModTime: newTime},
		{Path: update, Content: []byte("new-bytes"), ModTime: newTime},
		{Path: metadata, Content: []byte("same-bytes"), ModTime: newTime},
		{Path: unchanged, Content: []byte("forever"), ModTime: newTime},
		{Path: empty, Content: nil, ModTime: newTime},
		{Path: unicode, Content: []byte("unicode"), ModTime: newTime},
	}
	plan, err := BuildPlan(PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, specPaths(specs))})
	if err != nil {
		t.Fatalf("BuildPlan failed: %v", err)
	}

	byPath := map[string]Op{}
	for _, op := range plan.Ops {
		if _, dup := byPath[op.Path]; dup {
			t.Fatalf("duplicate op for %s", op.Path)
		}
		byPath[op.Path] = op
	}

	wantActions := map[string]Action{
		create:    ActionCreate,
		update:    ActionUpdate,
		metadata:  ActionSetTimes,
		empty:     ActionCreate,
		unicode:   ActionCreate,
	}
	for path, action := range wantActions {
		op, ok := byPath[path]
		if !ok {
			t.Fatalf("missing op for %s", path)
		}
		if op.Action != action {
			t.Fatalf("%s action = %s, want %s", path, op.Action, action)
		}
	}
	if _, ok := byPath[unchanged]; ok {
		t.Fatal("unchanged target must not produce an operation")
	}
	if op := byPath[empty]; op.AfterSize != 0 || op.AfterHash != hashBytes(nil) {
		t.Fatalf("empty file planned incorrectly: %+v", op)
	}

	// Directories must be created before files below them.
	for i, op := range plan.Ops {
		for j := i + 1; j < len(plan.Ops); j++ {
			if plan.Ops[j].Action == ActionMkdir && plan.Ops[i].Action != ActionMkdir &&
				filepath.Dir(plan.Ops[i].Path) == plan.Ops[j].Path {
				t.Fatalf("mkdir %s ordered after file %s", plan.Ops[j].Path, op.Path)
			}
		}
	}
}

func TestBuildPlanDeterministic(t *testing.T) {
	root := t.TempDir()
	specs := []FileSpec{
		{Path: filepath.Join(root, "b", "f2"), Content: []byte("2"), Mode: 0o600},
		{Path: filepath.Join(root, "a", "f1"), Content: []byte("1"), Mode: 0o600},
	}
	paths := specPaths(specs)
	p1, err := BuildPlan(PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, paths)})
	if err != nil {
		t.Fatal(err)
	}
	p2, err := BuildPlan(PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, paths)})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p1, p2) {
		t.Fatal("plans built twice differ")
	}
	if len(p1.ID) != 64 {
		t.Fatalf("plan id must be a sha256 hex string, got %q", p1.ID)
	}
	// Indexing follows the sorted order.
	for i, op := range p1.Ops {
		if op.Index != i {
			t.Fatalf("op index %d != position %d", op.Index, i)
		}
	}
}

func TestBuildPlanSymlinkTargets(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Symlink creation needs Developer Mode or elevation on Windows.
		if os.Getenv("K3S_TEST_SYMLINKS") == "" {
			t.Skip("symlink behavior on Windows requires privilege; set K3S_TEST_SYMLINKS=1 to run")
		}
	}
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	writeTestFile(t, outside, "out", time.Time{})

	escaping := filepath.Join(root, "escaping")
	if err := os.Symlink(outside, escaping); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	specs := []FileSpec{{Path: escaping, Content: []byte("x")}}
	_, err := BuildPlan(PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, specPaths(specs))})
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("expected ErrPathOutsideRoot for escaping symlink, got %v", err)
	}

	// An in-root symlink to a regular file is also refused.
	target := filepath.Join(root, "target")
	writeTestFile(t, target, "in", time.Time{})
	inRoot := filepath.Join(root, "inroot")
	if err := os.Symlink(target, inRoot); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	specs = []FileSpec{{Path: inRoot, Content: []byte("x")}}
	_, err = BuildPlan(PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, specPaths(specs))})
	if !errors.Is(err, ErrTargetIsLink) {
		t.Fatalf("expected ErrTargetIsLink for in-root symlink, got %v", err)
	}
}

func TestBuildPlanAncestorSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" && os.Getenv("K3S_TEST_SYMLINKS") == "" {
		t.Skip("symlink behavior on Windows requires privilege; set K3S_TEST_SYMLINKS=1 to run")
	}
	root := t.TempDir()
	outside := t.TempDir()

	// An ancestor directory that is a symlink resolving outside the root must
	// be rejected even though the target path itself is lexically inside.
	if err := os.Symlink(outside, filepath.Join(root, "tls")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	specs := []FileSpec{{Path: filepath.Join(root, "tls", "server-ca.crt"), Content: []byte("x")}}
	_, err := BuildPlan(PlanConfig{Root: root, Specs: specs, Base: mustSnap(t, root, specPaths(specs))})
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("expected ErrPathOutsideRoot for escaping ancestor symlink, got %v", err)
	}

	// A symlinked ancestor that stays inside the root is accepted.
	root2 := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root2, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root2, "real"), filepath.Join(root2, "link")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	specs = []FileSpec{{Path: filepath.Join(root2, "link", "f.crt"), Content: []byte("x")}}
	if _, err := BuildPlan(PlanConfig{Root: root2, Specs: specs, Base: mustSnap(t, root2, specPaths(specs))}); err != nil {
		t.Fatalf("in-root ancestor symlink must be accepted, got %v", err)
	}
}

func TestDefaultModes(t *testing.T) {
	if DefaultDirMode != 0o700 || DefaultFileMode != 0o600 {
		t.Fatalf("default modes changed: dir %o file %o", DefaultDirMode, DefaultFileMode)
	}
}
