package dryrun

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestExecutorZeroValueIsDryRun(t *testing.T) {
	root := t.TempDir()
	spec := FileSpec{Path: filepath.Join(root, "f"), Content: []byte("x")}
	var e Executor
	e.Root = root
	rep, err := e.Run(context.Background(), []FileSpec{spec})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.DryRun {
		t.Fatal("zero-value executor must default to dry-run")
	}
	if _, err := os.Stat(spec.Path); !os.IsNotExist(err) {
		t.Fatal("dry-run must not create the target file")
	}
}

func TestExecutorDryRunNoMutation(t *testing.T) {
	root := t.TempDir()
	when := time.Date(2023, 3, 4, 5, 6, 7, 0, time.UTC)
	writeTestFile(t, filepath.Join(root, "tls", "exists.crt"), "old", when)
	writeTestFile(t, filepath.Join(root, "tls", "same.key"), "same", when)

	specs := []FileSpec{
		{Path: filepath.Join(root, "tls", "exists.crt"), Content: []byte("new-content"), ModTime: when.Add(time.Hour)},
		{Path: filepath.Join(root, "tls", "same.key"), Content: []byte("same"), ModTime: when},
		{Path: filepath.Join(root, "tls", "etcd", "new.crt"), Content: []byte("brand-new"), ModTime: when},
	}

	before := walkState(t, root)
	e := Executor{Root: root}
	rep, err := e.Run(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	after := walkState(t, root)

	if len(before) != len(after) {
		t.Fatalf("entry count changed: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("persistent state changed at %s:\nbefore %+v\nafter  %+v", before[i].rel, before[i], after[i])
		}
	}
	assertNoTempFiles(t, root)
	if rep.Status != StatusPlanned {
		t.Fatalf("status = %s, want planned", rep.Status)
	}
	if rep.Summary.Added != 1 || rep.Summary.Modified != 1 || rep.Summary.Unchanged != 1 || rep.Summary.Dirs != 1 {
		t.Fatalf("unexpected summary: %+v", rep.Summary)
	}
	for _, p := range rep.Phases {
		if p.Name == PhaseApply {
			if p.Status != phaseOK || !strings.Contains(p.Note, "simulated") {
				t.Fatalf("apply phase not marked simulated: %+v", p)
			}
		}
	}
}

func TestExecutorDryRunHookFailureLeavesState(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "a.crt"), "old", time.Time{})
	specs := []FileSpec{{Path: filepath.Join(root, "a.crt"), Content: []byte("new")}}

	injected := Fail(CodeIO, PhaseApply, "injected fault")
	e := Executor{Root: root, Hook: func(Op) error { return injected }}
	before := walkState(t, root)
	rep, err := e.Run(context.Background(), specs)
	after := walkState(t, root)

	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}
	if AsError(err).Code != CodeIO || AsError(err).Phase != PhaseApply {
		t.Fatalf("error classification lost: %v", err)
	}
	if rep.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", rep.Status)
	}
	if len(before) != len(after) {
		t.Fatal("filesystem changed despite hook failure")
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("state changed at %s", before[i].rel)
		}
	}
	assertNoTempFiles(t, root)
}

func TestExecutorDryRunContextCancel(t *testing.T) {
	root := t.TempDir()
	specs := []FileSpec{{Path: filepath.Join(root, "a"), Content: []byte("x")}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	e := Executor{Root: root}
	_, err := e.Run(ctx, specs)
	if AsError(err) == nil || AsError(err).Code != CodeCancelled {
		t.Fatalf("expected E_CANCELLED, got %v", err)
	}
	assertNoTempFiles(t, root)
}

func TestExecutorApplyJournalRecovery(t *testing.T) {
	root := t.TempDir()
	journalPath := filepath.Join(t.TempDir(), "apply-journal.json")
	when := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	specs := recoverySpecs(root, when)

	// First run: fail after the first three operations have completed.
	var seen int
	failHook := func(op Op) error {
		seen++
		if seen == 4 {
			return Fail(CodeIO, PhaseApply, "simulated crash before %s", op.Relative)
		}
		return nil
	}
	e1 := Executor{Root: root, Apply: true, JournalPath: journalPath, Hook: failHook}
	rep1, err := e1.Run(context.Background(), specs)
	if AsError(err) == nil || AsError(err).Code != CodeIO {
		t.Fatalf("expected injected E_IO, got %v", err)
	}
	if rep1.Status != StatusFailed {
		t.Fatalf("status = %s", rep1.Status)
	}
	// Completed operations must have landed atomically; the pending target
	// must not exist as a half-written file.
	if _, statErr := os.Stat(filepath.Join(root, "tls", "etcd", "peer-ca.crt")); !os.IsNotExist(statErr) {
		t.Fatal("pending file appeared despite pre-op failure")
	}
	if _, statErr := os.Stat(journalPath); statErr != nil {
		t.Fatalf("journal missing after crash: %v", statErr)
	}

	// A stale temp file from the crash must be cleaned on resume.
	if err := os.WriteFile(filepath.Join(root, "tls", ".k3s-dryrun-tmp-deadbeef"), []byte("junk"), 0o600); err != nil {
		t.Fatal(err)
	}

	// Restart: a fresh executor instance resumes from the journal.
	e2 := Executor{Root: root, Apply: true, JournalPath: journalPath}
	rep2, err := e2.Run(context.Background(), specs)
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	var resumed int
	for _, p := range rep2.Phases {
		if p.Name == PhaseApply {
			resumed = p.Skipped
		}
	}
	if resumed == 0 {
		t.Fatal("expected at least one operation skipped from journal")
	}
	if rep2.Status != StatusApplied {
		t.Fatalf("status = %s, want applied", rep2.Status)
	}
	assertSpecsOnDisk(t, specs)
	assertNoTempFiles(t, root)
	if _, err := os.Stat(journalPath); !os.IsNotExist(err) {
		t.Fatal("journal must be removed after successful completion")
	}

	// Third run: identical desired state must be a no-op.
	e3 := Executor{Root: root, Apply: true, JournalPath: journalPath}
	rep3, err := e3.Run(context.Background(), specs)
	if err != nil {
		t.Fatalf("reapply failed: %v", err)
	}
	if rep3.Status != StatusNoChanges {
		t.Fatalf("status = %s, want no-changes", rep3.Status)
	}
	assertSpecsOnDisk(t, specs)
}

func TestExecutorApplyNoJournal(t *testing.T) {
	root := t.TempDir()
	when := time.Date(2022, 2, 2, 2, 2, 2, 0, time.UTC)
	specs := []FileSpec{
		{Path: filepath.Join(root, "d", "f"), Content: []byte("abc"), ModTime: when},
	}
	e := Executor{Root: root, Apply: true}
	rep, err := e.Run(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != StatusApplied {
		t.Fatalf("status = %s", rep.Status)
	}
	assertSpecsOnDisk(t, specs)
}

func TestExecutorJournalMismatch(t *testing.T) {
	root := t.TempDir()
	journalPath := filepath.Join(t.TempDir(), "j.json")
	stale := journalData{SchemaVersion: SchemaVersion, PlanID: "does-not-match", Root: root}
	if err := saveJournal(journalPath, &stale); err != nil {
		t.Fatal(err)
	}
	specs := []FileSpec{{Path: filepath.Join(root, "f"), Content: []byte("x")}}
	e := Executor{Root: root, Apply: true, JournalPath: journalPath}
	_, err := e.Run(context.Background(), specs)
	if !errors.Is(err, ErrJournalMismatch) {
		t.Fatalf("expected ErrJournalMismatch, got %v", err)
	}
	if AsError(err).Code != CodeConflict {
		t.Fatalf("mismatch code = %s", AsError(err).Code)
	}
	if _, err := os.Stat(filepath.Join(root, "f")); !os.IsNotExist(err) {
		t.Fatal("no operation may run after journal mismatch")
	}
}

func TestExecutorCorruptJournal(t *testing.T) {
	root := t.TempDir()
	journalPath := filepath.Join(t.TempDir(), "j.json")
	if err := os.WriteFile(journalPath, []byte("{not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	specs := []FileSpec{{Path: filepath.Join(root, "f"), Content: []byte("x")}}
	e := Executor{Root: root, Apply: true, JournalPath: journalPath}
	_, err := e.Run(context.Background(), specs)
	if AsError(err) == nil || AsError(err).Code != CodeCorrupt {
		t.Fatalf("expected E_CORRUPT, got %v", err)
	}
}

func TestExecutorVerifyDetectsDrift(t *testing.T) {
	root := t.TempDir()
	specs := []FileSpec{{Path: filepath.Join(root, "f"), Content: []byte("desired")}}
	e := Executor{Root: root, Apply: true}
	if _, err := e.Run(context.Background(), specs); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(specs[0].Path, []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	verifyErr := e.verify(context.Background(), specs)
	if !errors.Is(verifyErr, ErrStateDrift) {
		t.Fatalf("expected ErrStateDrift, got %v", verifyErr)
	}
	if AsError(verifyErr).Phase != PhaseVerify {
		t.Fatalf("phase = %s", AsError(verifyErr).Phase)
	}
}

func TestJournalJSONShape(t *testing.T) {
	jd := journalData{SchemaVersion: SchemaVersion, PlanID: "abc", Root: "/r", Entries: []journalEntry{{Relative: "f", Action: ActionCreate, SHA256: "h"}}}
	b, err := json.Marshal(&jd)
	if err != nil {
		t.Fatal(err)
	}
	var back journalData
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Entries[0].Action != ActionCreate || back.PlanID != "abc" {
		t.Fatalf("journal round trip failed: %+v", back)
	}
}

// TestExecutorJournalReplayDedupe covers the case where a checkpointed file is
// deleted before resume: the operation re-executes and its journal entry must
// be replaced rather than duplicated.
func TestExecutorJournalReplayDedupe(t *testing.T) {
	root := t.TempDir()
	journalPath := filepath.Join(t.TempDir(), "j.json")
	when := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	specs := []FileSpec{
		{Path: filepath.Join(root, "a"), Content: []byte("a-content"), ModTime: when},
		{Path: filepath.Join(root, "b"), Content: []byte("b-content"), ModTime: when},
	}

	var seen int
	e1 := Executor{Root: root, Apply: true, JournalPath: journalPath, Hook: func(op Op) error {
		seen++
		if seen == 2 {
			return Fail(CodeIO, PhaseApply, "crash before b")
		}
		return nil
	}}
	if _, err := e1.Run(context.Background(), specs); AsError(err) == nil || AsError(err).Code != CodeIO {
		t.Fatalf("expected injected crash, got %v", err)
	}
	// Simulate external loss of the already-checkpointed file.
	if err := os.Remove(filepath.Join(root, "a")); err != nil {
		t.Fatal(err)
	}

	e2 := Executor{Root: root, Apply: true, JournalPath: journalPath, Hook: func(op Op) error {
		if op.Relative != "a" {
			return nil
		}
		// While replaying "a", its stale checkpoint must still be the only
		// entry for that relative/action.
		jd, found, err := loadJournal(journalPath)
		if err != nil || !found {
			t.Fatalf("journal unavailable during replay: %v %v", jd, err)
		}
		count := 0
		for _, en := range jd.Entries {
			if en.Relative == "a" && en.Action == ActionCreate {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("expected exactly one checkpoint for a, got %d", count)
		}
		return nil
	}}
	if _, err := e2.Run(context.Background(), specs); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	assertSpecsOnDisk(t, specs)
}

// TestExecutorJournalRootMismatch proves a journal created for a different
// diagnostic root cannot be replayed even when plan IDs coincide.
func TestExecutorJournalRootMismatch(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()
	journalPath := filepath.Join(t.TempDir(), "j.json")
	specsA := []FileSpec{
		{Path: filepath.Join(rootA, "a"), Content: []byte("x")},
		{Path: filepath.Join(rootA, "b"), Content: []byte("y")},
	}
	var seen int
	crash := Executor{Root: rootA, Apply: true, JournalPath: journalPath, Hook: func(Op) error {
		seen++
		if seen == 2 {
			return Fail(CodeIO, PhaseApply, "crash")
		}
		return nil
	}}
	if _, err := crash.Run(context.Background(), specsA); err == nil {
		t.Fatal("expected crash")
	}
	specsB := []FileSpec{
		{Path: filepath.Join(rootB, "a"), Content: []byte("x")},
		{Path: filepath.Join(rootB, "b"), Content: []byte("y")},
	}
	other := Executor{Root: rootB, Apply: true, JournalPath: journalPath}
	_, err := other.Run(context.Background(), specsB)
	if !errors.Is(err, ErrJournalMismatch) {
		t.Fatalf("expected root mismatch conflict, got %v", err)
	}
}

// TestExecutorStaleJournalTempCleaned verifies a half-written journal temp
// file from a crashed process is removed on resume.
func TestExecutorStaleJournalTempCleaned(t *testing.T) {
	root := t.TempDir()
	journalPath := filepath.Join(t.TempDir(), "j.json")
	if err := os.WriteFile(journalPath+".tmp", []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	specs := []FileSpec{{Path: filepath.Join(root, "f"), Content: []byte("x")}}
	e := Executor{Root: root, Apply: true, JournalPath: journalPath}
	if _, err := e.Run(context.Background(), specs); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(journalPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("stale journal temp must be removed")
	}
}

// TestExecutorVerifyDetectsMtimeDrift proves verify also enforces timestamps.
func TestExecutorVerifyDetectsMtimeDrift(t *testing.T) {
	root := t.TempDir()
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 2, 3, 4, 5, 6, 0, time.UTC)
	specs := []FileSpec{{Path: filepath.Join(root, "f"), Content: []byte("desired"), ModTime: t1}}
	e := Executor{Root: root, Apply: true}
	if _, err := e.Run(context.Background(), specs); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(specs[0].Path, t2, t2); err != nil {
		t.Fatal(err)
	}
	if err := e.verify(context.Background(), specs); !errors.Is(err, ErrStateDrift) {
		t.Fatalf("expected drift on mtime change, got %v", err)
	}
}

// TestApplyAncestorSymlinkRejected ensures the apply entry point also refuses
// escaping symlink ancestors (BuildPlan is the single validation gate).
func TestApplyAncestorSymlinkRejected(t *testing.T) {
	if runtime.GOOS == "windows" && os.Getenv("K3S_TEST_SYMLINKS") == "" {
		t.Skip("symlink behavior on Windows requires privilege; set K3S_TEST_SYMLINKS=1 to run")
	}
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "tls")); err != nil {
		t.Skipf("cannot create symlink: %v", err)
	}
	specs := []FileSpec{{Path: filepath.Join(root, "tls", "f"), Content: []byte("x")}}
	e := Executor{Root: root, Apply: true}
	_, err := e.Run(context.Background(), specs)
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Fatalf("apply must reject escaping ancestor, got %v", err)
	}
	entries, _ := os.ReadDir(outside)
	if len(entries) != 0 {
		t.Fatal("apply wrote outside the root through the symlinked ancestor")
	}
}
