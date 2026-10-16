package dryrun

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
)

// TestConcurrentDryRunExecutions runs many dry-run diagnostics against the
// same live filesystem concurrently. Under -race this proves read-only
// parallelism is safe; the filesystem must be byte-identical afterwards and
// every report must agree.
func TestConcurrentDryRunExecutions(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "tls", "a.crt"), "old-a", time2020)
	writeTestFile(t, filepath.Join(root, "tls", "b.crt"), "stable", time2020)

	specs := []FileSpec{
		{Path: filepath.Join(root, "tls", "a.crt"), Content: []byte("new-a")},
		{Path: filepath.Join(root, "tls", "b.crt"), Content: []byte("stable")},
		{Path: filepath.Join(root, "tls", "c.crt"), Content: []byte("created")},
	}

	before := walkState(t, root)

	const goroutines = 16
	const iterations = 3
	var wg sync.WaitGroup
	reports := make([][]byte, goroutines*iterations)
	errs := make(chan error, goroutines*iterations)
	var idx sync.Mutex
	next := 0

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				e := Executor{Root: root, MaxConcurrency: 4}
				rep, err := e.Run(context.Background(), specs)
				if err != nil {
					errs <- err
					return
				}
				j, err := rep.JSON()
				if err != nil {
					errs <- err
					return
				}
				idx.Lock()
				reports[next] = j
				next++
				idx.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < len(reports); i++ {
		if !bytes.Equal(reports[0], reports[i]) {
			t.Fatalf("concurrent report %d differs from report 0", i)
		}
	}

	after := walkState(t, root)
	if len(before) != len(after) {
		t.Fatalf("entry count changed: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("state changed at %s", before[i].rel)
		}
	}
}

// TestConcurrentSnapshotAndPlanReads hammers the immutable published values
// from many goroutines.
func TestConcurrentSnapshotAndPlanReads(t *testing.T) {
	root := t.TempDir()
	paths := []string{
		filepath.Join(root, "a"),
		filepath.Join(root, "b"),
		filepath.Join(root, "d", "c"),
	}
	writeTestFile(t, paths[0], "a", time2020)
	writeTestFile(t, paths[1], "b", time2020)

	snap, err := capture(context.Background(), root, paths, 8)
	if err != nil {
		t.Fatal(err)
	}
	specs := []FileSpec{
		{Path: paths[0], Content: []byte("a")},
		{Path: paths[1], Content: []byte("b")},
		{Path: paths[2], Content: []byte("c")},
	}
	plan, err := BuildPlan(PlanConfig{Root: root, Specs: specs, Base: snap})
	if err != nil {
		t.Fatal(err)
	}

	const readers = 32
	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 100; k++ {
				_ = snap.Paths()
				states := snap.States()
				if _, ok := snap.State(paths[0]); !ok {
					t.Error("snapshot State lookup failed")
					return
				}
				ops := plan.ChangeOps()
				_ = plan.ID
				for _, s := range states {
					_ = s.SHA256
				}
				for _, op := range ops {
					_ = op.Relative
				}
			}
		}()
	}
	wg.Wait()
}
