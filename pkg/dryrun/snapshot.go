package dryrun

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// scanResult carries one worker outcome.
type scanResult struct {
	state FileState
	err   error
}

// defaultConcurrency bounds the number of concurrent filesystem readers.
func defaultConcurrency(n int) int {
	if n > 0 {
		return n
	}
	c := runtime.NumCPU()
	if c < 1 {
		return 1
	}
	if c > 8 {
		return 8
	}
	return c
}

// capture takes a read-only snapshot of the given paths under root. A path
// that does not exist is recorded with Exists == false rather than treated as
// an error; every other stat/read failure aborts with a classified error.
//
// The published Snapshot is immutable and safe for concurrent reads.
func capture(ctx context.Context, root string, paths []string, maxConc int) (Snapshot, error) {
	root = cleanPath(root)
	workers := defaultConcurrency(maxConc)

	// Deduplicate requested paths while preserving a deterministic feed order.
	seen := make(map[string]struct{}, len(paths))
	unique := make([]string, 0, len(paths))
	for _, p := range paths {
		abs := cleanPath(p)
		if abs == "" {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		unique = append(unique, abs)
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan string)
	results := make(chan scanResult)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-scanCtx.Done():
					return
				case p, ok := <-jobs:
					if !ok {
						return
					}
					st, err := scanOne(scanCtx, root, p)
					select {
					case results <- scanResult{state: st, err: err}:
					case <-scanCtx.Done():
						return
					}
					if err != nil {
						return
					}
				}
			}
		}()
	}

	// Feed workers.
	go func() {
		defer close(jobs)
		for _, p := range unique {
			select {
			case jobs <- p:
			case <-scanCtx.Done():
				return
			}
		}
	}()

	// Collect; a separate goroutine closes results once all workers exit.
	go func() {
		wg.Wait()
		close(results)
	}()

	files := make(map[string]FileState, len(unique))
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
				cancel()
			}
			continue
		}
		files[res.state.Path] = res.state
	}

	if firstErr != nil {
		return Snapshot{Root: root, files: files}, firstErr
	}
	if err := ctx.Err(); err != nil && firstErr == nil {
		return Snapshot{Root: root, files: files}, Wrap(err, PhaseScan, "")
	}
	return Snapshot{Root: root, files: files}, nil
}

// scanOne stats and hashes a single path without following it for writing;
// symlinks are detected and their escape from root is flagged.
func scanOne(ctx context.Context, root, abs string) (FileState, error) {
	select {
	case <-ctx.Done():
		return FileState{Path: abs}, Wrap(ctx.Err(), PhaseScan, abs)
	default:
	}

	li, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return FileState{Path: abs, Exists: false}, nil
		}
		return FileState{Path: abs}, Wrap(err, PhaseScan, abs)
	}

	st := FileState{
		Path:    abs,
		Exists:  true,
		Size:    li.Size(),
		Mode:    perm(li.Mode()),
		modTime: li.ModTime(),
	}

	if li.Mode()&os.ModeSymlink != 0 {
		st.IsLink = true
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			if !withinRoot(root, cleanPath(resolved)) {
				st.Escapes = true
			}
		}
	}

	if li.IsDir() {
		st.IsDir = true
		st.Size = 0
		st.SHA256 = ""
		return st, nil
	}

	// For symlinks to files, follow the link for content hashing; for
	// dangling links os.Stat below fails and the entry is reported missing.
	info := li
	if st.IsLink {
		info, err = os.Stat(abs)
		if err != nil {
			if os.IsNotExist(err) {
				return FileState{Path: abs, Exists: false, IsLink: true}, nil
			}
			return FileState{Path: abs}, Wrap(err, PhaseScan, abs)
		}
		if info.IsDir() {
			st.IsDir = true
			st.Size = 0
			return st, nil
		}
		st.Size = info.Size()
		st.Mode = perm(info.Mode())
		st.modTime = info.ModTime()
	} else if li.Mode()&os.ModeType != 0 {
		// Non-regular, non-symlink entry (device, socket, named pipe): record
		// metadata only, no content hash.
		return st, nil
	}

	f, err := os.Open(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return FileState{Path: abs, Exists: false}, nil
		}
		return FileState{Path: abs}, Wrap(err, PhaseScan, abs)
	}
	defer f.Close()

	sum, err := hashFile(f)
	if err != nil {
		return FileState{Path: abs}, Wrap(err, PhaseScan, abs)
	}
	st.SHA256 = sum
	return st, nil
}
