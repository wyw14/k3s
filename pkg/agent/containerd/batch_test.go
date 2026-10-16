package containerd

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/containerd/containerd/v2/core/images"
	"github.com/fsnotify/fsnotify"
	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fastBatchOptions returns batch options tuned for tests.
func fastBatchOptions(worker ItemWorker) BatchOptions {
	return BatchOptions{
		MaxAttempts: 3,
		Backoff:     time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		Worker:      worker,
	}
}

func Test_UnitRunBatch_AllSucceed(t *testing.T) {
	keys := []string{"a", "b", "c"}
	calls := map[string]int{}
	var mu sync.Mutex

	opts := fastBatchOptions(func(ctx context.Context, key string) ([]string, error) {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		return []string{"img-" + key}, nil
	})

	result := RunBatch(context.Background(), keys, opts)

	assert.Equal(t, BatchSucceeded, result.Phase)
	assert.Equal(t, 3, result.Total)
	assert.Equal(t, 3, result.Succeeded)
	assert.Equal(t, 0, result.Failed)
	assert.Equal(t, 0, result.Invalid)
	assert.Equal(t, 0, result.Canceled)
	assert.Equal(t, cacheVersionV2, result.Version)
	assert.NotEmpty(t, result.ID)
	assert.NotNil(t, result.CompletedAt)

	for _, key := range keys {
		item := result.Items[key]
		require.NotNil(t, item, "missing result item %s", key)
		assert.Equal(t, PhaseSucceeded, item.Phase)
		assert.Equal(t, 1, item.Attempts)
		assert.Equal(t, []string{"img-" + key}, item.Images)
		assert.NotNil(t, item.StartedAt)
		assert.NotNil(t, item.CompletedAt)
		assert.Empty(t, item.Reason)
		assert.Equal(t, 1, calls[key])
	}
}

func Test_UnitRunBatch_LargeBatch(t *testing.T) {
	const total = 300
	const failEvery = 37

	keys := make([]string, total)
	shouldFail := map[string]bool{}
	for i := range total {
		key := fmt.Sprintf("item-%03d", i)
		keys[i] = key
		if i%failEvery == 0 {
			shouldFail[key] = true
		}
	}

	workerCalls := 0
	var mu sync.Mutex
	opts := fastBatchOptions(func(ctx context.Context, key string) ([]string, error) {
		mu.Lock()
		workerCalls++
		mu.Unlock()
		if shouldFail[key] {
			// Corrupt source: a permanent failure that must not be retried.
			return nil, Permanent(stderrors.New("corrupt archive"))
		}
		return []string{"img-" + key}, nil
	})

	var progressPhases []BatchPhase
	opts.OnProgress = func(r *BatchResult) {
		progressPhases = append(progressPhases, r.Phase)
	}

	result := RunBatch(context.Background(), keys, opts)

	expectedFailed := 0
	for i := range total {
		if i%failEvery == 0 {
			expectedFailed++
		}
	}
	expectedSucceeded := total - expectedFailed

	assert.Equal(t, BatchPartial, result.Phase, "some failures among successes must be Partial")
	assert.Equal(t, total, result.Total)
	assert.Equal(t, expectedSucceeded, result.Succeeded)
	assert.Equal(t, expectedFailed, result.Failed)
	assert.Equal(t, total, workerCalls, "each unique item must be attempted exactly once")
	assert.NotEmpty(t, progressPhases, "progress must be reported during the batch")

	// Every item must have an explicit terminal status; nothing may be left pending.
	for _, key := range keys {
		item := result.Items[key]
		require.NotNil(t, item)
		assert.True(t, item.Phase.Terminal(), "item %s left in non-terminal phase %s", key, item.Phase)
		if shouldFail[key] {
			assert.Equal(t, PhaseFailed, item.Phase)
			assert.Equal(t, 1, item.Attempts, "permanent failures are not retried")
			assert.Contains(t, item.Reason, "corrupt archive")
		} else {
			assert.Equal(t, PhaseSucceeded, item.Phase)
			assert.Equal(t, 1, item.Attempts)
		}
	}
}

func Test_UnitRunBatch_DuplicateRequests(t *testing.T) {
	keys := []string{"a", "b", "a", "a", "b", "c"}
	calls := map[string]int{}
	var mu sync.Mutex

	opts := fastBatchOptions(func(ctx context.Context, key string) ([]string, error) {
		mu.Lock()
		calls[key]++
		mu.Unlock()
		return []string{"img-" + key}, nil
	})

	result := RunBatch(context.Background(), keys, opts)

	assert.Equal(t, BatchSucceeded, result.Phase)
	assert.Equal(t, 6, result.Total)
	assert.Equal(t, 3, result.Succeeded)
	assert.Equal(t, 3, result.Skipped, "repeated keys are duplicate requests")
	assert.Equal(t, 1, calls["a"], "duplicate item must only be processed once")
	assert.Equal(t, 1, calls["b"])
	assert.Equal(t, 1, calls["c"])
}

func Test_UnitRunBatch_RetryThenSucceed(t *testing.T) {
	failures := map[string]int{"a": 2}
	calls := map[string]int{}
	var mu sync.Mutex

	opts := fastBatchOptions(func(ctx context.Context, key string) ([]string, error) {
		mu.Lock()
		calls[key]++
		remaining := failures[key]
		if remaining > 0 {
			failures[key]--
		}
		mu.Unlock()
		if remaining > 0 {
			return nil, stderrors.New("temporary io error")
		}
		return []string{"img-" + key}, nil
	})

	result := RunBatch(context.Background(), []string{"a", "b"}, opts)

	assert.Equal(t, BatchSucceeded, result.Phase)
	assert.Equal(t, 0, result.Failed)
	assert.Equal(t, 3, calls["a"], "transient failures must be retried until success")
	assert.Equal(t, 1, calls["b"])
	assert.Equal(t, 3, result.Items["a"].Attempts, "attempt count includes retries")
	assert.Equal(t, PhaseSucceeded, result.Items["a"].Phase)
	assert.Equal(t, []string{"img-a"}, result.Items["a"].Images)
}

func Test_UnitRunBatch_RetryExhausted(t *testing.T) {
	calls := 0
	opts := fastBatchOptions(func(ctx context.Context, key string) ([]string, error) {
		calls++
		return nil, stderrors.New("network unavailable")
	})

	result := RunBatch(context.Background(), []string{"a"}, opts)

	assert.Equal(t, BatchFailed, result.Phase)
	assert.Equal(t, 1, result.Failed)
	assert.Equal(t, 3, calls, "retries are bounded by MaxAttempts")
	item := result.Items["a"]
	assert.Equal(t, PhaseFailed, item.Phase)
	assert.Equal(t, 3, item.Attempts)
	assert.Contains(t, item.Reason, "network unavailable")
	assert.NotNil(t, item.CompletedAt)
}

func Test_UnitRunBatch_PermanentNotRetried(t *testing.T) {
	calls := 0
	opts := fastBatchOptions(func(ctx context.Context, key string) ([]string, error) {
		calls++
		return nil, Permanent(stderrors.New("invalid tar magic"))
	})

	result := RunBatch(context.Background(), []string{"a"}, opts)

	assert.Equal(t, BatchFailed, result.Phase)
	assert.Equal(t, 1, calls, "permanent errors short-circuit retry")
	assert.Equal(t, 1, result.Items["a"].Attempts)
	assert.False(t, IsPermanent(stderrors.New("x")))
	assert.True(t, IsPermanent(Permanent(stderrors.New("x"))))
}

func Test_UnitRunBatch_InvalidItems(t *testing.T) {
	validator := func(key string) error {
		if key == "bad" || key == "bad2" {
			return fmt.Errorf("%w: %s", ErrInvalidInput, key)
		}
		return nil
	}
	workerCalls := 0
	var mu sync.Mutex
	worker := func(ctx context.Context, key string) ([]string, error) {
		mu.Lock()
		workerCalls++
		mu.Unlock()
		return []string{"img-" + key}, nil
	}

	t.Run("partial batch with invalid items", func(t *testing.T) {
		opts := fastBatchOptions(worker)
		opts.Validator = validator
		result := RunBatch(context.Background(), []string{"good1", "bad", "good2", "bad2"}, opts)

		assert.Equal(t, BatchPartial, result.Phase)
		assert.Equal(t, 2, result.Succeeded)
		assert.Equal(t, 2, result.Invalid)
		assert.Equal(t, 2, workerCalls, "invalid items never reach the worker")
		assert.Equal(t, PhaseInvalid, result.Items["bad"].Phase)
		assert.Contains(t, result.Items["bad"].Reason, "invalid input")
		assert.Equal(t, PhaseSucceeded, result.Items["good1"].Phase)
	})

	t.Run("every item invalid", func(t *testing.T) {
		opts := fastBatchOptions(worker)
		opts.Validator = validator
		result := RunBatch(context.Background(), []string{"bad", "bad2"}, opts)

		assert.Equal(t, BatchInvalid, result.Phase)
		assert.Equal(t, 2, result.Invalid)
		assert.Equal(t, 0, result.Succeeded)
	})

	t.Run("empty and misconfigured batches", func(t *testing.T) {
		result := RunBatch(context.Background(), nil, fastBatchOptions(worker))
		assert.Equal(t, BatchInvalid, result.Phase)
		assert.Contains(t, result.Reason, ErrNoItems.Error())

		result = RunBatch(context.Background(), []string{"a"}, BatchOptions{})
		assert.Equal(t, BatchInvalid, result.Phase)
		assert.Contains(t, result.Reason, ErrNoWorker.Error())
	})
}

func Test_UnitRunBatch_CancelAndResume(t *testing.T) {
	keys := []string{"a", "b", "c", "d"}
	ctx, cancel := context.WithCancel(context.Background())

	calls := map[string]int{}
	var mu sync.Mutex
	canceledOnce := false

	result := RunBatch(ctx, keys, BatchOptions{
		MaxAttempts: 5,
		Backoff:     5 * time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
		Worker: func(ctx context.Context, key string) ([]string, error) {
			mu.Lock()
			calls[key]++
			mu.Unlock()
			switch key {
			case "c":
				// Block until the batch is canceled, simulating a long import
				// interrupted mid-flight.
				<-ctx.Done()
				return nil, ctx.Err()
			case "d":
				t.Errorf("item d must not start after cancellation")
				return nil, nil
			default:
				return []string{"img-" + key}, nil
			}
		},
		OnProgress: func(r *BatchResult) {
			item := r.Items["c"]
			if !canceledOnce && item != nil && item.Phase == PhaseRunning {
				canceledOnce = true
				cancel()
			}
		},
	})

	assert.Equal(t, BatchCanceled, result.Phase)
	assert.Equal(t, 2, result.Succeeded)
	assert.Equal(t, 2, result.Canceled)
	assert.Equal(t, 0, result.Failed)
	assert.Equal(t, PhaseSucceeded, result.Items["a"].Phase)
	assert.Equal(t, PhaseSucceeded, result.Items["b"].Phase)
	assert.Equal(t, PhaseCanceled, result.Items["c"].Phase)
	assert.Equal(t, 1, result.Items["c"].Attempts)
	assert.Equal(t, PhaseCanceled, result.Items["d"].Phase, "never-started items are explicitly Canceled")
	assert.Equal(t, 0, result.Items["d"].Attempts)
	assert.Equal(t, 1, calls["c"])

	// Resume the interrupted run: completed items must be skipped, canceled
	// items retried, and prior attempts carried over.
	for _, key := range []string{"a", "b"} {
		result.Items[key].Fingerprint = "fp"
	}
	resumeCalls := map[string]int{}
	result2 := RunBatch(context.Background(), keys, BatchOptions{
		Backoff:     time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		Previous:    result,
		Fingerprint: func(key string) (string, error) { return "fp", nil },
		Worker: func(ctx context.Context, key string) ([]string, error) {
			mu.Lock()
			resumeCalls[key]++
			mu.Unlock()
			return []string{"img-" + key}, nil
		},
	})

	assert.Equal(t, BatchSucceeded, result2.Phase)
	assert.Equal(t, 2, result2.Succeeded)
	assert.Equal(t, 2, result2.Skipped, "previously completed items are not re-imported")
	assert.Equal(t, 0, result2.Canceled)
	assert.Equal(t, 0, result2.Failed)
	assert.NotContains(t, resumeCalls, "a")
	assert.NotContains(t, resumeCalls, "b")
	assert.Equal(t, 1, resumeCalls["c"])
	assert.Equal(t, 1, resumeCalls["d"])
	assert.Equal(t, 2, result2.Items["c"].Attempts, "attempts from the interrupted run carry over")
	assert.Equal(t, 1, result2.Items["d"].Attempts, "d never started before cancellation, so it runs once")
	assert.Equal(t, []string{"img-a"}, result2.Items["a"].Images, "resumed items retain their artifacts")
}

func Test_UnitRunBatch_CancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	var mu sync.Mutex
	result := RunBatch(ctx, []string{"a"}, BatchOptions{
		MaxAttempts: 10,
		Backoff:     10 * time.Second,
		MaxBackoff:  10 * time.Second,
		Worker: func(ctx context.Context, key string) ([]string, error) {
			mu.Lock()
			calls++
			mu.Unlock()
			return nil, stderrors.New("transient")
		},
		OnProgress: func(r *BatchResult) {
			if r.Items["a"].Attempts == 1 && r.Items["a"].Phase == PhaseRunning {
				time.AfterFunc(10*time.Millisecond, cancel)
			}
		},
	})

	assert.Equal(t, BatchCanceled, result.Phase)
	assert.Equal(t, 1, result.Canceled)
	assert.Equal(t, 1, calls, "cancellation during backoff does not trigger more attempts")
	assert.Equal(t, PhaseCanceled, result.Items["a"].Phase)
}

func Test_UnitRunBatch_ResumeFingerprintChecks(t *testing.T) {
	previous := &BatchResult{
		Version: cacheVersionV2,
		Items: map[string]*ItemResult{
			"unchanged": {Key: "unchanged", Phase: PhaseSucceeded, Fingerprint: "fp", Attempts: 1, Images: []string{"img"}},
			"changed":   {Key: "changed", Phase: PhaseSucceeded, Fingerprint: "old", Attempts: 1, Images: []string{"img"}},
			"failed":    {Key: "failed", Phase: PhaseFailed, Attempts: 2, Reason: "boom"},
		},
	}

	fingerprint := func(key string) (string, error) {
		switch key {
		case "unchanged":
			return "fp", nil
		case "changed":
			return "new", nil
		case "failed":
			return "fp", nil
		default:
			return "", nil
		}
	}

	calls := map[string]bool{}
	var mu sync.Mutex
	result := RunBatch(context.Background(), []string{"unchanged", "changed", "failed"}, BatchOptions{
		Backoff:     time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		Previous:    previous,
		Fingerprint: fingerprint,
		Worker: func(ctx context.Context, key string) ([]string, error) {
			mu.Lock()
			calls[key] = true
			mu.Unlock()
			if key == "failed" {
				return []string{"img-failed"}, nil
			}
			return []string{"img-" + key}, nil
		},
	})

	assert.Equal(t, BatchSucceeded, result.Phase)
	assert.Equal(t, PhaseSkipped, result.Items["unchanged"].Phase)
	assert.Equal(t, reasonAlreadyComplete, result.Items["unchanged"].Reason)
	assert.False(t, calls["unchanged"], "matching fingerprint skips work")
	assert.True(t, calls["changed"], "changed fingerprint forces reprocessing")
	assert.True(t, calls["failed"], "previously failed items are retried on resume")
	assert.Equal(t, 3, result.Items["failed"].Attempts, "failed item carries previous attempts plus this run")
}

func Test_UnitRetryDelay(t *testing.T) {
	assert.Equal(t, 10*time.Millisecond, retryDelay(10*time.Millisecond, time.Second, 0))
	assert.Equal(t, 20*time.Millisecond, retryDelay(10*time.Millisecond, time.Second, 1))
	assert.Equal(t, 40*time.Millisecond, retryDelay(10*time.Millisecond, time.Second, 2))
	assert.Equal(t, time.Second, retryDelay(10*time.Millisecond, time.Second, 10), "backoff is capped")
}

func Test_UnitParseImportCache(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		cache, err := parseImportCache(nil)
		require.NoError(t, err)
		assert.Equal(t, cacheVersionV2, cache.Version)
		assert.Empty(t, cache.Files)

		cache, err = parseImportCache([]byte("   \n "))
		require.NoError(t, err)
		assert.Empty(t, cache.Files)
	})

	t.Run("legacy unversioned format", func(t *testing.T) {
		// Exact shape written by older k3s releases: a bare map of path to
		// fileInfo with only size, modTime and images fields.
		legacy := []byte(`{
			"/var/lib/rancher/k3s/agent/images/a.tar": {
				"size": 42,
				"modTime": "2024-05-01T12:00:00Z",
				"images": ["docker.io/library/nginx:latest"]
			},
			"/var/lib/rancher/k3s/agent/images/b.txt": {
				"size": 25,
				"modTime": "2024-05-02T08:30:00Z",
				"images": []
			}
		}`)

		cache, err := parseImportCache(legacy)
		require.NoError(t, err)
		assert.Equal(t, cacheVersionV2, cache.Version)
		require.Len(t, cache.Files, 2)

		entry := cache.Files["/var/lib/rancher/k3s/agent/images/a.tar"]
		require.NotNil(t, entry)
		assert.Equal(t, int64(42), entry.Size)
		assert.Equal(t, []string{"docker.io/library/nginx:latest"}, entry.Images)
		assert.Equal(t, PhaseSucceeded, entry.phase(), "legacy entries with images are treated as imported")

		empty := cache.Files["/var/lib/rancher/k3s/agent/images/b.txt"]
		require.NotNil(t, empty)
		assert.Equal(t, PhasePending, empty.phase(), "legacy entries without images are pending")

		// Re-serialized cache uses the versioned envelope while preserving data.
		out, err := json.Marshal(&importCache{Version: cache.Version, Files: cache.Files})
		require.NoError(t, err)
		assert.Contains(t, string(out), `"version":"v2"`)
		assert.Contains(t, string(out), `"files"`)
		assert.Contains(t, string(out), "docker.io/library/nginx:latest")
	})

	t.Run("versioned format", func(t *testing.T) {
		v2 := []byte(`{
			"version": "v2",
			"updatedAt": "2024-06-01T00:00:00Z",
			"files": {
				"/x/a.tar": {"size": 1, "modTime": "2024-06-01T00:00:00Z", "images": ["x"], "status": "Succeeded", "attempts": 2},
				"/x/b.tar": {"size": 2, "modTime": "2024-06-01T00:00:00Z", "images": null, "status": "Failed", "attempts": 3, "error": "boom"}
			}
		}`)

		cache, err := parseImportCache(v2)
		require.NoError(t, err)
		assert.Equal(t, "v2", cache.Version)
		require.Len(t, cache.Files, 2)
		assert.Equal(t, PhaseSucceeded, cache.Files["/x/a.tar"].phase())
		assert.Equal(t, 2, cache.Files["/x/a.tar"].Attempts)
		assert.Equal(t, PhaseFailed, cache.Files["/x/b.tar"].phase())
		assert.Equal(t, "boom", cache.Files["/x/b.tar"].Error)
	})

	t.Run("corrupt", func(t *testing.T) {
		_, err := parseImportCache([]byte("{not json"))
		assert.Error(t, err)
	})
}

func Test_UnitValidateImportFile(t *testing.T) {
	dir := t.TempDir()

	validTar := filepath.Join(dir, "valid.tar")
	require.NoError(t, os.WriteFile(validTar, []byte("tar-bytes"), 0644))

	validList := filepath.Join(dir, "images.txt")
	require.NoError(t, os.WriteFile(validList, []byte(""), 0644))

	emptyTar := filepath.Join(dir, "empty.tar")
	require.NoError(t, os.WriteFile(emptyTar, []byte(""), 0644))

	assert.NoError(t, validateImportFile(validTar))
	assert.NoError(t, validateImportFile(validList), "empty .txt pull lists are allowed")

	err := validateImportFile(emptyTar)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidInput)

	err = validateImportFile(filepath.Join(dir, "unsupported.bin"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidInput)

	err = validateImportFile(dir)
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidInput)

	err = validateImportFile(filepath.Join(dir, "missing.tar"))
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidInput)
	assert.True(t, IsPermanent(err), "vanished sources are permanent failures")
}

func Test_UnitCachedFingerprintMatchesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.tar")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0644))

	info, err := os.Stat(path)
	require.NoError(t, err)
	entry := &fileInfo{
		Size:    info.Size(),
		ModTime: metav1.NewTime(info.ModTime()),
		Images:  []string{"img"},
		Status:  PhaseSucceeded,
	}
	fingerprint, err := fileFingerprint(path)
	require.NoError(t, err)
	assert.Equal(t, fingerprint, cachedFingerprint(entry))
}

func Test_UnitWatchQueueCacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".cache.json"), nil, 0644))

	w := &watchqueue{
		cfg:        &config.Node{Images: dir},
		filesCache: map[string]*fileInfo{},
	}
	w.filesCache[filepath.Join(dir, "a.tar")] = &fileInfo{
		Size: 10, ModTime: metav1.NewTime(time.Now()), Images: []string{"img-a"},
		Status: PhaseSucceeded, Attempts: 1, seen: true,
	}
	w.filesCache[filepath.Join(dir, "b.tar")] = &fileInfo{
		Size: 0, ModTime: metav1.NewTime(time.Now()),
		Status: PhaseCanceled, Attempts: 1, Error: "context canceled", seen: true,
	}
	w.syncCache()

	w2 := &watchqueue{cfg: &config.Node{Images: dir}, filesCache: map[string]*fileInfo{}}
	w2.loadCache()

	require.Len(t, w2.filesCache, 2)
	assert.Equal(t, PhaseSucceeded, w2.filesCache[filepath.Join(dir, "a.tar")].phase())
	assert.Equal(t, []string{"img-a"}, w2.filesCache[filepath.Join(dir, "a.tar")].Images)
	assert.Equal(t, PhaseCanceled, w2.filesCache[filepath.Join(dir, "b.tar")].phase())
	assert.Equal(t, "context canceled", w2.filesCache[filepath.Join(dir, "b.tar")].Error)

	// Raw content must be the versioned envelope.
	raw, err := os.ReadFile(filepath.Join(dir, ".cache.json"))
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"version":"v2"`)
}

func Test_UnitWatchQueueLoadsLegacyCache(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"/old/a.tar":{"size":7,"modTime":"2024-01-01T00:00:00Z","images":["docker.io/library/alpine:latest"]}}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".cache.json"), []byte(legacy), 0644))

	w := &watchqueue{cfg: &config.Node{Images: dir}, filesCache: map[string]*fileInfo{}}
	w.loadCache()

	require.Len(t, w.filesCache, 1)
	entry := w.filesCache["/old/a.tar"]
	require.NotNil(t, entry)
	assert.Equal(t, int64(7), entry.Size)
	assert.Equal(t, PhaseSucceeded, entry.phase())
	assert.Equal(t, []string{"docker.io/library/alpine:latest"}, entry.Images)

	// Previous batch built from the legacy entry is resumable and fingerprinted.
	previous := w.previousBatch()
	require.Contains(t, previous.Items, "/old/a.tar")
	assert.Equal(t, PhaseSucceeded, previous.Items["/old/a.tar"].Phase)
	assert.NotEmpty(t, previous.Items["/old/a.tar"].Fingerprint)
}

func Test_UnitApplyBatchResultAndResume(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.tar")
	pathB := filepath.Join(dir, "b.tar")
	require.NoError(t, os.WriteFile(pathA, []byte("aaaa"), 0644))
	require.NoError(t, os.WriteFile(pathB, []byte("bbbb"), 0644))

	w := &watchqueue{cfg: &config.Node{Images: dir}, filesCache: map[string]*fileInfo{}}

	first := RunBatch(context.Background(), []string{pathA, pathB}, BatchOptions{
		Backoff:    time.Millisecond,
		MaxBackoff: 2 * time.Millisecond,
		Worker: func(ctx context.Context, key string) ([]string, error) {
			if key == pathB {
				return nil, stderrors.New("import failed")
			}
			return []string{"img-a"}, nil
		},
	})
	assert.Equal(t, BatchPartial, first.Phase)
	w.applyBatchResult(first)

	assert.Equal(t, PhaseSucceeded, w.filesCache[pathA].phase())
	assert.Equal(t, []string{"img-a"}, w.filesCache[pathA].Images)
	assert.True(t, w.filesCache[pathA].seen)
	assert.Equal(t, PhaseFailed, w.filesCache[pathB].phase())
	assert.NotEmpty(t, w.filesCache[pathB].Error)

	// Resuming reuses cache: a is skipped, b succeeds.
	second := RunBatch(context.Background(), []string{pathA, pathB}, BatchOptions{
		Backoff:     time.Millisecond,
		MaxBackoff:  2 * time.Millisecond,
		Previous:    w.previousBatch(),
		Fingerprint: fileFingerprint,
		Worker: func(ctx context.Context, key string) ([]string, error) {
			if key == pathA {
				t.Errorf("unchanged file %s must not be re-imported on resume", key)
			}
			return []string{"img-b"}, nil
		},
	})
	assert.Equal(t, BatchSucceeded, second.Phase)
	assert.Equal(t, PhaseSkipped, second.Items[pathA].Phase)
	assert.Equal(t, PhaseSucceeded, second.Items[pathB].Phase)
}

func Test_UnitListImportFiles(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.tar"), []byte("x"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sub", "b.tgz"), []byte("y"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("img"), 0644)) // .txt is supported (pull list)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignore.bin"), []byte("z"), 0644))

	fswatcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	defer fswatcher.Close()

	w := &watchqueue{watcher: fswatcher}
	paths := w.listImportFiles(dir)

	assert.Contains(t, paths, filepath.Join(dir, "a.tar"))
	assert.Contains(t, paths, filepath.Join(dir, "sub", "b.tgz"))
	assert.Contains(t, paths, filepath.Join(dir, "ignore.txt"))
	assert.NotContains(t, paths, filepath.Join(dir, "ignore.bin"))

	// Missing image directory is not an error.
	assert.Empty(t, w.listImportFiles(filepath.Join(dir, "does-not-exist")))
}

// exportTestImage builds an images.Image value for export resolution tests.
func exportTestImage(name string) images.Image {
	return images.Image{
		Name: name,
		Target: ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    digest.FromString(name),
		},
	}
}

func Test_UnitValidateImageReference(t *testing.T) {
	assert.NoError(t, validateImageReference("docker.io/library/nginx:1.0"))
	assert.ErrorIs(t, validateImageReference("NOT A REFERENCE"), ErrInvalidInput)
	assert.ErrorIs(t, validateImageReference(""), ErrInvalidInput)
}

func Test_UnitResolveImagesForExport(t *testing.T) {
	store := newFakeImageStore()
	store.records["docker.io/library/nginx:1.0"] = exportTestImage("docker.io/library/nginx:1.0")
	store.records["docker.io/library/redis:2.0"] = exportTestImage("docker.io/library/redis:2.0")
	// docker.io/library/missing:3.0 is deliberately absent from the store.

	names := []string{
		"docker.io/library/nginx:1.0",
		"docker.io/library/redis:2.0",
		"docker.io/library/redis:2.0", // duplicate request
		"docker.io/library/missing:3.0",
		"NOT A REFERENCE",
	}

	getCalls := map[string]int{}
	var mu sync.Mutex
	countingStore := &countingGetStore{inner: store, calls: getCalls, mu: &mu}

	result, resolved := resolveImagesForExport(context.Background(), countingStore, names)

	assert.Equal(t, BatchPartial, result.Phase, "some references must resolve despite failures")
	assert.Equal(t, 5, result.Total)
	assert.Equal(t, 2, result.Succeeded)
	assert.Equal(t, 1, result.Skipped, "duplicate reference is skipped")
	assert.Equal(t, 1, result.Failed)
	assert.Equal(t, 1, result.Invalid)
	assert.Equal(t, PhaseFailed, result.Items["docker.io/library/missing:3.0"].Phase)
	assert.Equal(t, PhaseInvalid, result.Items["NOT A REFERENCE"].Phase)
	assert.Equal(t, 1, getCalls["docker.io/library/missing:3.0"], "missing images are permanent failures, not retried")
	assert.Equal(t, 1, getCalls["docker.io/library/nginx:1.0"])

	require.Len(t, resolved, 2, "only resolvable images are exported")
	assert.Equal(t, "docker.io/library/nginx:1.0", resolved[0].Name)
	assert.Equal(t, "docker.io/library/redis:2.0", resolved[1].Name)
}

func Test_UnitResolveImagesForExport_Canceled(t *testing.T) {
	store := newFakeImageStore()
	store.records["docker.io/library/nginx:1.0"] = exportTestImage("docker.io/library/nginx:1.0")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, resolved := resolveImagesForExport(ctx, store, []string{"docker.io/library/nginx:1.0"})
	assert.Equal(t, BatchCanceled, result.Phase)
	assert.Equal(t, 1, result.Canceled)
	assert.Empty(t, resolved)
}

// countingGetStore wraps an images.Store to count Get calls per reference.
type countingGetStore struct {
	images.Store
	inner images.Store
	calls map[string]int
	mu    *sync.Mutex
}

func (s *countingGetStore) Get(ctx context.Context, name string) (images.Image, error) {
	s.mu.Lock()
	s.calls[name]++
	s.mu.Unlock()
	return s.inner.Get(ctx, name)
}
