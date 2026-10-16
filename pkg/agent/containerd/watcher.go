package containerd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/fsnotify/fsnotify"
	"github.com/k3s-io/k3s/pkg/agent/cri"
	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/k3s-io/k3s/pkg/util/errors"
	"github.com/rancher/wharfie/pkg/tarfile"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type fileInfo struct {
	Size     int64       `json:"size"`
	ModTime  metav1.Time `json:"modTime"`
	Images   []string    `json:"images"`
	Status   ItemPhase   `json:"status,omitempty"`
	Attempts int         `json:"attempts,omitempty"`
	Error    string      `json:"error,omitempty"`
	seen     bool        // field is not serialized, and can be used to track if a file has been seen since the last restart
}

// phase returns the explicit import status of a cached file. Entries written by
// older k3s releases have no status field; a non-empty image list implies the
// file was imported successfully.
func (f *fileInfo) phase() ItemPhase {
	if f == nil {
		return PhasePending
	}
	if f.Status != "" {
		return f.Status
	}
	if len(f.Images) > 0 {
		return PhaseSucceeded
	}
	return PhasePending
}

// importCache is the on-disk serialization of the image import state. The
// versioned envelope was introduced with batch status tracking; unversioned
// legacy files (a bare map of path to fileInfo) remain readable, see parseImportCache.
type importCache struct {
	Version   string              `json:"version"`
	UpdatedAt time.Time           `json:"updatedAt,omitempty"`
	Files     map[string]*fileInfo `json:"files"`
}

type watchqueue struct {
	cfg        *config.Node
	watcher    *fsnotify.Watcher
	filesCache map[string]*fileInfo
	workqueue  workqueue.TypedDelayingInterface[string]
}

func createWatcher(path string) (*fsnotify.Watcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if err := watcher.Add(path); err != nil {
		return nil, err
	}

	return watcher, nil
}

func mustCreateWatcher(path string) *fsnotify.Watcher {
	watcher, err := createWatcher(path)
	if err != nil {
		panic("Failed to create image import watcher:" + err.Error())
	}
	return watcher
}

func isFileSupported(path string) bool {
	for _, ext := range append(tarfile.SupportedExtensions, ".txt") {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}

	return false
}

// newImageClients creates the containerd and CRI image service clients used to
// import and pull images. The returned cleanup function closes both connections.
func newImageClients(ctx context.Context, cfg *config.Node) (*containerd.Client, runtimeapi.ImageServiceClient, func(), error) {
	// create the connections to not create every time when processing a event
	client, err := Client(cfg.Containerd.Address)
	if err != nil {
		return nil, nil, nil, err
	}

	criConn, err := cri.Connection(ctx, cfg.Containerd.Address)
	if err != nil {
		client.Close()
		return nil, nil, nil, err
	}

	return client, runtimeapi.NewImageServiceClient(criConn), func() {
		criConn.Close()
		client.Close()
	}, nil
}

// runWorkerForImages connects to containerd and calls processNextEventForImages to process items from the workqueue.
// This blocks until the workqueue is shut down.
func (w *watchqueue) runWorkerForImages(ctx context.Context) {
	client, imageClient, closeClients, err := newImageClients(ctx, w.cfg)
	if err != nil {
		logrus.Errorf("Failed to create containerd client: %v", err)
		w.watcher.Close()
		return
	}

	defer closeClients()

	for w.processNextEventForImages(ctx, client, imageClient) {
	}
}

// processNextEventForImages retrieves a single event from the workqueue and processes it.
// It returns a boolean that is true if the workqueue is still open and this function should be called again.
func (w *watchqueue) processNextEventForImages(ctx context.Context, client *containerd.Client, imageClient runtimeapi.ImageServiceClient) bool {
	key, shutdown := w.workqueue.Get()

	if shutdown {
		return false
	}

	if err := w.processImageEvent(ctx, key, client, imageClient); err != nil {
		logrus.Errorf("Failed to process image event: %v", err)
	}

	return true
}

// processImageEvent processes a single item from the workqueue.
func (w *watchqueue) processImageEvent(ctx context.Context, key string, client *containerd.Client, imageClient runtimeapi.ImageServiceClient) error {
	defer w.workqueue.Done(key)

	// Watch is rooted at the parent dir of the images dir, but we only need to handle things within the images dir
	if !strings.HasPrefix(key, w.cfg.Images) {
		return nil
	}

	file, err := os.Stat(key)

	// if the file does not exists, we assume that the event was RENAMED or REMOVED
	if os.IsNotExist(err) {
		// if the whole images dir was removed, reset the fileinfo cache
		if key == w.cfg.Images {
			w.filesCache = make(map[string]*fileInfo)
			defer w.syncCache()
			return nil
		}

		if !isFileSupported(key) {
			return nil
		}

		delete(w.filesCache, key)
		defer w.syncCache()
		return nil
	} else if err != nil {
		return errors.WithMessagef(err, "failed to get fileinfo for image event %s", key)
	}

	if file.IsDir() {
		// Add to watch and list+enqueue directory contents, as notify is not recursive
		if err := w.watcher.Add(key); err != nil {
			return errors.WithMessagef(err, "failed to add watch of %s", key)
		}

		fileInfos, err := os.ReadDir(key)
		if err != nil {
			return errors.WithMessagef(err, "unable to list contents of %s", key)
		}

		for _, fileInfo := range fileInfos {
			w.workqueue.Add(filepath.Join(key, fileInfo.Name()))
		}
		return nil
	}

	if !isFileSupported(key) {
		return nil
	}

	lastFileState := w.filesCache[key]
	if lastFileState == nil || lastFileState.Images == nil || file.Size() != lastFileState.Size || file.ModTime().After(lastFileState.ModTime.Time) {
		// Validate the source up front; directories, special files, and empty
		// archives get an explicit Invalid status instead of repeated failures.
		validator := func(string) error {
			if file.IsDir() {
				return fmt.Errorf("%w: %s: is a directory", ErrInvalidInput, key)
			}
			if file.Mode()&os.ModeType != 0 {
				return fmt.Errorf("%w: %s: not a regular file", ErrInvalidInput, key)
			}
			if file.Size() == 0 && !strings.HasSuffix(key, ".txt") {
				return fmt.Errorf("%w: %s: empty archive", ErrInvalidInput, key)
			}
			return nil
		}

		result := RunBatch(ctx, []string{key}, BatchOptions{
			Validator: validator,
			Previous:  w.itemPrevious(key, lastFileState),
			Worker:    w.importFileWorker(client, imageClient),
		})
		item := result.Items[key]

		entry := &fileInfo{
			Size:     file.Size(),
			ModTime:  metav1.NewTime(file.ModTime()),
			Status:   item.Phase,
			Attempts: item.Attempts,
			Error:    item.Reason,
			seen:     true,
		}
		if item.Phase == PhaseSucceeded {
			entry.Images = item.Images
		} else if lastFileState != nil {
			// Preserve any previously imported image names on a failed retry.
			entry.Images = lastFileState.Images
		}
		w.filesCache[key] = entry
		defer w.syncCache()

		switch item.Phase {
		case PhaseInvalid:
			logrus.Warnf("Skipping invalid image source %s: %s", key, item.Reason)
			return nil
		case PhaseCanceled:
			logrus.Warnf("Canceled image import for %s: %s", key, item.Reason)
			return nil
		case PhaseFailed:
			return fmt.Errorf("failed to import %s after %d attempt(s): %s", key, item.Attempts, item.Reason)
		}
	} else if lastFileState != nil && !lastFileState.seen {
		// first time seeing this file this start, re-add pinned label since K3s clears all pins on startup
		if err := labelImagesByName(ctx, client, lastFileState.Images, filepath.Base(key)); err != nil {
			return errors.WithMessagef(err, "failed to add pinned label to cached images from %s", key)
		}
		lastFileState.seen = true
		// no need to sync as seen field is not serialized
	}

	return nil
}

// pruneCache removes entries for all files that have not been seen since the last restart,
// and syncs the cache to disk. This is done to ensure that the cache file does not grow without
// bounds by continuing to track files that do not exist.
func (w *watchqueue) pruneCache() {
	for path, fileState := range w.filesCache {
		if !fileState.seen {
			delete(w.filesCache, path)
		}
	}
	w.syncCache()
}

// syncCache writes the fileinfo cache to disk.
// if the cache file does not exist, this is a no-op. The file must be manually
// created by the user in order for the cache to be persisted across restarts.
func (w *watchqueue) syncCache() {
	filePath := filepath.Join(w.cfg.Images, ".cache.json")
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		if !os.IsNotExist(err) {
			logrus.Errorf("Failed to truncate image import fileinfo cache: %v", err)
		}
		return
	}
	defer f.Close()

	cache := &importCache{
		Version:   cacheVersionV2,
		UpdatedAt: time.Now(),
		Files:     w.filesCache,
	}
	b, err := json.Marshal(cache)
	if err != nil {
		logrus.Errorf("Failed to marshal image import fileinfo cache: %v", err)
		return
	}

	if _, err := f.Write(b); err != nil {
		logrus.Errorf("Failed to write image import fileinfo cache: %v", err)
	}
}

// loadCache reads the fileinfo cache from disk.
// It is not an error if this file exists or is empty.
func (w *watchqueue) loadCache() {
	filePath := filepath.Join(w.cfg.Images, ".cache.json")
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0664)
	if err != nil {
		if !os.IsNotExist(err) {
			logrus.Errorf("Failed to open image import fileinfo cache: %v", err)
		}
		return
	}
	defer f.Close()

	b, err := io.ReadAll(f)
	if err != nil {
		logrus.Errorf("Failed to read image import fileinfo cache: %v", err)
		return
	}

	cache, err := parseImportCache(b)
	if err != nil {
		logrus.Errorf("Failed to unmarshal image import fileinfo cache: %v", err)
		return
	}
	w.filesCache = cache.Files
}

// parseImportCache parses the versioned import cache. Empty input yields an
// empty cache. An unversioned document is accepted as the legacy format, which
// serialized a bare map of file path to fileInfo.
func parseImportCache(b []byte) (*importCache, error) {
	cache := &importCache{
		Version: cacheVersionV2,
		Files:   map[string]*fileInfo{},
	}

	// 0 byte file is fine, but don't try to load it - allows users to simply
	// touch the file to enable it for use.
	if len(bytes.TrimSpace(b)) == 0 {
		return cache, nil
	}

	// Probe for the versioned envelope; legacy files have no "version" key.
	probed := map[string]json.RawMessage{}
	if err := json.Unmarshal(b, &probed); err != nil {
		return nil, err
	}
	if _, ok := probed["version"]; ok {
		if err := json.Unmarshal(b, cache); err != nil {
			return nil, err
		}
		if cache.Files == nil {
			cache.Files = map[string]*fileInfo{}
		}
		if cache.Version == "" {
			cache.Version = cacheVersionV2
		}
		return cache, nil
	}

	// Legacy unversioned format: a bare map of file path to fileInfo.
	if err := json.Unmarshal(b, &cache.Files); err != nil {
		return nil, err
	}
	return cache, nil
}

// importAndWatchImages starts the image watcher and imports everything already
// present in the images directory as a single resumable batch. The filesystem
// event worker is started afterwards, and this function returns once the
// startup batch has reached a terminal phase (success, partial success, or
// cancellation).
func importAndWatchImages(ctx context.Context, cfg *config.Node) error {
	w, err := watchImages(ctx, cfg)
	if err != nil {
		return err
	}

	// Recursively list existing sources before starting the fs-event worker,
	// so the startup batch is the only writer of the files cache.
	paths := w.listImportFiles(cfg.Images)
	if len(paths) > 0 {
		client, imageClient, closeClients, err := newImageClients(ctx, cfg)
		if err != nil {
			w.watcher.Close()
			return err
		}

		// Persist after every status transition so that an interrupted large
		// batch can resume without re-importing completed files.
		result := runImportBatch(ctx, cfg, client, imageClient, paths, w.previousBatch(), func(progress *BatchResult) {
			w.applyBatchResult(progress)
			w.syncCache()
		})
		closeClients()

		w.applyBatchResult(result)
		w.syncCache()
		logBatchSummary(cfg.Images, result)
	}

	// prune unseen entries from last run once all existing files have been processed
	w.pruneCache()

	// Files created or changed after startup are handled by the fs-event worker.
	go w.runWorkerForImages(ctx)

	return nil
}

// runImportBatch imports the listed image sources through the batch engine.
func runImportBatch(ctx context.Context, cfg *config.Node, client *containerd.Client, imageClient runtimeapi.ImageServiceClient, paths []string, previous *BatchResult, onProgress func(*BatchResult)) *BatchResult {
	return RunBatch(ctx, paths, BatchOptions{
		Validator:   validateImportFile,
		Fingerprint: fileFingerprint,
		Previous:    previous,
		OnProgress:  onProgress,
		Worker:      importFileWorker(cfg, client, imageClient),
	})
}

// BatchImportFiles imports the listed image tarballs and image-list text files
// into containerd as a single batch. Each source is validated before import;
// transient failures are retried idempotently; the returned BatchResult reports
// the explicit status of every source, including partial success and
// cancellation. The optional previous result enables resume: sources completed
// by an earlier run are skipped when unchanged.
func BatchImportFiles(ctx context.Context, cfg *config.Node, client *containerd.Client, imageClient runtimeapi.ImageServiceClient, paths []string, previous *BatchResult) *BatchResult {
	return runImportBatch(ctx, cfg, client, imageClient, paths, previous, nil)
}

// importFileWorker returns an idempotent batch worker that imports a single
// image source through containerd and the CRI image service.
func importFileWorker(cfg *config.Node, client *containerd.Client, imageClient runtimeapi.ImageServiceClient) ItemWorker {
	return func(ctx context.Context, path string) ([]string, error) {
		start := time.Now()
		imported, err := preloadFile(ctx, cfg, client, imageClient, path)
		if err != nil {
			return nil, err
		}
		logrus.Infof("Imported %d images from %s in %s", len(imported), path, time.Since(start))
		names := make([]string, len(imported))
		for i, image := range imported {
			names[i] = image.Name
		}
		return names, nil
	}
}

// importFileWorker binds the import worker to the watchqueue configuration.
func (w *watchqueue) importFileWorker(client *containerd.Client, imageClient runtimeapi.ImageServiceClient) ItemWorker {
	return importFileWorker(w.cfg, client, imageClient)
}

// listImportFiles recursively enumerates supported import sources under root
// and registers every traversed directory with the fsnotify watcher, as
// fsnotify does not watch recursively. A missing root directory is not an
// error; the watcher enqueues files once the directory is created.
func (w *watchqueue) listImportFiles(root string) []string {
	paths := []string{}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if err := w.watcher.Add(path); err != nil {
				logrus.Debugf("Failed to add image import watch for %s: %v", path, err)
			}
			return nil
		}
		if isFileSupported(path) {
			paths = append(paths, path)
		}
		return nil
	})
	return paths
}

// cachedFingerprint returns the fingerprint of a cached file entry. It matches
// fileFingerprint output for an unchanged file.
func cachedFingerprint(info *fileInfo) string {
	return fmt.Sprintf("%d:%d", info.Size, info.ModTime.UnixNano())
}

// previousBatch converts successfully imported cache entries into a previous
// batch result used to resume an interrupted startup import.
func (w *watchqueue) previousBatch() *BatchResult {
	items := make(map[string]*ItemResult, len(w.filesCache))
	for path, info := range w.filesCache {
		if info.phase() != PhaseSucceeded {
			continue
		}
		items[path] = &ItemResult{
			Key:         path,
			Fingerprint: cachedFingerprint(info),
			Phase:       PhaseSucceeded,
			Attempts:    info.Attempts,
			Images:      info.Images,
		}
	}
	return &BatchResult{
		Version: cacheVersionV2,
		Phase:   BatchSucceeded,
		Items:   items,
	}
}

// itemPrevious converts a single cached entry into a previous batch result, so
// that event-driven retries carry over the attempt count and completed state.
func (w *watchqueue) itemPrevious(key string, info *fileInfo) *BatchResult {
	if info == nil {
		return nil
	}
	phase := info.phase()
	if phase != PhaseSucceeded && phase != PhaseFailed && phase != PhaseCanceled {
		return nil
	}
	return &BatchResult{
		Version: cacheVersionV2,
		Items: map[string]*ItemResult{
			key: {
				Key:         key,
				Fingerprint: cachedFingerprint(info),
				Phase:       phase,
				Attempts:    info.Attempts,
				Images:      info.Images,
			},
		},
	}
}

// applyBatchResult merges a batch result into the files cache. Entries for
// files that no longer exist retain the previous stat information, and are
// pruned later if they remain unseen.
func (w *watchqueue) applyBatchResult(result *BatchResult) {
	if result == nil {
		return
	}
	for key, item := range result.Items {
		entry := &fileInfo{
			Status:   item.Phase,
			Attempts: item.Attempts,
			Error:    item.Reason,
		}
		if info, err := os.Stat(key); err == nil {
			entry.Size = info.Size()
			entry.ModTime = metav1.NewTime(info.ModTime())
			entry.seen = true
		} else if existing := w.filesCache[key]; existing != nil {
			entry.Size = existing.Size
			entry.ModTime = existing.ModTime
			entry.seen = existing.seen
		}

		switch item.Phase {
		case PhaseSucceeded, PhaseSkipped:
			entry.Status = PhaseSucceeded
			entry.Images = item.Images
			if len(entry.Images) == 0 {
				if existing := w.filesCache[key]; existing != nil {
					entry.Images = existing.Images
				}
			}
		case PhaseRunning, PhasePending:
			// In-progress state is persisted only to mark interruption; it
			// never shadows previously imported image names.
			if existing := w.filesCache[key]; existing != nil {
				entry.Images = existing.Images
			}
		}
		w.filesCache[key] = entry
	}
}

// logBatchSummary records the terminal status of a startup import batch.
func logBatchSummary(imagesDir string, result *BatchResult) {
	switch result.Phase {
	case BatchSucceeded:
		logrus.Infof("Image import batch from %s complete: %d imported, %d skipped",
			imagesDir, result.Succeeded, result.Skipped)
	case BatchPartial:
		logrus.Warnf("Image import batch from %s partially complete: %d imported, %d failed, %d invalid, %d skipped",
			imagesDir, result.Succeeded, result.Failed, result.Invalid, result.Skipped)
	case BatchCanceled:
		logrus.Warnf("Image import batch from %s canceled: %d imported, %d canceled, %d failed",
			imagesDir, result.Succeeded, result.Canceled, result.Failed)
	case BatchFailed, BatchInvalid:
		logrus.Errorf("Image import batch from %s failed: %s", imagesDir, result.Reason)
	}
}

// watchImages starts a watcher on the parent of the images dir, and a workqueue to process events
// from the watch stream.
func watchImages(ctx context.Context, cfg *config.Node) (*watchqueue, error) {
	// watch the directory above the images dir, as it may not exist yet when the watch is started.
	watcher, err := createWatcher(filepath.Dir(cfg.Images))
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create image import watcher for %s", filepath.Dir(cfg.Images))
	}

	w := &watchqueue{
		cfg:        cfg,
		watcher:    watcher,
		filesCache: make(map[string]*fileInfo),
		workqueue:  workqueue.TypedNewDelayingQueue[string](),
	}
	logrus.Debugf("Image import watcher created")

	w.loadCache()

	go func() {
		<-ctx.Done()
		w.watcher.Close()
	}()

	// The image event worker is started by importAndWatchImages after the
	// startup import batch completes, so cache state only has a single writer.

	go func() {
		for {
			select {
			case event, ok := <-w.watcher.Events:
				if !ok {
					logrus.Info("Image import watcher event channel closed; retrying in 5 seconds")
					select {
					case <-time.After(time.Second * 5):
						w.watcher = mustCreateWatcher(filepath.Dir(cfg.Images))
					case <-ctx.Done():
						return
					}
				}

				// only enqueue event if it is for a path within the images dir - not the parent dir that we are watching
				if strings.HasPrefix(event.Name, cfg.Images) {
					w.workqueue.AddAfter(event.Name, time.Second*2)
				}

			case err, ok := <-w.watcher.Errors:
				if !ok {
					logrus.Info("Image import watcher error channel closed; retrying in 5 seconds")
					select {
					case <-time.After(time.Second * 5):
						w.watcher = mustCreateWatcher(filepath.Dir(cfg.Images))
					case <-ctx.Done():
						return
					}
				}
				logrus.Errorf("Image import watcher received an error: %v", err)
			}
		}
	}()

	return w, nil
}
