package containerd

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// cacheVersionV2 is the current version of the on-disk image import state.
// Version "v2" records explicit per-file batch status; the unversioned legacy
// format (a bare map of path to fileInfo) is still read on load.
const cacheVersionV2 = "v2"

// Batch retry defaults. Individual operations (containerd import, image pull)
// are idempotent: content is keyed by digest and re-applying labels/tags
// converges to the same state, so retrying a failed or interrupted item is safe.
const (
	defaultMaxAttempts     = 3
	defaultRetryBackoff    = 100 * time.Millisecond
	defaultMaxRetryBackoff = time.Second
)

// Stable reason strings reported on items that are not processed as new work.
const (
	// reasonDuplicate marks a repeated key within a single batch request.
	reasonDuplicate = "duplicate request"
	// reasonAlreadyComplete marks an item resumed from a previous successful run
	// whose fingerprint still matches.
	reasonAlreadyComplete = "already complete"
)

// ItemPhase is the explicit lifecycle state of a single item within a batch.
type ItemPhase string

const (
	// PhasePending means the item has not started processing yet.
	PhasePending ItemPhase = "Pending"
	// PhaseRunning means the item is currently being processed.
	PhaseRunning ItemPhase = "Running"
	// PhaseSucceeded means the item completed successfully.
	PhaseSucceeded ItemPhase = "Succeeded"
	// PhaseFailed means the item exhausted its retry attempts with an error.
	PhaseFailed ItemPhase = "Failed"
	// PhaseSkipped means the item was not processed as new work: it is either a
	// duplicate request or was already completed by a previous (resumed) run.
	PhaseSkipped ItemPhase = "Skipped"
	// PhaseInvalid means the item failed input validation and was never run.
	PhaseInvalid ItemPhase = "Invalid"
	// PhaseCanceled means processing of the item was interrupted by context
	// cancellation; it is eligible to be retried on resume.
	PhaseCanceled ItemPhase = "Canceled"
)

// Terminal reports whether the phase will not transition any further within the
// current run. Canceled and Failed are terminal for the run but not for a resume.
func (p ItemPhase) Terminal() bool {
	switch p {
	case PhaseSucceeded, PhaseFailed, PhaseSkipped, PhaseInvalid, PhaseCanceled:
		return true
	default:
		return false
	}
}

// BatchPhase is the aggregate state of an entire batch run.
type BatchPhase string

const (
	BatchPending   BatchPhase = "Pending"
	BatchRunning   BatchPhase = "Running"
	BatchSucceeded BatchPhase = "Succeeded"
	// BatchPartial means at least one item succeeded but some items failed,
	// were invalid, or were canceled.
	BatchPartial BatchPhase = "Partial"
	BatchFailed  BatchPhase = "Failed"
	// BatchInvalid means the batch request itself was invalid (for example, no
	// items, or every item failed validation).
	BatchInvalid BatchPhase = "Invalid"
	// BatchCanceled means the run was interrupted before every item completed.
	// Per-item phases and counters still report any partial successes.
	BatchCanceled BatchPhase = "Canceled"
)

var (
	// ErrInvalidInput is wrapped by validation failures, so callers can tell
	// bad input apart from transient processing failures.
	ErrInvalidInput = stderrors.New("invalid input")
	// ErrNoItems is returned in the batch reason when a batch has no items.
	ErrNoItems = stderrors.New("no items to process")
	// ErrNoWorker is reported when a batch is started without a worker.
	ErrNoWorker = stderrors.New("batch worker is not configured")
)

// PermanentError wraps an error that must not be retried, for example a
// corrupt archive or an image reference that does not exist in the store.
type PermanentError struct {
	cause error
}

// Permanent wraps err so that the batch engine does not retry the operation.
// A nil error returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{cause: err}
}

// IsPermanent reports whether err wraps a PermanentError.
func IsPermanent(err error) bool {
	var permanent *PermanentError
	return stderrors.As(err, &permanent)
}

func (e *PermanentError) Error() string {
	return e.cause.Error()
}

func (e *PermanentError) Unwrap() error {
	return e.cause
}

// ItemWorker performs the idempotent unit of work for a single batch item.
// Implementations must be safe to retry with the same key: repeating a call
// after a failure or interruption must converge to the same result without
// duplicating side effects. The returned strings describe the artifacts that
// the item produced (for example the names of imported images).
type ItemWorker func(ctx context.Context, key string) ([]string, error)

// ItemValidator validates a batch item before it is passed to the worker.
// A validation failure records an explicit Invalid phase and never runs the item.
type ItemValidator func(key string) error

// FingerprintFunc returns a stable fingerprint for an item (for example file
// size and modification time). It is used to decide whether an item completed
// by a previous run is still up to date when resuming.
type FingerprintFunc func(key string) (string, error)

// ItemResult is the per-item result recorded by the batch engine.
type ItemResult struct {
	// Key is the item identifier.
	Key string `json:"key"`
	// Fingerprint is the verified fingerprint at completion, if known.
	Fingerprint string `json:"fingerprint,omitempty"`
	// Phase is the current lifecycle phase of the item.
	Phase ItemPhase `json:"phase"`
	// Attempts is the total number of worker invocations, including attempts
	// made by previous runs that this run resumed from.
	Attempts int `json:"attempts,omitempty"`
	// Images lists artifacts produced by the item, such as imported image names.
	Images []string `json:"images,omitempty"`
	// Reason explains non-success phases (validation/error/skip reason).
	Reason string `json:"reason,omitempty"`
	// StartedAt records when processing of the item first started.
	StartedAt *time.Time `json:"startedAt,omitempty"`
	// CompletedAt records when the item reached a terminal phase.
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// BatchResult is the aggregate result of a batch run.
type BatchResult struct {
	// Version is the state format version.
	Version string `json:"version"`
	// ID is an idempotency identifier for the run.
	ID string `json:"id"`
	// Phase is the aggregate batch phase.
	Phase BatchPhase `json:"phase"`
	// Reason carries a batch-level error message, when applicable.
	Reason string `json:"reason,omitempty"`

	// Total is the number of requested entries, including duplicate keys.
	Total int `json:"total"`
	// Succeeded counts items that completed successfully this run.
	Succeeded int `json:"succeeded,omitempty"`
	// Failed counts items that failed after exhausting retries.
	Failed int `json:"failed,omitempty"`
	// Skipped counts duplicate requests and items already completed on resume.
	Skipped int `json:"skipped,omitempty"`
	// Invalid counts items rejected by input validation.
	Invalid int `json:"invalid,omitempty"`
	// Canceled counts items interrupted by context cancellation.
	Canceled int `json:"canceled,omitempty"`

	// Items holds the result of each unique key, keyed by the item key.
	Items map[string]*ItemResult `json:"items"`

	StartedAt   time.Time  `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

// BatchOptions configures a batch run.
type BatchOptions struct {
	// ID is an optional idempotency key for the run. One is generated if empty.
	ID string
	// MaxAttempts is the maximum number of worker attempts per item. Defaults
	// to defaultMaxAttempts when not positive.
	MaxAttempts int
	// Backoff is the initial retry delay; it doubles per attempt up to MaxBackoff.
	Backoff time.Duration
	// MaxBackoff caps the retry backoff. Defaults to defaultMaxRetryBackoff.
	MaxBackoff time.Duration
	// Validator validates each unique item before processing.
	Validator ItemValidator
	// Worker executes the item. Required.
	Worker ItemWorker
	// Fingerprint verifies previously completed items when resuming.
	Fingerprint FingerprintFunc
	// Previous is the result of an earlier run to resume from. Items that
	// previously succeeded are skipped when their fingerprint still matches;
	// attempts made before an interruption are carried over.
	Previous *BatchResult
	// OnProgress, if set, is invoked after every status transition. It allows
	// callers to persist progress so that an interrupted run can be resumed.
	OnProgress func(*BatchResult)
}

// RunBatch processes the supplied item keys through a single worker, with
// validation, bounded idempotent retries, explicit per-item status, and
// cancellation support. It always returns a non-nil result describing what
// happened; worker errors do not abort the batch but are recorded per item,
// so partial success is visible. Keys may repeat: only the first occurrence
// is processed, repeats are reported as Skipped.
func RunBatch(ctx context.Context, keys []string, opts BatchOptions) *BatchResult {
	started := time.Now()
	result := &BatchResult{
		Version:   cacheVersionV2,
		ID:        opts.ID,
		Phase:     BatchPending,
		Items:     map[string]*ItemResult{},
		StartedAt: started,
	}
	if result.ID == "" {
		result.ID = "batch-" + strconv.FormatInt(started.UnixNano(), 16)
	}

	if opts.Worker == nil {
		result.failBatch(ErrNoWorker)
		return result
	}
	if len(keys) == 0 {
		result.failBatch(ErrNoItems)
		return result
	}

	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	backoff := opts.Backoff
	if backoff <= 0 {
		backoff = defaultRetryBackoff
	}
	maxBackoff := opts.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxRetryBackoff
	}

	// Plan unique items in request order; repeated occurrences of a key are
	// recorded up front as duplicate requests.
	duplicateSkips := 0
	uniqueKeys := make([]string, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for _, key := range keys {
		if seen[key] {
			duplicateSkips++
			continue
		}
		seen[key] = true
		uniqueKeys = append(uniqueKeys, key)
		result.Items[key] = &ItemResult{Key: key, Phase: PhasePending}
	}
	result.recount(duplicateSkips)
	opts.reportProgress(result)

	result.Phase = BatchRunning
	for _, key := range uniqueKeys {
		item := result.Items[key]

		// Resume fast path: a previously completed item whose fingerprint
		// still matches is not processed again.
		if opts.resumeSkippable(ctx, item) {
			item.phase(PhaseSkipped, reasonAlreadyComplete)
			result.recount(duplicateSkips)
			opts.reportProgress(result)
			continue
		}

		// If the batch was interrupted before this item started, every
		// remaining item gets an explicit Canceled status.
		if ctx.Err() != nil {
			item.phase(PhaseCanceled, ctx.Err().Error())
			result.recount(duplicateSkips)
			opts.reportProgress(result)
			continue
		}

		if opts.Validator != nil {
			if err := opts.Validator(key); err != nil {
				item.phase(PhaseInvalid, err.Error())
				result.recount(duplicateSkips)
				opts.reportProgress(result)
				continue
			}
		}

		// Carry attempts made by a previous interrupted or failed run so the
		// retry history is visible across resumes.
		if prev := opts.previousItem(key); prev != nil {
			item.Attempts = prev.Attempts
			if prev.Fingerprint != "" {
				item.Fingerprint = prev.Fingerprint
			}
		}

		item.StartedAt = timePtr(time.Now())
		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if err := ctx.Err(); err != nil {
				lastErr = err
				item.phase(PhaseCanceled, lastErr.Error())
				break
			}

			item.Attempts++
			item.Phase = PhaseRunning
			item.Reason = ""
			result.recount(duplicateSkips)
			opts.reportProgress(result)

			images, err := opts.Worker(ctx, key)
			if err == nil {
				item.Images = images
				item.Reason = ""
				item.phase(PhaseSucceeded, "")
				lastErr = nil
				break
			}
			lastErr = err

			// Context cancellation is never a retryable failure.
			if ctx.Err() != nil || stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
				item.phase(PhaseCanceled, err.Error())
				lastErr = nil
				break
			}

			if IsPermanent(err) || attempt == maxAttempts {
				break
			}

			select {
			case <-ctx.Done():
				item.phase(PhaseCanceled, ctx.Err().Error())
				lastErr = nil
			case <-time.After(retryDelay(backoff, maxBackoff, attempt-1)):
				// retry
			}
			if item.Phase == PhaseCanceled {
				break
			}
		}

		if item.Phase == PhaseRunning {
			item.phase(PhaseFailed, lastErr.Error())
		}
		result.recount(duplicateSkips)
		opts.reportProgress(result)
	}

	result.CompletedAt = timePtr(time.Now())
	result.computePhase()
	opts.reportProgress(result)
	return result
}

// resumeSkippable reports whether the item can be skipped as already completed
// by a previous run. A missing fingerprint function trusts the previous result;
// a fingerprint mismatch or error forces reprocessing through the idempotent worker.
func (o BatchOptions) resumeSkippable(ctx context.Context, item *ItemResult) bool {
	prev := o.previousItem(item.Key)
	if prev == nil || prev.Phase != PhaseSucceeded {
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	if o.Fingerprint == nil {
		item.Images = prev.Images
		item.Attempts = prev.Attempts
		return true
	}
	fingerprint, err := o.Fingerprint(item.Key)
	if err != nil || fingerprint == "" || fingerprint != prev.Fingerprint {
		return false
	}
	item.Images = prev.Images
	item.Fingerprint = fingerprint
	item.Attempts = prev.Attempts
	return true
}

// previousItem returns the recorded item from a previous run, if any.
func (o BatchOptions) previousItem(key string) *ItemResult {
	if o.Previous == nil || o.Previous.Items == nil {
		return nil
	}
	return o.Previous.Items[key]
}

// phase transitions the item to a new phase, stamping CompletedAt on terminal phases.
func (i *ItemResult) phase(phase ItemPhase, reason string) {
	i.Phase = phase
	i.Reason = reason
	if phase.Terminal() {
		i.CompletedAt = timePtr(time.Now())
	}
}

// failBatch records an invalid batch request that never ran any items.
func (r *BatchResult) failBatch(err error) {
	r.Phase = BatchInvalid
	r.Reason = err.Error()
	r.CompletedAt = timePtr(time.Now())
}

// reportProgress invokes the progress callback, if configured.
func (o BatchOptions) reportProgress(result *BatchResult) {
	if o.OnProgress != nil {
		o.OnProgress(result)
	}
}

// recount refreshes aggregate counters from the unique items, adding the number
// of duplicate request occurrences that are represented only as counters.
func (r *BatchResult) recount(duplicateSkips int) {
	r.Total = len(r.Items) + duplicateSkips
	r.Succeeded = 0
	r.Failed = 0
	r.Skipped = duplicateSkips
	r.Invalid = 0
	r.Canceled = 0
	for _, item := range r.Items {
		switch item.Phase {
		case PhaseSucceeded:
			r.Succeeded++
		case PhaseFailed:
			r.Failed++
		case PhaseSkipped:
			r.Skipped++
		case PhaseInvalid:
			r.Invalid++
		case PhaseCanceled:
			r.Canceled++
		}
	}
}

// computePhase derives the aggregate batch phase from the final counters.
func (r *BatchResult) computePhase() {
	switch {
	case r.Canceled > 0:
		r.Phase = BatchCanceled
	case r.Failed == 0 && r.Invalid == 0:
		// Every effective item succeeded or was skipped as a duplicate/resume.
		r.Phase = BatchSucceeded
	case r.Succeeded == 0 && r.Skipped == 0:
		// Nothing useful completed.
		if r.Failed == 0 && r.Invalid > 0 {
			r.Phase = BatchInvalid
		} else {
			r.Phase = BatchFailed
		}
	default:
		// Some work completed and some did not: report partial success.
		r.Phase = BatchPartial
	}
}

// retryDelay returns the capped exponential backoff for a retry. attempt is
// zero-based.
func retryDelay(initial, max time.Duration, attempt int) time.Duration {
	delay := initial
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= max || delay <= 0 {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

func timePtr(t time.Time) *time.Time {
	return &t
}

// validateImportFile validates a candidate image source for batch import.
// Files with an unsupported extension, missing paths, directories, special
// files, and empty archives are rejected up front with ErrInvalidInput.
func validateImportFile(path string) error {
	if !isFileSupported(path) {
		return fmt.Errorf("%w: %s: unsupported file extension", ErrInvalidInput, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		// A vanished source cannot be retried into existence within the batch.
		return Permanent(fmt.Errorf("%w: %s: %v", ErrInvalidInput, path, err))
	}
	if info.IsDir() {
		return fmt.Errorf("%w: %s: is a directory", ErrInvalidInput, path)
	}
	if info.Mode()&os.ModeType != 0 {
		return fmt.Errorf("%w: %s: not a regular file", ErrInvalidInput, path)
	}
	if info.Size() == 0 && !strings.HasSuffix(path, ".txt") {
		return fmt.Errorf("%w: %s: empty archive", ErrInvalidInput, path)
	}
	return nil
}

// fileFingerprint returns a size and modification-time fingerprint for a file.
// It is used to detect whether a previously imported file has changed.
func fileFingerprint(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%d", info.Size(), info.ModTime().UnixNano()), nil
}
