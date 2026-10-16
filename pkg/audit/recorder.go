package audit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Audit error sentinels. Wrapped errors returned by this package always contain
// the offending sequence number, action and operation ID, and preserve the
// underlying error chain for errors.Is and errors.As.
var (
	// ErrInvalidEvent is returned when an event cannot be recorded as given.
	ErrInvalidEvent = errors.New("audit: invalid event")
	// ErrWriteFailed is returned when an event could not be written to the sink.
	ErrWriteFailed = errors.New("audit: event write failed")
	// ErrOperationCompleted is returned when an already completed operation is
	// recorded again, which would otherwise lose single-completion semantics.
	ErrOperationCompleted = errors.New("audit: operation already completed")
)

// Config configures a Recorder.
type Config struct {
	// Writer is the sink events are written to. Required.
	Writer Writer
	// Registerer optionally registers the audit metrics. A nil value keeps the
	// metrics functional but unregistered, which is useful for tests.
	Registerer prometheus.Registerer
	// Redactor masks sensitive attribute values. Defaults to DefaultRedactor.
	Redactor *Redactor
	// Now returns the current time. Defaults to time.Now and may be overridden
	// in tests to make timestamps and durations deterministic.
	Now func() time.Time
}

// Recorder records structured audit events. It is safe for concurrent use:
// sequence number assignment and sink writes happen under a single lock, so
// the order in which events reach the sink matches their sequence numbers
// exactly, with no gaps or duplicates.
type Recorder struct {
	mu         sync.Mutex
	sequence   int64
	writer     Writer
	redactor   *Redactor
	now        func() time.Time
	collectors *collectors
}

// NewRecorder builds a Recorder from cfg.
func NewRecorder(cfg Config) (*Recorder, error) {
	if cfg.Writer == nil {
		return nil, fmt.Errorf("%w: writer must not be nil", ErrInvalidEvent)
	}
	metricsCollectors, err := newCollectors(cfg.Registerer)
	if err != nil {
		return nil, err
	}
	redactor := cfg.Redactor
	if redactor == nil {
		redactor = DefaultRedactor()
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Recorder{
		writer:     cfg.Writer,
		redactor:   redactor,
		now:        now,
		collectors: metricsCollectors,
	}, nil
}

// Record writes a single event. Fields left empty are populated by the
// recorder: an operation ID is taken from ctx (or generated), the status
// defaults to success (or error when an error is attached), attributes are
// redacted, and the sequence number and timestamp are assigned.
//
// On sink failure the populated event is returned together with an error
// wrapping ErrWriteFailed and the event context, so callers retain the full
// event and can react to the failure instead of losing the audit record.
func (r *Recorder) Record(ctx context.Context, event Event) (*Event, error) {
	if event.Action == "" {
		return nil, fmt.Errorf("%w: missing action", ErrInvalidEvent)
	}
	if event.OperationID == "" {
		event.OperationID = ensureOperationID(ctx)
	}
	if event.Status == "" {
		if event.Error != nil || event.Cause != nil {
			event.Status = StatusError
		} else {
			event.Status = StatusSuccess
		}
	}
	if event.Attributes != nil {
		redacted, ok := r.redactor.Redact(event.Attributes).(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: action=%s operation_id=%s: attributes must be a map[string]any",
				ErrInvalidEvent, event.Action, event.OperationID)
		}
		event.Attributes = redacted
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.sequence++
	event.Sequence = r.sequence
	if event.Timestamp.IsZero() {
		event.Timestamp = r.now().UTC()
	}

	if err := r.writer.WriteEvent(ctx, &event); err != nil {
		return &event, fmt.Errorf("%w: sequence=%d action=%s operation_id=%s: %w",
			ErrWriteFailed, event.Sequence, event.Action, event.OperationID, err)
	}

	r.collectors.observe(event.Action, event.Status, event.Duration)
	return &event, nil
}

// RecordTransition records the terminal transition of an operation that began
// at startedAt. The operation ID is taken from ctx, or generated when absent.
// When cause is non-nil the event is marked as failed and the error chain is
// retained on the returned event.
func (r *Recorder) RecordTransition(ctx context.Context, action, fromState, toState string, startedAt time.Time, cause error, attributes map[string]any) (*Event, error) {
	event := r.newEvent(action, fromState, toState, startedAt, cause, attributes)
	return r.Record(ctx, event)
}

// Start begins a timed operation. The returned operation inherits the
// operation ID carried by ctx, or gets a freshly generated one. Callers must
// eventually call Complete until it succeeds (failed writes may be retried);
// intermediate states may be reported with Event.
func (r *Recorder) Start(ctx context.Context, action, fromState string) *Operation {
	return &Operation{
		recorder:    r,
		OperationID: ensureOperationID(ctx),
		Action:      action,
		fromState:   fromState,
		startedAt:   r.now(),
	}
}

// newEvent constructs an event with duration and error detail populated. The
// operation ID is resolved by Record from the context passed to it.
func (r *Recorder) newEvent(action, fromState, toState string, startedAt time.Time, cause error, attributes map[string]any) Event {
	event := Event{
		Action:     action,
		FromState:  fromState,
		ToState:    toState,
		Duration:   r.now().Sub(startedAt),
		Attributes: attributes,
		Cause:      cause,
	}
	if cause != nil {
		event.Status = StatusError
		event.Error = &ErrorDetail{Message: cause.Error()}
	}
	return event
}

// Operation tracks a single in-flight operation and its elapsed time.
type Operation struct {
	// OperationID correlates every event emitted by this operation.
	OperationID string
	// Action is the stable name of the audited action.
	Action string

	recorder  *Recorder
	fromState string
	startedAt time.Time

	mu   sync.Mutex
	done bool
}

// Event records an intermediate state transition without completing the
// operation. Events emitted this way share the operation ID and form a stable,
// ordered sequence: the FromState is the state reached by the previous event
// (or the operation start state), and the current state is advanced only after
// the event has been written, so a sink failure can be retried without
// corrupting the transition chain.
func (op *Operation) Event(ctx context.Context, toState string, cause error, attributes map[string]any) (*Event, error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.done {
		return nil, op.alreadyCompleted()
	}
	event := op.buildEvent(toState, cause, attributes)
	recorded, err := op.recorder.Record(WithOperationID(ctx, op.OperationID), event)
	if err != nil {
		return recorded, err
	}
	op.fromState = toState
	return recorded, nil
}

// Complete records the terminal transition of the operation and marks it
// completed only once the event has been written; a sink failure therefore
// leaves the operation open for retry instead of losing the terminal event.
// Calling Complete again after a successful write returns an error wrapping
// ErrOperationCompleted. The duration is measured from Start and the cause
// error chain is preserved on the returned event.
func (op *Operation) Complete(ctx context.Context, toState string, cause error, attributes map[string]any) (*Event, error) {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.done {
		return nil, op.alreadyCompleted()
	}
	event := op.buildEvent(toState, cause, attributes)
	recorded, err := op.recorder.Record(WithOperationID(ctx, op.OperationID), event)
	if err != nil {
		return recorded, err
	}
	op.done = true
	return recorded, nil
}

// buildEvent constructs an event for the operation's current state.
func (op *Operation) buildEvent(toState string, cause error, attributes map[string]any) Event {
	return op.recorder.newEvent(op.Action, op.fromState, toState, op.startedAt, cause, attributes)
}

// alreadyCompleted builds the error returned for duplicate terminal calls.
func (op *Operation) alreadyCompleted() error {
	return fmt.Errorf("%w: action=%s operation_id=%s", ErrOperationCompleted, op.Action, op.OperationID)
}
