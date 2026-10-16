// Package audit provides a structured audit event and metrics interface for
// recording key state transitions inside k3s.
//
// Events are emitted through a Recorder, which guarantees that every recorded
// event carries a strictly increasing, gap-free sequence number and a
// monotonically assigned timestamp, even when events are produced from many
// goroutines. All events belonging to the same logical unit of work share a
// correlatable operation ID that is propagated through context.Context, and
// terminal events carry the operation duration, both on the event and as a
// Prometheus histogram observation.
//
// Attribute values are passed through a configurable Redactor so that
// sensitive material (tokens, passwords, secrets, ...) is never written to an
// audit sink in clear text. When an operation or the audit sink itself fails,
// errors are wrapped together with the event context (sequence, action and
// operation ID) and the original error chain is retained, so failures never
// silently lose the context required to diagnose them.
package audit

import (
	"time"
)

// Event status values reported on audit events.
const (
	// StatusSuccess marks an event for an operation that completed without error.
	StatusSuccess = "success"
	// StatusError marks an event for an operation that completed with an error.
	StatusError = "error"
)

// ErrorDetail captures the serializable information about an operation error.
// The in-process error chain is preserved separately on the Event.Cause field
// and is not serialized to audit sinks.
type ErrorDetail struct {
	// Message holds the full (possibly wrapped) error message.
	Message string `json:"message"`
}

// Event is a single structured audit record describing a state transition.
// JSON field order follows the struct declaration order, which keeps the
// serialized event shape stable across releases.
type Event struct {
	// Sequence is the recorder-wide, strictly increasing event number.
	Sequence int64 `json:"sequence"`
	// Timestamp is the time at which the sequence number was assigned, in UTC.
	Timestamp time.Time `json:"timestamp"`
	// OperationID correlates all events emitted for the same logical operation.
	OperationID string `json:"operation_id"`
	// Action is the stable, machine-readable name of the audited action.
	Action string `json:"action"`
	// FromState is the state the transition originated from, if any.
	FromState string `json:"from_state,omitempty"`
	// ToState is the state the transition moved to, if any.
	ToState string `json:"to_state,omitempty"`
	// Status is StatusSuccess or StatusError.
	Status string `json:"status"`
	// Duration is the wall-clock time spent in the operation.
	Duration time.Duration `json:"duration_ns,omitempty"`
	// Attributes holds additional structured context, redacted before writing.
	Attributes map[string]any `json:"attributes,omitempty"`
	// Error holds the error detail when Status is StatusError.
	Error *ErrorDetail `json:"error,omitempty"`

	// Cause retains the in-process error chain, including wrapped errors, so
	// callers can use errors.Is/errors.As on failed operations. It is never
	// serialized to an audit sink.
	Cause error `json:"-"`
}
