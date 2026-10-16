package audit

import (
	"context"
	"encoding/json"
	"io"
	"sync"
)

// Writer is the audit event sink. Implementations must be safe to call when
// the recorder serializes calls; the built-in implementations are additionally
// safe for concurrent use.
type Writer interface {
	// WriteEvent persists a single fully populated event. Returning an error
	// fails the record call and the error is wrapped together with the event
	// context before being returned to the caller.
	WriteEvent(ctx context.Context, event *Event) error
}

// DiscardWriter drops every event. It is useful in deployments where audit
// collection is disabled.
type DiscardWriter struct{}

// WriteEvent implements Writer and always succeeds without emitting anything.
func (DiscardWriter) WriteEvent(context.Context, *Event) error {
	return nil
}

// JSONLWriter writes events as newline-delimited JSON (one JSON object per
// line) to an underlying io.Writer. Writes are serialized so that concurrent
// callers can never interleave bytes or corrupt the output stream.
type JSONLWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewJSONLWriter returns a JSONLWriter streaming events to w.
func NewJSONLWriter(w io.Writer) *JSONLWriter {
	enc := json.NewEncoder(w)
	// Preserve values such as '<' verbatim; audit sinks expect raw JSON.
	enc.SetEscapeHTML(false)
	return &JSONLWriter{enc: enc}
}

// WriteEvent implements Writer by encoding event as a single JSON line. The
// underlying writer error is returned unwrapped so that callers can inspect it
// with errors.Is/errors.As.
func (w *JSONLWriter) WriteEvent(_ context.Context, event *Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(event)
}
