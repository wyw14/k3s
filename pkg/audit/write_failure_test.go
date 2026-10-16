package audit_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/k3s/pkg/audit"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// failingWriter fails the first failUntil writes, then accepts events. All
// attempted events are retained so tests can assert sequence continuity.
type failingWriter struct {
	mu        sync.Mutex
	err       error
	failUntil int
	attempts  int
	events    []*audit.Event
}

func (w *failingWriter) WriteEvent(_ context.Context, event *audit.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attempts++
	w.events = append(w.events, event)
	if w.attempts <= w.failUntil {
		return w.err
	}
	return nil
}

func (w *failingWriter) sequences() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]int64, len(w.events))
	for i, event := range w.events {
		out[i] = event.Sequence
	}
	return out
}

// failingIOWriter makes a JSONLWriter fail at the underlying io.Writer level.
type failingIOWriter struct{}

func (failingIOWriter) Write([]byte) (int, error) {
	return 0, errors.New("broken pipe")
}

func TestWriteFailureWrapsFullEventContext(t *testing.T) {
	boom := errors.New("disk full")
	writer := &failingWriter{err: boom, failUntil: 2}
	recorder, _, registry := newTestRecorder(t, writer)

	ctx := audit.WithOperationID(context.Background(), "op-fail")

	first, err := recorder.Record(ctx, audit.Event{Action: "state.persist"})
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrWriteFailed), "error must wrap ErrWriteFailed: %v", err)
	require.True(t, errors.Is(err, boom), "error must retain sink error chain: %v", err)
	require.Contains(t, err.Error(), "sequence=1")
	require.Contains(t, err.Error(), "action=state.persist")
	require.Contains(t, err.Error(), "operation_id=op-fail")
	require.NotNil(t, first)
	require.Equal(t, int64(1), first.Sequence)
	require.Equal(t, "state.persist", first.Action)
	require.Equal(t, "op-fail", first.OperationID)

	second, err := recorder.Record(ctx, audit.Event{Action: "state.persist"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "sequence=2")
	require.NotNil(t, second)

	// The recorder keeps allocating gap-free sequences and recovers once the
	// sink is healthy again.
	third, err := recorder.Record(ctx, audit.Event{Action: "state.persist"})
	require.NoError(t, err)
	require.Equal(t, int64(3), third.Sequence)

	require.Equal(t, []int64{1, 2, 3}, writer.sequences())

	// Failed writes are not counted as successful audit events.
	families, err := registry.Gather()
	require.NoError(t, err)
	require.Equal(t, 1.0, counterValue(families, "k3s_audit_events_total",
		map[string]string{"action": "state.persist", "status": audit.StatusSuccess}))
	require.Equal(t, 0.0, counterValue(families, "k3s_audit_events_total",
		map[string]string{"action": "state.persist", "status": audit.StatusError}))
}

func TestJSONLWriterFailureBubblesUp(t *testing.T) {
	recorder, err := audit.NewRecorder(audit.Config{
		Writer: audit.NewJSONLWriter(failingIOWriter{}),
	})
	require.NoError(t, err)

	ctx := audit.WithOperationID(context.Background(), "op-json-fail")
	op := recorder.Start(ctx, "config.write", "staged")
	event, err := op.Complete(ctx, "persisted", nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrWriteFailed))
	require.Contains(t, err.Error(), "broken pipe")
	require.Contains(t, err.Error(), "operation_id=op-json-fail")
	// The fully populated event is handed back even when the sink rejects it.
	require.NotNil(t, event)
	require.Equal(t, "config.write", event.Action)
	require.Equal(t, "op-json-fail", event.OperationID)
}

func TestOperationCompleteRetryableAfterWriteFailure(t *testing.T) {
	boom := errors.New("transient nfs hiccup")
	writer := &failingWriter{err: boom, failUntil: 1}
	recorder, _, _ := newTestRecorder(t, writer)

	ctx := audit.WithOperationID(context.Background(), "op-retry")
	op := recorder.Start(ctx, "config.write", "staged")

	first, err := op.Complete(ctx, "persisted", nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrWriteFailed))
	require.NotNil(t, first)
	require.Equal(t, int64(1), first.Sequence)

	// The operation is still open; the retry gets a fresh sequence and writes.
	second, err := op.Complete(ctx, "persisted", nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), second.Sequence)

	// After success, completion is single-shot.
	_, err = op.Complete(ctx, "persisted-again", nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrOperationCompleted))

	require.Equal(t, []int64{1, 2}, writer.sequences())
}

func TestOperationCannotCompleteMoreThanOnce(t *testing.T) {
	writer := &sliceWriter{}
	recorder, _, _ := newTestRecorder(t, writer)

	op := recorder.Start(context.Background(), "one-shot", "init")
	_, err := op.Complete(context.Background(), "done", nil, nil)
	require.NoError(t, err)

	_, err = op.Complete(context.Background(), "again", nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrOperationCompleted))
	require.Contains(t, err.Error(), "action=one-shot")

	_, err = op.Event(context.Background(), "late", nil, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrOperationCompleted))

	require.Len(t, writer.snapshot(), 1)
}

func TestFailedOperationErrorIsPreservedThroughWriteFailure(t *testing.T) {
	boom := errors.New("quorum lost")
	writer := &failingWriter{err: boom, failUntil: 1}
	recorder, _, _ := newTestRecorder(t, writer)

	cause := errors.New("apply refused")
	event, err := recorder.Record(context.Background(), audit.Event{
		Action: "cluster.reconcile",
		Status: audit.StatusError,
		Error:  &audit.ErrorDetail{Message: cause.Error()},
		Cause:  cause,
	})
	require.Error(t, err)
	// Both the sink failure and the original operation context are retained.
	require.True(t, errors.Is(err, audit.ErrWriteFailed))
	require.True(t, errors.Is(err, boom))
	require.NotNil(t, event)
	require.Equal(t, audit.StatusError, event.Status)
	require.True(t, errors.Is(event.Cause, cause))
	require.Equal(t, "apply refused", event.Error.Message)
}

func TestNewRecorderRejectsMissingWriter(t *testing.T) {
	_, err := audit.NewRecorder(audit.Config{})
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrInvalidEvent))
	require.True(t, strings.Contains(err.Error(), "writer must not be nil"))
}

func TestRecordRejectsEmptyAction(t *testing.T) {
	recorder, err := audit.NewRecorder(audit.Config{Writer: audit.DiscardWriter{}})
	require.NoError(t, err)

	_, err = recorder.Record(context.Background(), audit.Event{})
	require.Error(t, err)
	require.True(t, errors.Is(err, audit.ErrInvalidEvent))
}

func TestWriteFailureWithSlowClockStillTimed(t *testing.T) {
	boom := errors.New("nfs stale")
	writer := &failingWriter{err: boom, failUntil: 1}
	recorder, clock, _ := newTestRecorder(t, writer)

	op := recorder.Start(context.Background(), "snapshot.upload", "ready")
	clock.advance(2 * time.Second)
	event, err := op.Complete(context.Background(), "uploaded", nil, nil)
	require.Error(t, err)
	require.NotNil(t, event)
	require.Equal(t, 2*time.Second, event.Duration, "timing context must survive a sink failure")
}

func counterValue(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	return sumCounter(families, name, labels)
}
