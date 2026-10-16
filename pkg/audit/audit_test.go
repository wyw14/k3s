package audit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/k3s-io/k3s/pkg/audit"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// fakeClock is a controllable time source for deterministic timestamps and
// durations.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// sliceWriter retains every event written to it for in-process assertions.
type sliceWriter struct {
	mu     sync.Mutex
	events []*audit.Event
}

func (w *sliceWriter) WriteEvent(_ context.Context, event *audit.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.events = append(w.events, event)
	return nil
}

func (w *sliceWriter) snapshot() []*audit.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]*audit.Event, len(w.events))
	copy(out, w.events)
	return out
}

func newTestRecorder(t *testing.T, w audit.Writer) (*audit.Recorder, *fakeClock, *prometheus.Registry) {
	t.Helper()
	clock := newFakeClock()
	registry := prometheus.NewRegistry()
	recorder, err := audit.NewRecorder(audit.Config{
		Writer:     w,
		Registerer: registry,
		Now:        clock.now,
	})
	require.NoError(t, err)
	return recorder, clock, registry
}

func TestRecordAssignsStableSequenceAndTimestamp(t *testing.T) {
	writer := &sliceWriter{}
	recorder, clock, _ := newTestRecorder(t, writer)

	first, err := recorder.Record(context.Background(), audit.Event{Action: "node.join"})
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Sequence)
	require.Equal(t, clock.now(), first.Timestamp)
	require.Equal(t, audit.StatusSuccess, first.Status)
	_, err = uuid.Parse(first.OperationID)
	require.NoError(t, err, "generated operation ID must be a valid UUID")

	clock.advance(250 * time.Millisecond)
	second, err := recorder.Record(context.Background(), audit.Event{
		Action:    "node.ready",
		FromState: "joining",
		ToState:   "ready",
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), second.Sequence)
	require.True(t, second.Timestamp.After(first.Timestamp))

	// Events must reach the sink in exactly the order of their sequences.
	stored := writer.snapshot()
	require.Len(t, stored, 2)
	require.Equal(t, []int64{1, 2}, []int64{stored[0].Sequence, stored[1].Sequence})
}

func TestOperationIDPropagatedFromContext(t *testing.T) {
	writer := &sliceWriter{}
	recorder, _, _ := newTestRecorder(t, writer)

	ctx := audit.WithOperationID(context.Background(), "op-correlate-1")

	recorded, err := recorder.Record(ctx, audit.Event{Action: "token.rotate"})
	require.NoError(t, err)
	require.Equal(t, "op-correlate-1", recorded.OperationID)

	op := recorder.Start(ctx, "cert.rotate", "valid")
	require.Equal(t, "op-correlate-1", op.OperationID)
	completed, err := op.Complete(ctx, "rotated", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "op-correlate-1", completed.OperationID)

	for _, event := range writer.snapshot() {
		require.Equal(t, "op-correlate-1", event.OperationID)
	}

	// Without a context operation ID, a fresh ID is generated.
	plain := recorder.Start(context.Background(), "plain.op", "")
	_, err = uuid.Parse(plain.OperationID)
	require.NoError(t, err)
}

func TestStateTransitionDurationStatusAndErrorContext(t *testing.T) {
	writer := &sliceWriter{}
	recorder, clock, _ := newTestRecorder(t, writer)

	sentinel := errors.New("etcd unavailable")
	cause := fmt.Errorf("snapshot failed: %w", sentinel)
	startedAt := clock.now()
	clock.advance(250 * time.Millisecond)

	failed, err := recorder.RecordTransition(context.Background(),
		"etcd.snapshot", "requested", "failed", startedAt, cause, map[string]any{"node": "server-1"})
	require.NoError(t, err)
	require.Equal(t, audit.StatusError, failed.Status)
	require.Equal(t, 250*time.Millisecond, failed.Duration)
	require.NotNil(t, failed.Error)
	require.Contains(t, failed.Error.Message, "snapshot failed: etcd unavailable")
	// The in-process error chain must survive for errors.Is/errors.As.
	require.True(t, errors.Is(failed.Cause, sentinel))

	// The Cause chain must never be serialized to the sink.
	data, err := json.Marshal(failed)
	require.NoError(t, err)
	var encoded map[string]any
	require.NoError(t, json.Unmarshal(data, &encoded))
	require.NotContains(t, encoded, "Cause")
	require.Equal(t, "snapshot failed: etcd unavailable", encoded["error"].(map[string]any)["message"])

	// Successful operations record their duration and no error detail.
	op := recorder.Start(context.Background(), "etcd.snapshot", "pending")
	clock.advance(100 * time.Millisecond)
	done, err := op.Complete(context.Background(), "stored", nil, nil)
	require.NoError(t, err)
	require.Equal(t, audit.StatusSuccess, done.Status)
	require.Equal(t, 100*time.Millisecond, done.Duration)
	require.Nil(t, done.Error)
	require.Nil(t, done.Cause)
}

func TestIntermediateEventsKeepStableTransitionOrder(t *testing.T) {
	writer := &sliceWriter{}
	recorder, _, _ := newTestRecorder(t, writer)

	ctx := audit.WithOperationID(context.Background(), "op-bootstrap")
	op := recorder.Start(ctx, "node.bootstrap", "none")

	joining, err := op.Event(ctx, "joining", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "none", joining.FromState)
	require.Equal(t, "joining", joining.ToState)

	ready, err := op.Complete(ctx, "ready", nil, nil)
	require.NoError(t, err)
	require.Equal(t, "joining", ready.FromState)
	require.Equal(t, "ready", ready.ToState)

	stored := writer.snapshot()
	require.Len(t, stored, 2)
	require.Equal(t, "op-bootstrap", stored[0].OperationID)
	require.Equal(t, "op-bootstrap", stored[1].OperationID)
	require.Equal(t, []int64{1, 2}, []int64{stored[0].Sequence, stored[1].Sequence})
}

func TestRedactionMasksConfiguredSensitiveAttributes(t *testing.T) {
	writer := &sliceWriter{}
	recorder, _, _ := newTestRecorder(t, writer)

	attributes := map[string]any{
		"node":     "server-1",
		"password": "hunter2",
		"token":    "super-secret-token",
		"nested": map[string]any{
			"secret": "deep",
			"count":  3,
		},
		"typed": map[string]string{
			"api_key": "abc123",
			"region":  "us-east-1",
		},
		"items": []any{
			map[string]any{"bootstrapToken": "bt"},
			"untouched",
		},
	}

	recorded, err := recorder.Record(context.Background(), audit.Event{
		Action:     "server.join",
		Attributes: attributes,
	})
	require.NoError(t, err)

	require.Equal(t, "server-1", recorded.Attributes["node"])
	require.Equal(t, audit.RedactedMask, recorded.Attributes["password"])
	require.Equal(t, audit.RedactedMask, recorded.Attributes["token"])

	nested := recorded.Attributes["nested"].(map[string]any)
	require.Equal(t, audit.RedactedMask, nested["secret"])
	require.Equal(t, 3, nested["count"])

	typed := recorded.Attributes["typed"].(map[string]any)
	require.Equal(t, audit.RedactedMask, typed["api_key"])
	require.Equal(t, "us-east-1", typed["region"])

	items := recorded.Attributes["items"].([]any)
	require.Equal(t, audit.RedactedMask, items[0].(map[string]any)["bootstrapToken"])
	require.Equal(t, "untouched", items[1])

	// Redaction must not mutate the caller's attribute map.
	require.Equal(t, "hunter2", attributes["password"])
	require.Equal(t, "abc123", attributes["typed"].(map[string]string)["api_key"])

	// Masked values must not leak through the JSON sink either.
	data, err := json.Marshal(recorded)
	require.NoError(t, err)
	require.NotContains(t, string(data), "hunter2")
	require.NotContains(t, string(data), "super-secret-token")
	require.NotContains(t, string(data), "abc123")
}

func TestRedactorUsesProvidedConfiguration(t *testing.T) {
	r := audit.NewRedactor("[REDACTED]", []string{"pin"})
	require.True(t, r.IsSensitive("PIN"))
	require.True(t, r.IsSensitive("card_pin"))
	require.False(t, r.IsSensitive("token"))

	masked := r.Redact(map[string]any{"pin": "1234", "user": "u"})
	require.Equal(t, "[REDACTED]", masked.(map[string]any)["pin"])
	require.Equal(t, "u", masked.(map[string]any)["user"])

	require.Equal(t, audit.RedactedMask, audit.NewRedactor("", nil).Mask())
}

func TestJSONLWriterStableFieldOrder(t *testing.T) {
	buf := &bytes.Buffer{}
	recorder, _, _ := newTestRecorder(t, audit.NewJSONLWriter(buf))

	_, err := recorder.Record(context.Background(), audit.Event{
		Action:    "node.drain",
		FromState: "ready",
		ToState:   "drained",
	})
	require.NoError(t, err)

	line := strings.TrimRight(buf.String(), "\n")
	require.Contains(t, line, `"sequence":1`)
	require.Contains(t, line, `"operation_id":`)
	require.Contains(t, line, `"action":"node.drain"`)
	require.Contains(t, line, `"from_state":"ready"`)
	require.Contains(t, line, `"to_state":"drained"`)

	// Struct declaration order defines the stable on-the-wire field order.
	positions := []int{
		strings.Index(line, `"sequence"`),
		strings.Index(line, `"timestamp"`),
		strings.Index(line, `"operation_id"`),
		strings.Index(line, `"action"`),
		strings.Index(line, `"status"`),
	}
	for _, p := range positions {
		require.GreaterOrEqual(t, p, 0, "all expected fields must be present")
	}
	require.Equal(t, []int{0, 1, 2, 3, 4}, fieldRanks(positions))

	var decoded audit.Event
	require.NoError(t, json.Unmarshal([]byte(line), &decoded))
	require.Equal(t, int64(1), decoded.Sequence)
	require.Equal(t, "node.drain", decoded.Action)
}

func TestMetricsObservedPerActionAndStatus(t *testing.T) {
	writer := &sliceWriter{}
	recorder, clock, registry := newTestRecorder(t, writer)

	startedAt := clock.now()
	clock.advance(50 * time.Millisecond)
	_, err := recorder.RecordTransition(context.Background(), "etcd.save", "", "done", startedAt, nil, nil)
	require.NoError(t, err)

	_, err = recorder.Record(context.Background(), audit.Event{
		Action: "etcd.save",
		Status: audit.StatusError,
		Error:  &audit.ErrorDetail{Message: "boom"},
	})
	require.NoError(t, err)

	families, err := registry.Gather()
	require.NoError(t, err)

	counters := sumCounter(families, "k3s_audit_events_total", map[string]string{
		"action": "etcd.save",
	})
	require.Equal(t, 2.0, counters)

	histograms := histogramSamples(families, "k3s_audit_operation_duration_seconds", map[string]string{
		"action": "etcd.save",
		"status": audit.StatusSuccess,
	})
	require.Len(t, histograms, 1)
	require.Equal(t, uint64(1), histograms[0].GetSampleCount())
	require.InDelta(t, 0.05, histograms[0].GetSampleSum(), 0.0001)
}

func TestDiscardWriterAndNilRegisterer(t *testing.T) {
	recorder, err := audit.NewRecorder(audit.Config{Writer: audit.DiscardWriter{}})
	require.NoError(t, err)
	event, err := recorder.Record(context.Background(), audit.Event{Action: "noop"})
	require.NoError(t, err)
	require.Equal(t, int64(1), event.Sequence)
}

// fieldRanks converts byte offsets into order ranks so the assertion is
// independent of the absolute field positions.
func fieldRanks(positions []int) []int {
	ranks := make([]int, len(positions))
	for i := range positions {
		for j := range positions {
			if positions[j] < positions[i] {
				ranks[i]++
			}
		}
	}
	return ranks
}

// metric lookup helpers -------------------------------------------------------

func sumCounter(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if hasLabels(metric, labels) {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

func histogramSamples(families []*dto.MetricFamily, name string, labels map[string]string) []*dto.Histogram {
	var out []*dto.Histogram
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if hasLabels(metric, labels) {
				out = append(out, metric.GetHistogram())
			}
		}
	}
	return out
}

func hasLabels(metric *dto.Metric, want map[string]string) bool {
	for key, value := range want {
		found := false
		for _, label := range metric.GetLabel() {
			if label.GetName() == key && label.GetValue() == value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
