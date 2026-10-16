package audit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/k3s-io/k3s/pkg/audit"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestConcurrentRecordPreservesStableOrdering hammers the recorder from many
// goroutines and verifies that the sink receives a gap-free, duplicate-free
// permutation of sequence 1..N, that events stay correlated to their
// operation ID, that sensitive attributes are redacted under concurrency, and
// that the metrics reflect every written event. Run with -race.
func TestConcurrentRecordPreservesStableOrdering(t *testing.T) {
	const goroutines = 32
	const perGoroutine = 100
	const total = goroutines * perGoroutine

	buf := &bytes.Buffer{}
	registry := prometheus.NewRegistry()
	recorder, err := audit.NewRecorder(audit.Config{
		Writer:     audit.NewJSONLWriter(buf),
		Registerer: registry,
	})
	require.NoError(t, err)

	errCh := make(chan error, total)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		operationID := "op-" + strconv.Itoa(g)
		ctx := audit.WithOperationID(context.Background(), operationID)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if i%2 == 0 {
					_, err := recorder.Record(ctx, audit.Event{
						Action: "concurrent.record",
						Attributes: map[string]any{
							"iteration": i,
							"token":     "clear-text-token",
						},
					})
					if err != nil {
						errCh <- err
						return
					}
					continue
				}
				op := recorder.Start(ctx, "concurrent.operation", "started")
				_, err := op.Complete(ctx, "done", nil, map[string]any{
					"iteration": i,
					"secret":    "clear-text-secret",
				})
				if err != nil {
					errCh <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	events := decodeLines(t, buf.Bytes())
	require.Len(t, events, total)

	seen := make(map[int64]bool, total)
	perOperation := make(map[string]int, goroutines)
	for _, event := range events {
		require.False(t, seen[event.Sequence], "duplicate sequence %d", event.Sequence)
		seen[event.Sequence] = true
		perOperation[event.OperationID]++

		require.Equal(t, audit.StatusSuccess, event.Status)
		require.NotZero(t, event.Timestamp)

		if event.Action == "concurrent.operation" {
			require.GreaterOrEqual(t, int64(event.Duration), int64(0))
		}

		mask := audit.RedactedMask
		switch event.Action {
		case "concurrent.record":
			require.Equal(t, mask, event.Attributes["token"])
		case "concurrent.operation":
			require.Equal(t, mask, event.Attributes["secret"])
		}
	}

	// Exactly 1..total, in a stable global order despite concurrent writers.
	for seq := int64(1); seq <= total; seq++ {
		require.True(t, seen[seq], "missing sequence %d", seq)
	}
	for operationID, count := range perOperation {
		require.Equal(t, perGoroutine, count, "operation %s event count", operationID)
	}

	// Every successful write must be reflected exactly once in the counters.
	families, err := registry.Gather()
	require.NoError(t, err)
	var counted float64
	for _, family := range families {
		if family.GetName() != "k3s_audit_events_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			counted += metric.GetCounter().GetValue()
		}
	}
	require.Equal(t, float64(total), counted)
}

// TestConcurrentOperationsSharingContext starts many operations that share the
// same propagated operation ID and verifies all of their events correlate.
func TestConcurrentOperationsSharingContext(t *testing.T) {
	const goroutines = 16

	buf := &bytes.Buffer{}
	recorder, err := audit.NewRecorder(audit.Config{Writer: audit.NewJSONLWriter(buf)})
	require.NoError(t, err)

	ctx := audit.WithOperationID(context.Background(), "shared-op")
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := recorder.Start(ctx, "shared.work", "queued")
			time.Sleep(time.Millisecond)
			if _, err := op.Complete(ctx, "finished", nil, nil); err != nil {
				errCh <- fmt.Errorf("complete: %w", err)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	events := decodeLines(t, buf.Bytes())
	require.Len(t, events, goroutines)
	previous := int64(0)
	for _, event := range events {
		require.Equal(t, "shared-op", event.OperationID)
		require.Equal(t, event.Sequence, previous+1)
		previous = event.Sequence
	}
}

func decodeLines(t *testing.T, data []byte) []audit.Event {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	var events []audit.Event
	for {
		var event audit.Event
		if err := decoder.Decode(&event); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decode audit event: %v", err)
		}
		events = append(events, event)
	}
	return events
}
