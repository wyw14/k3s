package audit

import (
	"time"

	"github.com/k3s-io/k3s/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
)

// Metric label names.
const (
	labelAction = "action"
	labelStatus = "status"
)

// collectors bundles the Prometheus metrics maintained per Recorder.
type collectors struct {
	operationDuration *prometheus.HistogramVec
	eventsTotal       *prometheus.CounterVec
}

// newCollectors builds the audit metrics and, when registerer is non-nil,
// registers them with it. A nil registerer leaves the collectors usable but
// unregistered, which is convenient for tests.
func newCollectors(registerer prometheus.Registerer) (*collectors, error) {
	c := &collectors{
		operationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    version.Program + "_audit_operation_duration_seconds",
			Help:    "Time in seconds spent on audited operations, labeled by action and success/error status.",
			Buckets: prometheus.DefBuckets,
		}, []string{labelAction, labelStatus}),
		eventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: version.Program + "_audit_events_total",
			Help: "Total number of audit events written, labeled by action and success/error status.",
		}, []string{labelAction, labelStatus}),
	}
	if registerer == nil {
		return c, nil
	}
	if err := registerer.Register(c.operationDuration); err != nil {
		return nil, err
	}
	if err := registerer.Register(c.eventsTotal); err != nil {
		return nil, err
	}
	return c, nil
}

// observe records one completed operation: its duration and one emitted event.
func (c *collectors) observe(action, status string, duration time.Duration) {
	c.operationDuration.WithLabelValues(action, status).Observe(duration.Seconds())
	c.eventsTotal.WithLabelValues(action, status).Inc()
}
