package nakamacluster

import (
	"context"
	"io"
	"time"

	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/prometheus"
	"go.uber.org/atomic"
)

type Metrics struct {
	cancelFn          context.CancelFunc
	snapshotRateSec   *atomic.Float64
	snapshotRecvKbSec *atomic.Float64
	snapshotSentKbSec *atomic.Float64

	currentReqCount  *atomic.Int64
	currentRecvCount *atomic.Int64
	currentRecvBytes *atomic.Int64
	currentSentBytes *atomic.Int64
	PrometheusScope  tally.Scope
	prometheusCloser io.Closer
}

func (m *Metrics) RecvBroadcast(recvBytes int64) {
	m.currentReqCount.Inc()
	m.currentRecvBytes.Add(recvBytes)
}

func (m *Metrics) SentBroadcast(sentBytes int64) {
	m.currentRecvCount.Inc()
	m.currentSentBytes.Add(sentBytes)
}

func (m *Metrics) PingMs(elapsed time.Duration) {
	m.PrometheusScope.Timer("overall_ping_ms").Record(elapsed)
}

func newMetrics(reporter prometheus.Reporter) *Metrics {
	ctx, cancelFn := context.WithCancel(context.Background())
	m := &Metrics{
		cancelFn:          cancelFn,
		snapshotRateSec:   atomic.NewFloat64(0),
		snapshotRecvKbSec: atomic.NewFloat64(0),
		snapshotSentKbSec: atomic.NewFloat64(0),

		currentRecvCount: atomic.NewInt64(0),
		currentReqCount:  atomic.NewInt64(0),
		currentRecvBytes: atomic.NewInt64(0),
		currentSentBytes: atomic.NewInt64(0),
	}

	go func() {
		const snapshotFrequencySec = 5
		ticker := time.NewTicker(snapshotFrequencySec * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reqCount := float64(m.currentReqCount.Swap(0))
				recvBytes := float64(m.currentRecvBytes.Swap(0))
				sentBytes := float64(m.currentSentBytes.Swap(0))

				m.snapshotRateSec.Store(reqCount / snapshotFrequencySec)
				m.snapshotRecvKbSec.Store((recvBytes / 1024) / snapshotFrequencySec)
				m.snapshotSentKbSec.Store((sentBytes / 1024) / snapshotFrequencySec)
			}
		}
	}()

	tags := map[string]string{"node_name": "dd"}
	tags["namespace"] = "namespace"
	m.PrometheusScope, m.prometheusCloser = tally.NewRootScope(tally.ScopeOptions{
		Prefix:          "",
		Tags:            tags,
		CachedReporter:  reporter,
		Separator:       prometheus.DefaultSeparator,
		SanitizeOptions: &prometheus.DefaultSanitizerOpts,
	}, time.Duration(60)*time.Second)
	// ReportingFreqSec
	return m
}
