package fvm

import (
	"time"

	"github.com/onflow/cadence/runtime/common"
)

// A MetricsCollector accumulates performance metrics reported by the Cadence runtime.
//
// A single collector instance will sum all reported values. For example, the "parsed" field will be
// incremented each time a program is parsed.
type MetricsCollector struct {
	parsed      time.Duration
	checked     time.Duration
	interpreted time.Duration
}

// NewMetricsCollectors returns a new runtime metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

func (m *MetricsCollector) Parsed() time.Duration      { return m.parsed }
func (m *MetricsCollector) Checked() time.Duration     { return m.checked }
func (m *MetricsCollector) Interpreted() time.Duration { return m.interpreted }

type metricsCollector struct {
	*MetricsCollector
}

func (m metricsCollector) ProgramParsed(location common.Location, duration time.Duration) {
	if m.MetricsCollector != nil {
		m.parsed += duration
	}
}

func (m metricsCollector) ProgramChecked(location common.Location, duration time.Duration) {
	if m.MetricsCollector != nil {
		m.checked += duration
	}
}

func (m metricsCollector) ProgramInterpreted(location common.Location, duration time.Duration) {
	if m.MetricsCollector != nil {
		m.interpreted += duration
	}
}

func (m metricsCollector) ValueEncoded(duration time.Duration) {}
func (m metricsCollector) ValueDecoded(duration time.Duration) {}

type noopMetricsCollector struct{}

func (m noopMetricsCollector) ProgramParsed(location common.Location, duration time.Duration)      {}
func (m noopMetricsCollector) ProgramChecked(location common.Location, duration time.Duration)     {}
func (m noopMetricsCollector) ProgramInterpreted(location common.Location, duration time.Duration) {}
func (m noopMetricsCollector) ValueEncoded(duration time.Duration)                                 {}
func (m noopMetricsCollector) ValueDecoded(duration time.Duration)                                 {}
