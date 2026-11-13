// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"sync"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const (
	metricNamespace      = "redfish"
	telemetryReadingName = "telemetry_reading"

	labelHostname      = "hostname"
	labelMetricID      = "metric_id"
	labelType          = "type"
	labelUnit          = "unit"
	labelOriginContext = "origin_context"

	// defaultMaxMetricBMCs bounds how many distinct BMC names the MetricReportSink
	// will track before dropping incoming samples for unknown hosts.
	defaultMaxMetricBMCs = 2000
	// defaultMaxMetricSeriesPerBMC bounds how many distinct
	// {metric_id,type,unit,origin_context} label tuples will be minted per BMC.
	// Real BMCs report tens to low-hundreds of distinct sensors; 1000 is well
	// above that while keeping total series under 2 million.
	defaultMaxMetricSeriesPerBMC = 1000
)

// seriesKey identifies one redfish_telemetry_reading time series.
type seriesKey struct {
	metricID      string
	valueType     string
	unit          string
	originContext string
}

// MetricReportSink publishes Redfish MetricReport events as the
// redfish_telemetry_reading gauge.
type MetricReportSink struct {
	gauge *prometheus.GaugeVec
	log   logr.Logger

	maxBMCs         int
	maxSeriesPerBMC int

	mu     sync.Mutex
	series map[string]map[seriesKey]struct{}
}

var _ sink.MetricReportSink = (*MetricReportSink)(nil)

// New creates and registers a MetricReportSink with the given Prometheus registerer.
func New(reg prometheus.Registerer) (*MetricReportSink, error) {
	return NewWithLogger(reg, logr.Discard())
}

// NewWithLogger creates a MetricReportSink with the given logger.
func NewWithLogger(reg prometheus.Registerer, log logr.Logger) (*MetricReportSink, error) {
	return NewMetricReportSinkWithOptions(reg, log, defaultMaxMetricBMCs, defaultMaxMetricSeriesPerBMC)
}

// NewMetricReportSinkWithOptions constructs a MetricReportSink with explicit
// cardinality caps. maxBMCs bounds distinct BMC names; maxSeriesPerBMC bounds
// distinct label tuples per BMC. Samples exceeding either cap are dropped.
func NewMetricReportSinkWithOptions(reg prometheus.Registerer, log logr.Logger, maxBMCs, maxSeriesPerBMC int) (*MetricReportSink, error) {
	s := &MetricReportSink{
		log:             log,
		maxBMCs:         maxBMCs,
		maxSeriesPerBMC: maxSeriesPerBMC,
		series:          make(map[string]map[seriesKey]struct{}),
	}
	s.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      telemetryReadingName,
		Help:      "Latest value from Redfish MetricReport SSE stream, labelled by hostname, metric_id, type, unit, and origin_context.",
	}, []string{labelHostname, labelMetricID, labelType, labelUnit, labelOriginContext})
	if err := reg.Register(s.gauge); err != nil {
		return nil, err
	}
	return s, nil
}

// PublishSamples updates the gauge for each sample. The mutex protects both the
// series map and the gauge.Set calls so a concurrent Forget cannot delete a key
// between the map write and the gauge write.
func (s *MetricReportSink) PublishSamples(_ context.Context, bmcName string, samples []sink.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	keys, ok := s.series[bmcName]
	if !ok {
		if len(s.series) >= s.maxBMCs {
			s.log.V(1).Info("Dropping samples: BMC name limit reached", "hostname", bmcName, "limit", s.maxBMCs)
			return nil
		}
		keys = make(map[seriesKey]struct{})
		s.series[bmcName] = keys
	}

	for _, sm := range samples {
		if sm.MetricID == "" {
			continue
		}
		key := seriesKey{
			metricID:      sm.MetricID,
			valueType:     sm.Type,
			unit:          sm.Unit,
			originContext: sm.MetricProperty,
		}
		if _, exists := keys[key]; !exists && len(keys) >= s.maxSeriesPerBMC {
			s.log.V(1).Info("Dropping sample: per-BMC series limit reached",
				"hostname", bmcName, "metricID", sm.MetricID, "limit", s.maxSeriesPerBMC)
			continue
		}
		keys[key] = struct{}{}
		s.gauge.With(prometheus.Labels{
			labelHostname:      bmcName,
			labelMetricID:      key.metricID,
			labelType:          key.valueType,
			labelUnit:          key.unit,
			labelOriginContext: key.originContext,
		}).Set(sm.Value)
	}
	s.log.V(2).Info("Published MetricReport samples", "hostname", bmcName, "count", len(samples))
	return nil
}

// Forget removes all gauge series for bmcName. Safe to call on a BMC that was
// never published.
func (s *MetricReportSink) Forget(bmcName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.series[bmcName]
	delete(s.series, bmcName)

	for key := range keys {
		s.gauge.Delete(prometheus.Labels{
			labelHostname:      bmcName,
			labelMetricID:      key.metricID,
			labelType:          key.valueType,
			labelUnit:          key.unit,
			labelOriginContext: key.originContext,
		})
	}
}
