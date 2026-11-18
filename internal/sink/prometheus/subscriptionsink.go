// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const streamConnectionStatusName = "stream_connection_status"

// streamKey uniquely identifies one Prometheus series by BMC name and stream type.
type streamKey struct {
	bmcName    string
	streamType string // "metrics" or "alerts"
}

// StreamConnectionSink records per-stream connection status for each BMC as
// the redfish_stream_connection_status gauge.
//
// Label set: {hostname, stream_type, reason}.
// Value is 1 when the stream is live (reason=="connected"), 0 otherwise.
// Exactly two series per BMC: one for stream_type="metrics", one for stream_type="alerts".
type StreamConnectionSink struct {
	gauge *prometheus.GaugeVec

	mu      sync.Mutex
	tracked map[streamKey]prometheus.Labels
}

func NewStreamConnectionSink(reg prometheus.Registerer) (*StreamConnectionSink, error) {
	s := &StreamConnectionSink{
		tracked: make(map[streamKey]prometheus.Labels),
	}
	s.gauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricNamespace,
		Name:      streamConnectionStatusName,
		Help: "Redfish stream connection status per BMC per stream type: 1 = connected, " +
			"0 = not connected. The reason label explains the current state.",
	}, []string{labelHostname, "stream_type", "reason"})
	if err := reg.Register(s.gauge); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *StreamConnectionSink) RecordStreamStatus(bmcName, streamType, reason string) {
	var v float64
	if reason == "connected" {
		v = 1
	}
	labels := prometheus.Labels{
		labelHostname: bmcName,
		"stream_type": streamType,
		"reason":      reason,
	}
	key := streamKey{bmcName, streamType}
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.tracked[key]; ok && !streamLabelsEqual(prev, labels) {
		s.gauge.Delete(prev)
	}
	s.tracked[key] = labels
	s.gauge.With(labels).Set(v)
}

// ForgetStreamStatus removes both stream_type series for the given BMC. Idempotent.
func (s *StreamConnectionSink) ForgetStreamStatus(bmcName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, streamType := range []string{"metrics", "alerts"} {
		key := streamKey{bmcName, streamType}
		if prev, ok := s.tracked[key]; ok {
			s.gauge.Delete(prev)
			delete(s.tracked, key)
		}
	}
}

func streamLabelsEqual(a, b prometheus.Labels) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
