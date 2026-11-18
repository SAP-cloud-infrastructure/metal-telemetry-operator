// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	promsink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
)

const (
	streamConnectionStatusMetric = "redfish_stream_connection_status"
	reasonConnected              = "connected"
)

func newStreamConnectionSink(t *testing.T) (*promsink.StreamConnectionSink, *promclient.Registry) {
	t.Helper()
	reg := promclient.NewRegistry()
	s, err := promsink.NewStreamConnectionSink(reg)
	if err != nil {
		t.Fatalf("NewStreamConnectionSink: %v", err)
	}
	return s, reg
}

// collectStreamStatus returns a map of stream_type → (reason, value) for the given hostname.
func collectStreamStatus(t *testing.T, reg *promclient.Registry, hostname string) map[string]struct {
	reason string
	value  float64
} {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	result := make(map[string]struct {
		reason string
		value  float64
	})
	for _, mf := range mfs {
		if mf.GetName() != streamConnectionStatusMetric {
			continue
		}
		for _, m := range mf.GetMetric() {
			var host, streamType, reason string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "hostname":
					host = lp.GetValue()
				case "stream_type":
					streamType = lp.GetValue()
				case "reason":
					reason = lp.GetValue()
				}
			}
			if host != hostname {
				continue
			}
			result[streamType] = struct {
				reason string
				value  float64
			}{reason, m.GetGauge().GetValue()}
		}
	}
	return result
}

func countStreamSeries(t *testing.T, reg *promclient.Registry) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == streamConnectionStatusMetric {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func labelNames(mf *dto.MetricFamily) []string {
	if len(mf.GetMetric()) == 0 {
		return nil
	}
	var names []string
	for _, lp := range mf.GetMetric()[0].GetLabel() {
		names = append(names, lp.GetName())
	}
	return names
}

// TestStreamConnectionSink_InitialRecord verifies that RecordStreamStatus writes
// two independent series (metrics and alerts) for a BMC on first call.
func TestStreamConnectionSink_InitialRecord(t *testing.T) {
	s, reg := newStreamConnectionSink(t)

	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)
	s.RecordStreamStatus("bmc1", "alerts", reasonConnected)

	series := collectStreamStatus(t, reg, "bmc1")
	if len(series) != 2 {
		t.Fatalf("expected 2 series for bmc1, got %d", len(series))
	}
	if series["metrics"].reason != reasonConnected || series["metrics"].value != 1.0 {
		t.Errorf("metrics: got reason=%q value=%v, want connected/1", series["metrics"].reason, series["metrics"].value)
	}
	if series["alerts"].reason != reasonConnected || series["alerts"].value != 1.0 {
		t.Errorf("alerts: got reason=%q value=%v, want connected/1", series["alerts"].reason, series["alerts"].value)
	}
}

// TestStreamConnectionSink_ValueZeroForNonConnected verifies value=0 for non-connected reasons.
func TestStreamConnectionSink_ValueZeroForNonConnected(t *testing.T) {
	s, reg := newStreamConnectionSink(t)

	for _, reason := range []string{"disconnected", "slots_full", "excluded", "probe_failed"} {
		s.RecordStreamStatus("bmc1", "metrics", reason)
		series := collectStreamStatus(t, reg, "bmc1")
		if series["metrics"].value != 0.0 {
			t.Errorf("reason=%q: expected value=0, got %v", reason, series["metrics"].value)
		}
	}
}

// TestStreamConnectionSink_ReasonChangeDeletesOld verifies atomic swap: old series is
// deleted before the new one is written when reason changes.
func TestStreamConnectionSink_ReasonChangeDeletesOld(t *testing.T) {
	s, reg := newStreamConnectionSink(t)

	s.RecordStreamStatus("bmc1", "metrics", "probe_failed")
	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)

	series := collectStreamStatus(t, reg, "bmc1")
	// Only one series for the metrics stream — the old probe_failed series must be gone.
	if len(series) != 1 {
		t.Fatalf("expected 1 series after reason change, got %d", len(series))
	}
	if series["metrics"].reason != reasonConnected {
		t.Errorf("expected reason=connected after swap, got %q", series["metrics"].reason)
	}
}

// TestStreamConnectionSink_ForgetStreamStatus verifies both stream_type series are removed.
func TestStreamConnectionSink_ForgetStreamStatus(t *testing.T) {
	s, reg := newStreamConnectionSink(t)

	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)
	s.RecordStreamStatus("bmc1", "alerts", "disconnected")
	s.RecordStreamStatus("bmc2", "metrics", reasonConnected)

	s.ForgetStreamStatus("bmc1")

	if n := countStreamSeries(t, reg); n != 1 {
		t.Fatalf("expected 1 series after ForgetStreamStatus, got %d", n)
	}
	if series := collectStreamStatus(t, reg, "bmc1"); len(series) != 0 {
		t.Errorf("expected no series for bmc1 after Forget, got %d", len(series))
	}
	if series := collectStreamStatus(t, reg, "bmc2"); series["metrics"].reason != reasonConnected {
		t.Errorf("bmc2 metrics series should still exist")
	}
}

// TestStreamConnectionSink_ForgetIdempotent verifies ForgetStreamStatus is safe to call twice.
func TestStreamConnectionSink_ForgetIdempotent(t *testing.T) {
	s, _ := newStreamConnectionSink(t)

	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)
	s.ForgetStreamStatus("bmc1")
	s.ForgetStreamStatus("bmc1") // must not panic or error
}

// TestStreamConnectionSink_IndependentStreams verifies that updating one stream_type
// does not affect the other stream_type series.
func TestStreamConnectionSink_IndependentStreams(t *testing.T) {
	s, reg := newStreamConnectionSink(t)

	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)
	s.RecordStreamStatus("bmc1", "alerts", reasonConnected)

	// Alerts stream disconnects; metrics stays connected.
	s.RecordStreamStatus("bmc1", "alerts", "disconnected")

	series := collectStreamStatus(t, reg, "bmc1")
	if series["metrics"].reason != reasonConnected || series["metrics"].value != 1.0 {
		t.Errorf("metrics should remain connected, got reason=%q value=%v", series["metrics"].reason, series["metrics"].value)
	}
	if series["alerts"].reason != "disconnected" || series["alerts"].value != 0.0 {
		t.Errorf("alerts should be disconnected, got reason=%q value=%v", series["alerts"].reason, series["alerts"].value)
	}
}

// TestStreamConnectionSink_MultipleBMCs verifies independent tracking per BMC.
func TestStreamConnectionSink_MultipleBMCs(t *testing.T) {
	s, reg := newStreamConnectionSink(t)

	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)
	s.RecordStreamStatus("bmc2", "metrics", "probe_failed")

	if n := countStreamSeries(t, reg); n != 2 {
		t.Fatalf("expected 2 series for 2 BMCs, got %d", n)
	}

	series1 := collectStreamStatus(t, reg, "bmc1")
	if series1["metrics"].value != 1.0 {
		t.Errorf("bmc1 metrics should be 1, got %v", series1["metrics"].value)
	}

	series2 := collectStreamStatus(t, reg, "bmc2")
	if series2["metrics"].reason != "probe_failed" {
		t.Errorf("bmc2 metrics should be probe_failed, got %q", series2["metrics"].reason)
	}
}

// TestStreamConnectionSink_NoVendorModelLabels verifies vendor and model labels are absent.
func TestStreamConnectionSink_NoVendorModelLabels(t *testing.T) {
	s, reg := newStreamConnectionSink(t)
	s.RecordStreamStatus("bmc1", "metrics", reasonConnected)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != streamConnectionStatusMetric {
			continue
		}
		for _, name := range labelNames(mf) {
			if name == "vendor" || name == "model" {
				t.Errorf("unexpected label %q in %s", name, streamConnectionStatusMetric)
			}
		}
	}
}
