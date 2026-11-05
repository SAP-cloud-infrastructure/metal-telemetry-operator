// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"context"
	"sync"
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	promsinkg "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	promsink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
)

func gaugeSink(t *testing.T) (*promsink.MetricReportSink, *promclient.Registry) {
	t.Helper()
	reg := promclient.NewRegistry()
	s, err := promsink.New(reg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, reg
}

func gaugeValue(t *testing.T, reg *promclient.Registry, bmc, metricID string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "redfish_telemetry_reading" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var gotBMC, gotID string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "hostname":
					gotBMC = lp.GetValue()
				case "metric_id":
					gotID = lp.GetValue()
				}
			}
			if gotBMC == bmc && gotID == metricID {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func countSeries(t *testing.T, reg *promclient.Registry) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	n := 0
	for _, mf := range mfs {
		if mf.GetName() == "redfish_telemetry_reading" {
			n += len(mf.GetMetric())
		}
	}
	return n
}

func TestPublishSamples_setsGauge(t *testing.T) {
	s, reg := gaugeSink(t)
	samples := []promsinkg.Sample{
		{MetricID: "AverageCPUUtilization", Value: 42.5, Type: "Gauge", Unit: "%"},
	}
	if err := s.PublishSamples(context.Background(), "node001", samples); err != nil {
		t.Fatalf("PublishSamples: %v", err)
	}
	v, ok := gaugeValue(t, reg, "node001", "AverageCPUUtilization")
	if !ok {
		t.Fatal("series not found")
	}
	if v != 42.5 {
		t.Errorf("expected 42.5, got %v", v)
	}
}

func TestPublishSamples_emptyNoop(t *testing.T) {
	s, reg := gaugeSink(t)
	if err := s.PublishSamples(context.Background(), "node001", nil); err != nil {
		t.Fatalf("PublishSamples: %v", err)
	}
	if n := countSeries(t, reg); n != 0 {
		t.Errorf("expected 0 series, got %d", n)
	}
}

func TestPublishSamples_skipsEmptyMetricID(t *testing.T) {
	s, reg := gaugeSink(t)
	samples := []promsinkg.Sample{
		{MetricID: "", Value: 1.0},
	}
	if err := s.PublishSamples(context.Background(), "node001", samples); err != nil {
		t.Fatalf("PublishSamples: %v", err)
	}
	if n := countSeries(t, reg); n != 0 {
		t.Errorf("expected 0 series, got %d", n)
	}
}

func TestForget_removesAllSeries(t *testing.T) {
	s, reg := gaugeSink(t)
	samples := []promsinkg.Sample{
		{MetricID: "CPU", Value: 10},
		{MetricID: "Temp", Value: 25},
	}
	if err := s.PublishSamples(context.Background(), "node001", samples); err != nil {
		t.Fatalf("PublishSamples: %v", err)
	}
	if n := countSeries(t, reg); n != 2 {
		t.Fatalf("expected 2 series, got %d", n)
	}
	s.Forget("node001")
	if n := countSeries(t, reg); n != 0 {
		t.Errorf("expected 0 series after Forget, got %d", n)
	}
}

func TestForget_idempotent(t *testing.T) {
	s, _ := gaugeSink(t)
	s.Forget("never-published") // must not panic
}

func TestPublishSamples_concurrent_noRace(t *testing.T) {
	s, _ := gaugeSink(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(2)
		bmc := "node001"
		go func() {
			defer wg.Done()
			_ = s.PublishSamples(context.Background(), bmc, []promsinkg.Sample{
				{MetricID: "CPU", Value: 1},
			})
		}()
		go func() {
			defer wg.Done()
			s.Forget(bmc)
		}()
	}
	wg.Wait()
}

// Ensure dto import is used (pulled in for gauge value reading).
var _ = dto.MetricFamily{}
