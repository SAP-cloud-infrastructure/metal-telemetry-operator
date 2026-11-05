// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse_test

import (
	"encoding/json"
	"testing"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

type mvBuilder struct {
	MetricID       string `json:"MetricId,omitempty"`
	MetricValue    string `json:"MetricValue"`
	MetricProperty string `json:"MetricProperty,omitempty"`
	Timestamp      string `json:"Timestamp,omitempty"`
}

const averageCPUUtilization = "AverageCPUUtilization"

func buildReport(mvs []mvBuilder) []byte {
	body, _ := json.Marshal(map[string]any{"MetricValues": mvs})
	return body
}

func TestParseMetricReport_dellMultiValueDedup(t *testing.T) {
	body := buildReport([]mvBuilder{
		{MetricID: averageCPUUtilization, MetricValue: "10", Timestamp: "2026-01-01T00:00:00Z"},
		{MetricID: averageCPUUtilization, MetricValue: "20", Timestamp: "2026-01-01T00:05:00Z"},
		{MetricID: averageCPUUtilization, MetricValue: "15", Timestamp: "2026-01-01T00:03:00Z"},
	})
	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		t.Fatalf("ParseMetricReport: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample (deduped), got %d", len(samples))
	}
	if samples[0].Value != 20 {
		t.Errorf("expected latest value 20, got %v", samples[0].Value)
	}
}

func TestParseMetricReport_lenovoSentinelFiltered(t *testing.T) {
	body := buildReport([]mvBuilder{
		{MetricID: "CPUUtilization", MetricValue: "255"},
	})
	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		t.Fatalf("ParseMetricReport: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("expected 0 samples (sentinel filtered), got %d", len(samples))
	}
}

func TestParseMetricReport_metricPropertyIDExtraction(t *testing.T) {
	body := buildReport([]mvBuilder{
		{
			MetricProperty: "/redfish/v1/Chassis/1/Thermal#/Temperature/0/ReadingCelsius",
			MetricValue:    "27",
		},
	})
	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		t.Fatalf("ParseMetricReport: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(samples))
	}
	if samples[0].MetricID != "Temperature/0/ReadingCelsius" {
		t.Errorf("expected fragment ID, got %q", samples[0].MetricID)
	}
	if samples[0].Value != 27 {
		t.Errorf("expected value 27, got %v", samples[0].Value)
	}
}

func TestParseMetricReport_emptyMetricValues(t *testing.T) {
	body := buildReport(nil)
	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		t.Fatalf("ParseMetricReport: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("expected 0 samples, got %d", len(samples))
	}
}

func TestParseMetricReport_noIDSkipped(t *testing.T) {
	body := buildReport([]mvBuilder{
		{MetricValue: "42"}, // no MetricId, no MetricProperty
	})
	samples, err := sse.ParseMetricReport(body)
	if err != nil {
		t.Fatalf("ParseMetricReport: %v", err)
	}
	if len(samples) != 0 {
		t.Errorf("expected 0 samples (no ID), got %d", len(samples))
	}
}
