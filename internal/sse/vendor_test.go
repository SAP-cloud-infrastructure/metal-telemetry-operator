// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

type eventServiceFixture struct {
	ODataType          string   `json:"@odata.type"`
	ServerSentEventURI string   `json:"ServerSentEventUri,omitempty"`
	ServiceEnabled     bool     `json:"ServiceEnabled,omitempty"`
	EventFormatTypes   []string `json:"EventFormatTypes,omitempty"`
	Subscriptions      *struct {
		ODataID string `json:"@odata.id"`
	} `json:"Subscriptions,omitempty"`
	SSEFilterPropertiesSupported *struct {
		EventFormatType bool `json:"EventFormatType"`
	} `json:"SSEFilterPropertiesSupported,omitempty"`
}

const testSSEURI = "/redfish/v1/EventService/ServerSentEvent"
const testODataType = "#EventService.v1_10_0.EventService"

func serveFixture(t *testing.T, fix eventServiceFixture) (host string, shutdown func()) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fix)
	}))
	// strip "https://"
	host = strings.TrimPrefix(srv.URL, "https://")
	return host, srv.Close
}

func TestProbeEventService_hpeILO_MetricReport(t *testing.T) {
	// HPE iLO: EventService.ServiceEnabled=true but no ServerSentEventUri.
	// Expected: ModeMetricReport (push subscriptions available, no SSE).
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType:      testODataType,
		ServiceEnabled: true,
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != sse.ModeMetricReport {
		t.Errorf("expected MetricReport, got %v", cfg.Mode)
	}
	if !cfg.ServiceEnabled {
		t.Errorf("expected ServiceEnabled=true")
	}
	if cfg.SSEURL() != "" {
		t.Errorf("expected empty SSEURL for MetricReport mode, got %q", cfg.SSEURL())
	}
}

func TestProbeEventService_noSSEURI_serviceDisabled(t *testing.T) {
	// No SSE URI and ServiceEnabled=false → Unsupported (not MetricReport).
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType: testODataType,
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != sse.ModeUnsupported {
		t.Errorf("expected Unsupported, got %v", cfg.Mode)
	}
}

func TestProbeEventService_vendorSpecificPathWithFilter(t *testing.T) {
	// Older Dell iDRAC9 firmware (v1_7_2/v1_8_0): uses a vendor-specific SSE path
	// (/redfish/v1/EventService/ServerSentEvent) and requires quoted OData filter syntax.
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType:          "#EventService.v1_8_0.EventService",
		ServerSentEventURI: testSSEURI,
		SSEFilterPropertiesSupported: &struct {
			EventFormatType bool `json:"EventFormatType"`
		}{EventFormatType: true},
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != sse.ModeSSE {
		t.Errorf("expected SSE, got %v", cfg.Mode)
	}
	if cfg.FilterSyntax != sse.FilterSyntaxQuoted {
		t.Errorf("expected Quoted, got %v", cfg.FilterSyntax)
	}
	if !strings.Contains(cfg.SSEURL(), "%27MetricReport%27") {
		t.Errorf("SSEURL should use quoted filter: %v", cfg.SSEURL())
	}
}

func TestProbeEventService_standardSSEPath(t *testing.T) {
	// Newer Dell iDRAC9 firmware (v1_10_2+) and HPE iLO: use the standardised
	// /redfish/v1/SSE path and require unquoted OData filter syntax.
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType:          "#EventService.v1_10_2.EventService",
		ServerSentEventURI: "/redfish/v1/SSE",
		SSEFilterPropertiesSupported: &struct {
			EventFormatType bool `json:"EventFormatType"`
		}{EventFormatType: true},
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FilterSyntax != sse.FilterSyntaxUnquoted {
		t.Errorf("expected Unquoted for /redfish/v1/SSE path, got %v", cfg.FilterSyntax)
	}
	if strings.Contains(cfg.SSEURL(), "%27") {
		t.Errorf("SSEURL should not use quoted filter: %v", cfg.SSEURL())
	}
}

func TestProbeEventService_dellIDRAC10(t *testing.T) {
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType:          "#EventService.v1_11_0.EventService",
		ServerSentEventURI: testSSEURI,
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FilterSyntax != sse.FilterSyntaxQuoted {
		t.Errorf("expected Quoted for iDRAC10, got %v", cfg.FilterSyntax)
	}
	if !strings.Contains(cfg.SSEURL(), "%27MetricReport%27") {
		t.Errorf("SSEURL should use quoted filter: %v", cfg.SSEURL())
	}
}

func TestProbeEventService_dellIDRAC9(t *testing.T) {
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType:          "#EventService.v1_10_0.EventService",
		ServerSentEventURI: testSSEURI,
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FilterSyntax != sse.FilterSyntaxUnquoted {
		t.Errorf("expected Unquoted for iDRAC9, got %v", cfg.FilterSyntax)
	}
}

func TestProbeEventService_serviceDisabled_subscriptionsPresent(t *testing.T) {
	// HPE iLO variant: ServiceEnabled=false but Subscriptions collection is present.
	// Old heuristic → Unsupported. New heuristic → MetricReport.
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType: testODataType,
		// ServiceEnabled intentionally omitted (false)
		Subscriptions: &struct {
			ODataID string `json:"@odata.id"`
		}{ODataID: "/redfish/v1/EventService/Subscriptions"},
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != sse.ModeMetricReport {
		t.Errorf("expected MetricReport (subscriptions link present), got %v", cfg.Mode)
	}
}

func TestProbeEventService_eventFormatTypes_MetricReport(t *testing.T) {
	// BMC that advertises EventFormatTypes=["MetricReport"] without ServiceEnabled.
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType:        "#EventService.v1_10_0.EventService",
		EventFormatTypes: []string{"MetricReport"},
		// ServiceEnabled intentionally omitted (false)
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != sse.ModeMetricReport {
		t.Errorf("expected MetricReport (EventFormatTypes contains MetricReport), got %v", cfg.Mode)
	}
	if len(cfg.EventFormatTypes) == 0 || cfg.EventFormatTypes[0] != "MetricReport" {
		t.Errorf("expected EventFormatTypes to be propagated, got %v", cfg.EventFormatTypes)
	}
}

func TestProbeEventService_noSignals_unsupported(t *testing.T) {
	// No SSE URI, no EventFormatTypes, no Subscriptions, ServiceEnabled=false.
	// Must remain Unsupported — not a false-positive MetricReport.
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType: testODataType,
	})
	defer shutdown()

	cfg, err := sse.ProbeEventService(context.Background(), host, "admin", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Mode != sse.ModeUnsupported {
		t.Errorf("expected Unsupported (no positive signals), got %v", cfg.Mode)
	}
}
