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
	ODataType                    string `json:"@odata.type"`
	ServerSentEventURI           string `json:"ServerSentEventUri,omitempty"`
	SSEFilterPropertiesSupported *struct {
		EventFormatType bool `json:"EventFormatType"`
	} `json:"SSEFilterPropertiesSupported,omitempty"`
}

const testSSEURI = "/redfish/v1/EventService/ServerSentEvent"

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

func TestProbeEventService_noSSEURI(t *testing.T) {
	host, shutdown := serveFixture(t, eventServiceFixture{
		ODataType: "#EventService.v1_10_0.EventService",
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

func TestProbeEventService_lenovo(t *testing.T) {
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
	if cfg.FilterSyntax != sse.FilterSyntaxUnquoted {
		t.Errorf("expected Unquoted, got %v", cfg.FilterSyntax)
	}
	if !strings.Contains(cfg.SSEURL(), "MetricReport") {
		t.Errorf("SSEURL missing MetricReport filter: %v", cfg.SSEURL())
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
