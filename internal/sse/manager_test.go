// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

const testStandardSSEURI = "/redfish/v1/SSE"

const (
	testUsername = "user"
	testPassword = "pass"
)

// blockingSSEServer returns a test TLS server that holds SSE connections open until
// the request context is cancelled.
func blockingSSEServer(t *testing.T) (host string, shutdown func()) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	return strings.TrimPrefix(srv.URL, "https://"), srv.Close
}

func TestConnectionManager_StopBMC_stopsBothStreams(t *testing.T) {
	host, shutdown := blockingSSEServer(t)
	defer shutdown()

	mgr := sse.NewConnectionManager(10, nil, nil, nil, nil, logr.Discard())

	baseCfg := sse.ConnConfig{
		Host:     host,
		Username: testUsername,
		Password: testPassword,
		Vendor: sse.VendorConfig{
			Mode:   sse.ModeSSE,
			SSEURI: testStandardSSEURI,
		},
	}

	metricsCfg := baseCfg

	alertsCfg := baseCfg
	alertsCfg.StreamKind = telemetryv1alpha1.StreamKindAlerts

	if err := mgr.Ensure(context.Background(), metricsCfg); err != nil {
		t.Fatalf("Ensure metrics: %v", err)
	}
	if err := mgr.Ensure(context.Background(), alertsCfg); err != nil {
		t.Fatalf("Ensure alerts: %v", err)
	}

	// StopBMC must return, proving both goroutines exited.
	done := make(chan struct{})
	go func() {
		mgr.StopBMC("bmc1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("StopBMC did not return within 5 s — goroutines likely leaked")
	}
}

func TestConnectionManager_StreamKinds_UseDistinctURLs(t *testing.T) {
	urlCh := make(chan string, 2)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		urlCh <- r.URL.RequestURI()
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "https://")

	mgr := sse.NewConnectionManager(10, nil, nil, nil, nil, logr.Discard())

	vendor := sse.VendorConfig{
		Mode:   sse.ModeSSE,
		SSEURI: testStandardSSEURI,
	}

	if err := mgr.Ensure(context.Background(), sse.ConnConfig{
		BMCName:    "bmc-urltest",
		Host:       host,
		Username:   testUsername,
		Password:   testPassword,
		Vendor:     vendor,
		StreamKind: telemetryv1alpha1.StreamKindMetrics,
	}); err != nil {
		t.Fatalf("Ensure metrics: %v", err)
	}
	if err := mgr.Ensure(context.Background(), sse.ConnConfig{
		BMCName:    "bmc-urltest",
		Host:       host,
		Username:   testUsername,
		Password:   testPassword,
		Vendor:     vendor,
		StreamKind: telemetryv1alpha1.StreamKindAlerts,
	}); err != nil {
		t.Fatalf("Ensure alerts: %v", err)
	}

	// Collect two URL paths — each SSE goroutine dials the server once.
	seen := make([]string, 0, 2)
	timeout := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case u := <-urlCh:
			seen = append(seen, u)
		case <-timeout:
			t.Fatalf("timed out waiting for 2 SSE connections; received so far: %v", seen)
		}
	}
	mgr.StopBMC("bmc-urltest")

	var metricsURL, alertsURL string
	for _, u := range seen {
		if strings.Contains(u, "MetricReport") {
			metricsURL = u
		} else {
			alertsURL = u
		}
	}
	if metricsURL == "" {
		t.Errorf("no MetricReport-filtered URL; got: %v", seen)
	}
	if alertsURL == "" {
		t.Errorf("no Event-filtered (alerts) URL; got: %v", seen)
	}
	if metricsURL == alertsURL {
		t.Errorf("expected distinct URLs; both: %s", metricsURL)
	}
}

func TestConnectionManager_StopBMC_leavesOtherBMC(t *testing.T) {
	host, shutdown := blockingSSEServer(t)
	defer shutdown()

	// Use maxConcurrent=3: 2 for bmc1, 1 for bmc2.
	// If StopBMC("bmc1") leaks slots, the final Ensure for bmc1 will deadlock.
	mgr := sse.NewConnectionManager(3, nil, nil, nil, nil, logr.Discard())

	makeCfg := func(bmc string, kind telemetryv1alpha1.StreamKind) sse.ConnConfig {
		return sse.ConnConfig{
			BMCName:    bmc,
			Host:       host,
			Username:   testUsername,
			Password:   testPassword,
			Vendor:     sse.VendorConfig{Mode: sse.ModeSSE, SSEURI: testStandardSSEURI},
			StreamKind: kind,
		}
	}

	if err := mgr.Ensure(context.Background(), makeCfg("bmc1", telemetryv1alpha1.StreamKindMetrics)); err != nil {
		t.Fatalf("Ensure bmc1-metrics: %v", err)
	}
	if err := mgr.Ensure(context.Background(), makeCfg("bmc1", telemetryv1alpha1.StreamKindAlerts)); err != nil {
		t.Fatalf("Ensure bmc1-alerts: %v", err)
	}
	if err := mgr.Ensure(context.Background(), makeCfg("bmc2", telemetryv1alpha1.StreamKindMetrics)); err != nil {
		t.Fatalf("Ensure bmc2-metrics: %v", err)
	}

	// Stop bmc1 — releases 2 semaphore slots.
	mgr.StopBMC("bmc1")

	// Re-ensure bmc1 streams; with only 1 slot still held by bmc2, 2 more should fit.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mgr.Ensure(ctx, makeCfg("bmc1", telemetryv1alpha1.StreamKindMetrics)); err != nil {
		t.Fatalf("re-Ensure bmc1-metrics after StopBMC: %v", err)
	}
	if err := mgr.Ensure(ctx, makeCfg("bmc1", telemetryv1alpha1.StreamKindAlerts)); err != nil {
		t.Fatalf("re-Ensure bmc1-alerts after StopBMC: %v", err)
	}

	mgr.StopAll()
}
