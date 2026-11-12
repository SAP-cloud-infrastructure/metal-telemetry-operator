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

	mgr := sse.NewConnectionManager(10, nil, nil, logr.Discard())

	baseCfg := sse.ConnConfig{
		BMCName:  "bmc1",
		Host:     host,
		Username: "user",
		Password: "pass",
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

func TestConnectionManager_StopBMC_leavesOtherBMC(t *testing.T) {
	host, shutdown := blockingSSEServer(t)
	defer shutdown()

	// Use maxConcurrent=3: 2 for bmc1, 1 for bmc2.
	// If StopBMC("bmc1") leaks slots, the final Ensure for bmc1 will deadlock.
	mgr := sse.NewConnectionManager(3, nil, nil, logr.Discard())

	makeCfg := func(bmc string, kind telemetryv1alpha1.StreamKind) sse.ConnConfig {
		return sse.ConnConfig{
			BMCName:    bmc,
			Host:       host,
			Username:   "user",
			Password:   "pass",
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
