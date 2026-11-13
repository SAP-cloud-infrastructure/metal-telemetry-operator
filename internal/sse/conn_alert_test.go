// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

// alertSSEServer returns a TLS test server that sends one real alert event and one
// test event (Base.1.0.TestMessage), then holds the connection open.
func alertSSEServer(t *testing.T) (host string, shutdown func()) {
	t.Helper()

	realEvent := "id: 1\n" +
		`data: {"@odata.type":"#Event.v1_8_0.Event","Events":[{"EventId":"42","MessageId":"ResourceEvent.ResourceChanged","Severity":"Warning","MessageSeverity":"Warning","EventTimestamp":"2026-01-01T00:00:00Z","OriginOfCondition":{"@odata.id":"/redfish/v1/Chassis/1"}}]}` +
		"\n\n"

	testEvent := "id: 2\n" +
		`data: {"@odata.type":"#Event.v1_3_0.Event","Events":[{"EventId":"","MessageId":"Base.1.0.TestMessage","Severity":"","MessageSeverity":"","EventTimestamp":"","OriginOfCondition":null}]}` +
		"\n\n"

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		_, _ = w.Write([]byte(realEvent))
		f.Flush()
		_, _ = w.Write([]byte(testEvent))
		f.Flush()
		<-r.Context().Done()
	}))
	return strings.TrimPrefix(srv.URL, "https://"), srv.Close
}

// mockEventSink records received events and signals via a channel.
type mockEventSink struct {
	mu     sync.Mutex
	events []sink.Event
	ch     chan []sink.Event
}

func newMockEventSink() *mockEventSink {
	return &mockEventSink{ch: make(chan []sink.Event, 10)}
}

func (m *mockEventSink) PublishEvents(_ context.Context, _ string, events []sink.Event) error {
	m.mu.Lock()
	m.events = append(m.events, events...)
	m.mu.Unlock()
	m.ch <- events
	return nil
}

func (m *mockEventSink) Forget(_ string) {}

// mockConsumeIfTest returns true for Base.1.0.TestMessage and records what it consumed.
type mockConsumeIfTest struct {
	mu       sync.Mutex
	consumed []string
}

func (f *mockConsumeIfTest) ConsumeIfTest(_ string, messageID string) bool {
	if messageID == "Base.1.0.TestMessage" {
		f.mu.Lock()
		f.consumed = append(f.consumed, messageID)
		f.mu.Unlock()
		return true
	}
	return false
}

func TestConn_AlertEventRouting(t *testing.T) {
	host, shutdown := alertSSEServer(t)
	defer shutdown()

	es := newMockEventSink()
	tf := &mockConsumeIfTest{}

	mgr := sse.NewConnectionManager(10, nil, nil, es, tf, logr.Discard())

	cfg := sse.ConnConfig{
		BMCName:    "bmc-alerttest",
		Host:       host,
		Username:   testUsername,
		Password:   testPassword,
		Vendor:     sse.VendorConfig{Mode: sse.ModeSSE, SSEURI: testStandardSSEURI},
		StreamKind: telemetryv1alpha1.StreamKindAlerts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mgr.Ensure(ctx, cfg); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Wait for exactly one PublishEvents call (real event only; test event is filtered).
	select {
	case received := <-es.ch:
		if len(received) != 1 {
			t.Errorf("expected 1 event, got %d: %+v", len(received), received)
		} else if received[0].MessageID != "ResourceEvent.ResourceChanged" {
			t.Errorf("expected MessageID ResourceEvent.ResourceChanged, got %s", received[0].MessageID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for PublishEvents call")
	}

	// Verify no second call arrives (test event must not reach PublishEvents).
	select {
	case unexpected := <-es.ch:
		t.Errorf("unexpected second PublishEvents call with %d events", len(unexpected))
	case <-time.After(300 * time.Millisecond):
		// Good — no second call.
	}

	// Verify ConsumeIfTest was called for the test event.
	tf.mu.Lock()
	consumed := make([]string, len(tf.consumed))
	copy(consumed, tf.consumed)
	tf.mu.Unlock()

	if len(consumed) != 1 || consumed[0] != "Base.1.0.TestMessage" {
		t.Errorf("expected ConsumeIfTest called once for Base.1.0.TestMessage, got %v", consumed)
	}

	mgr.StopBMC("bmc-alerttest")
}

func TestConn_AlertEventRouting_NilTestFilter(t *testing.T) {
	host, shutdown := alertSSEServer(t)
	defer shutdown()

	es := newMockEventSink()

	// No test filter — all events including Base.1.0.TestMessage reach the sink.
	mgr := sse.NewConnectionManager(10, nil, nil, es, nil, logr.Discard())

	cfg := sse.ConnConfig{
		BMCName:    "bmc-notestfilter",
		Host:       host,
		Username:   testUsername,
		Password:   testPassword,
		Vendor:     sse.VendorConfig{Mode: sse.ModeSSE, SSEURI: testStandardSSEURI},
		StreamKind: telemetryv1alpha1.StreamKindAlerts,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mgr.Ensure(ctx, cfg); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// Both real and test events should be forwarded when no filter is set.
	received := make([]sink.Event, 0, 2)
	timeout := time.After(5 * time.Second)
	for len(received) < 2 {
		select {
		case batch := <-es.ch:
			received = append(received, batch...)
		case <-timeout:
			t.Fatalf("timed out; only got %d events", len(received))
		}
	}

	mgr.StopBMC("bmc-notestfilter")
}
