// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// fakeSink records published samples.
type fakeSink struct {
	mu     sync.Mutex
	calls  []fakeCall
	retErr error
}

type fakeCall struct {
	bmcName string
	count   int
}

func (s *fakeSink) PublishSamples(_ context.Context, bmcName string, samples []sink.Sample) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, fakeCall{bmcName: bmcName, count: len(samples)})
	return s.retErr
}

func (s *fakeSink) Forget(_ string) {}

func (s *fakeSink) totalCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *fakeSink) lastCall() (fakeCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.calls) == 0 {
		return fakeCall{}, false
	}
	return s.calls[len(s.calls)-1], true
}

// fakeStatusWriter records RecordMetricReport and RecordAlertEvent calls.
type fakeStatusWriter struct {
	mu          sync.Mutex
	metricNames []string
	alertNames  []string
}

func (f *fakeStatusWriter) RecordMetricReport(_ context.Context, bmcName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metricNames = append(f.metricNames, bmcName)
}

func (f *fakeStatusWriter) RecordAlertEvent(_ context.Context, bmcName string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alertNames = append(f.alertNames, bmcName)
}

func (f *fakeStatusWriter) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]string, len(f.metricNames))
	copy(cp, f.metricNames)
	return cp
}

func (f *fakeStatusWriter) recordedAlerts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]string, len(f.alertNames))
	copy(cp, f.alertNames)
	return cp
}

// minimalMetricReport builds a minimal MetricReport JSON with one MetricValue.
func minimalMetricReport(metricID, value string) []byte {
	type mv struct {
		MetricID    string `json:"MetricID"`
		MetricValue string `json:"MetricValue"`
		Timestamp   string `json:"Timestamp"`
	}
	type mr struct {
		MetricValues []mv `json:"MetricValues"`
	}
	raw, _ := json.Marshal(mr{MetricValues: []mv{{
		MetricID:    metricID,
		MetricValue: value,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
	}}})
	return raw
}

// alertBody builds a minimal Redfish alert event body.
func alertBody(messageID string) []byte {
	type ev struct {
		MessageID string `json:"MessageId"`
	}
	type env struct {
		Events []ev `json:"Events"`
	}
	raw, _ := json.Marshal(env{Events: []ev{{MessageID: messageID}}})
	return raw
}

func newTestReceiver(t *testing.T, subscriberID string, s sink.MetricReportSink) *push.Receiver {
	t.Helper()
	r, err := push.NewReceiver(push.ReceiverConfig{
		Addr:         ":0",
		SubscriberID: subscriberID,
		Sink:         s,
	})
	if err != nil {
		t.Fatalf("NewReceiver: %v", err)
	}
	return r
}

// --- NewReceiver validation ---

func TestNewReceiver_missingAddr(t *testing.T) {
	_, err := push.NewReceiver(push.ReceiverConfig{Sink: &fakeSink{}})
	if err == nil {
		t.Error("expected error for missing Addr, got nil")
	}
}

func TestNewReceiver_missingSink(t *testing.T) {
	_, err := push.NewReceiver(push.ReceiverConfig{Addr: ":0"})
	if err == nil {
		t.Error("expected error for missing Sink, got nil")
	}
}

// --- MetricReport handler ---

func TestHandleMetricReport_dispatches_samples(t *testing.T) {
	fs := &fakeSink{}
	r := newTestReceiver(t, "sub1", fs)

	body := minimalMetricReport("CPU.Percent", "42")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/metricsreport/node42", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	call, ok := fs.lastCall()
	if !ok {
		t.Fatal("expected sink to be called")
	}
	if call.bmcName != "node42" {
		t.Errorf("got bmcName %q, want node42", call.bmcName)
	}
	if call.count == 0 {
		t.Error("expected at least one sample")
	}
}

func TestHandleMetricReport_statusWriter_called(t *testing.T) {
	fs := &fakeSink{}
	sw := &fakeStatusWriter{}
	r := newTestReceiver(t, "sub1", fs)
	r.SetStatusWriter(sw)

	body := minimalMetricReport("CPU.Percent", "42")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/metricsreport/node99", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	recorded := sw.recorded()
	if len(recorded) != 1 || recorded[0] != "node99" {
		t.Errorf("expected StatusWriter called once with node99, got %v", recorded)
	}
}

func TestHandleMetricReport_noSubscriberID(t *testing.T) {
	fs := &fakeSink{}
	r := newTestReceiver(t, "", fs)

	body := minimalMetricReport("CPU.Percent", "1")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/metricsreport/node1", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
}

func TestHandleMetricReport_wrongMethod(t *testing.T) {
	r := newTestReceiver(t, "s", &fakeSink{})
	req := httptest.NewRequest(http.MethodGet, "/serverevents/s/metricsreport/n1", nil)
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got status %d, want 405", w.Code)
	}
}

func TestHandleMetricReport_missingBMCName(t *testing.T) {
	r := newTestReceiver(t, "sub1", &fakeSink{})
	// Prefix present but no BMC name after it
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/metricsreport/", nil)
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want 400", w.Code)
	}
}

func TestHandleMetricReport_invalidJSON_returns400(t *testing.T) {
	r := newTestReceiver(t, "sub1", &fakeSink{})
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/metricsreport/node1", strings.NewReader("not-json"))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want 400", w.Code)
	}
}

func TestHandleMetricReport_emptySamples_200(t *testing.T) {
	// A valid JSON body with no MetricValues still returns 200 (sink not called).
	fs := &fakeSink{}
	r := newTestReceiver(t, "sub1", fs)
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/metricsreport/node2", strings.NewReader(`{"MetricValues":[]}`))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	if fs.totalCalls() != 0 {
		t.Error("expected sink not called for empty sample list")
	}
}

// --- Alert handler ---

func TestHandleAlerts_resourceRemoved_enqueues(t *testing.T) {
	r := newTestReceiver(t, "sub1", &fakeSink{})
	enqueued := make(chan string, 1)
	r.SetOnResourceRemoved(func(name string) { enqueued <- name })

	body := alertBody("ResourceEvent.1.0.ResourceRemoved")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/alerts/node5", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	select {
	case name := <-enqueued:
		if name != "node5" {
			t.Errorf("enqueued %q, want node5", name)
		}
	case <-time.After(time.Second):
		t.Error("timeout: ResourceRemoved callback not called")
	}
}

func TestHandleAlerts_nonResourceRemoved_does_not_enqueue(t *testing.T) {
	r := newTestReceiver(t, "sub1", &fakeSink{})
	called := false
	r.SetOnResourceRemoved(func(_ string) { called = true })

	body := alertBody("Alert.1.0.Alert")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/alerts/node6", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	if called {
		t.Error("callback should not be called for non-ResourceRemoved event")
	}
}

func TestHandleAlerts_invalidJSON_still_200(t *testing.T) {
	// Alert parse failures must still return 200 to prevent BMC retries.
	r := newTestReceiver(t, "sub1", &fakeSink{})
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/alerts/node7", strings.NewReader("bad json"))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
}

func TestHandleAlerts_wrongMethod(t *testing.T) {
	r := newTestReceiver(t, "s", &fakeSink{})
	req := httptest.NewRequest(http.MethodGet, "/serverevents/s/alerts/n1", nil)
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("got status %d, want 405", w.Code)
	}
}

// --- Health check ---

func TestHealthz(t *testing.T) {
	r := newTestReceiver(t, "sub1", &fakeSink{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
}

func TestHandleAlerts_nonResourceRemoved_calls_RecordAlertEvent(t *testing.T) {
	fs := &fakeSink{}
	sw := &fakeStatusWriter{}
	r := newTestReceiver(t, "sub1", fs)
	r.SetStatusWriter(sw)
	body := alertBody("Alert.1.0.Alert")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/alerts/node8", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	recorded := sw.recordedAlerts()
	if len(recorded) != 1 || recorded[0] != "node8" {
		t.Errorf("expected RecordAlertEvent called once with node8, got %v", recorded)
	}
}

func TestHandleAlerts_resourceRemoved_does_not_call_RecordAlertEvent(t *testing.T) {
	fs := &fakeSink{}
	sw := &fakeStatusWriter{}
	r := newTestReceiver(t, "sub1", fs)
	r.SetStatusWriter(sw)
	body := alertBody("ResourceEvent.1.0.ResourceRemoved")
	req := httptest.NewRequest(http.MethodPost, "/serverevents/sub1/alerts/node9", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	recorded := sw.recordedAlerts()
	if len(recorded) != 0 {
		t.Errorf("expected RecordAlertEvent not called for ResourceRemoved, got %v", recorded)
	}
}

// --- NeedLeaderElection ---

func TestNeedLeaderElection_false(t *testing.T) {
	r := newTestReceiver(t, "sub1", &fakeSink{})
	if r.NeedLeaderElection() {
		t.Error("expected NeedLeaderElection() = false")
	}
}
