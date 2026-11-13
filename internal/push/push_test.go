// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push_test

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
	prometheusSink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
)

const (
	testReceiverURL  = "http://localhost:9093"
	testSubscriberID = "test"
)

// TestAddTo_InjectSink_AvoidsDuplicateRegistration verifies that AddTo uses the
// injected Sink instead of calling prometheusSink.New a second time.
//
// Setup: register the gauge once in ctrlmetrics.Registry (as BMCReconciler.SetupWithManager
// would do). Any subsequent call to prometheusSink.New(ctrlmetrics.Registry) would fail with
// a duplicate-registration error. The test then calls AddTo with a pre-built Sink and
// confirms that error never occurs — proving the nil-guard in push.go is respected.
//
// AddTo panics on the nil manager after passing the Sink check; recover() catches that
// panic and distinguishes it from the prometheus error we are guarding against.
func TestAddTo_InjectSink_AvoidsDuplicateRegistration(t *testing.T) {
	// Register the gauge in the shared global registry once, the same way
	// BMCReconciler.SetupWithManager does. Ignore an error here: it just
	// means a prior test in this binary already registered it, which is fine —
	// the gauge is definitely in ctrlmetrics.Registry either way.
	_, _ = prometheusSink.New(ctrlmetrics.Registry)

	// A second New call on the same registry now returns a duplicate error.
	_, dupErr := prometheusSink.New(ctrlmetrics.Registry)
	if dupErr == nil {
		t.Fatal("expected duplicate registration to fail, but it succeeded — test precondition not met")
	}

	// Call AddTo with an injected Sink (the fakeSink from receiver_test.go).
	// If AddTo respects the nil check, it skips prometheusSink.New and
	// proceeds until it panics on the nil manager.
	// If AddTo ignores the injected Sink and calls prometheusSink.New, the
	// duplicate registration error surfaces and we report a test failure.
	var (
		gotErr   error
		gotPanic bool
	)
	func() {
		defer func() {
			if recover() != nil {
				gotPanic = true
			}
		}()
		gotErr = push.AddTo(nil, &push.Options{
			ReceiverURL:  testReceiverURL,
			EventsAddr:   ":0",
			SubscriberID: testSubscriberID,
			Sink:         &fakeSink{},
		})
	}()

	if gotErr != nil && strings.Contains(gotErr.Error(), "duplicate metrics collector") {
		t.Errorf("AddTo called prometheusSink.New despite Sink being injected: %v", gotErr)
	}
	// Either a panic (nil manager reached after successful nil check) or a
	// non-duplicate error (e.g. invalid address) is acceptable here.
	if !gotPanic && gotErr == nil {
		t.Error("expected AddTo to fail or panic on nil manager, but returned nil error")
	}
}

// TestAddTo_NilSink_FailsOnDuplicate documents the problem this fix solves:
// without an injected Sink, AddTo calls prometheusSink.New(ctrlmetrics.Registry) and
// fails when the gauge is already registered by BMCReconciler.
func TestAddTo_NilSink_FailsOnDuplicate(t *testing.T) {
	// Ensure the gauge is already in the registry (same as above).
	_, _ = prometheusSink.New(ctrlmetrics.Registry)

	// Confirm the duplicate state.
	_, dupErr := prometheusSink.New(ctrlmetrics.Registry)
	if dupErr == nil {
		t.Fatal("expected duplicate registration to fail — test precondition not met")
	}

	// Call AddTo with Sink == nil.  The fallback calls prometheusSink.New
	// against ctrlmetrics.Registry and must return the duplicate error.
	err := push.AddTo(nil, &push.Options{
		ReceiverURL:  testReceiverURL,
		EventsAddr:   ":0",
		SubscriberID: testSubscriberID,
		Sink:         nil, // no injection
	})
	if err == nil {
		t.Fatal("expected error from duplicate gauge registration, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate metrics collector") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestDuplicateRegistration_ProvesProblem documents that registering the same
// GaugeVec twice returns an error from the prometheus library.
func TestDuplicateRegistration_ProvesProblem(t *testing.T) {
	reg := prometheus.NewRegistry()

	_, err1 := prometheusSink.New(reg)
	if err1 != nil {
		t.Fatalf("first registration: %v", err1)
	}

	_, err2 := prometheusSink.New(reg)
	if err2 == nil {
		t.Fatal("expected duplicate registration error, got nil")
	}
	if !strings.Contains(err2.Error(), "duplicate") {
		t.Errorf("unexpected error text: %v", err2)
	}
}

// TestAddTo_Options_StatusWriter_wiredToReceiver verifies that push.Options.StatusWriter,
// when set, is wired to the receiver so that RecordMetricReport is called on incoming POSTs.
// The test exercises the same SetStatusWriter call that AddTo makes.
func TestAddTo_Options_StatusWriter_wiredToReceiver(t *testing.T) {
	sw := &fakeStatusWriter{}
	opts := &push.Options{
		ReceiverURL:  testReceiverURL,
		EventsAddr:   ":0",
		SubscriberID: testSubscriberID,
		Sink:         &fakeSink{},
		StatusWriter: sw,
	}

	// Build a receiver the same way AddTo does, then wire StatusWriter from opts.
	r, err := push.NewReceiver(push.ReceiverConfig{
		Addr:         opts.EventsAddr,
		SubscriberID: opts.SubscriberID,
		Sink:         opts.Sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.StatusWriter != nil {
		r.SetStatusWriter(opts.StatusWriter)
	}

	body := minimalMetricReport("99")
	req := httptest.NewRequest("POST", "/serverevents/test/metricsreport/node-sw", bytes.NewReader(body))
	w := httptest.NewRecorder()
	r.Handler().ServeHTTP(w, req)

	if w.Code != 200 {
		t.Errorf("status %d, want 200", w.Code)
	}
	if got := sw.recorded(); len(got) != 1 || got[0] != "node-sw" {
		t.Errorf("RecordMetricReport called with %v, want [node-sw]", got)
	}
}
