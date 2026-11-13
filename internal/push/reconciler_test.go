// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push_test

import (
	"context"
	"sync"
	"testing"
	"time"

	redfishschemas "github.com/stmcginnis/gofish/schemas"

	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
)

// ---------------------------------------------------------------------------
// Fake Redfish client
// ---------------------------------------------------------------------------

const (
	fmtMetricReport = "MetricReport"
	fmtEvent        = "Event"
)

type fakeRedfishClient struct {
	mu   sync.Mutex
	subs []push.Subscription

	createCalls     []createCall
	deleteCalls     []string
	submitTestCalls []string
	createErr       error
	listErr         error
}

type createCall struct {
	destination     string
	eventFormatType string
}

func (f *fakeRedfishClient) ListEventSubscriptions(_ context.Context) ([]push.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	cp := make([]push.Subscription, len(f.subs))
	copy(cp, f.subs)
	return cp, nil
}

func (f *fakeRedfishClient) CreateEventSubscription(_ context.Context, destination string, eventFormatType redfishschemas.EventFormatType, _ redfishschemas.DeliveryRetryPolicy, _ map[string]string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return "", f.createErr
	}
	fmtStr := string(eventFormatType)
	f.createCalls = append(f.createCalls, createCall{destination: destination, eventFormatType: fmtStr})
	uri := "/redfish/v1/EventService/Subscriptions/" + fmtStr
	f.subs = append(f.subs, push.Subscription{URI: uri, Destination: destination, EventFormat: fmtStr})
	return uri, nil
}

func (f *fakeRedfishClient) DeleteEventSubscription(_ context.Context, uri string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, uri)
	filtered := f.subs[:0]
	for _, s := range f.subs {
		if s.URI != uri {
			filtered = append(filtered, s)
		}
	}
	f.subs = filtered
	return nil
}

func (f *fakeRedfishClient) Logout() {}

func (f *fakeRedfishClient) ProbeEventService(_ context.Context) (*push.EventServiceCapabilities, error) {
	return &push.EventServiceCapabilities{ServiceEnabled: true, HasSubscriptions: true}, nil
}

func (f *fakeRedfishClient) SubmitTestEvent(_ context.Context, msgID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.submitTestCalls = append(f.submitTestCalls, msgID)
	return nil
}

type fakeFactory struct {
	mu     sync.Mutex
	client *fakeRedfishClient
}

func (ff *fakeFactory) NewClient(_ context.Context, _, _, _ string, _ bool) (push.Client, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.client, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := metalv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme metal: %v", err)
	}
	if err := telemetryv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme telemetry: %v", err)
	}
	return s
}

func testBMC(name, ip string) *metalv1alpha1.BMC {
	return &metalv1alpha1.BMC{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: metalv1alpha1.BMCSpec{
			BMCSecretRef: corev1.LocalObjectReference{Name: name + "-secret"},
			Protocol: metalv1alpha1.Protocol{
				Name: metalv1alpha1.ProtocolRedfish,
				Port: 443,
			},
		},
		Status: metalv1alpha1.BMCStatus{
			IP: metalv1alpha1.MustParseIP(ip),
		},
	}
}

func testBMCSecret(name string) *metalv1alpha1.BMCSecret {
	return &metalv1alpha1.BMCSecret{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-secret"},
		Data: map[string][]byte{
			metalv1alpha1.BMCSecretUsernameKeyName: []byte("admin"),
			metalv1alpha1.BMCSecretPasswordKeyName: []byte("pass"),
		},
	}
}

func testBMCTelemetry(name string, mode telemetryv1alpha1.TelemetryMode) *telemetryv1alpha1.BMCTelemetry {
	bt := &telemetryv1alpha1.BMCTelemetry{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
	bt.Status.Mode = mode
	return bt
}

func newReconciler(t *testing.T, ff *fakeFactory, receiverURL, subscriberID string, objs ...runtime.Object) *push.SubscriptionReconciler {
	t.Helper()
	return newReconcilerWithLoader(t, ff, receiverURL, subscriberID, nil, objs...)
}

func newReconcilerWithLoader(t *testing.T, ff *fakeFactory, receiverURL, subscriberID string, loader *push.ConfigLoader, objs ...runtime.Object) *push.SubscriptionReconciler {
	t.Helper()
	s := newTestScheme(t)
	b := fake.NewClientBuilder().WithScheme(s)
	for _, o := range objs {
		b = b.WithRuntimeObjects(o)
	}
	fakeClient := b.Build()
	return push.NewSubscriptionReconciler(fakeClient, push.ReconcilerConfig{
		ReceiverURL:  receiverURL,
		SubscriberID: subscriberID,
		Factory:      ff,
		ConfigLoader: loader,
	})
}

func reconcileOnce(t *testing.T, r *push.SubscriptionReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	req := ctrl.Request{}
	req.Name = name
	return r.Reconcile(context.Background(), req)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestSubscriptionReconciler_createsSubscriptionsOnFirstReconcile(t *testing.T) {
	fc := &fakeRedfishClient{}
	ff := &fakeFactory{client: fc}
	b := testBMC("node1", "10.0.0.1")
	bt := testBMCTelemetry("node1", telemetryv1alpha1.TelemetryModeMetricReport)
	sec := testBMCSecret("node1")

	r := newReconciler(t, ff, "http://receiver:9093", "sub1", b, bt, sec)
	_, err := reconcileOnce(t, r, "node1")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	calls := fc.createCalls
	fc.mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("expected 2 CreateEventSubscription calls, got %d: %+v", len(calls), calls)
	}
	hasMetrics, hasAlerts := false, false
	for _, c := range calls {
		if c.eventFormatType == fmtMetricReport {
			hasMetrics = true
		}
		if c.eventFormatType == fmtEvent {
			hasAlerts = true
		}
	}
	if !hasMetrics {
		t.Error("expected MetricReport subscription to be created")
	}
	if !hasAlerts {
		t.Error("expected Event (alerts) subscription to be created")
	}
}

func TestSubscriptionReconciler_doesNotDuplicateExistingSubscriptions(t *testing.T) {
	const receiverURL = "http://receiver:9093"
	const subID = "sub1"
	const bmcName = "node2"

	// Pre-populate the fake client with subscriptions matching our receiver.
	fc := &fakeRedfishClient{
		subs: []push.Subscription{
			{
				URI:         "/redfish/v1/EventService/Subscriptions/1",
				Destination: receiverURL + "/serverevents/" + subID + "/metricsreport/" + bmcName,
				EventFormat: fmtMetricReport,
			},
			{
				URI:         "/redfish/v1/EventService/Subscriptions/2",
				Destination: receiverURL + "/serverevents/" + subID + "/alerts/" + bmcName,
				EventFormat: fmtEvent,
			},
		},
	}
	ff := &fakeFactory{client: fc}

	b := testBMC(bmcName, "10.0.0.2")
	bt := testBMCTelemetry(bmcName, telemetryv1alpha1.TelemetryModeMetricReport)
	sec := testBMCSecret(bmcName)

	r := newReconciler(t, ff, receiverURL, subID, b, bt, sec)
	_, err := reconcileOnce(t, r, bmcName)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	calls := fc.createCalls
	fc.mu.Unlock()

	if len(calls) != 0 {
		t.Errorf("expected no CreateEventSubscription calls (subs already exist), got %d: %+v", len(calls), calls)
	}
}

func TestSubscriptionReconciler_deletesSubscriptionsWhenModeNotMetricReport(t *testing.T) {
	const receiverURL = "http://receiver:9093"
	const subID = "sub1"
	const bmcName = "node3"

	fc := &fakeRedfishClient{
		subs: []push.Subscription{
			{
				URI:         "/redfish/v1/EventService/Subscriptions/1",
				Destination: receiverURL + "/serverevents/" + subID + "/metricsreport/" + bmcName,
				EventFormat: fmtMetricReport,
			},
			{
				URI:         "/redfish/v1/EventService/Subscriptions/2",
				Destination: receiverURL + "/serverevents/" + subID + "/alerts/" + bmcName,
				EventFormat: fmtEvent,
			},
		},
	}
	ff := &fakeFactory{client: fc}

	b := testBMC(bmcName, "10.0.0.3")
	bt := testBMCTelemetry(bmcName, telemetryv1alpha1.TelemetryModeSSE) // not MetricReport
	sec := testBMCSecret(bmcName)

	r := newReconciler(t, ff, receiverURL, subID, b, bt, sec)
	_, err := reconcileOnce(t, r, bmcName)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	deletes := fc.deleteCalls
	fc.mu.Unlock()

	if len(deletes) != 2 {
		t.Errorf("expected 2 DeleteEventSubscription calls, got %d: %+v", len(deletes), deletes)
	}
}

func TestSubscriptionReconciler_deletesStaleAndCreatesNew(t *testing.T) {
	const newURL = "http://new-receiver:9093"
	const oldURL = "http://old-receiver:9093"
	const subID = "sub1"
	const bmcName = "node4"

	// Stale subscription from the old receiver instance.
	fc := &fakeRedfishClient{
		subs: []push.Subscription{
			{
				URI:         "/redfish/v1/EventService/Subscriptions/old",
				Destination: oldURL + "/serverevents/" + subID + "/metricsreport/" + bmcName,
				EventFormat: fmtMetricReport,
			},
		},
	}
	ff := &fakeFactory{client: fc}

	b := testBMC(bmcName, "10.0.0.4")
	bt := testBMCTelemetry(bmcName, telemetryv1alpha1.TelemetryModeMetricReport)
	sec := testBMCSecret(bmcName)

	r := newReconciler(t, ff, newURL, subID, b, bt, sec)
	_, err := reconcileOnce(t, r, bmcName)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	deletes := fc.deleteCalls
	creates := fc.createCalls
	fc.mu.Unlock()

	if len(deletes) != 1 || deletes[0] != "/redfish/v1/EventService/Subscriptions/old" {
		t.Errorf("expected stale subscription deleted, got: %v", deletes)
	}
	if len(creates) != 2 {
		t.Errorf("expected 2 new subscriptions created, got %d: %+v", len(creates), creates)
	}
	for _, c := range creates {
		if !containsStr(c.destination, newURL) {
			t.Errorf("created destination should point to new receiver: %q", c.destination)
		}
	}
}

func TestSubscriptionReconciler_skipWhenBMCTelemetryNotFound(t *testing.T) {
	fc := &fakeRedfishClient{}
	ff := &fakeFactory{client: fc}
	b := testBMC("node5", "10.0.0.5")
	sec := testBMCSecret("node5")
	// No BMCTelemetry — omit from objects.

	r := newReconciler(t, ff, "http://receiver:9093", "sub1", b, sec)
	_, err := reconcileOnce(t, r, "node5")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	creates := fc.createCalls
	fc.mu.Unlock()

	if len(creates) != 0 {
		t.Errorf("expected no Redfish calls when BMCTelemetry absent, got %d", len(creates))
	}
}

func TestSubscriptionReconciler_skipWhenBMCNotFound(t *testing.T) {
	fc := &fakeRedfishClient{}
	ff := &fakeFactory{client: fc}
	// No BMC object — empty fake client.

	r := newReconciler(t, ff, "http://receiver:9093", "sub1")
	_, err := reconcileOnce(t, r, "node6")
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	creates := fc.createCalls
	fc.mu.Unlock()

	if len(creates) != 0 {
		t.Errorf("expected no Redfish calls when BMC absent, got %d", len(creates))
	}
}

// ---------------------------------------------------------------------------
// classify tests
// ---------------------------------------------------------------------------

func TestClassify_currentAndStale(t *testing.T) {
	const newURL = "http://new:9093"
	const oldURL = "http://old:9093"
	const subID = "s"
	const bmcName = "bmc1"

	fc := &fakeRedfishClient{}
	ff := &fakeFactory{client: fc}
	b := testBMC(bmcName, "10.0.0.1")
	bt := testBMCTelemetry(bmcName, telemetryv1alpha1.TelemetryModeMetricReport)
	sec := testBMCSecret(bmcName)

	// Put two stale + two current subscriptions on the fake client.
	fc.subs = []push.Subscription{
		{URI: "/r/old1", Destination: oldURL + "/serverevents/" + subID + "/metricsreport/" + bmcName, EventFormat: "MetricReport"},
		{URI: "/r/old2", Destination: oldURL + "/serverevents/" + subID + "/alerts/" + bmcName, EventFormat: fmtEvent},
		{URI: "/r/new1", Destination: newURL + "/serverevents/" + subID + "/metricsreport/" + bmcName, EventFormat: fmtMetricReport},
		{URI: "/r/new2", Destination: newURL + "/serverevents/" + subID + "/alerts/" + bmcName, EventFormat: fmtEvent},
		// Foreign subscription — should be ignored.
		{URI: "/r/other", Destination: "http://other-system/webhook", EventFormat: fmtEvent},
	}

	r := newReconciler(t, ff, newURL, subID, b, bt, sec)
	_, err := reconcileOnce(t, r, bmcName)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	fc.mu.Lock()
	deletes := fc.deleteCalls
	creates := fc.createCalls
	fc.mu.Unlock()

	// Old ones should be deleted, new ones kept (no create needed).
	if len(deletes) != 2 {
		t.Errorf("expected 2 stale deletions, got %d: %v", len(deletes), deletes)
	}
	if len(creates) != 0 {
		t.Errorf("expected 0 creates (current already present), got %d: %+v", len(creates), creates)
	}
}

// ---------------------------------------------------------------------------
// EnqueueBMC: no panic when called before SetupWithManager
// ---------------------------------------------------------------------------

func TestEnqueueBMC_beforeSetup_noPanic(t *testing.T) {
	r := push.NewSubscriptionReconciler(nil, push.ReconcilerConfig{
		ReceiverURL:  "http://r:9093",
		SubscriberID: "s",
	})
	// Must not panic even though eventsCh is nil.
	r.EnqueueBMC("node1")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}())
}

// ---------------------------------------------------------------------------
// Test event round-trip
// ---------------------------------------------------------------------------

func TestSubscriptionReconciler_submitsTestEventAfterCreate(t *testing.T) {
	const receiverURL = "http://receiver:9093"
	const subID = "sub1"
	const bmcName = "node1"

	fc := &fakeRedfishClient{}
	ff := &fakeFactory{client: fc}
	b := testBMC(bmcName, "10.0.0.1")
	bt := testBMCTelemetry(bmcName, telemetryv1alpha1.TelemetryModeMetricReport)
	sec := testBMCSecret(bmcName)

	loader := &push.ConfigLoader{}
	loader.StoreConfig(&push.Config{
		APIVersion:        "telemetry.metal.ironcore.dev/v1alpha1",
		TestEventInterval: 5 * time.Minute,
		TestEventTimeout:  10 * time.Second,
	})

	r := newReconcilerWithLoader(t, ff, receiverURL, subID, loader, b, bt, sec)

	result, err := reconcileOnce(t, r, bmcName)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Two subscriptions should have been created.
	fc.mu.Lock()
	creates := fc.createCalls
	testCalls := fc.submitTestCalls
	fc.mu.Unlock()

	if len(creates) != 2 {
		t.Errorf("expected 2 CreateEventSubscription calls, got %d", len(creates))
	}

	// SubmitTestEvent should have been called once.
	if len(testCalls) != 1 {
		t.Fatalf("expected 1 SubmitTestEvent call, got %d: %v", len(testCalls), testCalls)
	}
	msgID := testCalls[0]
	if !containsStr(msgID, "TestEvent.MTO.") {
		t.Errorf("expected msgID to contain TestEvent.MTO., got %q", msgID)
	}

	// Result should requeue after the timeout to check if round-trip arrived.
	if result.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter > 0, got %v", result.RequeueAfter)
	}

	// Simulate the round-trip arriving: receiver calls NotifyTestEvent.
	r.NotifyTestEvent(bmcName, msgID)

	// Second reconcile: test event was confirmed, no new SubmitTestEvent since
	// the interval hasn't elapsed (lastTestTime was just reset).
	result2, err := reconcileOnce(t, r, bmcName)
	if err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if result2.RequeueAfter > 0 {
		t.Errorf("expected no requeue on second reconcile (interval not elapsed), got %v", result2.RequeueAfter)
	}

	fc.mu.Lock()
	testCallsAfter := fc.submitTestCalls
	fc.mu.Unlock()
	if len(testCallsAfter) != 1 {
		t.Errorf("expected still 1 SubmitTestEvent call after confirmed round-trip, got %d", len(testCallsAfter))
	}
}
