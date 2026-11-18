// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	telemetryv1alpha1 "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/api/v1alpha1"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/push"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sse"
)

// ---------------------------------------------------------------------------
// Stubs
// ---------------------------------------------------------------------------

const (
	testVendorHPE         = "HPE"
	testAnnotationExclude = "true"
	testBMCMetrics        = "test-bmc-metrics"
)

type nopSink struct{}

func (s *nopSink) PublishSamples(_ context.Context, _ string, _ []sink.Sample) error { return nil }
func (s *nopSink) Forget(_ string)                                                   {}

type nopStatusWriter struct{}

func (sw *nopStatusWriter) SetConnected(_ context.Context, _ string, _ telemetryv1alpha1.StreamKind, _ bool) error {
	return nil
}
func (sw *nopStatusWriter) IncrReconnectCount(_ context.Context, _ string, _ telemetryv1alpha1.StreamKind) error {
	return nil
}
func (sw *nopStatusWriter) SetLastEventTime(_ context.Context, _ string, _ telemetryv1alpha1.StreamKind, _ time.Time) error {
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newReconcilerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := metalv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme metal: %v", err)
	}
	if err := telemetryv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme telemetry: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme core: %v", err)
	}
	return s
}

// newDispatchReconciler builds a minimal BMCReconciler backed by a fake client.
// It initialises connMgr directly (same-package access) to avoid requiring a
// full controller-runtime Manager.
func newDispatchReconciler(t *testing.T, objs ...runtime.Object) *BMCReconciler {
	t.Helper()
	s := newReconcilerScheme(t)
	b := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&telemetryv1alpha1.BMCTelemetry{})
	for _, o := range objs {
		b = b.WithRuntimeObjects(o)
	}
	fakeClient := b.Build()
	cm := sse.NewConnectionManager(10, &nopSink{}, &nopStatusWriter{}, nil, nil, nil, nil, logr.Discard())
	return &BMCReconciler{
		Client:   fakeClient,
		Scheme:   s,
		connMgr:  cm,
		sinkInst: &nopSink{},
	}
}

func ptrMode(m telemetryv1alpha1.TelemetryMode) *telemetryv1alpha1.TelemetryMode {
	p := new(telemetryv1alpha1.TelemetryMode)
	*p = m
	return p
}

func enabledBMC(name string, annotations map[string]string) *metalv1alpha1.BMC {
	b := &metalv1alpha1.BMC{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
		Spec: metalv1alpha1.BMCSpec{
			BMCSecretRef: corev1.LocalObjectReference{Name: name + "-secret"},
			Protocol:     metalv1alpha1.Protocol{Name: metalv1alpha1.ProtocolRedfish, Port: 443},
		},
		Status: metalv1alpha1.BMCStatus{
			State: metalv1alpha1.BMCStateEnabled,
		},
	}
	return b
}

func bmcTelemetry(name string, override *telemetryv1alpha1.TelemetryMode) *telemetryv1alpha1.BMCTelemetry {
	return &telemetryv1alpha1.BMCTelemetry{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       telemetryv1alpha1.BMCTelemetrySpec{ModeOverride: override},
	}
}

// ---------------------------------------------------------------------------
// resolveEarlyMode: priority chain unit tests
// ---------------------------------------------------------------------------

// Step 1: exclusion annotation beats everything else.
func TestResolveEarlyMode_annotationBeatsOverride(t *testing.T) {
	bmc := &metalv1alpha1.BMC{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{push.ExcludeAnnotation: testAnnotationExclude}},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{
		Spec: telemetryv1alpha1.BMCTelemetrySpec{ModeOverride: ptrMode(telemetryv1alpha1.TelemetryModeSSE)},
	}
	r := &BMCReconciler{}
	got := r.resolveEarlyMode(bmc, tel, nil)
	if got != telemetryv1alpha1.TelemetryModeExcluded {
		t.Errorf("annotation + override: got %q, want Excluded", got)
	}
}

// Step 1: ConfigMap excludeHardware rule beats spec.ModeOverride.
func TestResolveEarlyMode_cfgExcludeBeatsOverride(t *testing.T) {
	bmc := &metalv1alpha1.BMC{
		Status: metalv1alpha1.BMCStatus{Manufacturer: testVendorHPE, Model: "ProLiant DL380"},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{
		Spec: telemetryv1alpha1.BMCTelemetrySpec{ModeOverride: ptrMode(telemetryv1alpha1.TelemetryModeSSE)},
	}
	cfg := &push.Config{
		ExcludeHardware: []push.ExcludeHardwareRule{{Vendor: testVendorHPE, Models: []string{"*"}}},
	}
	r := &BMCReconciler{}
	got := r.resolveEarlyMode(bmc, tel, cfg)
	if got != telemetryv1alpha1.TelemetryModeExcluded {
		t.Errorf("cfg exclude + override: got %q, want Excluded", got)
	}
}

// Step 2: spec.ModeOverride beats ConfigMap modeOverrides rule.
func TestResolveEarlyMode_overrideBeatsConfigMap(t *testing.T) {
	bmc := &metalv1alpha1.BMC{
		Status: metalv1alpha1.BMCStatus{Manufacturer: testVendorHPE, Model: "DL380"},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{
		Spec: telemetryv1alpha1.BMCTelemetrySpec{ModeOverride: ptrMode(telemetryv1alpha1.TelemetryModeSSE)},
	}
	cfg := &push.Config{
		ModeOverrides: []push.ModeOverrideRule{
			{Vendor: testVendorHPE, Models: []string{"*"}, Mode: telemetryv1alpha1.TelemetryModeMetricReport},
		},
	}
	r := &BMCReconciler{}
	got := r.resolveEarlyMode(bmc, tel, cfg)
	if got != telemetryv1alpha1.TelemetryModeSSE {
		t.Errorf("spec.ModeOverride vs ConfigMap rule: got %q, want SSE", got)
	}
}

// Step 3: ConfigMap modeOverrides rule beats probe (returns non-empty mode).
func TestResolveEarlyMode_configMapBeatsProbe(t *testing.T) {
	bmc := &metalv1alpha1.BMC{
		Status: metalv1alpha1.BMCStatus{Manufacturer: testVendorHPE, Model: "DL380"},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{} // no override
	cfg := &push.Config{
		ModeOverrides: []push.ModeOverrideRule{
			{Vendor: testVendorHPE, Models: []string{"*"}, Mode: telemetryv1alpha1.TelemetryModeMetricReport},
		},
	}
	r := &BMCReconciler{}
	got := r.resolveEarlyMode(bmc, tel, cfg)
	if got != telemetryv1alpha1.TelemetryModeMetricReport {
		t.Errorf("ConfigMap rule vs probe: got %q, want MetricReport", got)
	}
}

// Step 4 fallthrough: no annotation, no override, no rule → empty (probe decides).
func TestResolveEarlyMode_noMatch(t *testing.T) {
	bmc := &metalv1alpha1.BMC{}
	tel := &telemetryv1alpha1.BMCTelemetry{}
	r := &BMCReconciler{}
	got := r.resolveEarlyMode(bmc, tel, nil)
	if got != "" {
		t.Errorf("no match: got %q, want empty string", got)
	}
}

// nil ConfigLoader (no push config) → only annotation is checked.
func TestResolveEarlyMode_nilConfig_noAnnotation_noMatch(t *testing.T) {
	bmc := &metalv1alpha1.BMC{}
	tel := &telemetryv1alpha1.BMCTelemetry{}
	r := &BMCReconciler{} // ConfigLoader == nil → pushConfig() returns nil
	got := r.resolveEarlyMode(bmc, tel, nil)
	if got != "" {
		t.Errorf("nil config, no annotation: got %q, want empty string", got)
	}
}

// ---------------------------------------------------------------------------
// probeModeToTelemetryMode unit tests
// ---------------------------------------------------------------------------

func TestProbeModeToTelemetryMode(t *testing.T) {
	cases := []struct {
		input sse.Mode
		want  telemetryv1alpha1.TelemetryMode
	}{
		{sse.ModeSSE, telemetryv1alpha1.TelemetryModeSSE},
		{sse.ModeMetricReport, telemetryv1alpha1.TelemetryModeMetricReport},
		{sse.ModeUnsupported, telemetryv1alpha1.TelemetryModeUnsupported},
		{"", telemetryv1alpha1.TelemetryModeUnsupported},
		{"unknown-mode", telemetryv1alpha1.TelemetryModeUnsupported},
	}
	for _, tc := range cases {
		t.Run(string(tc.input), func(t *testing.T) {
			got := probeModeToTelemetryMode(tc.input)
			if got != tc.want {
				t.Errorf("input %q: got %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Dispatch branch tests: verify EnqueuePushBMC callback behaviour.
// These tests exercise the early-exit path (steps 1–3) so no BMC probe is made.
// ---------------------------------------------------------------------------

// Excluded mode (annotation) → EnqueuePushBMC IS called.
func TestDispatch_excluded_annotation_enqueues(t *testing.T) {
	bmc := enabledBMC("bmc1", map[string]string{push.ExcludeAnnotation: testAnnotationExclude})
	tel := bmcTelemetry("bmc1", nil)

	var mu sync.Mutex
	var enqueued []string
	r := newDispatchReconciler(t, bmc, tel)
	r.EnqueuePushBMC = func(name string) {
		mu.Lock()
		defer mu.Unlock()
		enqueued = append(enqueued, name)
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "bmc1"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(enqueued) != 1 || enqueued[0] != "bmc1" {
		t.Errorf("expected EnqueuePushBMC called once with bmc1, got %v", enqueued)
	}
}

// MetricReport mode (spec.ModeOverride) → EnqueuePushBMC IS called.
func TestDispatch_metricReport_override_enqueues(t *testing.T) {
	bmc := enabledBMC("bmc2", nil)
	tel := bmcTelemetry("bmc2-metrics", ptrMode(telemetryv1alpha1.TelemetryModeMetricReport))

	var mu sync.Mutex
	var enqueued []string
	r := newDispatchReconciler(t, bmc, tel)
	r.EnqueuePushBMC = func(name string) {
		mu.Lock()
		defer mu.Unlock()
		enqueued = append(enqueued, name)
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "bmc2"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(enqueued) != 1 || enqueued[0] != "bmc2" {
		t.Errorf("expected EnqueuePushBMC called once with bmc2, got %v", enqueued)
	}
}

// Unsupported mode (spec.ModeOverride) → EnqueuePushBMC is NOT called.
func TestDispatch_unsupported_override_doesNotEnqueue(t *testing.T) {
	bmc := enabledBMC("bmc3", nil)
	tel := bmcTelemetry("bmc3-metrics", ptrMode(telemetryv1alpha1.TelemetryModeUnsupported))

	called := false
	r := newDispatchReconciler(t, bmc, tel)
	r.EnqueuePushBMC = func(_ string) { called = true }

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "bmc3"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if called {
		t.Error("EnqueuePushBMC must not be called for Unsupported mode")
	}
}

// nil EnqueuePushBMC: Excluded mode reconcile must not panic.
func TestDispatch_excluded_nilCallback_noPanic(t *testing.T) {
	bmc := enabledBMC("bmc4", map[string]string{push.ExcludeAnnotation: testAnnotationExclude})
	tel := bmcTelemetry("bmc4", nil)

	r := newDispatchReconciler(t, bmc, tel)
	// EnqueuePushBMC intentionally left nil.

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "bmc4"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func TestBMCReconciler_RecordMetricReport_updatesLastEventTime(t *testing.T) {
	tel := bmcTelemetry("bmc5-metrics", nil)
	r := newDispatchReconciler(t, tel)
	r.RecordMetricReport(context.Background(), "bmc5")
	got := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "bmc5-metrics"}, got); err != nil {
		t.Fatalf("Get bmc5-metrics: %v", err)
	}
	if got.Status.LastEventTime == nil {
		t.Error("expected LastEventTime to be set after RecordMetricReport")
	}
}

func TestBMCReconciler_RecordAlertEvent_updatesLastEventTime(t *testing.T) {
	tel := bmcTelemetry("bmc6-alerts", nil)
	r := newDispatchReconciler(t, tel)
	r.RecordAlertEvent(context.Background(), "bmc6")
	got := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: "bmc6-alerts"}, got); err != nil {
		t.Fatalf("Get bmc6-alerts: %v", err)
	}
	if got.Status.LastEventTime == nil {
		t.Error("expected LastEventTime to be set after RecordAlertEvent")
	}
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			return &conditions[i]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// normaliseProbeError unit tests
// ---------------------------------------------------------------------------

func TestNormaliseProbeError(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "strips_http_get_and_tcp_prefix",
			input: `Get "https://10.245.252.135/redfish/v1/EventService": read tcp 10.42.0.207:55420->10.245.252.135:443: connection reset by peer`,
			want:  "connection reset by peer",
		},
		{
			name:  "strips_tcp_prefix_only",
			input: `read tcp 10.42.0.207:55421->10.245.252.135:443: i/o timeout`,
			want:  "i/o timeout",
		},
		{
			name:  "passes_through_plain_http_401",
			input: "401 Unauthorized",
			want:  "401 Unauthorized",
		},
		{
			name:  "passes_through_tls_error",
			input: "tls: failed to verify certificate: x509: certificate signed by unknown authority",
			want:  "tls: failed to verify certificate: x509: certificate signed by unknown authority",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normaliseProbeError(errors.New(tc.input))
			if got != tc.want {
				t.Errorf("normaliseProbeError(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// setSchemaDetectedCondition idempotency unit tests
// ---------------------------------------------------------------------------

func TestSetSchemaDetectedCondition_idempotent(t *testing.T) {
	tel := &telemetryv1alpha1.BMCTelemetry{
		ObjectMeta: metav1.ObjectMeta{Name: testBMCMetrics},
	}
	r := newDispatchReconciler(t, tel)
	ctx := context.Background()

	// First call: condition does not yet exist → should patch.
	if err := r.setSchemaDetectedCondition(ctx, tel, false, "probe failed: connection reset by peer (next retry in ~5m)"); err != nil {
		t.Fatalf("first setSchemaDetectedCondition: %v", err)
	}
	got := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: testBMCMetrics}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	rv1 := got.ResourceVersion
	cond := findCondition(got.Status.Conditions, telemetryv1alpha1.ConditionSchemaDetected)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ProbeFailed" {
		t.Fatalf("expected SchemaDetected=False/ProbeFailed after first call, got %+v", cond)
	}

	// Refresh tel to reflect stored state.
	*tel = *got

	// Second call with same status+reason → must be a no-op (ResourceVersion unchanged).
	if err := r.setSchemaDetectedCondition(ctx, tel, false, "probe failed: connection reset by peer (next retry in ~5m)"); err != nil {
		t.Fatalf("second setSchemaDetectedCondition: %v", err)
	}
	got2 := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: testBMCMetrics}, got2); err != nil {
		t.Fatalf("Get after second call: %v", err)
	}
	if got2.ResourceVersion != rv1 {
		t.Errorf("second call with same status+reason must not patch: rv before=%s, rv after=%s", rv1, got2.ResourceVersion)
	}

	// Third call with different status → must patch (ResourceVersion changes).
	*tel = *got2
	if err := r.setSchemaDetectedCondition(ctx, tel, true, "EventService probed successfully"); err != nil {
		t.Fatalf("third setSchemaDetectedCondition: %v", err)
	}
	got3 := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: testBMCMetrics}, got3); err != nil {
		t.Fatalf("Get after third call: %v", err)
	}
	if got3.ResourceVersion == rv1 {
		t.Error("third call with different status must patch: ResourceVersion should change")
	}
	cond3 := findCondition(got3.Status.Conditions, telemetryv1alpha1.ConditionSchemaDetected)
	if cond3 == nil || cond3.Status != metav1.ConditionTrue || cond3.Reason != "Probed" {
		t.Fatalf("expected SchemaDetected=True/Probed after third call, got %+v", cond3)
	}
}

// ---------------------------------------------------------------------------
// probeBackoff unit tests
// ---------------------------------------------------------------------------

func TestProbeBackoff(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{1, 5 * time.Minute},
		{2, 10 * time.Minute},
		{3, 20 * time.Minute},
		{4, 30 * time.Minute},
		{100, 30 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("failures=%d", tc.failures), func(t *testing.T) {
			got := probeBackoff(tc.failures)
			if got != tc.want {
				t.Errorf("probeBackoff(%d) = %v, want %v", tc.failures, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// setConnectedCondition unit tests
// ---------------------------------------------------------------------------

func TestProbeFailureClearsConnectedCondition(t *testing.T) {
	tel := &telemetryv1alpha1.BMCTelemetry{
		ObjectMeta: metav1.ObjectMeta{Name: testBMCMetrics},
		Status: telemetryv1alpha1.BMCTelemetryStatus{
			ConnectionState: telemetryv1alpha1.ConnectionStateConnected,
			Conditions: []metav1.Condition{
				{
					Type:               telemetryv1alpha1.ConditionConnected,
					Status:             metav1.ConditionTrue,
					Reason:             "Connected",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}
	r := newDispatchReconciler(t, tel)
	ctx := context.Background()

	// First call: Connected=True → should patch to False/ProbeFailed + ProbeBackoff.
	if err := r.setConnectedCondition(ctx, tel); err != nil {
		t.Fatalf("first setConnectedCondition: %v", err)
	}
	got := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: testBMCMetrics}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	rv1 := got.ResourceVersion
	cond := findCondition(got.Status.Conditions, telemetryv1alpha1.ConditionConnected)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ProbeFailed" {
		t.Fatalf("expected Connected=False/ProbeFailed, got %+v", cond)
	}
	if got.Status.ConnectionState != telemetryv1alpha1.ConnectionStateProbeBackoff {
		t.Errorf("expected ConnectionState=ProbeBackoff, got %q", got.Status.ConnectionState)
	}

	// Refresh tel to reflect stored state.
	*tel = *got

	// Second call with same state → must be a no-op.
	if err := r.setConnectedCondition(ctx, tel); err != nil {
		t.Fatalf("second setConnectedCondition: %v", err)
	}
	got2 := &telemetryv1alpha1.BMCTelemetry{}
	if err := r.Get(ctx, types.NamespacedName{Name: testBMCMetrics}, got2); err != nil {
		t.Fatalf("Get after second call: %v", err)
	}
	if got2.ResourceVersion != rv1 {
		t.Errorf("second call must not patch: rv before=%s, rv after=%s", rv1, got2.ResourceVersion)
	}
}
