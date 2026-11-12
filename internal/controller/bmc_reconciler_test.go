// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
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

type nopSink struct{}

func (s *nopSink) PublishSamples(_ context.Context, _ string, _ []sink.Sample) error { return nil }
func (s *nopSink) Forget(_ string)                                                   {}

type nopStatusWriter struct{}

func (sw *nopStatusWriter) SetConnected(_ context.Context, _ string, _ bool) error { return nil }
func (sw *nopStatusWriter) IncrReconnectCount(_ context.Context, _ string) error   { return nil }
func (sw *nopStatusWriter) SetLastEventTime(_ context.Context, _ string, _ time.Time) error {
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
	cm := sse.NewConnectionManager(10, &nopSink{}, &nopStatusWriter{}, logr.Discard())
	return &BMCReconciler{
		Client:  fakeClient,
		Scheme:  s,
		connMgr: cm,
		sink:    &nopSink{},
	}
}

func ptrMode(m telemetryv1alpha1.TelemetryMode) *telemetryv1alpha1.TelemetryMode { return new(m) }

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
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{push.ExcludeAnnotation: "true"}},
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
		Status: metalv1alpha1.BMCStatus{Manufacturer: "HPE", Model: "ProLiant DL380"},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{
		Spec: telemetryv1alpha1.BMCTelemetrySpec{ModeOverride: ptrMode(telemetryv1alpha1.TelemetryModeSSE)},
	}
	cfg := &push.Config{
		ExcludeHardware: []push.ExcludeHardwareRule{{Vendor: "HPE", Models: []string{"*"}}},
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
		Status: metalv1alpha1.BMCStatus{Manufacturer: "HPE", Model: "DL380"},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{
		Spec: telemetryv1alpha1.BMCTelemetrySpec{ModeOverride: ptrMode(telemetryv1alpha1.TelemetryModeSSE)},
	}
	cfg := &push.Config{
		ModeOverrides: []push.ModeOverrideRule{
			{Vendor: "HPE", Models: []string{"*"}, Mode: telemetryv1alpha1.TelemetryModeMetricReport},
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
		Status: metalv1alpha1.BMCStatus{Manufacturer: "HPE", Model: "DL380"},
	}
	tel := &telemetryv1alpha1.BMCTelemetry{} // no override
	cfg := &push.Config{
		ModeOverrides: []push.ModeOverrideRule{
			{Vendor: "HPE", Models: []string{"*"}, Mode: telemetryv1alpha1.TelemetryModeMetricReport},
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

func reconcileWith(t *testing.T, r *BMCReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
}

// Excluded mode (annotation) → EnqueuePushBMC IS called.
func TestDispatch_excluded_annotation_enqueues(t *testing.T) {
	bmc := enabledBMC("bmc1", map[string]string{push.ExcludeAnnotation: "true"})
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
	tel := bmcTelemetry("bmc2", ptrMode(telemetryv1alpha1.TelemetryModeMetricReport))

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
	tel := bmcTelemetry("bmc3", ptrMode(telemetryv1alpha1.TelemetryModeUnsupported))

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
	bmc := enabledBMC("bmc4", map[string]string{push.ExcludeAnnotation: "true"})
	tel := bmcTelemetry("bmc4", nil)

	r := newDispatchReconciler(t, bmc, tel)
	// EnqueuePushBMC intentionally left nil.

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "bmc4"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}
