// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package criticalevent

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const (
	testNamespace     = "default"
	testCriticalMsgID = "Base.1.0.PSGoodToBad"
	testCriticalSev   = "Critical"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := metalv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme metal: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme core: %v", err)
	}
	return s
}

func bmcRefIndexer(obj client.Object) []string {
	server, ok := obj.(*metalv1alpha1.Server)
	if !ok {
		return nil
	}
	if server.Spec.BMCRef != nil {
		return []string{server.Spec.BMCRef.Name}
	}
	return nil
}

func makeServer(name, bmcRefName string) *metalv1alpha1.Server {
	s := &metalv1alpha1.Server{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
	}
	if bmcRefName != "" {
		s.Spec.BMCRef = &corev1.LocalObjectReference{Name: bmcRefName}
	}
	return s
}

func newHandler(t *testing.T, servers ...*metalv1alpha1.Server) (*ConditionHandler, *events.FakeRecorder, *prometheus.GaugeVec) {
	t.Helper()
	s := testScheme(t)
	b := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&metalv1alpha1.Server{}).
		WithIndex(&metalv1alpha1.Server{}, BMCRefField, bmcRefIndexer)
	for _, sv := range servers {
		b = b.WithObjects(sv)
	}
	fakeClient := b.Build()
	rec := events.NewFakeRecorder(20)
	gauge := NewCriticalAlertGauge()
	h := &ConditionHandler{
		Client:        fakeClient,
		Log:           logr.Discard(),
		EventRecorder: rec,
		AlertGauge:    gauge,
	}
	return h, rec, gauge
}

// gaugeValue reads the gauge metric value for the given hostname by collecting
// directly from the GaugeVec.
func gaugeValue(gauge *prometheus.GaugeVec, hostname string) (float64, bool) {
	ch := make(chan prometheus.Metric, 20)
	gauge.Collect(ch)
	close(ch)
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			continue
		}
		for _, lp := range d.GetLabel() {
			if lp.GetName() == "hostname" && lp.GetValue() == hostname {
				if g := d.GetGauge(); g != nil {
					return g.GetValue(), true
				}
			}
		}
	}
	return 0, false
}

func TestHandleCritical_SetsConditionTrue(t *testing.T) {
	server := makeServer("srv-001", "bmc-001")
	h, _, _ := newHandler(t, server)

	ev := sink.Event{EventID: "eid-1", MessageID: testCriticalMsgID, Severity: testCriticalSev,
		Message: "Power supply failed"}
	if err := h.HandleCritical(context.Background(), "bmc-001", ev); err != nil {
		t.Fatalf("HandleCritical: %v", err)
	}

	updated := &metalv1alpha1.Server{}
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: "srv-001", Namespace: testNamespace}, updated); err != nil {
		t.Fatalf("Get server: %v", err)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, CriticalEventConditionType)
	if cond == nil {
		t.Fatal("condition not set")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("condition status = %v, want True", cond.Status)
	}
}

func TestHandleCritical_SetsGaugeToOne(t *testing.T) {
	server := makeServer("srv-002", "bmc-002")
	h, _, gauge := newHandler(t, server)

	ev := sink.Event{EventID: "eid-2", MessageID: testCriticalMsgID, Severity: testCriticalSev}
	if err := h.HandleCritical(context.Background(), "bmc-002", ev); err != nil {
		t.Fatalf("HandleCritical: %v", err)
	}

	v, ok := gaugeValue(gauge, "bmc-002")
	if !ok || v != 1 {
		t.Errorf("alert gauge = %v (ok=%v), want 1", v, ok)
	}
}

func TestHandleCritical_EmitsK8sEvent(t *testing.T) {
	server := makeServer("srv-003", "bmc-003")
	h, rec, _ := newHandler(t, server)

	ev := sink.Event{EventID: "eid-3", MessageID: testCriticalMsgID, Severity: testCriticalSev,
		Message: "Power supply failed"}
	if err := h.HandleCritical(context.Background(), "bmc-003", ev); err != nil {
		t.Fatalf("HandleCritical: %v", err)
	}

	select {
	case msg := <-rec.Events:
		if msg == "" {
			t.Error("K8s event was empty")
		}
	default:
		t.Error("expected a K8s event, none in recorder channel")
	}
}

func TestHandleCritical_NoMatchingServer(t *testing.T) {
	h, rec, gauge := newHandler(t)
	ev := sink.Event{EventID: "eid-4", MessageID: "Base.1.0.X", Severity: testCriticalSev}
	if err := h.HandleCritical(context.Background(), "bmc-004", ev); err != nil {
		t.Fatalf("HandleCritical with no servers: %v", err)
	}
	select {
	case <-rec.Events:
		t.Error("expected no K8s event when no servers match")
	default:
	}
	_, ok := gaugeValue(gauge, "bmc-004")
	if ok {
		t.Error("gauge should not be set when no servers match")
	}
}

func TestHandleCritical_EmptyBMCName(t *testing.T) {
	server := makeServer("srv-005", "bmc-005")
	h, _, _ := newHandler(t, server)
	ev := sink.Event{EventID: "eid-5", Severity: testCriticalSev}
	if err := h.HandleCritical(context.Background(), "", ev); err != nil {
		t.Fatalf("HandleCritical with empty bmc: %v", err)
	}
}

func TestHandleClear_ClearsCondition(t *testing.T) {
	server := makeServer("srv-006", "bmc-006")
	h, _, gauge := newHandler(t, server)

	ev := sink.Event{EventID: "eid-6a", MessageID: testCriticalMsgID, Severity: testCriticalSev}
	_ = h.HandleCritical(context.Background(), "bmc-006", ev)

	if err := h.HandleClear(context.Background(), "bmc-006"); err != nil {
		t.Fatalf("HandleClear: %v", err)
	}

	updated := &metalv1alpha1.Server{}
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: "srv-006", Namespace: testNamespace}, updated); err != nil {
		t.Fatalf("Get server: %v", err)
	}
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, CriticalEventConditionType)
	if cond == nil {
		t.Fatal("condition missing after clear")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("condition status = %v, want False", cond.Status)
	}
	v, ok := gaugeValue(gauge, "bmc-006")
	if !ok || v != 0 {
		t.Errorf("alert gauge after clear = %v (ok=%v), want 0", v, ok)
	}
}

func TestHandleClear_NoMatchingServer(t *testing.T) {
	h, _, _ := newHandler(t)
	if err := h.HandleClear(context.Background(), "bmc-007"); err != nil {
		t.Fatalf("HandleClear with no servers: %v", err)
	}
}

func TestForget_DeletesGaugeSeries(t *testing.T) {
	gauge := NewCriticalAlertGauge()
	h := &ConditionHandler{Log: logr.Discard(), AlertGauge: gauge}
	h.SetAlertGauge("bmc-008", 1)
	h.Forget("bmc-008")
	_, ok := gaugeValue(gauge, "bmc-008")
	if ok {
		t.Error("gauge series should be deleted after Forget")
	}
}

func TestSanitizeEventID_AlphanumericPassthrough(t *testing.T) {
	got := sanitizeEventID("ABC123xyz_")
	if got != "ABC123xyz_" {
		t.Errorf("sanitizeEventID(%q) = %q, want same", "ABC123xyz_", got)
	}
}

func TestSanitizeEventID_StripsSpecialChars(t *testing.T) {
	got := sanitizeEventID("evt-id.12:34")
	want := "evtid1234"
	if got != want {
		t.Errorf("sanitizeEventID = %q, want %q", got, want)
	}
}

func TestSanitizeEventID_EmptyYieldsUnknown(t *testing.T) {
	if got := sanitizeEventID(""); got != "Unknown" {
		t.Errorf("empty id should yield Unknown; got %q", got)
	}
}

func TestSanitizeEventID_AllSpecialYieldsUnknown(t *testing.T) {
	if got := sanitizeEventID("---..."); got != "Unknown" {
		t.Errorf("all-special id should yield Unknown; got %q", got)
	}
}
