// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package warningevent_test

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alert/criticalevent"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alert/warningevent"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const (
	testNamespace    = "default"
	testWarnSeverity = "Warning"
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

func newHandler(t *testing.T, servers ...*metalv1alpha1.Server) (*warningevent.Handler, *events.FakeRecorder) {
	t.Helper()
	s := testScheme(t)
	b := fake.NewClientBuilder().
		WithScheme(s).
		WithIndex(&metalv1alpha1.Server{}, criticalevent.BMCRefField, bmcRefIndexer)
	for _, sv := range servers {
		b = b.WithObjects(sv)
	}
	fakeClient := b.Build()
	rec := events.NewFakeRecorder(20)
	h := &warningevent.Handler{
		Client:        fakeClient,
		Log:           logr.Discard(),
		EventRecorder: rec,
	}
	return h, rec
}

func TestHandleWarning_EmitsK8sEvent(t *testing.T) {
	server := makeServer("srv-w01", "bmc-w01")
	h, rec := newHandler(t, server)

	ev := sink.Event{EventID: "wid-1", MessageID: "Base.1.0.TemperatureAboveUpperCautionThreshold",
		Severity: testWarnSeverity, Message: "CPU temp high"}
	if err := h.HandleWarning(context.Background(), "bmc-w01", ev); err != nil {
		t.Fatalf("HandleWarning: %v", err)
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

func TestHandleWarning_NoConditionSet(t *testing.T) {
	server := makeServer("srv-w02", "bmc-w02")
	h, _ := newHandler(t, server)

	ev := sink.Event{EventID: "wid-2", MessageID: "Base.1.0.X", Severity: testWarnSeverity}
	if err := h.HandleWarning(context.Background(), "bmc-w02", ev); err != nil {
		t.Fatalf("HandleWarning: %v", err)
	}

	// Conditions should remain empty
	updated := &metalv1alpha1.Server{}
	if err := h.Client.Get(context.Background(), client.ObjectKey{Name: "srv-w02", Namespace: testNamespace}, updated); err != nil {
		t.Fatalf("Get server: %v", err)
	}
	if len(updated.Status.Conditions) != 0 {
		t.Errorf("HandleWarning must not set conditions; got %d condition(s)", len(updated.Status.Conditions))
	}
}

func TestHandleWarning_NoMatchingServer(t *testing.T) {
	h, rec := newHandler(t)
	ev := sink.Event{EventID: "wid-3", Severity: testWarnSeverity}
	if err := h.HandleWarning(context.Background(), "bmc-w03", ev); err != nil {
		t.Fatalf("HandleWarning with no servers: %v", err)
	}
	select {
	case <-rec.Events:
		t.Error("expected no K8s event when no servers match")
	default:
	}
}

func TestHandleWarning_EmptyBMCName(t *testing.T) {
	server := makeServer("srv-w04", "bmc-w04")
	h, _ := newHandler(t, server)
	ev := sink.Event{EventID: "wid-4", Severity: testWarnSeverity}
	if err := h.HandleWarning(context.Background(), "", ev); err != nil {
		t.Fatalf("HandleWarning with empty bmc: %v", err)
	}
}

func TestHandleWarning_NoCriticalEventCondition(t *testing.T) {
	server := makeServer("srv-w05", "bmc-w05")
	h, _ := newHandler(t, server)

	ev := sink.Event{EventID: "wid-5", Severity: testWarnSeverity}
	_ = h.HandleWarning(context.Background(), "bmc-w05", ev)

	updated := &metalv1alpha1.Server{}
	_ = h.Client.Get(context.Background(), client.ObjectKey{Name: "srv-w05", Namespace: testNamespace}, updated)
	cond := apimeta.FindStatusCondition(updated.Status.Conditions, "CriticalEventReceived")
	if cond != nil {
		t.Error("HandleWarning must not set CriticalEventReceived condition")
	}
}
