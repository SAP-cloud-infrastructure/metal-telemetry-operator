// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alert/criticalevent"
)

func prewarmScheme(t *testing.T) *runtime.Scheme {
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

func prewarmGaugeValue(gauge *prometheus.GaugeVec, hostname string) (float64, bool) {
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

func makeConditionedServer(name, bmcRefName string, condStatus metav1.ConditionStatus) *metalv1alpha1.Server {
	s := &metalv1alpha1.Server{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
	}
	if bmcRefName != "" {
		s.Spec.BMCRef = &corev1.LocalObjectReference{Name: bmcRefName}
	}
	if condStatus != "" {
		s.Status.Conditions = []metav1.Condition{{
			Type:               criticalevent.CriticalEventConditionType,
			Status:             condStatus,
			Reason:             "Test",
			LastTransitionTime: metav1.Now(),
		}}
	}
	return s
}

func TestPrewarmAlertGauge_SetsGaugeForCriticalServers(t *testing.T) {
	s1 := makeConditionedServer("srv-p01", "bmc-p01", metav1.ConditionTrue)
	s2 := makeConditionedServer("srv-p02", "bmc-p02", metav1.ConditionFalse)
	s3 := makeConditionedServer("srv-p03", "bmc-p03", "") // no condition

	fc := fake.NewClientBuilder().
		WithScheme(prewarmScheme(t)).
		WithObjects(s1, s2, s3).
		WithStatusSubresource(&metalv1alpha1.Server{}).
		Build()

	gauge := criticalevent.NewCriticalAlertGauge()
	handler := &criticalevent.ConditionHandler{
		Log:        logr.Discard(),
		AlertGauge: gauge,
	}

	if err := prewarmAlertGauge(context.Background(), fc, handler); err != nil {
		t.Fatalf("prewarmAlertGauge: %v", err)
	}

	if v, ok := prewarmGaugeValue(gauge, "bmc-p01"); !ok || v != 1 {
		t.Errorf("gauge for bmc-p01 = %v (ok=%v), want 1", v, ok)
	}
	if v, ok := prewarmGaugeValue(gauge, "bmc-p02"); !ok || v != 0 {
		t.Errorf("gauge for bmc-p02 = %v (ok=%v), want 0", v, ok)
	}
	// bmc-p03 has no condition: no gauge entry expected
	if _, ok := prewarmGaugeValue(gauge, "bmc-p03"); ok {
		t.Error("gauge for bmc-p03 should not be set (no condition)")
	}
}

func TestPrewarmAlertGauge_NoBMCRef(t *testing.T) {
	s := makeConditionedServer("srv-p04", "", metav1.ConditionTrue) // no BMCRef

	fc := fake.NewClientBuilder().
		WithScheme(prewarmScheme(t)).
		WithObjects(s).
		Build()

	gauge := criticalevent.NewCriticalAlertGauge()
	handler := &criticalevent.ConditionHandler{
		Log:        logr.Discard(),
		AlertGauge: gauge,
	}

	if err := prewarmAlertGauge(context.Background(), fc, handler); err != nil {
		t.Fatalf("prewarmAlertGauge: %v", err)
	}
	// gauge should remain empty
	ch := make(chan prometheus.Metric, 5)
	gauge.Collect(ch)
	close(ch)
	if len(ch) != 0 {
		t.Errorf("expected no gauge entries, got %d", len(ch))
	}
}

func TestPrewarmAlertGauge_EmptyServerList(t *testing.T) {
	fc := fake.NewClientBuilder().
		WithScheme(prewarmScheme(t)).
		Build()

	gauge := criticalevent.NewCriticalAlertGauge()
	handler := &criticalevent.ConditionHandler{
		Log:        logr.Discard(),
		AlertGauge: gauge,
	}

	if err := prewarmAlertGauge(context.Background(), fc, handler); err != nil {
		t.Fatalf("prewarmAlertGauge with no servers: %v", err)
	}
}
