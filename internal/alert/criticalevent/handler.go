// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package criticalevent translates Critical-severity Redfish events into
// declarative Server-readiness state.
package criticalevent

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

const (
	// BMCRefField is the field-indexer key for Server lookup by BMC name.
	BMCRefField = "spec.bmcRef.name"

	// CriticalEventConditionType is the condition Type set on Server resources.
	CriticalEventConditionType = "CriticalEventReceived"

	// AlertGaugeMetricName is the Prometheus metric name for the per-Server critical-alert gauge.
	AlertGaugeMetricName = "redfish_server_critical_alert"

	alertGaugeLabelHostname = "hostname"
)

// NewCriticalAlertGauge creates the redfish_server_critical_alert GaugeVec.
func NewCriticalAlertGauge() *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: AlertGaugeMetricName,
		Help: "1 if the Server has an active CriticalEventReceived condition, 0 otherwise. " +
			"Derived from Server.status.conditions and survives operator restarts.",
	}, []string{alertGaugeLabelHostname})
}

// ConditionHandler sets CriticalEventReceived on every Server whose
// spec.bmcRef.name matches bmcName. Client must be cache-backed so
// MatchingFields works against the BMCRefField indexer.
type ConditionHandler struct {
	Client        client.Client
	Log           logr.Logger
	EventRecorder recorder.EventRecorder
	AlertGauge    *prometheus.GaugeVec
}

// HandleCritical lists Servers indexed by bmcName, sets CriticalEventReceived=True
// on each, emits a Kubernetes Warning Event, and sets the alert gauge to 1.
func (h *ConditionHandler) HandleCritical(ctx context.Context, bmcName string, event sink.Event) error {
	if bmcName == "" {
		return nil
	}

	serverList := &metalv1alpha1.ServerList{}
	if err := h.Client.List(ctx, serverList, client.MatchingFields{BMCRefField: bmcName}); err != nil {
		return fmt.Errorf("list Servers by %s=%s: %w", BMCRefField, bmcName, err)
	}
	if len(serverList.Items) == 0 {
		h.Log.V(2).Info("No Servers matched the critical event", "bmc", bmcName, "eventID", event.EventID)
		return nil
	}

	condition := metav1.Condition{
		Type:   CriticalEventConditionType,
		Status: metav1.ConditionTrue,
		Reason: fmt.Sprintf("CriticalEvent%s", sanitizeEventID(event.EventID)),
		Message: fmt.Sprintf("Critical Redfish event [%s]: %s (component: %s, at: %s)",
			event.MessageID, event.Message, event.OriginOfCondition, event.EventTimestamp),
	}

	var failed []error
	for i := range serverList.Items {
		server := &serverList.Items[i]
		if err := h.patchCondition(ctx, server, condition); err != nil {
			h.Log.Error(err, "Failed to patch Server condition",
				"server", server.Name, "bmc", bmcName, "eventID", event.EventID)
			failed = append(failed, err)
			continue
		}
		h.Log.V(1).Info("Critical event condition applied",
			"server", server.Name, "bmc", bmcName, "eventID", event.EventID)
		if h.AlertGauge != nil {
			h.AlertGauge.With(prometheus.Labels{alertGaugeLabelHostname: bmcName}).Set(1)
		}
		if h.EventRecorder != nil {
			h.EventRecorder.Eventf(server, nil, corev1.EventTypeWarning, "HardwareAlert", "HardwareAlertReceived",
				"Critical Redfish event [%s]: %s (hostname: %s, component: %s, at: %s)",
				event.MessageID, event.Message, bmcName, event.OriginOfCondition, event.EventTimestamp)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("failed to patch %d/%d servers: %w", len(failed), len(serverList.Items), errors.Join(failed...))
	}
	return nil
}

// HandleClear lists Servers indexed by bmcName and clears CriticalEventReceived on each.
// Call this when the BMC sends an OK or Info event indicating a fault resolved.
func (h *ConditionHandler) HandleClear(ctx context.Context, bmcName string) error {
	if bmcName == "" {
		return nil
	}

	serverList := &metalv1alpha1.ServerList{}
	if err := h.Client.List(ctx, serverList, client.MatchingFields{BMCRefField: bmcName}); err != nil {
		return fmt.Errorf("list Servers by %s=%s: %w", BMCRefField, bmcName, err)
	}
	if len(serverList.Items) == 0 {
		return nil
	}

	var failed []error
	for i := range serverList.Items {
		server := &serverList.Items[i]
		if err := h.ClearCriticalAlert(ctx, server, "OKEventReceived",
			"Hardware fault resolved: OK/Info event received from BMC"); err != nil {
			h.Log.Error(err, "Failed to clear Server condition", "server", server.Name, "bmc", bmcName)
			failed = append(failed, err)
			continue
		}
		h.Log.V(1).Info("Critical event condition cleared", "server", server.Name, "bmc", bmcName)
	}
	if len(failed) > 0 {
		h.SetAlertGauge(bmcName, 1)
		return fmt.Errorf("failed to clear %d/%d servers: %w", len(failed), len(serverList.Items), errors.Join(failed...))
	}
	return nil
}

// ClearCriticalAlert sets CriticalEventReceived=False on the Server and the alert gauge to 0.
func (h *ConditionHandler) ClearCriticalAlert(ctx context.Context, server *metalv1alpha1.Server, reason, message string) error {
	condition := metav1.Condition{
		Type:               CriticalEventConditionType,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: server.Generation,
	}
	if err := h.patchCondition(ctx, server, condition); err != nil {
		return err
	}
	hostname := ""
	if server.Spec.BMCRef != nil {
		hostname = server.Spec.BMCRef.Name
	}
	h.SetAlertGauge(hostname, 0)
	return nil
}

// SetAlertGauge sets the alert gauge to the given value (0 or 1) for a BMC hostname.
// Used for startup pre-warming and condition clearing.
func (h *ConditionHandler) SetAlertGauge(hostname string, value float64) {
	if h.AlertGauge == nil {
		return
	}
	h.AlertGauge.With(prometheus.Labels{alertGaugeLabelHostname: hostname}).Set(value)
}

// Forget deletes the alert gauge series for hostname.
func (h *ConditionHandler) Forget(hostname string) {
	if h.AlertGauge == nil {
		return
	}
	h.AlertGauge.Delete(prometheus.Labels{alertGaugeLabelHostname: hostname})
}

func (h *ConditionHandler) patchCondition(ctx context.Context, server *metalv1alpha1.Server, condition metav1.Condition) error {
	condition.ObservedGeneration = server.Generation
	base := server.DeepCopy()
	if !apimeta.SetStatusCondition(&server.Status.Conditions, condition) {
		return nil
	}
	if err := h.Client.Status().Patch(ctx, server,
		client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("patch server %s status: %w", server.Name, err)
	}
	return nil
}

// sanitizeEventID strips characters that apiserver's Reason validation rejects.
func sanitizeEventID(id string) string {
	if id == "" {
		return "Unknown"
	}
	out := make([]byte, 0, len(id))
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'A' && c <= 'Z',
			c >= 'a' && c <= 'z',
			c >= '0' && c <= '9',
			c == '_':
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return "Unknown"
	}
	return string(out)
}
