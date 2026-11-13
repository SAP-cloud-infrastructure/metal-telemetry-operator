// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

// Package warningevent emits Kubernetes Warning Events for Warning-severity
// Redfish alert events without setting any Server condition.
package warningevent

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	metalv1alpha1 "github.com/ironcore-dev/metal-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/recorder"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/alert/criticalevent"
	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// Handler emits a Kubernetes Warning Event on every Server whose
// spec.bmcRef.name matches the source BMC name. It does not modify
// Server conditions so the alert does not block provisioning.
type Handler struct {
	Client        client.Client
	Log           logr.Logger
	EventRecorder recorder.EventRecorder
}

// HandleWarning lists Servers indexed by bmcName and emits a K8s Warning
// Event on each matching Server. No condition is set.
func (h *Handler) HandleWarning(ctx context.Context, bmcName string, event sink.Event) error {
	if bmcName == "" {
		return nil
	}

	serverList := &metalv1alpha1.ServerList{}
	if err := h.Client.List(ctx, serverList,
		client.MatchingFields{criticalevent.BMCRefField: bmcName}); err != nil {
		return fmt.Errorf("list Servers by %s=%s: %w", criticalevent.BMCRefField, bmcName, err)
	}
	if len(serverList.Items) == 0 {
		h.Log.V(2).Info("No Servers matched the warning event", "bmc", bmcName, "eventID", event.EventID)
		return nil
	}

	if h.EventRecorder == nil {
		return nil
	}

	for i := range serverList.Items {
		server := &serverList.Items[i]
		h.EventRecorder.Eventf(server, nil, corev1.EventTypeWarning, "HardwareWarning", "HardwareWarningReceived",
			"Warning Redfish event [%s]: %s (hostname: %s, component: %s, at: %s)",
			event.MessageID, event.Message, bmcName, event.OriginOfCondition, event.EventTimestamp)
		h.Log.V(1).Info("Warning event emitted",
			"server", server.Name, "bmc", bmcName, "eventID", event.EventID)
	}
	return nil
}
