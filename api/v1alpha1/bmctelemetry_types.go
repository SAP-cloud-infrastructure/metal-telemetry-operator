// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TelemetryMode describes how telemetry is collected from this BMC.
// +kubebuilder:validation:Enum=SSE;MetricReport;Unsupported;Excluded
type TelemetryMode string

const (
	// TelemetryModeSSE indicates the BMC supports Redfish SSE MetricReport streaming.
	TelemetryModeSSE TelemetryMode = "SSE"
	// TelemetryModeMetricReport indicates the BMC supports push-based Redfish MetricReport subscriptions.
	TelemetryModeMetricReport TelemetryMode = "MetricReport"
	// TelemetryModeUnsupported indicates the BMC has no ServerSentEventUri and no EventService support.
	TelemetryModeUnsupported TelemetryMode = "Unsupported"
	// TelemetryModeExcluded indicates telemetry collection has been explicitly disabled for this BMC.
	TelemetryModeExcluded TelemetryMode = "Excluded"
)

// FilterSyntax describes the SSE filter syntax variant required by this BMC.
// +kubebuilder:validation:Enum=Quoted;Unquoted
type FilterSyntax string

const (
	// FilterSyntaxQuoted uses single-quoted value: eq 'MetricReport' (Dell iDRAC10).
	FilterSyntaxQuoted FilterSyntax = "Quoted"
	// FilterSyntaxUnquoted uses unquoted value: eq MetricReport (Dell iDRAC9, Lenovo XCC3).
	FilterSyntaxUnquoted FilterSyntax = "Unquoted"
)

// ConnectionState describes the current SSE connection health.
// +kubebuilder:validation:Enum=Connected;Reconnecting;Failed;SubscriptionActive;SlotsFull
type ConnectionState string

const (
	// ConnectionStateConnected means the SSE stream is active.
	ConnectionStateConnected ConnectionState = "Connected"
	// ConnectionStateReconnecting means the connection dropped and the operator is backing off.
	ConnectionStateReconnecting ConnectionState = "Reconnecting"
	// ConnectionStateFailed means all reconnect attempts have been exhausted.
	ConnectionStateFailed ConnectionState = "Failed"
	// ConnectionStateSubscriptionActive means a MetricReport push subscription is present and receiving.
	ConnectionStateSubscriptionActive ConnectionState = "SubscriptionActive"
	// ConnectionStateSlotsFull means the BMC has no free EventDestination subscription slots.
	ConnectionStateSlotsFull ConnectionState = "SlotsFull"
)

// StreamKind identifies which of the two monitored streams a BMCTelemetry resource represents.
// +kubebuilder:validation:Enum=Metrics;Alerts
type StreamKind string

const (
	// StreamKindMetrics represents the MetricReport (or SSE MetricReport) stream.
	StreamKindMetrics StreamKind = "Metrics"
	// StreamKindAlerts represents the Event (or SSE Event) alerts stream.
	StreamKindAlerts StreamKind = "Alerts"
)

const (
	// ConditionConnected is True when the SSE stream is active and delivering events.
	ConditionConnected = "Connected"
	// ConditionSchemaDetected is True when the EventService probe has completed successfully.
	ConditionSchemaDetected = "SchemaDetected"
)

// BMCTelemetrySpec defines the desired state of BMCTelemetry.
type BMCTelemetrySpec struct {
	// modeOverride forces the telemetry mode for this BMC, bypassing auto-detection
	// and ConfigMap rules. When unset, the mode is resolved from the ConfigMap rules
	// and the EventService probe.
	// +optional
	// +kubebuilder:validation:Enum=SSE;MetricReport;Unsupported;Excluded
	ModeOverride *TelemetryMode `json:"modeOverride,omitempty"`

	// streamKind identifies which of the two monitored streams this resource represents.
	// +optional
	StreamKind StreamKind `json:"streamKind,omitempty"`
}

// BMCTelemetryStatus defines the observed state of BMCTelemetry.
type BMCTelemetryStatus struct {
	// mode describes how telemetry is collected from this BMC.
	// +optional
	Mode TelemetryMode `json:"mode,omitempty"`

	// sseURI is the detected ServerSentEventUri from the BMC EventService.
	// +optional
	SSEURI string `json:"sseURI,omitempty"`

	// filterSyntax is the SSE filter syntax variant detected for this BMC vendor.
	// +optional
	FilterSyntax FilterSyntax `json:"filterSyntax,omitempty"`

	// connectionState reflects the current SSE connection health.
	// +optional
	ConnectionState ConnectionState `json:"connectionState,omitempty"`

	// lastEventTime is the timestamp of the most recently received MetricReport event.
	// +optional
	LastEventTime *metav1.Time `json:"lastEventTime,omitempty"`

	// reconnectCount is the number of times the SSE connection has been re-established.
	// +optional
	ReconnectCount int64 `json:"reconnectCount,omitempty"`

	// conditions represent the current state of the BMCTelemetry resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=bmctel
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.status.mode`
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.connectionState`
// +kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.streamKind`
// +kubebuilder:printcolumn:name="Last Event",type=date,JSONPath=`.status.lastEventTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BMCTelemetry tracks the telemetry collection state for one BMC.
type BMCTelemetry struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec BMCTelemetrySpec `json:"spec,omitempty"`

	// +optional
	Status BMCTelemetryStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BMCTelemetryList contains a list of BMCTelemetry.
type BMCTelemetryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BMCTelemetry `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &BMCTelemetry{}, &BMCTelemetryList{})
		return nil
	})
}
