// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"encoding/json"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// wireEvent holds the fields that Redfish vendors may send in an alert
// event payload. It handles two naming generations:
//
//  1. Redfish v1.5 renamed EventId→MessageId and Severity→MessageSeverity.
//     We accept both, with the newer names winning when both are present.
//
//  2. Dell iDRAC sends OriginOfCondition as a plain URI string rather than
//     the spec-mandated {"@odata.id":"..."} object. originOfConditionRef
//     accepts either form.
type wireEvent struct {
	EventID         string               `json:"EventId"`
	MessageID       string               `json:"MessageId"`
	Message         string               `json:"Message"`
	Severity        string               `json:"Severity"`        // deprecated since Redfish v1.5
	MessageSeverity string               `json:"MessageSeverity"` // preferred since Redfish v1.5
	EventTimestamp  string               `json:"EventTimestamp"`
	Origin          originOfConditionRef `json:"OriginOfCondition"`
}

type originOfConditionRef struct {
	ODataID string
}

// UnmarshalJSON accepts both the spec object form {"@odata.id":"..."} and a
// plain string (Dell iDRAC quirk). Both resolve to the ODataID field.
func (o *originOfConditionRef) UnmarshalJSON(data []byte) error {
	var ref struct {
		ODataID string `json:"@odata.id"`
	}
	if err := json.Unmarshal(data, &ref); err == nil {
		o.ODataID = ref.ODataID
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	o.ODataID = s
	return nil
}

// toEvent collapses a wire-format event into the canonical sink.Event.
//
// MessageSeverity (Redfish v1.5+) wins over the deprecated Severity.
// EventID prefers the wire EventId (per-instance by spec) and falls back
// to MessageId — without this, two distinct failures of the same type
// would share an EventId and the second would be silently deduped.
func (e wireEvent) toEvent() sink.Event {
	out := sink.Event{
		Message:           e.Message,
		MessageID:         e.MessageID,
		EventTimestamp:    e.EventTimestamp,
		OriginOfCondition: e.Origin.ODataID,
	}
	if e.MessageSeverity != "" {
		out.Severity = e.MessageSeverity
	} else {
		out.Severity = e.Severity
	}
	if e.EventID != "" {
		out.EventID = e.EventID
	} else {
		out.EventID = e.MessageID
	}
	return out
}

// eventEnvelope accepts both vendor variants of the SEL push payload:
// some BMCs use "Events", others use "Alerts" as the top-level key.
type eventEnvelope struct {
	Events []wireEvent `json:"Events,omitempty"`
	Alerts []wireEvent `json:"Alerts,omitempty"`
}

func (e eventEnvelope) flatten() []sink.Event {
	src := e.Events
	if len(src) == 0 {
		src = e.Alerts
	}
	if len(src) == 0 {
		return nil
	}
	out := make([]sink.Event, 0, len(src))
	for i := range src {
		out = append(out, src[i].toEvent())
	}
	return out
}
