// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package sse

import (
	"encoding/json"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

// ConsumeIfTest returns true when messageId matches an in-flight SubmitTestEvent
// heartbeat sent for bmcName. When it returns true the caller must not route the
// event through the normal alert pipeline.
//
// Defined locally to avoid an import cycle: push already imports sse, so sse
// cannot import push. Go structural typing means *push.SubscriptionReconciler
// satisfies this interface automatically.
type ConsumeIfTest interface {
	ConsumeIfTest(bmcName, messageId string) bool
}

// alertWireEvent mirrors push.wireEvent but is local to the sse package.
// Both Events[] and Alerts[] array members decode into this type.
type alertWireEvent struct {
	EventID         string         `json:"EventId"`
	MessageID       string         `json:"MessageId"`
	Message         string         `json:"Message"`
	Severity        string         `json:"Severity"`
	MessageSeverity string         `json:"MessageSeverity"`
	EventTimestamp  string         `json:"EventTimestamp"`
	Origin          alertOriginRef `json:"OriginOfCondition"`
}

// alertOriginRef unmarshals the Redfish OriginOfCondition field, which may be
// a {"@odata.id":"..."} object (spec-compliant) or a plain string (Dell quirk).
type alertOriginRef struct {
	ODataID string
}

func (o *alertOriginRef) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return nil
	}
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

func (e alertWireEvent) toSinkEvent() sink.Event {
	severity := e.MessageSeverity
	if severity == "" {
		severity = e.Severity
	}
	return sink.Event{
		EventID:           e.EventID,
		MessageID:         e.MessageID,
		Message:           e.Message,
		Severity:          severity,
		EventTimestamp:    e.EventTimestamp,
		OriginOfCondition: e.Origin.ODataID,
	}
}

// alertEnvelope accepts both Events[] (Dell, Lenovo) and Alerts[] variants.
type alertEnvelope struct {
	Events []alertWireEvent `json:"Events,omitempty"`
	Alerts []alertWireEvent `json:"Alerts,omitempty"`
}

func (e alertEnvelope) flatten() []sink.Event {
	src := e.Events
	if len(src) == 0 {
		src = e.Alerts
	}
	if len(src) == 0 {
		return nil
	}
	out := make([]sink.Event, 0, len(src))
	for i := range src {
		out = append(out, src[i].toSinkEvent())
	}
	return out
}
