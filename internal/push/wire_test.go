// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package push

import (
	"encoding/json"
	"testing"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
)

func TestEventEnvelope_EventsKey(t *testing.T) {
	body := `{"Events":[{"MessageId":"Test.1.0.Alert","EventTimestamp":"2024-01-01T00:00:00Z"}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	events := env.flatten()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].MessageID != "Test.1.0.Alert" {
		t.Errorf("MessageID = %q, want %q", events[0].MessageID, "Test.1.0.Alert")
	}
}

func TestEventEnvelope_AlertsKey(t *testing.T) {
	body := `{"Alerts":[{"MessageId":"Test.1.0.Alert"}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	events := env.flatten()
	if len(events) != 1 {
		t.Fatalf("want 1 event via Alerts key, got %d", len(events))
	}
}

func TestEventEnvelope_EventsKeyWinsOverAlerts(t *testing.T) {
	body := `{"Events":[{"MessageId":"from-events"}],"Alerts":[{"MessageId":"from-alerts"}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	events := env.flatten()
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].MessageID != "from-events" {
		t.Errorf("Events key should win; got MessageID %q", events[0].MessageID)
	}
}

func TestWireEvent_DeprecatedFieldNames(t *testing.T) {
	body := `{"Events":[{"EventId":"eid-123","Severity":"Warning","MessageId":"Reg.1.0.X","MessageSeverity":""}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	ev := env.flatten()[0]
	if ev.EventID != "eid-123" {
		t.Errorf("EventID = %q, want eid-123", ev.EventID)
	}
	if ev.Severity != "Warning" {
		t.Errorf("Severity = %q, want Warning (deprecated field used as fallback)", ev.Severity)
	}
}

func TestWireEvent_MessageSeverityWinsOverSeverity(t *testing.T) {
	body := `{"Events":[{"Severity":"Warning","MessageSeverity":"Critical"}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	ev := env.flatten()[0]
	if ev.Severity != "Critical" {
		t.Errorf("MessageSeverity should win; got Severity %q", ev.Severity)
	}
}

func TestWireEvent_EventIDFallsBackToMessageID(t *testing.T) {
	body := `{"Events":[{"MessageId":"Reg.1.0.X"}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	ev := env.flatten()[0]
	if ev.EventID != "Reg.1.0.X" {
		t.Errorf("EventID should fall back to MessageID; got %q", ev.EventID)
	}
}

func TestOriginOfCondition_ODataObject(t *testing.T) {
	body := `{"Events":[{"OriginOfCondition":{"@odata.id":"/redfish/v1/Systems/1"}}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	ev := env.flatten()[0]
	if ev.OriginOfCondition != "/redfish/v1/Systems/1" {
		t.Errorf("OriginOfCondition = %q, want /redfish/v1/Systems/1", ev.OriginOfCondition)
	}
}

func TestOriginOfCondition_DellPlainString(t *testing.T) {
	body := `{"Events":[{"OriginOfCondition":"/redfish/v1/Systems/1/Processors/CPU.Socket.1"}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	ev := env.flatten()[0]
	want := "/redfish/v1/Systems/1/Processors/CPU.Socket.1"
	if ev.OriginOfCondition != want {
		t.Errorf("OriginOfCondition = %q, want %q", ev.OriginOfCondition, want)
	}
}

func TestFlatten_EmptyPayload(t *testing.T) {
	var env eventEnvelope
	if got := env.flatten(); got != nil {
		t.Errorf("empty envelope should return nil, got %v", got)
	}
}

func TestToEvent_AllFields(t *testing.T) {
	body := `{"Events":[{
		"EventId":"eid-1",
		"MessageId":"Base.1.0.GeneralError",
		"Message":"A fault occurred",
		"MessageSeverity":"Critical",
		"EventTimestamp":"2024-06-01T12:00:00Z",
		"OriginOfCondition":{"@odata.id":"/redfish/v1/Systems/1/Processors/CPU0"}
	}]}`
	var env eventEnvelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatal(err)
	}
	got := env.flatten()[0]
	want := sink.Event{
		EventID:           "eid-1",
		MessageID:         "Base.1.0.GeneralError",
		Message:           "A fault occurred",
		Severity:          "Critical",
		EventTimestamp:    "2024-06-01T12:00:00Z",
		OriginOfCondition: "/redfish/v1/Systems/1/Processors/CPU0",
	}
	if got != want {
		t.Errorf("toEvent() =\n  %+v\nwant\n  %+v", got, want)
	}
}
