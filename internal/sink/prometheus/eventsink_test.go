// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and IronCore contributors
// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"context"
	"errors"
	"testing"

	promclient "github.com/prometheus/client_golang/prometheus"

	"github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink"
	promsink "github.com/SAP-cloud-infrastructure/metal-telemetry-operator/internal/sink/prometheus"
)

const (
	testAlertMsgID   = "Test.1.0.Alert"
	testAlertOrigin  = "/redfish/v1/Systems/1/Processors/CPU0"
	testGenericMsgID = "Test.1.0.X"
	testWarnSev      = "Warning"
	testEventID1     = "eid-1"
)

func eventSink(t *testing.T) (*promsink.EventSink, *promclient.Registry) {
	t.Helper()
	reg := promclient.NewRegistry()
	s, err := promsink.NewEventSink(reg)
	if err != nil {
		t.Fatalf("NewEventSink: %v", err)
	}
	return s, reg
}

// counterValue reads the redfish_event_alert_total counter for the given labels.
func counterValue(t *testing.T, reg *promclient.Registry, bmc, severity, messageID, component string) (float64, bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "redfish_event_alert_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["hostname"] == bmc &&
				labels["severity"] == severity &&
				labels["message_id"] == messageID &&
				labels["component"] == component {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func alertSeriesCount(t *testing.T, reg *promclient.Registry) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "redfish_event_alert_total" {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func TestEventSink_CountsNewEvent(t *testing.T) {
	s, reg := eventSink(t)
	events := []sink.Event{
		{EventID: testEventID1, MessageID: testAlertMsgID, Severity: testWarnSev,
			OriginOfCondition: testAlertOrigin},
	}
	if err := s.PublishEvents(context.Background(), "node001", events); err != nil {
		t.Fatalf("PublishEvents: %v", err)
	}
	v, ok := counterValue(t, reg, "node001", testWarnSev, testAlertMsgID, "CPU0")
	if !ok {
		t.Fatal("counter series not found")
	}
	if v != 1 {
		t.Errorf("counter = %v, want 1", v)
	}
}

func TestEventSink_NoDoubleCountOnResentEventID(t *testing.T) {
	s, reg := eventSink(t)
	ev := sink.Event{EventID: testEventID1, MessageID: testAlertMsgID, Severity: testWarnSev,
		OriginOfCondition: testAlertOrigin}
	// publish same event twice
	_ = s.PublishEvents(context.Background(), "node001", []sink.Event{ev})
	_ = s.PublishEvents(context.Background(), "node001", []sink.Event{ev})

	v, _ := counterValue(t, reg, "node001", testWarnSev, testAlertMsgID, "CPU0")
	if v != 1 {
		t.Errorf("counter = %v after resent event, want 1 (no double-count)", v)
	}
}

func TestEventSink_ForgetClearsDedup(t *testing.T) {
	s, reg := eventSink(t)
	ev := sink.Event{EventID: testEventID1, MessageID: testAlertMsgID, Severity: testWarnSev,
		OriginOfCondition: testAlertOrigin}
	_ = s.PublishEvents(context.Background(), "node001", []sink.Event{ev})
	s.Forget("node001")

	if n := alertSeriesCount(t, reg); n != 0 {
		t.Errorf("expected 0 series after Forget, got %d", n)
	}
	// publish again after Forget — should count again
	_ = s.PublishEvents(context.Background(), "node001", []sink.Event{ev})
	v, ok := counterValue(t, reg, "node001", testWarnSev, testAlertMsgID, "CPU0")
	if !ok || v != 1 {
		t.Errorf("counter after Forget+re-publish = %v (ok=%v), want 1", v, ok)
	}
}

func TestEventSink_OnCriticalFired(t *testing.T) {
	s, _ := eventSink(t)
	var gotBMC string
	var gotEvent sink.Event
	s.OnCritical = func(_ context.Context, bmc string, ev sink.Event) error {
		gotBMC = bmc
		gotEvent = ev
		return nil
	}
	ev := sink.Event{EventID: "eid-2", MessageID: "Base.1.0.PSGoodToBad", Severity: "Critical"}
	if err := s.PublishEvents(context.Background(), "node002", []sink.Event{ev}); err != nil {
		t.Fatalf("PublishEvents: %v", err)
	}
	if gotBMC != "node002" {
		t.Errorf("OnCritical bmcName = %q, want node002", gotBMC)
	}
	if gotEvent.EventID != "eid-2" {
		t.Errorf("OnCritical event.EventID = %q, want eid-2", gotEvent.EventID)
	}
}

func TestEventSink_OnWarningFired(t *testing.T) {
	s, _ := eventSink(t)
	var gotBMC string
	s.OnWarning = func(_ context.Context, bmc string, _ sink.Event) error {
		gotBMC = bmc
		return nil
	}
	ev := sink.Event{EventID: "eid-3", MessageID: "Test.1.0.W", Severity: testWarnSev}
	_ = s.PublishEvents(context.Background(), "node003", []sink.Event{ev})
	if gotBMC != "node003" {
		t.Errorf("OnWarning bmcName = %q, want node003", gotBMC)
	}
}

func TestEventSink_OnClearFired(t *testing.T) {
	s, _ := eventSink(t)
	var gotBMC string
	s.OnClear = func(_ context.Context, bmc string) error {
		gotBMC = bmc
		return nil
	}
	ev := sink.Event{EventID: "eid-4", MessageID: "Test.1.0.OK", Severity: "OK"}
	_ = s.PublishEvents(context.Background(), "node004", []sink.Event{ev})
	if gotBMC != "node004" {
		t.Errorf("OnClear bmcName = %q, want node004", gotBMC)
	}
}

func TestEventSink_CriticalSeverityCaseNormalized(t *testing.T) {
	s, reg := eventSink(t)
	// "critical" lowercase — should normalize to "Critical"
	ev := sink.Event{EventID: "eid-5", MessageID: testGenericMsgID, Severity: "critical",
		OriginOfCondition: "/redfish/v1/Systems/1"}
	_ = s.PublishEvents(context.Background(), "node005", []sink.Event{ev})
	v, ok := counterValue(t, reg, "node005", "Critical", testGenericMsgID, "1")
	if !ok || v != 1 {
		t.Errorf("canonical severity not applied; counter = %v ok=%v", v, ok)
	}
}

func TestEventSink_HandlerFailurePreventsDedup(t *testing.T) {
	s, _ := eventSink(t)
	callCount := 0
	s.OnCritical = func(_ context.Context, _ string, _ sink.Event) error {
		callCount++
		return errors.New("transient error")
	}
	ev := sink.Event{EventID: "eid-6", MessageID: testGenericMsgID, Severity: "Critical"}
	_ = s.PublishEvents(context.Background(), "node006", []sink.Event{ev})
	// publish again — handler must be called again (dedup not committed on first failure)
	_ = s.PublishEvents(context.Background(), "node006", []sink.Event{ev})
	if callCount != 2 {
		t.Errorf("OnCritical called %d times, want 2 (deferred commit should retry)", callCount)
	}
}

func TestEventSink_EmptyEventIDSkipped(t *testing.T) {
	s, reg := eventSink(t)
	ev := sink.Event{EventID: "", MessageID: testGenericMsgID, Severity: testWarnSev}
	_ = s.PublishEvents(context.Background(), "node007", []sink.Event{ev})
	if n := alertSeriesCount(t, reg); n != 0 {
		t.Errorf("event with empty EventID should be skipped; got %d series", n)
	}
}

func TestEventSink_ForgetNeverPublishedNoPanic(t *testing.T) {
	s, _ := eventSink(t)
	s.Forget("never-seen") // must not panic
}

func TestEventSink_ComponentFromOrigin(t *testing.T) {
	s, reg := eventSink(t)
	ev := sink.Event{
		EventID: "eid-comp", MessageID: testGenericMsgID, Severity: testWarnSev,
		OriginOfCondition: "/redfish/v1/Systems/1/Processors/CPU.Socket.1",
	}
	_ = s.PublishEvents(context.Background(), "node008", []sink.Event{ev})
	_, ok := counterValue(t, reg, "node008", testWarnSev, testGenericMsgID, "CPU.Socket.1")
	if !ok {
		t.Error("component not extracted correctly from OriginOfCondition")
	}
}

func TestEventSink_EmptyOriginYieldsSystem(t *testing.T) {
	s, reg := eventSink(t)
	ev := sink.Event{EventID: "eid-sys", MessageID: testGenericMsgID, Severity: testWarnSev}
	_ = s.PublishEvents(context.Background(), "node009", []sink.Event{ev})
	_, ok := counterValue(t, reg, "node009", testWarnSev, testGenericMsgID, "system")
	if !ok {
		t.Error("empty OriginOfCondition should yield component=system")
	}
}
