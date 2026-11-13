# Spec Delta

## Purpose

Defines how MTO processes Redfish alert events from BMCs — counting them, routing by severity to Server conditions and Kubernetes Events, deduplicating resent events, and verifying pipeline health via periodic test-event round-trips.

## ADDED Requirements

### Requirement: Alert events counted per severity
The system SHALL maintain a `redfish_event_alert_total` counter with labels `hostname`, `severity`, `message_id`, and `component` that increments once per unique alert event received from a BMC.

#### Scenario: New alert event increments counter
- **WHEN** a Redfish alert event with a unique EventID arrives for a BMC
- **THEN** `redfish_event_alert_total` is incremented with the canonical severity, MessageId, and component derived from the OriginOfCondition URI

#### Scenario: Resent event not double-counted
- **WHEN** a Redfish alert event with an EventID already seen for that BMC arrives again
- **THEN** `redfish_event_alert_total` is NOT incremented a second time for that event

#### Scenario: BMC removed clears dedup state
- **WHEN** a BMC is deleted or transitions away from an active telemetry mode
- **THEN** its per-BMC dedup state is cleared so future events from that BMC are not silently suppressed

### Requirement: Critical alert sets Server condition
When an alert event with Critical severity arrives, the system SHALL set a `CriticalEventReceived=True` condition on every Server resource whose `spec.bmcRef.name` matches the BMC name.

#### Scenario: Critical alert sets condition and emits Kubernetes Event
- **WHEN** a Critical-severity alert event arrives for a BMC
- **THEN** the system sets `CriticalEventReceived=True` on all matching Server resources, emits a Kubernetes Warning Event on each matching Server, and sets `redfish_server_critical_alert` gauge to 1

#### Scenario: OK/Info alert clears Server condition
- **WHEN** an OK or Info severity alert event arrives for a BMC
- **THEN** the system sets `CriticalEventReceived=False` on all matching Server resources and sets `redfish_server_critical_alert` gauge to 0

### Requirement: Warning alert emits Kubernetes Event
When an alert event with Warning severity arrives, the system SHALL emit a Kubernetes Warning Event on every Server whose `spec.bmcRef.name` matches the BMC name, without changing any Server condition.

#### Scenario: Warning alert emits event, no condition change
- **WHEN** a Warning-severity alert event arrives for a BMC
- **THEN** the system emits a Kubernetes Warning Event on all matching Server resources and does NOT modify any condition on those Servers

### Requirement: Critical alert gauge pre-warmed on startup
On startup, before the first new alert event arrives, the system SHALL set `redfish_server_critical_alert` gauge=1 for each Server that already carries a `CriticalEventReceived=True` condition.

#### Scenario: Pod restart restores gauge immediately
- **WHEN** the operator pod restarts and at least one Server already has `CriticalEventReceived=True`
- **THEN** the `redfish_server_critical_alert` gauge reflects those Servers as critical before any new alert event is processed

### Requirement: Alert event decode accepts vendor field variants
The system SHALL decode Redfish alert payloads that use either `Events` or `Alerts` as the top-level array key, accept both `EventId`/`MessageId` (pre/post Redfish v1.5) and `Severity`/`MessageSeverity` (pre/post Redfish v1.5), and tolerate `OriginOfCondition` sent as either a JSON object with `@odata.id` or a plain string.

#### Scenario: Alerts array key accepted
- **WHEN** a BMC posts an alert payload with a top-level `Alerts` key instead of `Events`
- **THEN** the system processes those events identically to an `Events`-keyed payload

#### Scenario: Deprecated field names accepted
- **WHEN** a BMC sends `EventId` instead of `MessageId` or `Severity` instead of `MessageSeverity`
- **THEN** the system uses the deprecated field value and processes the event correctly

#### Scenario: OriginOfCondition plain string accepted
- **WHEN** a BMC sends `OriginOfCondition` as a plain string URI rather than `{"@odata.id": "..."}` object
- **THEN** the system extracts the component name from that string without error

### Requirement: Test-event heartbeat verifies pipeline health
The system SHALL periodically fire `SubmitTestEvent` on each subscribed BMC and track whether the resulting event round-trips back through the alert receiver, surfacing pipeline health as a metric.

#### Scenario: Test event round-trip recorded
- **WHEN** the system fires a `SubmitTestEvent` for a BMC and the resulting alert event arrives at the receiver within the timeout window
- **THEN** a pipeline health metric records a successful round-trip for that BMC

#### Scenario: Test event not mistaken for hardware alert
- **WHEN** an alert event whose MessageId matches an in-flight test event arrives at the receiver
- **THEN** the system records the round-trip health metric and does NOT route the event to the alert pipeline (no Server condition change, no counter increment, no Kubernetes Event emission)

#### Scenario: Test event does not update LastEventTime
- **WHEN** an alert event identified as a test event arrives
- **THEN** `BMCTelemetryStatus.LastEventTime` on the `{bmc-name}-alerts` resource is NOT updated
