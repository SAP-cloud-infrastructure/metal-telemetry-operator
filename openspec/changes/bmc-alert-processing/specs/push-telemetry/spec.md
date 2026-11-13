# Spec Delta

## MODIFIED Requirements

### Requirement: Receiver accepts alert event POSTs
The system SHALL accept HTTP POST requests from BMCs at the alerts endpoint, decode the Redfish Event payload (accepting both `Events` and `Alerts` top-level keys), and route decoded events to the alert pipeline. Real hardware events SHALL be published to the `EventSink`; test events identified by `ConsumeIfTest` SHALL be consumed for pipeline health tracking only without entering the alert pipeline.

#### Scenario: Alert event POST processed and routed to EventSink
- **WHEN** a BMC POSTs a Redfish Event payload to the receiver's alerts endpoint and the event is not identified as a test event by `ConsumeIfTest`
- **THEN** the system decodes the payload, extracts all events, and publishes them to the `EventSink` for severity-based routing

#### Scenario: Test event consumed for health tracking only
- **WHEN** a BMC POSTs an alert event whose MessageId matches an in-flight test event tracked by `ConsumeIfTest`
- **THEN** the system records the pipeline round-trip health metric and does NOT publish the event to the `EventSink`

#### Scenario: Non-ResourceRemoved alert does not trigger re-subscription
- **WHEN** a BMC POSTs an alert event whose MessageId is not a ResourceRemoved variant
- **THEN** the system processes the event without scheduling a subscription reconcile for that BMC

### Requirement: Alert events update alerts stream status
The system SHALL update `LastEventTime` on the `{bmc-name}-alerts` BMCTelemetry resource when a non-ResourceRemoved, non-test alert event POST is received from a BMC.

#### Scenario: Alert POST updates alerts stream LastEventTime
- **WHEN** a BMC POSTs a Redfish Event payload that is not a ResourceRemoved event and not identified as a test event
- **THEN** the system updates `LastEventTime` on the `{bmc-name}-alerts` BMCTelemetry resource for that BMC

#### Scenario: ResourceRemoved alert does not update LastEventTime
- **WHEN** a BMC POSTs an alert event with a ResourceRemoved MessageId
- **THEN** the system triggers re-subscription but does NOT update `LastEventTime` on the alerts stream resource

#### Scenario: Test event does not update LastEventTime
- **WHEN** a BMC POSTs an alert event identified as a test event by `ConsumeIfTest`
- **THEN** `LastEventTime` on the `{bmc-name}-alerts` BMCTelemetry resource is NOT updated
