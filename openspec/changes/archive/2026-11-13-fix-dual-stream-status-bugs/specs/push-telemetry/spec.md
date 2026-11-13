# Spec Delta

## MODIFIED Requirements

### Requirement: BMCTelemetry status updated on push activity
The system SHALL update `BMCTelemetryStatus.LastEventTime` when MetricReport data is successfully received from a BMC, and SHALL update `BMCTelemetryStatus.ConnectionState` when a subscription is created, lost, or re-created.

#### Scenario: LastEventTime updated on MetricReport POST
- **WHEN** the push receiver processes a valid MetricReport POST for a BMC
- **THEN** `BMCTelemetryStatus.LastEventTime` on the corresponding metrics BMCTelemetry resource is set to the current time

#### Scenario: LastEventTime updated on alert POST
- **WHEN** the push receiver processes a non-ResourceRemoved alert event POST for a BMC
- **THEN** `BMCTelemetryStatus.LastEventTime` on the corresponding alerts BMCTelemetry resource is set to the current time

#### Scenario: LastEventTime not updated for ResourceRemoved alerts
- **WHEN** the push receiver processes an alert POST whose MessageId indicates ResourceRemoved
- **THEN** `BMCTelemetryStatus.LastEventTime` is NOT updated (the event triggers re-subscription, not a data receipt timestamp)
