# Spec Delta

## MODIFIED Requirements

### Requirement: BMCTelemetry status updated on push activity
The system SHALL update `LastEventTime` on the `{bmc-name}-metrics` BMCTelemetry resource when a MetricReport POST is successfully processed for a BMC, and SHALL update `ConnectionState` on the affected stream resource when that stream's subscription is created, lost, or re-created.

#### Scenario: LastEventTime set on metrics stream after MetricReport receive
- **WHEN** a MetricReport POST is processed and samples are published for BMC `node001`
- **THEN** the system updates `LastEventTime` on the `node001-metrics` BMCTelemetry resource

#### Scenario: ConnectionState on affected stream reflects subscription loss
- **WHEN** the MetricReport subscription for BMC `node001` is terminated and re-creation has not yet completed
- **THEN** `node001-metrics.status.connectionState` reflects the disconnected state

#### Scenario: Alert subscription loss updates alerts stream state
- **WHEN** the alerts subscription for BMC `node001` is terminated and re-creation has not yet completed
- **THEN** `node001-alerts.status.connectionState` reflects the disconnected state

## ADDED Requirements

### Requirement: Alert events update alerts stream status
The system SHALL update `LastEventTime` on the `{bmc-name}-alerts` BMCTelemetry resource when a non-ResourceRemoved alert event POST is received from a BMC.

#### Scenario: Alert POST updates alerts stream LastEventTime
- **WHEN** a BMC POSTs a Redfish Event payload that is not a ResourceRemoved event
- **THEN** the system updates `LastEventTime` on the `{bmc-name}-alerts` BMCTelemetry resource for that BMC

#### Scenario: ResourceRemoved alert does not update LastEventTime
- **WHEN** a BMC POSTs an alert event with a ResourceRemoved MessageId
- **THEN** the system triggers re-subscription but does NOT update `LastEventTime` on the alerts stream resource

### Requirement: Subscription slot exhaustion handled gracefully
When a BMC returns HTTP 409 with a `ResourceInUse` or `EventSubscriptionLimitExceeded` message during subscription creation, the system SHALL set `ConnectionState` to `SlotsFull` on the affected stream resource and requeue after a long backoff of approximately 10 minutes.

#### Scenario: 409 ResourceInUse sets SlotsFull and backs off
- **WHEN** `CreateEventSubscription` returns HTTP 409 with a `ResourceInUse` message
- **THEN** the system sets `ConnectionState: SlotsFull` on the affected stream resource and does not retry within the current reconcile cycle

#### Scenario: SlotsFull BMC is requeued after long backoff
- **WHEN** the subscription reconciler has set `ConnectionState: SlotsFull` on a stream resource
- **THEN** the system requeues that BMC for reconcile after approximately 10 minutes

#### Scenario: SlotsFull applies per stream, not per BMC
- **WHEN** only the MetricReport subscription slot is exhausted
- **THEN** `SlotsFull` is set on `{bmc-name}-metrics` only; the alerts stream state is set independently
