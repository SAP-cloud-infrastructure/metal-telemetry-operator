# Spec Delta

## ADDED Requirements

### Requirement: StreamKind identifies the monitored stream
Each `BMCTelemetry` resource SHALL carry a `StreamKind` field in its spec with one of two values — `Metrics` (for the MetricReport or SSE-MetricReport stream) or `Alerts` (for the Event or SSE-Event stream) — identifying which of the two monitored streams it represents.

#### Scenario: Metrics resource carries StreamKind Metrics
- **WHEN** a `BMCTelemetry` resource is created for the metrics stream of BMC `node001`
- **THEN** `spec.streamKind` is `Metrics`

#### Scenario: Alerts resource carries StreamKind Alerts
- **WHEN** a `BMCTelemetry` resource is created for the alerts stream of BMC `node001`
- **THEN** `spec.streamKind` is `Alerts`

### Requirement: Two BMCTelemetry resources per BMC
For each active BMC, the system SHALL maintain exactly two BMCTelemetry resources: `{bmc-name}-metrics` (owned by the BMC, `StreamKind: Metrics`) and `{bmc-name}-alerts` (owned by the BMC, `StreamKind: Alerts`).

#### Scenario: Both stream resources created on first reconcile
- **WHEN** a BMC transitions to an active state and no BMCTelemetry resources exist for it
- **THEN** the system creates `{bmc-name}-metrics` and `{bmc-name}-alerts`, both owned by the BMC

#### Scenario: Both stream resources deleted when BMC is removed
- **WHEN** a BMC is deleted or transitions out of an active state
- **THEN** both `{bmc-name}-metrics` and `{bmc-name}-alerts` are deleted

#### Scenario: Independent ConnectionState per stream
- **WHEN** the MetricReport stream for BMC `node001` is connected but the alerts stream has not yet established
- **THEN** `node001-metrics.status.connectionState` is `Connected` or `SubscriptionActive` while `node001-alerts.status.connectionState` is `Reconnecting` or `Failed`

### Requirement: SubscriptionActive connection state value
The `ConnectionState` enum SHALL include the value `SubscriptionActive`, indicating that a MetricReport-mode subscription for the stream is present on the BMC and events are being received.

#### Scenario: SubscriptionActive set after subscription created
- **WHEN** a MetricReport-mode BMC has an active EventDestination subscription and an event POST is received
- **THEN** the stream's `connectionState` is `SubscriptionActive`

#### Scenario: SubscriptionActive is a valid ConnectionState
- **WHEN** a `BMCTelemetry` resource has `status.connectionState` set to `SubscriptionActive`
- **THEN** the resource is accepted by the Kubernetes API without a validation error

### Requirement: SlotsFull connection state value
The `ConnectionState` enum SHALL include the value `SlotsFull`, indicating that the BMC has no available EventDestination subscription slots and the stream cannot be established until a slot is freed.

#### Scenario: SlotsFull is a valid ConnectionState
- **WHEN** a `BMCTelemetry` resource has `status.connectionState` set to `SlotsFull`
- **THEN** the resource is accepted by the Kubernetes API without a validation error

#### Scenario: SlotsFull visible in CLI output
- **WHEN** `kubectl get bmctelemetry {bmc-name}-metrics` is run for a BMC whose subscription slots are exhausted
- **THEN** the STATE column displays `SlotsFull`
