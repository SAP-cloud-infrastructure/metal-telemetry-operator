# push-telemetry Specification

## Purpose

Enables MTO to collect BMC telemetry from HPE iLO servers by accepting Redfish MetricReport POSTs on a plain HTTP receiver and maintaining the subscriptions that trigger those pushes.

## Requirements

### Requirement: Receiver accepts MetricReport POSTs
The system SHALL accept HTTP POST requests from BMCs at the MetricReport endpoint and translate the Redfish MetricReport JSON payload into Prometheus metric samples for the identified BMC.

#### Scenario: MetricReport POST processed
- **WHEN** a BMC POSTs a Redfish MetricReport payload to the receiver's MetricReport endpoint for a known BMC name
- **THEN** the system extracts metric samples from the payload and publishes them to the metrics sink

#### Scenario: POST for unknown BMC ignored
- **WHEN** a POST arrives at the MetricReport endpoint with a BMC name that has no active subscription entry
- **THEN** the system returns a 2xx response and discards the payload without publishing samples

### Requirement: Receiver accepts alert event POSTs
The system SHALL accept HTTP POST requests from BMCs at the alerts endpoint and process Redfish Event payloads.

#### Scenario: Alert event POST processed
- **WHEN** a BMC POSTs a Redfish Event payload to the receiver's alerts endpoint for a known BMC name
- **THEN** the system parses the event and dispatches it for handling

#### Scenario: Non-ResourceRemoved alert does not trigger re-subscription
- **WHEN** a BMC POSTs an alert event whose MessageId is not a ResourceRemoved variant
- **THEN** the system processes the event without scheduling a subscription reconcile for that BMC

### Requirement: ResourceRemoved triggers immediate re-subscription
When the receiver processes an alert event whose MessageId indicates a `ResourceRemoved` type (signaling HPE's `TerminateAfterRetries` policy killed a subscription), the system SHALL enqueue that BMC for immediate subscription reconcile without waiting for the next scheduled reconcile cycle.

#### Scenario: ResourceRemoved alert re-subscribes fast
- **WHEN** a BMC POSTs an alert event with MessageId matching the ResourceRemoved pattern for a MetricReport subscription
- **THEN** the system triggers a subscription reconcile for that BMC within seconds

### Requirement: Receiver runs without leader election
The push receiver HTTP server SHALL start on all pods regardless of leader election state, so BMC-initiated POSTs are never dropped due to a leader failover.

#### Scenario: Receiver binds on non-leader pod
- **WHEN** the operator pod is not the current leader-election holder
- **THEN** the receiver HTTP server is still bound and accepting requests on the configured address

### Requirement: Subscription created for MetricReport-mode BMC
For each BMC whose effective telemetry mode is `MetricReport`, the system SHALL create and maintain a Redfish EventDestination subscription for MetricReport events and a separate EventDestination subscription for alert events on that BMC.

#### Scenario: Subscriptions created on first reconcile
- **WHEN** a BMC enters MetricReport mode and no EventDestination subscriptions exist for MTO's subscriber ID
- **THEN** the system creates a MetricReport subscription and an alerts subscription on the BMC

#### Scenario: Existing subscriptions not recreated
- **WHEN** the BMC already has active EventDestination subscriptions matching MTO's subscriber ID
- **THEN** the system does not create duplicate subscriptions

### Requirement: Subscription re-created after operator restart
On startup, for each BMC in MetricReport mode, the system SHALL detect absent subscriptions and re-create them without operator intervention.

#### Scenario: Missing subscription restored on startup
- **WHEN** the operator restarts and a MetricReport-mode BMC has no active EventDestination subscriptions
- **THEN** the subscription reconciler creates the missing subscriptions during its first reconcile pass

### Requirement: Receiver listens on plain HTTP
The push receiver SHALL bind a plain HTTP (not HTTPS) listener, bypassing the TLS certificate chain validation that iLO 7's `SecureStandard` policy enforces for HTTPS-destination subscriptions.

#### Scenario: Receiver URL is HTTP scheme
- **WHEN** the operator starts with `--push-receiver-url` set to an `http://` URL
- **THEN** the receiver binds a plain HTTP server on the configured address without error

### Requirement: Receiver and subscription options are flag-configurable
The receiver's listen address, external URL, subscriber ID, and ConfigMap coordinates SHALL each be configurable via dedicated operator flags at startup.

#### Scenario: Non-default port used
- **WHEN** the operator starts with `--push-events-addr` set to a non-default address
- **THEN** the receiver binds on that address and the subscription destinations reference the URL from `--push-receiver-url`

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

### Requirement: Push pipeline shares Prometheus sink with SSE pipeline
The system SHALL register the `redfish_telemetry_reading` Prometheus gauge exactly once across both the SSE and push/MetricReport collection paths, so the operator starts without error when both SSE-mode and MetricReport-mode BMCs are present.

#### Scenario: Operator starts with both SSE and MetricReport BMCs
- **WHEN** the operator starts with `--push-receiver-url` configured and SSE-capable BMCs exist
- **THEN** the operator starts without error and both the SSE and push receiver paths publish samples to the same `redfish_telemetry_reading` gauge

#### Scenario: Forget on mode transition cleans up all series
- **WHEN** a BMC transitions from MetricReport mode to SSE mode (or the reverse) and `Forget` is called for that BMC
- **THEN** all `redfish_telemetry_reading` gauge series for that BMC are deleted, regardless of which collection path originally wrote them
