# Spec Delta

## Purpose

Enables MTO to collect BMC telemetry from HPE iLO servers by accepting Redfish MetricReport POSTs on a plain HTTP receiver and maintaining the subscriptions that trigger those pushes.

## ADDED Requirements

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

#### Scenario: LastEventTime set after successful receive
- **WHEN** a MetricReport POST is processed and samples are published for a BMC
- **THEN** the system updates `BMCTelemetryStatus.LastEventTime` for that BMC

#### Scenario: ConnectionState reflects subscription loss
- **WHEN** a subscription is terminated (e.g. ResourceRemoved received) and re-creation has not yet completed
- **THEN** `BMCTelemetryStatus.ConnectionState` reflects the disconnected state
