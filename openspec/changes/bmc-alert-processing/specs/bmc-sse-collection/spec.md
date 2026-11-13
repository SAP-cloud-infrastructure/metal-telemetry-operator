# Spec Delta

## MODIFIED Requirements

### Requirement: Persistent SSE connection per BMC
The operator maintains one long-lived HTTP GET connection per SSE-capable BMC per stream kind. For `StreamKindMetrics` connections the operator streams `#MetricReport` format events; for `StreamKindAlerts` connections the operator streams `#Event` format events.

#### Scenario: SSE connection established
- **WHEN** a BMC is detected as SSE-capable with a valid SSE URI and credentials
- **THEN** the operator opens an HTTP GET to the appropriate SSE URL (metrics or alerts) with TLS and Basic Auth, and begins consuming the event stream

#### Scenario: SSE stream delivers MetricReport event on metrics connection
- **WHEN** an SSE event with `@odata.type` matching `#MetricReport.*` arrives on a `StreamKindMetrics` connection
- **THEN** the operator parses the event body and forwards MetricValues to the metric sink

#### Scenario: SSE stream delivers alert event on alerts connection
- **WHEN** an SSE event with `@odata.type` matching `#Event.*` arrives on a `StreamKindAlerts` connection and the event is not identified as a test event by `ConsumeIfTest`
- **THEN** the operator decodes the event payload and publishes it to the `EventSink` for severity-based routing

#### Scenario: Test event on alerts SSE connection consumed for health tracking only
- **WHEN** an SSE `#Event.*` frame arrives on a `StreamKindAlerts` connection and its MessageId is identified as a test event by `ConsumeIfTest`
- **THEN** the system records the pipeline round-trip health metric and does NOT publish the event to the `EventSink`

#### Scenario: Non-MetricReport event on metrics connection discarded
- **WHEN** an SSE event with `@odata.type` NOT matching `#MetricReport.*` arrives on a `StreamKindMetrics` connection
- **THEN** the operator discards the event without publishing metrics or logging an error

#### Scenario: SSE connection drops
- **WHEN** the HTTP stream is closed, errors, or times out
- **THEN** the operator reconnects with exponential backoff and jitter; `BMCTelemetry.status.connectionState` transitions to `Reconnecting` during the backoff interval

## ADDED Requirements

### Requirement: SSE alert stream LastEventTime updated for real events only
The system SHALL update `BMCTelemetryStatus.LastEventTime` on the `{bmc-name}-alerts` resource when a non-test alert event is received on the SSE alerts connection, and SHALL NOT update it for test events.

#### Scenario: Real alert event updates alerts stream LastEventTime
- **WHEN** a real hardware `#Event.*` frame arrives on a `StreamKindAlerts` SSE connection
- **THEN** `BMCTelemetryStatus.LastEventTime` on the `{bmc-name}-alerts` resource is updated

#### Scenario: Test event does not update LastEventTime
- **WHEN** a `#Event.*` frame identified as a test event arrives on a `StreamKindAlerts` SSE connection
- **THEN** `BMCTelemetryStatus.LastEventTime` is NOT updated
