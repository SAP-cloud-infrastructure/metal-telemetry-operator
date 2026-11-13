# Spec Delta

## Purpose

Defines how the operator discovers BMC SSE capability, selects the correct filter syntax per vendor, maintains persistent SSE connections, and recovers from failures — the end-to-end lifecycle of streaming MetricReport events from a BMC.

## ADDED Requirements

### Requirement: SSE capability detection

For each BMC resource, the operator probes `/redfish/v1/EventService` to determine whether SSE MetricReport streaming is available and which filter syntax variant to use.

#### Scenario: BMC has no SSE URI
- **WHEN** `EventService.ServerSentEventUri` is absent or empty
- **THEN** the operator marks the BMC with mode `Unsupported`, makes no connection attempt, and does not requeue for SSE-related reconciliation

#### Scenario: Lenovo XCC3 detected
- **WHEN** `EventService` response contains an `SSEFilterPropertiesSupported` object
- **THEN** the operator selects filter syntax `Unquoted` (`eq MetricReport`)

#### Scenario: Dell iDRAC10 detected
- **WHEN** `EventService.@odata.type` contains `v1_11_0` and `SSEFilterPropertiesSupported` is absent
- **THEN** the operator selects filter syntax `Quoted` (`eq 'MetricReport'`)

#### Scenario: Dell iDRAC9 or unknown vendor with SSE URI
- **WHEN** `ServerSentEventUri` is present, `SSEFilterPropertiesSupported` is absent, and `@odata.type` does not contain `v1_11_0`
- **THEN** the operator selects filter syntax `Unquoted` (`eq MetricReport`)

---

### Requirement: Persistent SSE connection per BMC

The operator maintains one long-lived HTTP GET connection per SSE-capable BMC, streaming `#MetricReport` format events.

#### Scenario: SSE connection established
- **WHEN** a BMC is detected as SSE-capable with a valid SSE URI and credentials
- **THEN** the operator opens an HTTP GET to `<ServerSentEventUri>?$filter=EventFormatType eq ['MetricReport']` with TLS and Basic Auth, and begins consuming the event stream

#### Scenario: SSE stream delivers MetricReport event
- **WHEN** an SSE event with `@odata.type` matching `#MetricReport.*` arrives on the stream
- **THEN** the operator parses the event body and forwards MetricValues to the metric sink

#### Scenario: SSE stream delivers non-MetricReport event
- **WHEN** an SSE event with `@odata.type` NOT matching `#MetricReport.*` arrives (e.g. `#Event.*`)
- **THEN** the operator discards the event without publishing metrics or logging an error

#### Scenario: SSE connection drops
- **WHEN** the HTTP stream is closed, errors, or times out
- **THEN** the operator reconnects with exponential backoff and jitter; `BMCTelemetry.status.connectionState` transitions to `Reconnecting` during the backoff interval

---

### Requirement: Bounded concurrent connections (thundering herd prevention)

The operator bounds the number of simultaneously in-flight SSE connect attempts to prevent a fleet-wide restart from overwhelming the network.

#### Scenario: Startup with many BMCs
- **WHEN** the operator starts and reconciles a large number of SSE-capable BMCs simultaneously
- **THEN** new SSE connect attempts are serialized through a semaphore (configurable `maxConcurrentConnects`, default 50); BMCs beyond the limit queue and connect as slots become available

#### Scenario: Reconnect after failure
- **WHEN** an existing SSE connection drops and the operator attempts to reconnect
- **THEN** the reconnect attempt also acquires the semaphore before dialing, subject to the same concurrency limit

---

### Requirement: Credential rotation handling

When a `BMCSecret` referenced by a BMC is updated, the operator reconnects with the new credentials.

#### Scenario: Credential update detected
- **WHEN** the reconciler observes that the credentials for a connected BMC have changed (e.g. password rotation)
- **THEN** the existing SSE connection is cancelled and a new connection is established with the updated credentials

---

### Requirement: BMC deletion cleanup

When a BMC resource is deleted, the operator terminates its SSE connection and removes associated metrics.

#### Scenario: BMC deleted
- **WHEN** a BMC resource is deleted from Kubernetes
- **THEN** the operator cancels the BMC's SSE goroutine, removes the connection from the manager, deletes the `BMCTelemetry` resource, and calls `Forget(bmcName)` on the metric sink to remove all associated Prometheus series
