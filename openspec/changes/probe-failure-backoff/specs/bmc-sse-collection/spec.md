# Spec Delta

## ADDED Requirements

### Requirement: EventService probe failure backoff
When the EventService probe fails, the operator SHALL apply exponential backoff per BMC, beginning at 5 minutes and doubling on each consecutive failure up to a maximum of 30 minutes. The failure counter SHALL reset when a new reconcile is triggered for the BMC (not only on success), so that any external change — credential rotation, spec update, or BMC object change — causes an immediate retry.

#### Scenario: First probe failure requeues after 5 minutes
- **WHEN** the EventService probe fails for the first time for a given BMC
- **THEN** the reconciler requeues after 5 minutes

#### Scenario: Consecutive failures double the requeue interval
- **WHEN** the EventService probe fails on consecutive reconcile cycles for the same BMC
- **THEN** each failure doubles the requeue interval (5 min → 10 min → 20 min → 30 min), and the interval does not grow beyond 30 minutes

#### Scenario: Failure counter resets on any reconcile trigger
- **WHEN** a new reconcile is triggered for a BMC that was in probe backoff (e.g. via credential rotation or a BMC spec change)
- **THEN** the failure counter is reset and the next probe failure starts backoff from 5 minutes again

#### Scenario: Successful probe clears backoff
- **WHEN** the EventService probe succeeds after one or more prior failures
- **THEN** the failure counter is reset and the BMC proceeds to SSE connection setup with no residual backoff

### Requirement: Idempotent EventService probe condition updates
The operator SHALL NOT issue a Kubernetes status patch for the `SchemaDetected` condition when the condition's `Status` and `Reason` fields are unchanged from their current values. This prevents the `Owns(BMCTelemetry)` watch from re-enqueueing an immediate reconcile on every probe failure, which would otherwise bypass the probe backoff.

#### Scenario: Repeated probe failure does not re-enqueue via watch
- **WHEN** the EventService probe fails and the `SchemaDetected` condition is already `False/ProbeFailed`
- **THEN** no status patch is issued and no watch-triggered reconcile is enqueued for the owning BMC

#### Scenario: First probe failure writes the condition
- **WHEN** the EventService probe fails and `SchemaDetected` is not yet `False/ProbeFailed`
- **THEN** the condition is patched to `False/ProbeFailed`

#### Scenario: Probe recovery writes the condition
- **WHEN** the EventService probe succeeds and `SchemaDetected` is not yet `True/Probed`
- **THEN** the condition is patched to `True/Probed`

### Requirement: Stable EventService probe error messages
The error message stored in the `SchemaDetected` condition SHALL NOT include ephemeral TCP connection details (source address, source port). The message SHALL convey the failure type in a stable, human-readable form so that repeated failures for the same root cause produce an identical condition message. The message SHALL also append the computed next-retry duration in the form `(next retry in ~Xm)` so that operators can understand the current backoff state without inspecting controller logs.

#### Scenario: Connection reset error is normalised
- **WHEN** the EventService probe fails with a `connection reset by peer` TCP error
- **THEN** the `SchemaDetected` condition message is `"EventService probe failed: connection reset by peer"` (no port or address)

#### Scenario: Other network errors are normalised
- **WHEN** the EventService probe fails with any network-level error containing ephemeral address or port data
- **THEN** the `SchemaDetected` condition message omits those ephemeral parts and retains only the stable error class

#### Scenario: Retry hint is appended to condition message
- **WHEN** the EventService probe fails and the backoff duration is 20 minutes
- **THEN** the `SchemaDetected` condition message ends with `(next retry in ~20m)`

### Requirement: Connected condition coherence during probe backoff
When the EventService probe fails, the operator SHALL set the `Connected` condition on the BMCTelemetry resource to `False` with reason `ProbeFailed`. The operator SHALL NOT issue a status patch for the `Connected` condition when it is already `False/ProbeFailed`. The condition returns to `True` only via the SSE stream layer when a stream is successfully established.

#### Scenario: Probe failure clears stale Connected condition
- **WHEN** the EventService probe fails and the `Connected` condition is currently `True`
- **THEN** the `Connected` condition is patched to `False/ProbeFailed`

#### Scenario: Repeated probe failure does not re-patch Connected
- **WHEN** the EventService probe fails and the `Connected` condition is already `False/ProbeFailed`
- **THEN** no status patch is issued for the `Connected` condition

#### Scenario: Probe success does not directly update Connected
- **WHEN** the EventService probe succeeds after prior failures
- **THEN** the `Connected` condition is managed by the SSE stream layer (not set to `True` by the probe itself)

### Requirement: ProbeBackoff ConnectionState during probe failure
When the EventService probe fails, the operator SHALL set `status.connectionState` to `ProbeBackoff`. The `ConnectionState` type in the CRD SHALL include `ProbeBackoff` as a valid enum value. The field is updated by the SSE stream layer when a stream is established; the probe only writes `ProbeBackoff` on failure.

#### Scenario: Probe failure sets ProbeBackoff connectionState
- **WHEN** the EventService probe fails (regardless of prior connectionState value)
- **THEN** `status.connectionState` is set to `ProbeBackoff`

#### Scenario: ProbeBackoff is a valid CRD enum value
- **WHEN** a BMCTelemetry resource has `connectionState: ProbeBackoff`
- **THEN** the Kubernetes API server accepts the value without a validation error
