# Spec Delta

## Purpose

Defines the `BMCTelemetry` CRD — a per-BMC status resource that makes the telemetry operator's internal state observable via Kubernetes, including connection mode, health, detected SSE parameters, and reconnect history.

## ADDED Requirements

### Requirement: One BMCTelemetry resource per BMC

The operator creates and owns one `BMCTelemetry` resource per `BMC` resource in the same namespace.

#### Scenario: BMC discovered
- **WHEN** the reconciler processes a new `BMC` resource
- **THEN** a `BMCTelemetry` resource with the same name is created in the same namespace if it does not already exist

#### Scenario: BMC deleted
- **WHEN** a `BMC` resource is deleted
- **THEN** the corresponding `BMCTelemetry` resource is garbage-collected via owner reference

---

### Requirement: BMCTelemetry status reflects connection state

`BMCTelemetry.status` is updated by the reconciler and the SSE connection manager to reflect the current state of telemetry collection for that BMC.

#### Scenario: SSE successfully connected
- **WHEN** the SSE connection for a BMC is established and streaming
- **THEN** `status.connectionState` is `Connected`, `status.lastEventTime` is updated on each received event, and the `Connected` condition is `True`

#### Scenario: SSE reconnecting after failure
- **WHEN** the SSE connection for a BMC drops and the operator is backing off before retry
- **THEN** `status.connectionState` is `Reconnecting` and `status.reconnectCount` is incremented

#### Scenario: BMC is SSE-unsupported
- **WHEN** the reconciler determines the BMC has no `ServerSentEventUri`
- **THEN** `status.mode` is `Unsupported` and `status.connectionState` is not set

---

### Requirement: BMCTelemetry exposes detected SSE parameters

After a successful EventService probe, the detected parameters are written to status for observability and debugging.

#### Scenario: SSE parameters detected
- **WHEN** the reconciler successfully probes `EventService` and detects SSE capability
- **THEN** `status.sseURI` is set to the detected `ServerSentEventUri`, `status.filterSyntax` is set to `Quoted` or `Unquoted`, and the `SchemaDetected` condition is `True`

---

### Requirement: BMCTelemetry status conditions

`BMCTelemetry.status.conditions` follows standard Kubernetes condition conventions.

#### Scenario: Conditions available
- **WHEN** a `BMCTelemetry` resource is read
- **THEN** `.status.conditions` contains at minimum: `Connected` (True when SSE stream is active) and `SchemaDetected` (True when EventService probe has completed successfully)
