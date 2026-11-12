# Spec Delta

## ADDED Requirements

### Requirement: Push pipeline shares Prometheus sink with SSE pipeline
The system SHALL register the `redfish_telemetry_reading` Prometheus gauge exactly once across both the SSE and push/MetricReport collection paths, so the operator starts without error when both SSE-mode and MetricReport-mode BMCs are present.

#### Scenario: Operator starts with both SSE and MetricReport BMCs
- **WHEN** the operator starts with `--push-receiver-url` configured and SSE-capable BMCs exist
- **THEN** the operator starts without error and both the SSE and push receiver paths publish samples to the same `redfish_telemetry_reading` gauge

#### Scenario: Forget on mode transition cleans up all series
- **WHEN** a BMC transitions from MetricReport mode to SSE mode (or the reverse) and `Forget` is called for that BMC
- **THEN** all `redfish_telemetry_reading` gauge series for that BMC are deleted, regardless of which collection path originally wrote them
