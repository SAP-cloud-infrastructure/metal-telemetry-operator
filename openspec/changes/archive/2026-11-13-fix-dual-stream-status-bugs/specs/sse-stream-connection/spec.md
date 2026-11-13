# Spec Delta

## Purpose

Manages two independent SSE streams per BMC (metrics and alerts), each connected to the correct Redfish event-type-filtered endpoint, with per-stream connection state tracked in the corresponding BMCTelemetry resource.

## ADDED Requirements

### Requirement: Metrics SSE stream uses MetricReport-filtered URL
The metrics SSE stream SHALL connect to the SSE endpoint filtered to `EventFormatType eq MetricReport`, so only MetricReport events are delivered on that stream.

#### Scenario: Metrics stream connects to correct URL
- **WHEN** an SSE connection of stream kind `Metrics` is established for a BMC
- **THEN** the HTTP request targets the SSE URI with an `EventFormatType eq MetricReport` filter query parameter

### Requirement: Alerts SSE stream uses Event-filtered URL
The alerts SSE stream SHALL connect to the SSE endpoint filtered to `EventFormatType eq Event`, so only alert events are delivered on that stream.

#### Scenario: Alerts stream connects to correct URL
- **WHEN** an SSE connection of stream kind `Alerts` is established for a BMC
- **THEN** the HTTP request targets the SSE URI with an `EventFormatType eq Event` filter query parameter

#### Scenario: Metrics and alerts streams use different URLs
- **WHEN** both SSE streams are active for the same BMC
- **THEN** the metrics stream URL and the alerts stream URL differ only in their filter parameter value

### Requirement: Per-stream ConnectionState reflects stream health
Each SSE stream's `ConnectionState` SHALL be tracked independently: `Connected` when the HTTP stream is established, `Reconnecting` when the stream has ended and the operator is waiting before retrying.

#### Scenario: Connected state set on stream establishment
- **WHEN** the SSE HTTP connection returns HTTP 200 for a given stream kind
- **THEN** the BMCTelemetry resource for that stream kind transitions to `ConnectionState: Connected`

#### Scenario: Reconnecting state set on stream end
- **WHEN** a previously connected SSE stream ends (error or EOF)
- **THEN** the BMCTelemetry resource for that stream kind transitions to `ConnectionState: Reconnecting`

#### Scenario: Metrics stream state is independent of alerts stream state
- **WHEN** the alerts SSE stream disconnects and enters reconnect backoff
- **THEN** the metrics BMCTelemetry resource retains its `Connected` state if the metrics stream is still active
