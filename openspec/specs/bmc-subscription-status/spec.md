# BMC Subscription Status Specification

## Purpose

Exposes the push-subscription health of every BMC as a Prometheus gauge, enabling fleet-level queries and alerting on subscription failures without accessing Kubernetes CRD status.

## Requirements

### Requirement: Per-BMC subscription status published as Prometheus gauge

The system SHALL publish a `redfish_stream_connection_status` gauge for every BMC it manages,
emitting exactly two series per BMC: one for `stream_type="metrics"` and one for
`stream_type="alerts"`. The gauge value is `1` when that stream is live and `0` otherwise.
Labels are `hostname`, `stream_type`, and `reason`. The `vendor` and `model` labels are
removed; topology information is available via `kube_ironcore_bmc_info` joins.

#### Scenario: BMC has active MetricReport subscription
- **WHEN** the push reconciler confirms both the MetricReport EventDestination and the alerts
  EventDestination subscriptions are active on a BMC
- **THEN** both `stream_type="metrics"` and `stream_type="alerts"` series are set to `1`
  with `reason="connected"`

#### Scenario: BMC is in SSE mode
- **WHEN** an SSE HTTP connection of a given stream kind returns HTTP 200 (stream established)
- **THEN** the corresponding `stream_type` series is set to `1` with `reason="connected"`;
  when that stream later drops and enters reconnect backoff, the series is set to `0`
  with `reason="disconnected"`

#### Scenario: SSE metrics and alerts streams tracked independently
- **WHEN** the alerts SSE stream disconnects while the metrics SSE stream remains connected
- **THEN** the `stream_type="alerts"` series transitions to `0` with `reason="disconnected"`
  and the `stream_type="metrics"` series retains `1` with `reason="connected"`

#### Scenario: BMC matched by ExcludeHardware rule
- **WHEN** the operator resolves the effective telemetry mode and the BMC matches an
  `ExcludeHardware` config rule
- **THEN** both `stream_type="metrics"` and `stream_type="alerts"` series are set to `0`
  with `reason="excluded"`

#### Scenario: Vendor probe fails
- **WHEN** the BMC reconciler's vendor probe returns an error or reaches its retry cap
- **THEN** both `stream_type="metrics"` and `stream_type="alerts"` series are set to `0`
  with `reason="probe_failed"`

#### Scenario: Push subscription slot exhausted
- **WHEN** the push reconciler receives HTTP 409 when creating the MetricReport subscription,
  the `stream_type="metrics"` series is set to `0` with `reason="slots_full"`
- **AND WHEN** the push reconciler receives HTTP 409 when creating the alerts subscription,
  the `stream_type="alerts"` series is set to `0` with `reason="slots_full"`
- **THEN** each stream's status is tracked independently so a slots_full failure on one
  subscription does not affect the other stream's recorded state

### Requirement: Subscription status reason label reflects label-set changes atomically

If the reason for a `{hostname, stream_type}` pair changes, the previous series SHALL be
deleted before the new one is written so at most one series per pair exists at any time.

#### Scenario: BMC transitions from probe_failed to subscribed
- **WHEN** a BMC that previously had both streams at `reason="probe_failed"` successfully
  establishes both streams (via push subscription or SSE connection)
- **THEN** both `reason="probe_failed"` series are deleted and new `reason="connected"` series
  with value `1` are written; no stale series with the old reason remain

#### Scenario: SSE stream reason change
- **WHEN** a BMC's `stream_type="metrics"` series transitions from `reason="disconnected"` to
  `reason="connected"` after the SSE stream reconnects
- **THEN** the `{hostname, stream_type="metrics", reason="disconnected"}` series is deleted
  and a new `{hostname, stream_type="metrics", reason="connected"}` series with value `1`
  is written

### Requirement: Gauge series removed on BMC deletion

When a BMC is deleted, both its `stream_type="metrics"` and `stream_type="alerts"` gauge
series SHALL be removed to prevent stale metrics.

#### Scenario: BMC resource deleted
- **WHEN** the operator processes a BMC deletion event
- **THEN** both the `stream_type="metrics"` and `stream_type="alerts"` series for that
  BMC's hostname are removed from the Prometheus registry
