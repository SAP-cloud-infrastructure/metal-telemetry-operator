# Spec Delta

## Purpose

Exposes the push-subscription health of every BMC as a Prometheus gauge, enabling fleet-level queries and alerting on subscription failures without accessing Kubernetes CRD status.

## ADDED Requirements

### Requirement: Per-BMC subscription status published as Prometheus gauge

The system SHALL publish a `redfish_bmc_event_subscription_status` gauge for every BMC the operator manages. The gauge value is `1` when the BMC has an active push subscription and `0` otherwise. Labels are `hostname`, `vendor`, `model`, and `reason`.

#### Scenario: BMC has active MetricReport subscription
- **WHEN** the push subscription reconciler confirms both MetricReport and alerts EventDestination subscriptions are active on a BMC
- **THEN** the gauge is set to `1` with `reason="subscribed"` for that BMC

#### Scenario: BMC is in SSE mode
- **WHEN** the vendor probe determines the BMC uses SSE (no `ServerSentEventUri` absent, `ServiceEnabled=true`)
- **THEN** the gauge is set to `0` with `reason="sse_mode"` to distinguish active SSE collection from an untracked BMC

#### Scenario: BMC matched by ExcludeHardware rule
- **WHEN** the operator resolves the effective telemetry mode and the BMC matches an `ExcludeHardware` config rule
- **THEN** the gauge is set to `0` with `reason="excluded"`

#### Scenario: Vendor probe fails
- **WHEN** the BMC reconciler's vendor probe returns an error or reaches its retry cap
- **THEN** the gauge is set to `0` with `reason="probe_failed"`

#### Scenario: Push subscription slot exhausted
- **WHEN** `CreateEventSubscription` returns `ErrSubscriptionSlotsFull` (HTTP 409 ResourceInUse)
- **THEN** the gauge is set to `0` with `reason="slots_full"` for the affected BMC

### Requirement: Subscription status reason label reflects label-set changes atomically

If the reason for a BMC changes (e.g., from `probe_failed` to `subscribed`), the previous label-set series SHALL be deleted before the new one is written so at most one series per BMC exists at any time.

#### Scenario: BMC transitions from probe_failed to subscribed
- **WHEN** a BMC that previously had `reason="probe_failed"` succeeds in creating subscriptions
- **THEN** the `reason="probe_failed"` series is deleted and a new `reason="subscribed"` series with value `1` is written; no stale series with the old reason remains

### Requirement: Gauge series removed on BMC deletion

When a BMC is deleted, its subscription status gauge series SHALL be removed to prevent stale metrics.

#### Scenario: BMC resource deleted
- **WHEN** the operator processes a BMC deletion event
- **THEN** the `redfish_bmc_event_subscription_status` series for that BMC is removed from the Prometheus registry
