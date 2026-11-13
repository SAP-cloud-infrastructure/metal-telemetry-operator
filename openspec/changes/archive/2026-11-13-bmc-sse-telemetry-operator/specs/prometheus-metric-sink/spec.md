# Spec Delta

## Purpose

Defines how MetricReport events from the SSE stream are normalized and published as Prometheus gauge metrics, including label schema, deduplication of batched historical samples, sentinel value filtering, and stale metric cleanup.

## ADDED Requirements

### Requirement: MetricReport events published as Prometheus gauges

Each `MetricValue` entry in a received MetricReport event is published as a Prometheus gauge named `redfish_telemetry_reading`.

#### Scenario: MetricReport event received
- **WHEN** the SSE consumer receives a `#MetricReport.*` event with non-empty `MetricValues`
- **THEN** each `MetricValue` with a parseable numeric value is published to the gauge with labels `hostname`, `metric_id`, `type`, `unit`, `origin_context`

#### Scenario: MetricReport event has empty MetricValues
- **WHEN** a MetricReport event arrives with `MetricValues@odata.count: 0` or an empty `MetricValues` array
- **THEN** no gauge updates are made and no error is logged

---

### Requirement: Prometheus label normalization

`metric_id` is normalized from the MetricReport payload to a consistent short identifier.

#### Scenario: MetricId field is present
- **WHEN** a `MetricValue` entry contains a non-empty `MetricId` field (e.g. `AverageCPUUtilization`)
- **THEN** `metric_id` label uses `MetricId` verbatim

#### Scenario: MetricId absent, MetricProperty present
- **WHEN** a `MetricValue` entry has no `MetricId` but has a `MetricProperty` URL (e.g. `/redfish/v1/Chassis/1/Thermal#/Temperature/0/ReadingCelsius`)
- **THEN** `metric_id` label is set to the fragment after `#/` in the URL (e.g. `Temperature/0/ReadingCelsius`)

#### Scenario: Neither MetricId nor MetricProperty present
- **WHEN** a `MetricValue` entry has neither `MetricId` nor `MetricProperty`
- **THEN** the entry is skipped and not published to Prometheus

---

### Requirement: Batched historical samples are deduplicated

MetricReport events may contain many timestamped samples for the same metric key. Only the latest sample per key is used for the gauge update.

#### Scenario: Multiple timestamps for same metric key in one event
- **WHEN** a MetricReport event contains multiple `MetricValue` entries for the same `(hostname, metric_id, MetricProperty)` key with different timestamps
- **THEN** only the entry with the most recent `Timestamp` is published; earlier samples are discarded

---

### Requirement: Sentinel values are filtered

Vendor-specific sentinel values indicating "not available" are not published to Prometheus.

#### Scenario: Lenovo sentinel value 255
- **WHEN** a `MetricValue.MetricValue` field parses to `255` (Lenovo XCC3 "not available" sentinel)
- **THEN** the entry is not published to Prometheus

---

### Requirement: Stale metrics removed on BMC deletion

When a BMC is removed, all Prometheus gauge series for that BMC are deleted to prevent stale metrics from persisting indefinitely.

#### Scenario: BMC resource deleted
- **WHEN** a `BMC` resource is deleted and the operator processes the deletion
- **THEN** all gauge series with `hostname` label matching that BMC name are removed from the Prometheus registry

---

### Requirement: Metric series are thread-safe

The metric sink is accessed from multiple concurrent SSE goroutines.

#### Scenario: Concurrent MetricReport events from multiple BMCs
- **WHEN** SSE goroutines for multiple BMCs publish metric samples simultaneously
- **THEN** gauge updates are serialized correctly with no data races or panics
