# metal-telemetry-operator

A Kubernetes operator that streams hardware telemetry and alert events from Baseboard Management Controllers (BMCs) via the Redfish API and publishes them as Prometheus metrics.

## Description

The Metal Telemetry Operator (MTO) watches `BMC` resources and maintains persistent Redfish connections to each BMC, collecting two classes of data:

**Telemetry** — continuous MetricReport streams delivered either via Server-Sent Events (SSE) for Dell iDRAC and Lenovo XCC3, or via HTTP push webhooks for HPE iLO. Each metric reading is published as a Prometheus gauge labelled with the BMC hostname, metric ID, unit, and origin context.

**Alerts** — Redfish event notifications (hardware faults, component state changes) delivered over the same SSE/push streams. Alert events are counted per severity, deduplicated, and critical events are surfaced as a `CriticalEventReceived=True` condition on the corresponding `Server` resource, triggering a Kubernetes Warning Event.

Metrics are exposed via a Prometheus-compatible `/metrics` endpoint on port `8443` and are intended to be scraped by Prometheus and queried in Thanos for cross-region fleet observability.

## Metrics

### `redfish_telemetry_reading` — Gauge

Latest value from a Redfish MetricReport, updated on every event received from the BMC.

| Label | Description |
|-------|-------------|
| `hostname` | BMC resource name in Kubernetes |
| `metric_id` | Redfish `MetricId` field, or the fragment after `#/` in the `MetricProperty` URL |
| `type` | MetricValue type field |
| `unit` | Unit of measurement (e.g. `Cel`, `W`, `%`) |
| `origin_context` | Full `MetricProperty` URI |

Notes:
- Only the latest sample per metric key is published when a batched report contains multiple timestamps.
- Lenovo XCC3 sentinel value `255` ("not available") is filtered and never published.
- All series for a BMC are removed when its `BMC` resource is deleted.

**Example queries:**

```promql
# Per-node CPU utilization
redfish_telemetry_reading{metric_id="AverageCPUUtilization"}

# Inlet temperature across the fleet
redfish_telemetry_reading{metric_id=~".*Temperature.*", unit="Cel"}

# Total power draw by region
sum by (region) (redfish_telemetry_reading{metric_id=~".*Power.*", unit="W"})
```

---

### `redfish_event_alert_total` — Counter

Total Redfish alert events received per BMC, counted once per unique event (deduplicated by EventID with a 4096-entry per-BMC FIFO window). Only `Critical` and `Warning` severity events increment the counter; `OK` and `Info` events trigger clears only.

| Label | Description |
|-------|-------------|
| `hostname` | BMC resource name in Kubernetes |
| `severity` | Canonical Redfish severity: `Critical`, `Warning`, `OK`, or `Info` |
| `message_id` | Redfish `MessageId` field |
| `component` | Trailing path segment of `OriginOfCondition` URI; `system` if absent |

**Example queries:**

```promql
# Rate of critical hardware events across the fleet
rate(redfish_event_alert_total{severity="Critical"}[5m])

# Most frequent fault message IDs in the last 24h
topk(10, increase(redfish_event_alert_total{severity="Critical"}[24h])) by (message_id)

# Per-component alert distribution
sum by (component, severity) (redfish_event_alert_total)
```

---

### `redfish_server_critical_alert` — Gauge

`1` if a Server currently has an active `CriticalEventReceived=True` condition; `0` when the condition has been cleared by an OK/Info event. Pre-warmed from existing Server conditions on operator startup, so the gauge is accurate immediately after a restart.

| Label | Description |
|-------|-------------|
| `hostname` | BMC resource name in Kubernetes |

**Example queries:**

```promql
# Servers currently in a critical hardware fault state
redfish_server_critical_alert == 1

# Alert: any server has been in critical state for more than 10 minutes
(redfish_server_critical_alert == 1) and (changes(redfish_server_critical_alert[10m]) == 0)
```

---

### Standard controller-runtime metrics

The operator also exposes the standard `controller_runtime_*` and `workqueue_*` metrics provided by the controller-runtime framework, including reconcile counts, error rates, and queue depth per controller.

## Getting Started

### Prerequisites
- go version v1.24.6+
- docker version 17.03+
- kubectl version v1.11.3+
- Access to a Kubernetes v1.11.3+ cluster

### To Deploy on the cluster

**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/metal-telemetry-operator:tag
```

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/metal-telemetry-operator:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**

```sh
kubectl apply -k config/samples/
```

### To Uninstall

**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**Undeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/metal-telemetry-operator:tag
```

2. Apply the generated `dist/install.yaml`:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/metal-telemetry-operator/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

The chart is generated under `dist/chart`. To regenerate after project changes:

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

> **NOTE**: If you create webhooks, use the `--force` flag and manually re-apply any custom configuration to `dist/chart/values.yaml` or `dist/chart/manager/manager.yaml` afterwards.

## Contributing

**NOTE:** Run `make help` for more information on all potential `make` targets.

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html).

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
