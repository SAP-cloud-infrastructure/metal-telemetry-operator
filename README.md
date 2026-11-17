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

# All BMCs currently reporting at least one reading
count by (hostname) (redfish_telemetry_reading)

# All distinct metric_id values being reported across the fleet
count by (metric_id) (redfish_telemetry_reading)

# Which metric_ids a specific BMC reports
count by (metric_id) (redfish_telemetry_reading{hostname="<bmc-name>"})

# Metric coverage by hardware manufacturer and model (requires kube-state-metrics integration)
# Joins redfish_telemetry_reading with kube_ironcore_bmc_info to attach manufacturer/model labels
count by (manufacturer, model, metric_id) (
  redfish_telemetry_reading
  * on(hostname) group_left(manufacturer, model)
  label_replace(kube_ironcore_bmc_info, "hostname", "$1", "name", "(.*)")
)
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

### `redfish_event_pipeline_test_timestamp` — Gauge

Unix timestamp (seconds) of the last successful end-to-end test event round-trip for a BMC. The operator periodically calls the Redfish `SubmitTestEvent` action on each BMC and waits for the resulting event to arrive back through the same pipeline. A successful round-trip updates this gauge; a timeout records a `redfish_event_pipeline_test_result_total{result="timeout"}` increment instead.

The gauge is absent for a BMC until its first successful round-trip, so `absent()` can detect BMCs that have never completed one.

| Label | Description |
|-------|-------------|
| `hostname` | BMC resource name in Kubernetes |

**Example queries:**

```promql
# BMCs whose last successful test event was more than 30 minutes ago
(time() - redfish_event_pipeline_test_timestamp) > 1800

# BMCs with no successful test event in Prometheus memory (never received one)
absent(redfish_event_pipeline_test_timestamp)

# Alert: pipeline health degraded for more than 20 minutes
(time() - redfish_event_pipeline_test_timestamp) > 1200
```

---

### Standard controller-runtime metrics

The operator also exposes the standard `controller_runtime_*` and `workqueue_*` metrics provided by the controller-runtime framework, including reconcile counts, error rates, and queue depth per controller.

## BMC Configuration

The operator loads its runtime tuning and per-vendor test event parameters from a Kubernetes ConfigMap, identified by the `--bmc-config-name` and `--bmc-config-namespace` flags. The ConfigMap is **live-reloaded** — changes take effect on the next reconcile cycle without a pod restart.

The ConfigMap must contain a single key `config.yaml` with YAML content conforming to `apiVersion: telemetry.metal.ironcore.dev/v1alpha1`.

### Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `apiVersion` | string | — | Must be `telemetry.metal.ironcore.dev/v1alpha1` when set |
| `reconcileInterval` | duration | `10m` | How often the push reconciler re-verifies each BMC's subscription |
| `testEventInterval` | duration | `reconcileInterval` | How often to fire a SubmitTestEvent heartbeat per BMC |
| `testEventTimeout` | duration | `30s` | How long to wait for a test event round-trip before recording a timeout |
| `testEventOverrides` | list | — | Per-vendor SubmitTestEvent parameters; first matching rule wins |
| `modeOverrides` | list | — | Force a specific `TelemetryMode` (`SSE` or `Push`) for matching vendor/model combinations |
| `excludeHardware` | list | — | Opt specific vendor/model combinations out of all telemetry collection |

### `testEventOverrides`

Each rule specifies the Redfish `SubmitTestEvent` body for a vendor+model combination. The first rule whose `vendor` and one of its `models` match the BMC is used. Use `"*"` in `models` to match all models for a vendor. When no rule matches, the defaults `Base.1.0.TestMessage` / `OK` / `/redfish/v1/Managers/1` are used.

| Field | Required | Description |
|-------|----------|-------------|
| `vendor` | yes | Must match `bmc.status.manufacturer` (e.g. `HPE`, `Dell`, `Lenovo`, `BULL`) |
| `models` | yes | List of `bmc.status.model` values to match; `["*"]` matches all models |
| `messageId` | yes | Redfish `MessageId` to send in SubmitTestEvent |
| `receiveMessageId` | no | `MessageId` expected back in the stream. Set when the vendor delivers a different form than what was sent (e.g. Dell iDRAC9 accepts `LOG006` but delivers `IDRAC.2.9.LOG006`). When empty, `messageId` is used for matching. |
| `severity` | no | `Severity` field; defaults to `OK` |
| `originOfCondition` | no | `OriginOfCondition` field; defaults to `/redfish/v1/Managers/1` |

### `modeOverrides`

Overrides auto-detected telemetry mode for matching BMCs.

| Field | Required | Description |
|-------|----------|-------------|
| `vendor` | yes | Must match `bmc.status.manufacturer` |
| `models` | yes | List of `bmc.status.model` values; `["*"]` matches all |
| `mode` | yes | `SSE` or `Push` |

### `excludeHardware`

Opts vendor/model combinations out of all telemetry collection entirely.

| Field | Required | Description |
|-------|----------|-------------|
| `vendor` | yes | Must match `bmc.status.manufacturer` |
| `models` | yes | List of `bmc.status.model` values; `["*"]` excludes all models for this vendor |
| `minFirmware` | no | When set, only firmware versions strictly below this semver are excluded |

### Example ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: metal-telemetry-operator-bmc-config
  namespace: kube-system
data:
  config.yaml: |
    apiVersion: telemetry.metal.ironcore.dev/v1alpha1
    reconcileInterval: "10m"
    testEventInterval: "5m"
    testEventTimeout: "30s"

    testEventOverrides:
      - vendor: "HPE"
        models:
          - "iLO 6"
          - "iLO 7"
        messageId: "iLOEvents.1.0.TestAlert"
        severity: "OK"
        originOfCondition: "/redfish/v1/Managers/1"
      - vendor: "Dell"
        models:
          - "16G Monolithic"
        messageId: "LOG006"
        receiveMessageId: "IDRAC.2.9.LOG006"
        severity: "Informational"
      - vendor: "Dell"
        models:
          - "17G Monolithic"
        messageId: "Base.1.0.TestMessage"
        severity: "Informational"
      - vendor: "Lenovo"
        models:
          - "Lenovo XClarity Controller"
        messageId: "EventRegistry.1.0.0.FQXSPCA0012I"
      - vendor: "Lenovo"
        models:
          - "Lenovo XClarity Controller 3"
        messageId: "Base.1.0.TestMessage"
      - vendor: "BULL"
        models:
          - "OpenBmc"
        messageId: "OpenBMC.0.1.BMCBooted"

    modeOverrides:
      - vendor: "HPE"
        models: ["*"]
        mode: Push

    excludeHardware:
      - vendor: "Supermicro"
        models: ["*"]
        minFirmware: "3.0.0"
```

## Getting Started
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
