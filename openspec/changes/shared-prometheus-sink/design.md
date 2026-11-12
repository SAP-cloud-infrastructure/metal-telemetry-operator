# Design

## Context

See proposal.md — Why for the problem statement.

Two components independently call `prometheusSink.New(ctrlmetrics.Registry)`:

- `BMCReconciler.SetupWithManager` ([bmc_reconciler.go:55](internal/controller/bmc_reconciler.go#L55)) — creates the sink for SSE goroutines.
- `push.AddTo` ([push.go:43](internal/push/push.go#L43)) — creates the sink for the push HTTP receiver.

The `MetricReportSink` (`internal/sink/prometheus/metricsink.go`) holds a `*prometheus.GaugeVec` and a `series map[bmcName → set[seriesKey]]`. The `series` map is used by `Forget(bmcName)` to delete stale gauge series. With two separate instances, each tracks only the series it wrote; a mode transition (MetricReport → SSE or reverse) leaves the other instance's series in place permanently.

## Goals / Non-Goals

**Goals:**
- Single Prometheus gauge registration regardless of which collection paths are enabled.
- Single `series` map so `Forget` cleans up all series for a BMC regardless of which path wrote them.
- Backward-compatible: no change in externally observable metric behavior.

**Non-Goals:**
- Changing the metric name, label set, or gauge semantics.
- Making the sink injectable via Kubernetes config or flags.
- Changing the push receiver or SSE connection logic.

## Decisions

### Decision 1: Create sink in `main.go`, inject into both components

The sink is created once in `main.go` before either component is constructed, then passed as a field:

```
main()
  sink := prometheusSink.New(ctrlmetrics.Registry)

  BMCReconciler{Sink: sink}.SetupWithManager(mgr)
  push.AddTo(mgr, &push.Options{Sink: sink, ...})
```

**Why:** Injection at the composition root is the standard Go pattern for shared dependencies. It requires minimal changes, keeps both components independently testable (inject a mock sink in tests), and makes the shared ownership explicit.

**Alternative: `AlreadyRegisteredError` recovery in `prometheusSink.New`**

On the second `reg.Register` call, Prometheus returns a `prometheus.AlreadyRegisteredError` carrying the existing collector. `New` could recover the existing `*GaugeVec` and return a new `MetricReportSink` wrapping it. This silences the crash but does NOT fix the `series` map split: two instances still track independent series sets, so `Forget` on one leaves the other's series in place. Rejected.

**Alternative: Package-level singleton in the `prometheus` sink package**

A `var defaultSink *MetricReportSink` initialized with `sync.Once`. Simpler callsite, but hidden global state makes tests harder to isolate and couples any future multi-registry use. Rejected.

---

### Decision 2: Nil-check guards in `SetupWithManager` and `AddTo` (backward compat)

When `BMCReconciler.Sink` is nil (e.g. in existing tests that construct the struct directly), `SetupWithManager` falls back to `prometheusSink.New` as today. Same pattern in `push.AddTo`.

**Why:** No existing test or integration harness needs to change. The only caller that sets both paths at the same time is `main.go`, which will now inject the shared instance.

---

### Decision 3: No interface widening

The `sink.MetricReportSink` interface (`PublishSamples`, `Forget`) is unchanged. `BMCReconciler` and `push.Options` both declare the field as `sink.MetricReportSink`, so tests continue to inject mocks freely.

## Risks / Trade-offs

**Single instance, two concurrent writers** → The existing `MetricReportSink` already uses a `sync.Mutex` around both `PublishSamples` and `Forget`, so concurrent SSE goroutines and the push receiver serialize correctly on the shared instance. No new concurrency risk.

**`Forget` called from both paths on the same BMC** → This is now correct behavior (whichever path calls it first wins; the other path would call it with an already-empty entry, which is a no-op).

## Migration Plan

Deploy is a drop-in replacement. No CRD or Helm values changes are required. Rollback by reverting the three file edits.
