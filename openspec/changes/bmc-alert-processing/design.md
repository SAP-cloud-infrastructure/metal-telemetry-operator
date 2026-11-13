# Design

## Context

See [proposal.md](proposal.md) for motivation.

MTO receives Redfish alert POSTs at its push receiver (`internal/push/receiver.go`) and SSE alert frames in `internal/sse/conn.go`. Both currently discard alert content. The MMO codebase has a complete reference implementation: `internal/telemetry/events/` (receiver), `internal/telemetry/sink/prometheus/eventsink.go` (counter + dedup), `internal/telemetry/criticalevent/` (Server conditions), and `internal/telemetry/warningevent/` (K8s Events). This design ports that pipeline into MTO with one key difference: the test-event heartbeat must be shared across both transport modes via a common interface.

Key existing MTO structures:
- `internal/sink/sink.go` — defines `MetricReportSink` and `Sample`; `EventSink` and `Event` go here
- `internal/push/receiver.go` — `StatusWriter.RecordAlertEvent()` is the only current alert hook
- `internal/sse/conn.go` — `sseConn` has `sink.MetricReportSink` and `StatusWriter`; needs `EventSink` + `ConsumeIfTest` for alerts stream
- `internal/controller/bmc_reconciler.go` — wires everything; no field indexers today

## Goals / Non-Goals

**Goals:**
- Full alert event pipeline in push mode (Phase 1): decode → dedup → counter → severity routing → Server conditions + K8s Events
- Test-event heartbeat that works across both push and SSE transports without updating `LastEventTime`
- Gauge prewarm on startup so `redfish_server_critical_alert` is correct immediately after pod restart
- SSE alert stream routing in Phase 2 (same pipeline, different transport entry point)

**Non-Goals:**
- Removing MMO's alert pipeline (tracked separately)
- SSE alert wire format spike (a task in this change, but the Phase 2 design waits for those findings)
- Changes to the CRD API or `BMCTelemetry` schema

## Decisions

### D1: Port MMO's event pipeline packages directly rather than sharing code

**Decision**: Copy and adapt `wireEvent`, `eventEnvelope`, `EventSink`, `criticalevent`, and `warningevent` into MTO rather than extracting a shared library.

**Rationale**: MTO and MMO will diverge (MMO's pipeline is eventually removed). A shared library creates a coupling that makes that cleanup harder. The code is small and well-understood; copying it avoids a new dependency and lets each operator evolve independently.

**Alternative considered**: Extract a `redfish-alert-pipeline` shared module. Rejected — premature abstraction with a short intended lifespan.

### D2: `ConsumeIfTest` as a shared interface, not a push-specific concern

**Decision**: Define a `ConsumeIfTest(bmcName, messageId string) bool` interface in `internal/push/receiver.go` (alongside the existing `StatusWriter`) and thread it into both the push receiver and the SSE `ConnConfig`.

**Rationale**: The test-event filtering requirement is identical for both transports. A shared interface keeps the logic in the reconciler (which owns the in-flight test event state) and avoids duplicating filter logic in two places. The SSE conn goroutine calls `ConsumeIfTest` before routing a decoded event, same as the push receiver.

**Alternative considered**: Filter test events purely in the reconciler by comparing `LastEventTime` before/after. Rejected — introduces timing races and makes the push receiver's behavior depend on reconciler polling cadence.

### D3: Deferred-commit dedup (same as MMO)

**Decision**: For Critical and Warning events, dispatch to the handler first; only add the EventID to the per-BMC dedup set and increment `redfish_event_alert_total` if the handler returns without error.

**Rationale**: If the handler fails (e.g., API server transient error while patching Server condition) and the caller retries, the event must be processed again. Adding to the dedup set before the handler would silently suppress the retry. This matches MMO's pattern exactly.

**Trade-off**: A handler succeeding but returning a transient error on the counter increment means the event is counted twice on retry. This is accepted as rare and harmless (overcounting vs undercounting).

### D4: Bounded FIFO dedup set (4096 per BMC)

**Decision**: Keep at most 4096 seen EventIDs per BMC in a FIFO ring, discarding the oldest when the limit is reached.

**Rationale**: Prevents unbounded memory growth on long-running pods. 4096 entries at ~64 bytes each is ~256 KB per BMC; at 1000 BMCs that's 256 MB worst-case, acceptable. Events older than the ring window are allowed to re-trigger handlers — acceptable since they indicate a genuinely new fault wave.

### D5: Server field indexer registered in main setup

**Decision**: Register the `spec.bmcRef.name` field index on the Server informer in `SetupWithManager`, using the same `constants.BMCRefField` key that MMO uses.

**Rationale**: Without the index, finding all Servers for a given BMC requires listing all Servers and filtering in-process — O(n) at every alert, and expensive at scale. The index makes it O(k) where k is the number of Servers per BMC.

**Note**: MTO currently has no field indexers. This is the first.

### D6: SSE Phase 2 design is deferred until after the QA BMC spike

**Decision**: The spike task (probe Dell, HPE, Lenovo for SSE alert stream wire format) runs after Phase 1. Phase 2 SSE implementation tasks are placeholders in tasks.md and will be refined with actual findings.

**Rationale**: The alert SSE stream format (which `@odata.type` values appear, which vendors use `Alerts` vs `Events`, any SSE-specific quirks) is unknown. Designing Phase 2 tasks before the spike risks building the wrong decoder.

## Risks / Trade-offs

- **Handler fan-out on large fleets**: `HandleCritical` lists all Servers filtered by BMC name and patches each one. With many Servers per BMC, this is multiple API server writes per alert. Mitigated by the field index (D5) keeping the list short, and by the dedup set preventing repeated patches for the same fault.
- **RBAC regeneration**: Adding `servers`, `servers/status`, and `core/v1 events` markers requires `make generate` to regenerate the ClusterRole. If this step is missed, the operator will log permission errors silently (alert processing skipped, no startup crash). Mitigation: task list includes the generate step.
- **SSE Phase 2 coupling**: `startConn` in `internal/sse/conn.go` takes a `MetricReportSink`; adding `EventSink` and `ConsumeIfTest` changes its signature. `ConnectionManager.Ensure` must be updated too. This is a localized change but touches the SSE package interface.
- **Test event MessageId collision**: If a real hardware event coincidentally has the same MessageId as an in-flight test event, it is silently consumed as a health check. Probability is very low (MessageIds are registry-prefixed strings like `iLO.2.0.PowerThresholdExceeded`); test events use a deliberately distinct MessageId from the registry discovery step. Accepted.

## Migration Plan

No CRD API changes. No configuration changes required.

On upgrade:
1. Operator starts, prewarm fires — `redfish_server_critical_alert` set correctly from existing Server conditions.
2. Push receiver immediately begins full alert processing for incoming events.
3. SSE alert connections already exist (from dual-stream change); they continue forwarding `#Event.*` frames but Phase 2 routing is not wired until Phase 2 is released.

Rollback: revert the binary; no state migration needed. Server conditions set by the new pipeline persist (they are Kubernetes resources) but are benign if the handler is absent — they become stale until cleared by a future alert.

## Open Questions

- **SSE alert wire format per vendor**: Which `@odata.type` value does each vendor use? Do HPE and Lenovo use `Alerts` or `Events` as the top-level key? Does Dell send anything at all on the SSE alerts stream? — answered by the Phase 1 spike task before Phase 2 design is finalized.
