# Spike 8.1 — SSE Alert Stream Wire Format

Probed three vendor BMCs in `m-qa-de-100` on 2026-11-13. HPE iLO has no SSE capability. Dell and Lenovo both have it.

## Summary Table

| Vendor | Node | IP | EventService type | SSE URI | Filter | HTTP 200? |
|---|---|---|---|---|---|---|
| Dell iDRAC10 | node003-bb098 | 10.245.252.132 | `v1_11_0` | `/redfish/v1/SSE` | Quoted | 200 |
| HPE iLO 7 | node001-bb098 | 10.245.252.130 | `v1_8_0` | **none** | — | — |
| Lenovo XCC3 | node005-bb098 | 10.245.252.134 | `v1_11_0` | `/redfish/v1/EventService/ServerSentEvent` | Quoted | 204 |
| HPE iLO 6 | node009-bb097 | 10.246.86.12 | `v1_7_2` | **none** | — | — |

HPE (both iLO 6 and iLO 7): `ServerSentEventUri` is empty — push mode only, no SSE.

## SSE Alert URL

Both SSE-capable vendors use the quoted filter syntax (driven by `v1_11_0` type in `detectVendor`):

```
Dell:   https://<ip>/redfish/v1/SSE?$filter=EventFormatType%20eq%20%27Event%27
Lenovo: https://<ip>/redfish/v1/EventService/ServerSentEvent?$filter=EventFormatType%20eq%20%27Event%27
```

## Wire Format

### Dell iDRAC10 — regular event

Single-line `data:` per frame. Regular polling events come at high frequency (~1 Hz) and batch many `ResourceChanged` events.

```
id: 121724
data: {"@odata.context":"...","@odata.type":"#Event.v1_8_0.Event","Name":"Redfish Event","Events":[{"EventId":"812773","EventTimestamp":"2026-11-13T11:22:42+00:00","EventType":"Other","MemberId":"0","MessageId":"ResourceEvent.ResourceChanged","OriginOfCondition":{"@odata.id":"/redfish/v1/Chassis/System.Embedded.1/Sensors/Fan1_1"}},...],"Events@odata.count":18,"Oem":{"Dell":{"@odata.type":"#DellEvent.v1_1_0.DellEvent","ServerHostname":"MINWINPC"}}}
```

Key fields: `@odata.type: "#Event.v1_8_0.Event"`, top-level key `Events[]`, OEM `Oem.Dell.ServerHostname`.

### Dell iDRAC10 — test event (`SubmitTestEvent`)

```
id: 121726
data: {"@odata.context":"/redfish/v1/$metadata#Event.Event","@odata.type":"#Event.v1_3_0.Event","Name":"Event Array","Events":[{"EventType":"Alert","EventId":"","EventTimestamp":"","MessageSeverity":"","Message":"","MessageId":"Base.1.0.TestMessage","MemberId":"0","MessageArgs":[],"OriginOfCondition":null,"Oem":{"Dell":{"ServerHostname":"MINWINPC"}}}],"Id":"121726","Context":"SSE-Subscription-26"}
```

Test event markers on Dell:
- `@odata.type` drops to `#Event.v1_3_0.Event`
- `EventType: "Alert"` (regular events use `"Other"`)
- `Context: "SSE-Subscription-26"` (subscription context echoed back)
- `EventTimestamp: ""` (empty string, not omitted)
- `MessageSeverity: ""` (empty string)
- `OriginOfCondition: null`

SubmitTestEvent returns HTTP 200.

### Lenovo XCC3 — regular event

**Multi-line `data:` fields**: Lenovo pretty-prints the JSON, one line per `data:` field. The SSE spec says consecutive `data:` lines are concatenated with `\n`; our parser strips them and concatenates without separator, producing valid JSON.

Stream opens with a keep-alive comment:
```
: stream keep-alive
```

Regular event:
```
id: 84
data: {
data:   "@odata.context": "/redfish/v1/$metadata#Event.Event",
data:   "@odata.type": "#Event.v1_7_0.Event",
data:   "Events": [
data:     {
data:       "EventId": "392612",
data:       "EventTimestamp": "2026-11-13T11:24:19+00:00",
data:       "MessageId": "EventRegistry.1.0.FQXSPSE4032I",
data:       "MessageSeverity": "OK",
data:       "OriginOfCondition": {"@odata.id": "/redfish/v1/Systems/1"},
data:       "Oem": {
data:         "Lenovo": {
data:           "@odata.type": "#LenovoEvent.v1_0_0.LenovoEventRecord",
data:           "EventInformation": {
data:             "OriginOfCondition": "/redfish/v1/Managers/1",
data:             ...
data:           }
data:         }
data:       }
data:     }
data:   ]
data: }
```

Key fields: `@odata.type: "#Event.v1_7_0.Event"`, top-level key `Events[]`. The standard `OriginOfCondition` is a proper `{"@odata.id": "..."}` object. The plain-string `OriginOfCondition` from the Dell quirk does NOT appear in the standard field — it appears only inside `Oem.Lenovo.EventInformation.OriginOfCondition`.

### Lenovo XCC3 — test event (`SubmitTestEvent`)

```
id: 85
data: {
data:   "@odata.type": "#Event.v1_4_0.Event",
data:   "Events": [
data:     {
data:       "EventId": "0",
data:       "EventTimestamp": "2026-11-13T11:24:21+00:00",
data:       "Message": "Generated test event",
data:       "MessageArgs": [],
data:       "MessageId": "Base.1.0.TestMessage",
data:       "MessageSeverity": "OK",
data:       "OriginOfCondition": {"@odata.id": "/redfish/v1/EventService/"}
data:     }
data:   ],
data:   "Id": "0",
data:   "Name": "SubmitTestEvent"
data: }
```

Test event markers on Lenovo:
- `Name: "SubmitTestEvent"`
- `EventId: "0"` (special sentinel value)
- `MessageId: "Base.1.0.TestMessage"`
- `MessageSeverity: "OK"` (not empty like Dell)
- `OriginOfCondition: {"@odata.id": "/redfish/v1/EventService/"}` (not null)

SubmitTestEvent returns HTTP 204.

## Implications for Implementation

### Wire decoder (`wire.go`) — no changes needed

Both vendors use `Events[]` as the top-level key. The `Alerts[]` variant in the decoder is forward-compat only. No vendor-specific `@odata.type` checks are needed for the alert array key.

### SSE parser (`conn.go`) — already correct

The existing parser strips and concatenates `data:` lines without a separator. For pretty-printed JSON (Lenovo), this correctly reconstructs the full JSON object because JSON ignores whitespace between tokens.

### Test event identification — use `MessageId`

The most robust cross-vendor test event identifier is `MessageId == "Base.1.0.TestMessage"`. This is exactly what `ConsumeIfTest` in the push reconciler tracks. Dell and Lenovo both use this MessageId for `SubmitTestEvent` responses.

Do NOT use `EventType: "Alert"` or `Name: "SubmitTestEvent"` — those are vendor-specific. Do NOT rely on `OriginOfCondition` being null or empty — Lenovo populates it.

### SSE Phase 2 tasks (9.1–9.4) — no spec changes needed

The wire format matches what was assumed in the Phase 2 tasks. Proceed as specified.
