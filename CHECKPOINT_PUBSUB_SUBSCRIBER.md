# Handoff: build the checkpoint Pub/Sub subscriber in weavex-cerebro

**Audience:** whoever implements the subscriber side in the Kotlin repo (`knitapi/weavex`).
**Status of the other half:** the publisher is written and merged-ready in `weavex-core`.
Nothing consumes the topic yet, so **checkpointing is currently non-functional end to end.**
See "Why this is urgent" before scheduling.

---

## 1. What happened and why

The `weavex_core.checkpoint` package used to call four HTTP routes on weavex-cerebro. All four
are now gone from the client:

| Operation | Before | After |
|---|---|---|
| `init` | `POST /checkpoint.init` | reads/writes Firestore directly via `weavex_core/dao/` |
| `is_complete` | `POST /checkpoint.get` | reads Firestore directly via `weavex_core/dao/` |
| `success` | `POST /checkpoint.set` | **publishes `checkpoint.set` to Pub/Sub** |
| `fail` | `POST /checkpoint.set` | **publishes `checkpoint.set` to Pub/Sub** |
| `clear` | `POST /checkpoint.clear` | **publishes `checkpoint.clear` to Pub/Sub** |

The reads moved cleanly because they are pure Firestore lookups. The writes could not: both
write routes do server-side work that has no business living in a client library — they
mutate the project document and start a Temporal workflow. So the writes became **events**,
and **your job is to build the consumer that performs that server-side work.**

The motivation was reliability. `weavex_api_service.py` carries a long comment documenting
requests that weavex-cerebro never logged receiving at all, against a client that cleanly hit
its read timeout — transient Cloud Run to Cloud Run network flakiness. Every checkpoint write
sat on that path, inside a Temporal activity, and a failed write records a healthy step as
failed. Pub/Sub gives us retries and durability for free.

---

## 2. Your job, in one sentence

Add a Pub/Sub subscriber to weavex-cerebro that consumes topic `weavex-checkpoints-eu` and
performs, for each message, exactly what the body of `POST /checkpoint.set` or
`POST /checkpoint.clear` does today in
`src/main/kotlin/weavex/dev/server/routes/CheckpointRoutes.kt`.

The route handlers stay in place during the transition (nothing calls them anymore, but
leaving them lets you diff behaviour). Delete them once you have verified parity.

---

## 3. The wire contract

### Topic and delivery

- **Topic:** `weavex-checkpoints-eu` in GCP project `weavex-475116`.
  (Publisher default is `weavex-checkpoints`, with `-eu` appended by the standard
  `WEAVEX_SERVICE_REGION` suffix rule used by `state.py` / `storage.py`.)
- **Ordering key:** `{projectId}:{executionId}` — the same composite id as the Firestore
  checkpoint document, so the two views line up when you debug.
  **The subscription must have `enable_message_ordering` set,** or the key is meaningless on
  delivery and you will process a `clear` before the `success` that preceded it.
- **Delivery is at-least-once.** Deduplicate on `eventId`. See §6.

### Message attributes

Strings only. Usable in subscription filters and readable in a console pull without decoding
the body.

| Attribute | `checkpoint.set` | `checkpoint.clear` |
|---|---|---|
| `eventType` | `"checkpoint.set"` | `"checkpoint.clear"` |
| `schemaVersion` | `"1"` | `"1"` |
| `eventId` | uuid4 | uuid4 |
| `projectId` | yes | yes |
| `executionId` | yes | yes |
| `stepId` | yes | **absent** |

### Body — `checkpoint.set` on success

```json
{
  "eventId": "0f0f2f4c-6b2a-4a1e-9c33-2f5b1d0e7a91",
  "eventType": "checkpoint.set",
  "version": 1,
  "publishedAt": "2026-07-28T09:14:02.481293+00:00",
  "source": "weavex-core",
  "projectId": "prj_abc",
  "executionId": "ex_9f21",
  "stepId": "bamboohr_fetch_employees",
  "checkpoint": {
    "step_id": "bamboohr_fetch_employees",
    "status": "success",
    "error": null
  },
  "stepContext": {
    "bamboohr_fetch_employees": { "processed_count": 42 }
  }
}
```

### Body — `checkpoint.set` on failure

Identical, except `"status": "failed"`, `"error"` is the `WeavexError.to_dict()` object, and
**there is no `stepContext` key at all** — matching `SetCheckpointRequest.stepContext:
JsonElement? = null` and the old HTTP payload, which omitted it on failure.

### Body — `checkpoint.clear`

Envelope only: `eventId`, `eventType`, `version`, `publishedAt`, `source`, `projectId`,
`executionId`. No `stepId`, no `checkpoint`.

### Three things about this schema that are deliberate — do not "clean them up"

1. **`checkpoint` and `stepContext` are nested JSON objects, not pre-stringified strings.**
   The route does `request.checkpoint.toString()` and stores *that*
   (`CheckpointRoutes.kt:118`). Keeping them nested means kotlinx stays the single serializer
   producing the persisted string, so a Pub/Sub write lands the same bytes the HTTP route
   does today — and the same bytes `is_complete` parses back out. Pre-stringifying on the
   Python side would couple the stored value to `json.dumps` matching kotlinx's formatting.

2. **The body decodes straight into the existing DTOs.** The shared `json` object in
   `weavex/dev/commons/Utils.kt:134` already has `ignoreUnknownKeys = true`, so
   `json.decodeFromString<SetCheckpointRequest>(body)` works as-is — the envelope fields are
   ignored extras. **Your subscriber is literally `decode → existing route body`.** Decode
   the envelope separately (a small `@Serializable EventEnvelope`) for `eventId` and
   `eventType`.

3. **Casing is mixed on purpose.** camelCase envelope (the Kotlin DTO field names),
   snake_case *inside* `checkpoint` (that is `StepCheckpoint.to_dict()` on the Python side,
   which is what `is_complete` and `markCheckpointFixing` read back out of Firestore).

---

## 4. What each handler must do

Port these verbatim. Line numbers are current as of this writing.

### `checkpoint.set` — from `CheckpointRoutes.kt:107-174`

1. `getProject(null, projectId)`. If null → **the route 404s; you must ack and log**. There
   is no caller to return a status to. A missing project is not retryable.
2. If `project.status != ProjectStatus.TESTING` → no-op, ack. **Re-check this even though the
   publisher already gates on it** — see §6.
3. `saveCheckpoint(projectId, executionId, stepId, checkpoint.toString(), stepContext?.toString())`.
4. Derive `entryStepId = checkpoint["step_id"] ?: request.stepId` and
   `entryStatus = checkpoint["status"] ?: ""`.
5. Upsert `TestCheckpoint(entryStepId, entryStatus, fixAttempted)` into
   `project.testCheckpoints`, **preserving the existing `fixAttempted` flag** if an entry for
   that `step_id` already exists.
6. Persist the project (but see **R3** below — do *not* use `saveProject` as-is).
7. If `entryStatus == "failed" && !alreadyFixAttempted`, start `TestAndFixFlow.runFix`:
   - task queue `Temporal.FIX_TASK_QUEUE` (`"weavex-fix-queue"`)
   - workflowId `"wvx-fix-${projectId}-${entryStepId}"`
   - execution timeout 1h, workflow task timeout 2min, retry `maximumAttempts(1)`
   - catch `WorkflowExecutionAlreadyStarted` and skip
   - `errorLog` extraction: `checkpoint["error"]` null or `JsonNull` → `"Unknown error"`;
     otherwise `try { jsonPrimitive.content } catch { toString() }`

### `checkpoint.clear` — from `CheckpointRoutes.kt:223-238`

1. `getProject(null, projectId)`. **If null OR not TESTING → no-op, ack.** Note the
   asymmetry with `set`, which treats a missing project as an error: `clear` returns 200.
2. `deleteCheckpoint(projectId, executionId)`.
3. `project.buildTestStatus = "success"`, then persist.

### Unknown `eventType` or `schemaVersion != "1"`

**Log and ack.** Do not nack. With ordering enabled a nacked message blocks its key forever,
so one unrecognised message would wedge that entire execution.

---

## 5. Where to wire it up

`Server.kt:77-106`'s `main()` already does:

```kotlin
fun main() {
    Temporal.createWorker()
    Temporal.createFixWorker()
    // ...
    embeddedServer(Netty, port = 8080, host = "0.0.0.0", module = Application::module)
        .start(wait = true)
}
```

Add a `CheckpointSubscriber.start()` alongside the two worker calls, **before** the blocking
`embeddedServer(...).start(wait = true)`. Follow the `Temporal` object's shape: an object with
a lazily-built client and a `start()`.

**`build.gradle.kts` has no Pub/Sub dependency today** — it has storage, bigquery, build and
run, but not pubsub. Add:

```kotlin
implementation("com.google.cloud:google-cloud-pubsub:1.132.0")   // check for current
```

Use `Subscriber.newBuilder(subscriptionName, receiver).build()` (streaming pull). It respects
ordering when the subscription has it enabled. Note that `AppDBFactory.instance` methods are
`suspend` — you will need a `runBlocking` inside the receiver, as `TestAndFixActivitiesImpl`
already does.

---

## 6. The five things that will bite you

These are ranked. **R1 and R3 are not optional.**

### R1 — `TestAndFixFlow` reads the checkpoint with zero grace period

`TestAndFixFlow.kt:99-106`:

```kotlin
activities.markCheckpointFixing(projectId, executionId, stepId)   // writes status "fixing"
val fixResult = activities.runFixAgent(...)                       // re-runs the step
val checkpointStatus = activities.checkCheckpointStatus(...)      // reads IMMEDIATELY
val verified = checkpointStatus == "success"
```

Today the re-run's `checkpoint.set` is a synchronous HTTP call that completes before
`runFixAgent` returns, so the read sees `"success"`. **With async publishing there is a
publish → broker → subscriber → Firestore round trip that this workflow does not wait for**,
so the read will often still see `"fixing"` → `verified = false` → **a working fix gets
discarded and the user is told it could not be fixed.**

Fix it in the same PR. Either:
- make the read a bounded poll —
  `Workflow.await(Duration.ofSeconds(30)) { activities.checkCheckpointStatus(...) != "fixing" }`
  before evaluating `verified`; or
- have the subscriber signal the workflow when it writes a terminal status for that step.

The poll is the smaller change and is enough.

### R2 — the interim window: nothing works until you ship

With nothing consuming the topic:
- `clear()` never sets `buildTestStatus = "success"`, so `BuildTestFlow`'s
  `checkTestStatus` never returns a terminal value and **every build test runs the full 15
  minutes and ends in `markTestTimedOut`** (`BuildTestFlow.kt:114-117`). Every test appears
  to fail.
- `fail()` never starts `TestAndFixFlow` — **auto-fix is silently off.**
- Resume-from-failure is off; retries re-run every step.

**Create the subscription before the publisher deploys.** Messages published to a topic with
zero subscriptions are dropped and unrecoverable. With the subscription in place and 7-day
retention, your first deploy drains the backlog instead of losing it.

### R3 — `saveProject` is a whole-document overwrite and can roll back generated code

`FirestoreAppDB.kt:214+` — `saveProject` serialises the entire `ProjectState`, gzips it into
`compressedState` (which holds the generated step code) and `set()`s the whole document with
no merge. A stale read followed by a full write **destroys concurrent writes to unrelated
fields.**

The ordering key serialises events within one `{projectId, executionId}` but gives you nothing
against other writers of the same project document — notably
`TestAndFixFlow.uploadFixedCode`. The race exists today; async delivery lengthens the window.

**Your subscriber must not call `saveProject`.** Use a Firestore transaction, or a
field-scoped `update()` touching only `testCheckpoints` and `buildTestStatus`.

### R4 — at-least-once delivery re-triggers the fix workflow

The fixed workflowId `wvx-fix-{projectId}-{stepId}` plus the `WorkflowExecutionAlreadyStarted`
catch makes redelivery idempotent *while the fix workflow is running*, but not after it
completes — a redelivered `failed` checkpoint would start a second fix. **Dedup on `eventId`**
(a small Firestore collection or the existing cache, TTL a few hours is plenty).

### R5 — head-of-line blocking

Ordered delivery means a nacked or repeatedly-failing message blocks every later message for
that execution. Configure a DLQ with `--max-delivery-attempts` so a poison message eventually
moves aside instead of wedging the execution permanently. Ack anything you have decided not to
process (unknown event type, missing project, non-TESTING).

### Bonus, unrelated but adjacent — `BuildTestFlow`'s poll deadline is never reset

`BuildTestFlow.kt:85` computes `pollDeadlineMillis` once. The `reTriggerRequested` branch
restarts the test but leaves the original deadline running, so a fix-and-retest gets only
whatever is left of the first 15 minutes. Added latency makes this bite more often. One-line
fix: recompute the deadline inside the re-trigger branch.

---

## 7. Infra prerequisites (project `weavex-475116` — there is no IaC anywhere)

```bash
# Mirror the existing logs topic's config
gcloud pubsub topics describe weavex-logs-eu --project weavex-475116

gcloud pubsub topics create weavex-checkpoints-eu --project weavex-475116 \
  --message-retention-duration=7d --message-storage-policy-allowed-regions=europe-west1
gcloud pubsub topics create weavex-checkpoints-eu-dlq --project weavex-475116

# CREATE THIS BEFORE THE PUBLISHER DEPLOYS. Ordering must be enabled here or the
# ordering key means nothing on delivery.
gcloud pubsub subscriptions create weavex-checkpoints-eu-sub --project weavex-475116 \
  --topic weavex-checkpoints-eu --enable-message-ordering --ack-deadline=60 \
  --message-retention-duration=7d \
  --dead-letter-topic=weavex-checkpoints-eu-dlq --max-delivery-attempts=5

gcloud pubsub topics add-iam-policy-binding weavex-checkpoints-eu --project weavex-475116 \
  --member="serviceAccount:<workflow-runner-runtime-sa>" --role="roles/pubsub.publisher"

gcloud pubsub subscriptions add-iam-policy-binding weavex-checkpoints-eu-sub \
  --project weavex-475116 \
  --member="serviceAccount:<weavex-cerebro-runtime-sa>" --role="roles/pubsub.subscriber"
```

Plus the Pub/Sub service-agent bindings the DLQ needs. Set `WEAVEX_GCP_PROJECT=weavex-475116`
on the workflow-runner service if it isn't already — otherwise the publisher falls through to
ADC and a project mismatch produces `NotFound` on every publish, silently.

---

## 8. Acceptance criteria

1. A TESTING project run end to end produces the same Firestore checkpoint document contents
   as before the migration — byte-identical stored strings for `{stepId}` and `step_context`.
2. `project.testCheckpoints` ends in the same state, with `fixAttempted` preserved across a
   fix cycle.
3. A failing step still starts exactly one `TestAndFixFlow`, and a redelivered message does
   not start a second.
4. A successful run's `clear` sets `buildTestStatus = "success"` and `BuildTestFlow`
   terminates on the status poll rather than the 15-minute timeout.
5. A fix that actually works reports `verified = true` (this is R1 — it will fail without the
   companion change).
6. A message for a non-TESTING or missing project is acked and logged, not retried.
7. Nothing in the subscriber calls `saveProject`.

## 9. Reference material on the publisher side

Everything lives in `weavex-core/weavex_core/checkpoint/`:

- `events.py` — `EventPublisher` / `PubSubEventPublisher`; the authoritative topic resolution
  and ordering-key handling.
- `checkpointer.py` — `_emit()` builds the envelope; its docstring states the wire contract.
…and the app-database access layer it reads through lives in `weavex-core/weavex_core/dao/`
(`base.py` for the interface, `firestore_db.py` for document ids and collection names).
- `test_events.py` — 19 offline checks pinning the schema. **If you need a schema change,
  change it here first** and the publisher second; this file is the spec. Run with
  `python -m weavex_core.checkpoint.test_events`.
- `test_events_live.py` — publishes real messages to the real topic and pulls them back.
  Useful for generating sample payloads to develop against: set `TESTING_PROJECT_ID` /
  `TESTING_ORG_ID` at the top and run `python -m weavex_core.checkpoint.test_events_live`.
