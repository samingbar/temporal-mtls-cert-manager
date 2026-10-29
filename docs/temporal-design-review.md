# Workflow Design Critique

Reviewed 2026-10-29 with the `temporal-workflow-design-critic` skill. Scope: the supplied specification, TypeScript Credential workflow, contracts, runnable fake integrations, worker configuration, tests, and runbook. The original review below describes the pre-fix implementation. See the resolution addendum for the subsequent changes requested by the user.

## Verdict

- status: **needs_revision**
- summary: Credential rotation is a good fit for Temporal. The parent uses deterministic orchestration, explicit business timers, immutable child snapshots, bounded cancellation, and handler draining before Continue-As-New. The existing 40 tests establish useful behavior, but three additional isolated Temporal probes reproduced correctness defects outside that coverage. Two affect the executable fake rotation; one affects the Credential workflow itself.

The design also needs stronger bounds for a long-lived production entity. Full state carryover is explicitly required by the specification and acknowledged in the README, so that finding is a production design limitation rather than an implementation deviation. Concrete Temporal Cloud, Vault, Kubernetes, and production Rotate implementations remain intentionally deferred; their absence is not a defect in this phase.

## Top Issues

1. **[high] The fake adapter shares idempotency records across different Credentials.**
   - Evidence: `demos/credential-supervisor/fake-activities.ts:121-131` indexes a worker-wide operation map by `request.operationId`; the generated CA label also uses only that value. Business operation IDs are deduplicated separately within each Credential, so distinct Credentials can legitimately both use `maintenance-001` or independently generate `scheduled-1`.
   - Why it matters: the second child skips every already-completed step and returns `SUCCEEDED` while its namespaces remain unchanged. This defeats the example's multiple-Credential model even when the entities manage disjoint namespaces.
   - Reproduced: two entities with separate `CA-A` and `CA-B` inventories both used `maintenance-001`. Entity B reported `SUCCEEDED` but still trusted and used `CA-B`; only entity A's five mutation events existed.
   - Recommended fix: key operation state and generated fake CA identity by an unambiguous tuple of complete Credential identity and operation ID. Add a two-entity regression using both identical manual IDs and independent scheduled IDs.

2. **[high] Ordinary Activity failures close the fake Rotate child without compensation.**
   - Evidence: `demos/credential-supervisor/fake-workflows.ts:68-70` compensates cancellation but rethrows other failures. The deliberate `fail` mode has its own compensation call, which masks this broader gap. `fake-activities.ts:144-156` can fail retirement after client rollout and removal of old trust.
   - Why it matters: the parent records failure and clears its active child. Its `restore` Update can then no longer initiate reversion of that operation.
   - Reproduced: rotate only `orders-dev` from the default inventory. Retirement correctly refuses to retire a CA still referenced by `payments-dev`. The child has already switched the selected clients and removed their old trust; no `revert` event runs, `activeRotation` becomes absent, and a subsequent restore request is rejected. This establishes missing compensation, not a claim that live workloads suffered downtime.
   - Recommended fix: route failures after a possible mutation through bounded, noncancellable compensation. Preserve the original failure and distinguish successful compensation from unresolved compensation. An Activity can mutate successfully before its response fails, so compensation must also handle ambiguous completion.

3. **[high] Continue-As-New carries an unbounded state payload.**
   - Evidence: `src/workflows/credential.ts:431-433` copies the entire state into the next start input. Operation IDs, rotation summaries, Update outcomes, failures, and queued requests have no retention or admission bounds. `validateActor` at lines 510-514 also permits arbitrarily long reason/requester text. The default state Query copies the entire state before filtering it.
   - Why it matters: a fresh Event History does not shrink that input. Accumulated summaries or several large accepted requests can exceed the next run's payload limit. Full-history Query responses and replay work also grow. Temporal documents a 2 MB payload limit and finite history limits in its [worker guidance](https://docs.temporal.io/best-practices/worker).
   - Recommended fix: archive detailed summaries and failures durably outside the Workflow, carry bounded summaries plus references, define an operation-ID deduplication ledger or an explicit retry window, and bound input text and pending work. Removing deduplication IDs without a replacement would weaken correctness. This requires revising the specification's complete-history carryover requirement.

4. **[medium] An obsolete inventory failure pauses a corrected scope.**
   - Evidence: `src/workflows/credential.ts:263-270` captures the options revision, but `Promise.all` can reject before the stale-revision check. The catches at lines 449-454 and 467-470 then unconditionally set the entity to failed.
   - Why it matters: an operator can successfully replace broken scope A with valid scope B, then have A's delayed failure stop rotations for B. An additional activation is required even though the failing configuration was already removed.
   - Reproduced: hold an inventory read for `old-failing`, replace the scope with valid `orders-dev`, then fail the old read. Current options show `orders-dev`, while status becomes `FAILED` with the old scope's `InvalidScope` error.
   - Recommended fix: catch read failures inside `refreshInventory`; discard/retry them when their captured revision is obsolete, and propagate only failures belonging to the current revision. Apply the same protection at both refresh call sites.

5. **[medium] History rollover requires a successful rotation.**
   - Evidence: `src/workflows/credential.ts:388-390` requests rollover after a successful child, and lines 428-433 contain the only Continue-As-New branch. Suspended, failed, and no-certificate entities can continue receiving accepted Updates without that success path.
   - Why it matters: these long-lived runs can accumulate Update history until the service rejects further Updates or history reaches its limits, even before the state payload issue above is reached. Calendar time alone does not grow history; accepted work does. See [Temporal Cloud limits](https://docs.temporal.io/evaluate/temporal-cloud/limits).
   - Recommended fix: inspect the server's rollover suggestion in the main loop and its wake predicates, and Continue-As-New at safe boundaries with no active child and drained handlers. Define admission limits while a child prevents rollover. Bound the carried state separately; more frequent rollover does not solve finding 3.

## Category Review

### Temporal fit

**pass.** This is a durable control-plane process with waits, external operations, operator interactions, retries, and compensation. It does not move private keys or application traffic through Workflow history.

### Workflows

**pass with findings.** SDK-controlled time, Activity-only external I/O, initialized state, and immutable snapshots support determinism. The stale-failure race and incomplete history bounds need changes. No unjustified Workflow retry policy or execution/run timeout is configured.

### Child Workflows

**pass for the parent; fail for the fake child's general compensation.** Rotation has its own lifecycle, queries, revert/cancel operations, and compensation ownership, which justify a child. One child runs at a time. `WAIT_CANCELLATION_COMPLETED`, `REQUEST_CANCEL`, and bounded cleanup are explicit. A cleanup deadline expiring is correctly recorded as unresolved rather than as successful reversion.

### Activities

**fail for demo idempotency isolation; production behavior inconclusive.** The client-mediated child Update is appropriate. Inventory reads are parallel and bounded. The short local configuration merge is an explicit specification decision and contains no external I/O in this example. Real adapter idempotency, authorization classification, transport deadlines, heartbeat needs, and operation-specific timeout settings cannot be established from contracts alone.

### Signals

**not_applicable.** The API uses Updates for validated operator mutations.

### Queries

**pass for read-only behavior; payload bounds fail.** Parent Queries do not mutate state or perform I/O. Whole-state copying and complete default history can become expensive. A minor demo issue is that the child Query initially reports `SUCCEEDED` with `finishedAtMs: 0` while work is still running (`fake-workflows.ts:21-32`); an explicit running phase would make that state truthful.

### Updates

**pass with limits to resolve.** Manual operation IDs persist across Continue-As-New, validators are read-only, and handler-time validation errors become Temporal failures rather than repeated Workflow Task failures. The queue and request sizes remain unbounded. Repeated option/scope Updates deliberately add outcomes and revisions even when their desired values match current state.

### Workers and Task Queues

**pass for the local example; production inconclusive.** A single capability-specific queue and consistent registrations are reasonable. The demo explicitly configures a versioned worker and pinned start routing. Its fake external state is worker-local memory and cannot support restart durability or interchangeable replicas; that limitation is documented. Production HA, concurrency, rate limiting, and permissions remain deferred.

### Timers / Schedules / Cron

**pass.** State-dependent certificate expiration belongs in durable Workflow timers. One rotation executes at a time. No calendar Schedule or Cron is needed. Bounds for very small caller-supplied durations and fleet-wide rotation bursts need production sizing decisions.

### Data / Payloads / Converters

**fail on bounds; otherwise appropriate for the example.** Public JSON contracts use explicit epoch/duration milliseconds and no private keys. Full inventory snapshots, unrestricted text, and accumulated state lack payload limits. Compression can reduce size but cannot provide a bound for an indefinitely growing collection. Production codec/encryption requirements are inconclusive.

### Visibility

**pass for phase one.** State Queries and Event History expose operations without relying on eventually consistent visibility for control flow. Search attributes are not used as state storage. Minor issue: `putOptions` with `executionOptions.suspend` leaves `nextRotationAtMs` populated while suspended (`credential.ts:198-203`), unlike the explicit suspend handler. Clear that timestamp when applying suspension. Fleet-wide search and alerting are deferred.

### Versioning

**partially addressed; production evolution inconclusive.** Workflows are pinned and the demo routes starts to a versioned worker. The pinned start override is inherited across Continue-As-New, so rollover must not be assumed to migrate the entity to a new build. The installed SDK documents that inheritance in `@temporalio/common/lib/worker-deployments.d.ts:65-83`. A version retirement/migration procedure and saved-history replay validation are not provided. The current integration tests are not saved-history replay tests.

### Long-running execution

**fail on bounds.** Handler draining, queued-work carryover, timer reconstruction, and operation-ID persistence are sound. Findings 3 and 5 prevent treating the entity as indefinitely sustainable. The default closed-Workflow ID reuse policy and deliberately allowed restart behavior also need an explicit operational decision.

## Open Questions

- What are the maximum managed namespace/Deployment counts, inventory response sizes, and peak Update rate per Credential?
- How long must operation IDs remain deduplicated, and where will old summaries and their durable references live?
- When should a closed Credential identity be reusable, and what must happen if an older Rotate child remains unresolved?
- How will pinned executions migrate, and how long will old worker builds remain available for execution and Queries?
- How will a real adapter identify the exact managed CA, preserve unrelated namespace trust, and account for dependencies removed from the current scope before retiring shared material?
- What naming rule enforces uniqueness of the current hyphen-joined Workflow identity? Different allowed component tuples can produce the same ID (`options.ts:23-27`); the README acknowledges this convention-based tradeoff.

## Checklist Result

Statuses apply to this phase-one example. Production-only checks remain inconclusive where no implementation or operational policy exists.

| Category | Item | Result | Basis |
| --- | --- | --- | --- |
| Workflows | Use case is appropriate | pass | Durable multi-step credential control process |
| Workflows | Logic is deterministic | pass | No direct external I/O; SDK-controlled time; canonical ordering |
| Workflows | Replay/versioning concerns addressed | inconclusive | Pinning exists; migration and saved-history replay procedure absent |
| Workflows | Event history growth managed | fail | No rollover trigger outside successful rotation |
| Workflows | Workflow ID strategy intentional | pass | Business identity; documented tuple-ambiguity/reuse questions remain |
| Workflows | Timeouts not misused | pass | Business timers; no execution/run deadline |
| Workflows | Failure semantics understood | pass | Handler guards and explicit failure state |
| Workflows | No unjustified Workflow retry policy | pass | None configured |
| Child Workflows | Use is justified | pass | Separate durable rotation and compensation lifecycle |
| Child Workflows | Number of children controlled | pass | One active rotation |
| Child Workflows | Parent close policy intentional | pass | REQUEST_CANCEL preserves opportunity to compensate |
| Child Workflows | Communication explicit | pass | Result plus Activity/client-mediated revert Update |
| Activities | Idempotent | fail | Shared demo cache uses entity-local operation ID |
| Activities | Granularity appropriate | pass | Separate inventory/configuration/revert boundaries |
| Activities | Long-running work heartbeats | not_applicable | Current fake Activities are short; real adapters deferred |
| Activities | Payload sizes safe | fail | Inventory/request bounds unspecified |
| Activities | Large data passed by reference | inconclusive | No production inventory size budget or external storage design |
| Activities | Polling efficient | not_applicable | No Activity polling loop implemented |
| Activities | Timeouts activity-specific | inconclusive | Shared bounded defaults; no upstream latency data |
| Activities | Retry policy intentional | pass | Finite limits; real error classification still deferred |
| Activities | Cancellation handling correct | inconclusive | Parent handles cancellation; real adapter behavior deferred |
| Activities | Local Activities justified | pass | Explicit short worker configuration snapshot; no network I/O |
| Signals | Runtime state changes use Signals where needed | not_applicable | Updates chosen instead |
| Signals | Handlers deterministic/idempotent | not_applicable | No Signal handlers |
| Signals | Handlers avoid heavy work | not_applicable | No Signal handlers |
| Signals | Volume safe | not_applicable | No Signals |
| Signals | Continue-As-New compatible | not_applicable | No Signals |
| Queries | Read-only | pass | Synchronous snapshots |
| Queries | Appropriate state reads | pass | Worker-backed state Queries; child progress reporting needs refinement |
| Updates | Deterministic/idempotent | pass | Rotation business-ID deduplication; desired-state replacements |
| Updates | Validation appropriate | pass | Validators and handler-time validation guards |
| Updates | Handler scope appropriate | pass | Early rotate acceptance; justified bounded revert bridge |
| Workers | Count supports HA | inconclusive | One local worker; production deployment deferred |
| Workers | Registrations consistent per queue | pass | One demo bundle/adapter registration |
| Workers | Queue strategy intentional | pass | One credential-operations capability queue |
| Workers | Rate limiting considered | inconclusive | No capacity/rate budget provided |
| Workers | Ordering assumptions valid | pass | Parent serialization; no reliance on queue FIFO |
| Timers | Used appropriately | pass | Durable state-relative expiration/deadline timers |
| Schedules | Usage justified | not_applicable | No calendar launch requirement |
| Schedules | Overlap intentional | not_applicable | No Schedule; parent serializes rotations |
| Schedules | Jitter/backfill addressed | not_applicable | No Schedule; fleet burst capacity remains open |
| Cron | Avoided unless required | pass | None used |
| Data | Converter choices intentional | pass | Explicit JSON-only public contracts and milliseconds |
| Data | Compression/encryption handled | inconclusive | No private keys; production governance and size policy deferred |
| Data | Cross-language datetime serialization addressed | pass | Explicit numeric milliseconds |
| Visibility | Search attributes used only for visibility | not_applicable | No search attributes |
| Visibility | Sensitive data excluded from search attributes | not_applicable | No search attributes |
| Visibility | Eventual consistency understood | pass | No visibility-driven business decisions |
| Versioning | Replay-safe evolution strategy exists | inconclusive | Pinning configured; replay/migration workflow not demonstrated |
| Versioning | Patches have cleanup plan | not_applicable | No patches |
| Long-running | Continue-As-New strategy sufficient | fail | Success-only trigger and unbounded carryover |
| Long-running | Timer/state carryover handled | pass | Current options, queue, IDs, summaries and sequence carried; timers recomputed |

## Validation Evidence

The previous implementation verification completed 40 tests and its standalone demo. This review added no implementation changes and did not rerun that unchanged suite. It ran three separate, passing assertion probes against an isolated Temporal server:

1. `PROBE_CROSS_ENTITY`: entity B's summary was `SUCCEEDED` while its actual trusted/client CA remained `CA-B`.
2. `PROBE_SUBSET_FAILURE`: state was `FAILED`, no active child remained, mutation events ended at `remove-old-trust`, and a restore request was rejected.
3. `PROBE_STALE_ERROR`: current scope was `orders-dev`, but the obsolete read's `InvalidScope` error changed status to `FAILED`.

The probes were temporary local review artifacts. Checked-in regression coverage for the high-severity fixes lives in `test/rotation-fixes.test.ts` and `test/workflow-bounds.test.ts`. The probes used fake metadata and no live cloud or PKI credentials; their local server was shut down after execution.

## Resolution Addendum

The subsequent implementation addresses the three high-severity findings:

- **Finding 1:** fake mutation records and generated CA fingerprints now use the complete Credential identity and operation ID. A real Temporal regression verifies that two entities independently complete the same business operation ID.
- **Finding 2:** ordinary failures after possible mutation now run compensation before returning a failed summary. Original and compensation failures are separate fields. Unresolved compensation keeps the child open for Queries and another restore request; distinct restore attempts no longer share one permanently deduplicated child Update ID. Regressions cover failed retirement, lost mutation responses, and repeated restore attempts after ambiguous compensation results.
- **Finding 3:** old summaries and operation IDs are archived before leaving state. The demo worker uses a persistent SQLite adapter; production storage remains an explicit Activity contract. Queries return recent windows, while archived summaries are inspected in pages. The implementation bounds requests, inventories, summaries, pending work, recent diagnostics, recent history bytes, and Continue-As-New input. Archive writes and later lookups verify the last acknowledged archive reference. Failed writes preserve retained IDs and summaries; after storage repair, activation retries the checkpoint before normal scheduling resumes.

The related success-only history rollover finding is also addressed: rollover can follow 128 accepted Updates, a server history recommendation, or the recent-history byte threshold, including while suspended or failed. Tests cover both successful and repeatedly failed large rotations, storage reopen and process exit, transaction rollback, concurrent deduplication lookups, and Update handlers arriving during checkpointing.

Verification after these changes: all 67 regression tests passed, including real Temporal tests for large successful and failed rotations. Additional focused unit checks cover environment configuration exceeding the effective-options byte limit. The archive CLI smoke test and local walkthrough also pass.

The medium-severity stale-inventory failure race and the documented Workflow-ID naming ambiguity remain outside the requested high-severity fixes. The example still has fake PKI/Cloud/Kubernetes integrations and supplies no production availability guarantee; the SQLite archive is a single-host demonstration, not a multi-host production storage service. Recent diagnostic snapshots do not promise permanent archival of every Update outcome or nonrotation failure; Temporal Event History remains the authoritative execution record within its retention period.
