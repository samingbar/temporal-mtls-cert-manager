# Credential Supervisor demo

This demo makes the orchestration visible using fake namespace trust, fake client credentials, and a fake Rotate child. It runs the real Credential Workflow against a local Temporal server. No certificates or private keys are created, and no Temporal Cloud, Vault, or Kubernetes calls are made.

## Automated walkthrough

From the repository root with Node.js 22.13 or later:

```sh
npm install
npm run demo
```

The script starts an isolated local Temporal server and worker with an ephemeral archive, runs its assertions, then shuts them down. The first run may download the development server binary. A successful run ends with `Demo assertions passed`.

The walkthrough:

1. Activates the Credential and discovers three fake namespaces from the union of explicit names and tag matches, along with fake Deployment inventory.
2. Queues a manual rotation, pauses it after client rollout, and verifies both CAs remain trusted while the fake clients use the new CA.
3. Changes the parent scope to `orders-dev` while the child retains its original three-namespace snapshot.
4. Requests restore through the Activity/client bridge and verifies compensation returns all three namespaces to their earlier fake state.
5. Activates the Credential again with the complete three-namespace scope before running a successful rotation. The fake child removes old trust only after client rollout, then retires the old CA after checking that no fake namespace still trusts or uses it.
6. Retries the same operation ID after Continue-As-New and verifies deduplication, then suspends and deactivates the Credential.

The fake step names are `issue`, `add-trust`, `rollout-clients`, `remove-old-trust`, `retire-old`, and `revert`. Operation records and fake CA fingerprints include the complete Credential identity, so two Credentials using the same business operation ID have independent rotation state. The child lives only in `fake-workflows.ts`; `src/` defines its production contract without registering a concrete Rotate implementation.

## Interactive worker and CLI

Start a server and worker in separate terminals:

```sh
temporal server start-dev
```

```sh
npm run worker:demo
```

Then use a third terminal:

```sh
npm run ctl -- start
npm run ctl -- state
npm run ctl -- rotate maintenance-001
npm run ctl -- state
npm run ctl -- deactivate
```

The `rotate` response acknowledges that the manual request was queued. Query state until its summary is terminal to observe completion. Reuse an operation ID when retrying the same business request.

| Command | Effect |
| --- | --- |
| `start` | Starts the Credential with complete fixture input |
| `state [--failures]` | Prints current status, options, inventory, active snapshot, history, and optional error details |
| `archive [limit] [offset]` | Prints a page of distinct archived summaries and diagnostic snapshots for this identity |
| `rotate <operation-id>` | Queues a manual rotation |
| `scope <name,name,...>` | Replaces the complete namespace scope, removing the fixture tag selector |
| `options <ttl-ms> <offset-ms>` | Changes the next decision's certificate options |
| `suspend` | Waits for the active child, then pauses scheduling |
| `activate` | Activates using the complete fixture request, restoring its scope and options |
| `restore` | Asks the active fake child to compensate through a client Activity |
| `deactivate` | Waits for active work and completes the Credential |

The [runbook](../../runbooks/credential-supervisor/README.md) includes verification steps, an accelerated scheduling scenario, restore, and native cancellation.

### Configuration

| Variable | Default | Used by |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` | `localhost:7233` | Interactive worker and CLI |
| `TEMPORAL_NAMESPACE` | `default` | Interactive worker and CLI |
| `FAKE_ROTATION_MODE` | `complete` | Interactive worker |
| `CREDENTIAL_ARCHIVE_PATH` | `.credential-data/archive.sqlite` under the current directory | Interactive worker and archive CLI command |
| `CREDENTIAL_ENVIRONMENT` | `dev` | CLI |
| `CREDENTIAL_TEAM` | `platform` | CLI |
| `CREDENTIAL_SCOPE` | `demo` | CLI |

The task queue is `credentialoperations`. Identity variables must match across CLI commands for the same Credential.

Use `FAKE_ROTATION_MODE=hold` to pause after adding trust, `hold-after-rollout` to pause after switching fake clients, or `fail` to compensate and return a failed summary after dual trust. A held child proceeds to compensation when restored or cancelled; its parent's rotation timeout also triggers child cancellation. Ordinary failures after a possible mutation also compensate before returning. If compensation fails, the child stays open for Queries and another restore request, preserving the original failure and the compensation failure separately. The fixture uses a 30 second rotation timeout. Complete any active scenario before restarting the worker, because its fake external inventory resets on restart.

### Persistent history and bounded state

The interactive worker uses SQLite to preserve completed rotation summaries and accepted operation IDs across runs and worker restarts. All workers handling the same Credentials must share the same persistent file on one host. The standalone walkthrough and default fake adapters used in tests have isolated in-memory archives; their history is ephemeral.

State Queries provide recent history. After an acknowledged archive checkpoint, carryover retains up to 16 rotations, 128 operation IDs, 64 Update outcomes, and 32 failures, with IDs for pending work retained as well. Every rotation summary and accepted operation ID is archived before leaving state. Update outcomes and nonrotation failures are recent diagnostic windows; archive checkpoints contain snapshots of those windows. Temporal Event History retains the full execution record according to the server's retention policy.

```sh
npm run ctl -- archive 20 0
```

`limit` defaults to 20 and accepts 1-100; `offset` defaults to zero. Follow `nextOffset` for another page. Rotation summaries, Update outcomes, and failures each use that limit and offset. Archive pages deduplicate overlapping snapshots. `state.historyArchive` contains the latest archive reference and durable rotation and operation counts.

The Workflow drains active Update handlers before checkpointing and prunes only after the archive Activity succeeds. It rolls over after successful rotations, after 128 accepted Updates in a run, or when Temporal recommends it, including while suspended or failed. Its Continue-As-New input is capped at 512 KiB; initial requests are capped at 32 KiB, Updates at 8 KiB, inventory responses and individual summaries at 64 KiB of JSON. Recent rotation history is additionally capped at 128 KiB. The manual queue holds at most 32 requests. This replaces the supplied specification's growing full-history carryover.

Preserve the SQLite file and use SQLite-aware backups. Once a running chain has an acknowledged checkpoint, later archive reads and writes verify that checkpoint and fail with `ArchiveUnavailable` if the file is missing or incorrect. A new chain without a prior reference cannot detect lost deduplication records. This single-host example does not provide a shared archive across hosts or a production availability guarantee. A production adapter needs durable shared storage, access control, concurrency rules, and recovery procedures. The archive contains public metadata and operator text; no private keys are stored.

## What the demo proves

Its assertions check the parent/child interaction, Update acceptance, immutable scope snapshots, simulated trust order, compensation, state history, operation-ID deduplication across Continue-As-New, and graceful suspension/deactivation. The fake retirement check also illustrates why a real PKI adapter needs complete knowledge of credential dependencies. The runbook additionally exposes scheduling and cancellation paths for inspection.

It does not test certificate issuance, real CA propagation, SDK client reload behavior, Kubernetes rollout availability, Vault/VSO behavior, production external mutation retries, Worker Deployment routing, or live connectivity. A successful fake rollover provides no production guarantee of zero downtime. The production integration boundary and payload limits are described in the [main README](../../README.md).
