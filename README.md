# mTLS Certificate Manager

A TypeScript example of the Credential Supervisor specification. A long-lived Temporal Workflow owns a credential's scope, schedules rotations, accepts operator Updates, and records rotation summaries. It starts a separate Rotate child with an immutable snapshot of the options and inventory for that operation.

The production boundary is phase 1: contracts, option validation, and the Credential Workflow. The Rotate implementation and Activities under `demos/` use fake data so the example can run without Temporal Cloud, Kubernetes, Vault, certificates, or private keys. They demonstrate orchestration and trust ordering; they do not establish that a production rollover will avoid downtime.

For adoption into existing provisioning and application bindings, start with the [deployment guide](docs/deployment-guide.md). It covers the configuration-file handoff, Terraform ownership, separate leaf renewal and CA rollover lifecycles, namespace authentication prerequisites, SDK credential loading, and the evidence required from a real pilot.

## Run it

Use Node.js 22.13 or later.

```sh
npm install
npm test
npm run demo
```

The demo starts a local Temporal test environment, runs the Credential Workflow with fake adapters, prints the observed state, and shuts down. Its first run may download the Temporal test server. See [the demo guide](demos/credential-supervisor/README.md) for the scenarios and interactive commands, and [the runbook](runbooks/credential-supervisor/README.md) for actions with verification steps.

## What is implemented

The Java packages and DTOs in the supplied specification become TypeScript interfaces in [`src/contracts.ts`](src/contracts.ts). Durations and timestamps use milliseconds. Update and Query definitions live in [`src/messages.ts`](src/messages.ts). The `api-key` credential type remains part of the contract, but this execution example accepts only `certificate`.

The Credential Workflow:

- Calls `mergeOptions` as its first Local Activity in every run, then fetches namespace certificates and Deployment inventory in parallel.
- Computes the next rotation from the earliest managed certificate expiration minus the rotation offset. A past recommendation runs immediately; no certificates means no scheduled rotation.
- Refreshes external inventory before a rotation and gives the child a snapshot. Changes to scope or options during a rotation apply to the next operation.
- Accepts a manual `rotate` Update by durably queuing its required business operation ID. The acceptance response does not mean the child has completed.
- Supports startup suspension, `activate`, `suspend`, `restore`, and graceful `deactivate` Updates, plus a state Query with recent rotation history.
- Cancels a child after `rotationTimeoutMs`, then allows up to the same duration for the child's compensation to settle. Native parent cancellation follows the same bounded cleanup path.
- Archives completed summaries and accepted operation IDs before pruning state and continuing as new. A successful rotation, 128 accepted Updates in a run, or Temporal's history recommendation triggers rollover at a safe boundary, including while suspended or failed.
- Carries current options, queued work, an archive reference, and bounded recent history into the next run. Each run fetches fresh inventory. Manual rotation deduplication checks both recent IDs and the durable archive.

The Workflow is annotated `PINNED`. The demo registers a versioned worker and explicitly pins starts to its demo version; production Worker Deployment configuration is deferred. The entity has no Workflow Execution or Run Timeout; business waits use durable timers.

## Scope and safety boundaries

Namespace scope is the deduplicated union of explicit names and the tag selector's matches. Deployment selectors are canonicalized, and matching Deployment inventory must be deduplicated by Kubernetes namespace and Deployment name. Kubernetes integration can be disabled, yielding an explicitly skipped inventory.

Only public inventory metadata belongs in Workflow payloads. This example uses fake CA labels, names, and fingerprints. It does not create, load, or persist private keys.

Workflow identity components and rotation operation IDs are business-safe identifiers: one to 64 characters, beginning with a letter or digit and containing only letters, digits, dots, underscores, or hyphens. The IDs retain the specification's hyphen concatenation format. Different tuples can therefore produce the same string. Adopt a unique naming convention for the complete tuple and `scopeKey`; the format does not encode an unambiguous tuple by itself.

The example changes the specification's full-history carryover requirement to prevent payload growth. State retains recent windows of up to 16 rotations (also capped at 128 KiB), 128 operation IDs, 64 Update outcomes, and 32 failures after checkpointing. Accepted pending requests keep their IDs as well. Every rotation summary, including its failure details, and every accepted operation ID is archived before it can leave state. Nonrotation failure logs and Update outcomes are recent diagnostic windows; their checkpoints do not replace the full execution record in Temporal Event History, which remains subject to server retention.

| Limit | Maximum |
| --- | --- |
| Initial request | 32 KiB of JSON |
| Operator Update | 8 KiB of JSON |
| Inventory response | 64 KiB of JSON |
| Rotation summary | 64 KiB of JSON |
| Continue-As-New input | 512 KiB of JSON |
| Pending manual rotations | 32 |
| Operator reason / requester | 1,024 / 128 characters |

The demo worker writes the archive to `.credential-data/archive.sqlite`; `CREDENTIAL_ARCHIVE_PATH` selects another persistent file. Every worker handling the same Credentials must share that file on the same host. SQLite transactions commit summaries and operation deduplication together. An archive write failure prevents pruning and rollover. Once a running Workflow chain has an acknowledged checkpoint, later archive reads and writes require that checkpoint and reject a missing or incorrect archive. Preserve and back up the file: a new Workflow chain without an earlier reference cannot detect lost historical deduplication records. Tests and the automated walkthrough use isolated, ephemeral archives.

Use `npm run ctl -- archive [limit] [offset]` to inspect archived history in pages, and `state` for the current recent window. The state includes `historyArchive` with an archive reference and durable rotation and operation counts. The [demo guide](demos/credential-supervisor/README.md) explains archive configuration and inspection.

## Production integrations still required

The concrete Rotate implementation must establish new namespace trust while retaining old trust, roll out client credentials, verify both deployment convergence and authenticated connectivity, remove old namespace trust, and retire the old material. The fake child includes an explicit `verify-clients` gate with separate simulated checks; either failure blocks old-trust removal and retirement and initiates compensation. Compensation belongs to that child. It compensates ordinary failures after a possible mutation before returning `FAILED`, and preserves the original failure. If compensation fails, the child remains open for Queries and later restore requests, with a separate compensation failure. A bounded wait that expires is an unresolved outcome, not evidence that compensation finished.

Future adapters need to read and update namespace trust through the Temporal Cloud Operations API, integrate with the chosen PKI, and access the local Kubernetes cluster through an appropriate service account. The supplied Vault path uses Vault PKI and Vault Secrets Operator: VSO reissues workload Secrets and restarts its configured Deployment targets; the worker verifies the resulting rollout. Those integrations, permissions, deployment manifests, and production Worker registration are outside this example. No real RBAC resources or manifests are deployed.

A fake inventory cannot verify live client connectivity, CA propagation, workload availability, or recovery after external partial failures. The demo worker stores its fake external inventory in memory, so restarting it resets that inventory while the SQLite archive persists. Production deployment also needs a durable shared archive adapter with defined concurrency, access control, and backup procedures; this SQLite example supplies no availability guarantee across hosts.

## Repository

| Path | Purpose |
| --- | --- |
| `src/contracts.ts` | Public request, response, state, Activity, and child contracts |
| `src/archive-contracts.ts` | Durable history and operation lookup Activity contracts |
| `src/messages.ts` | Typed Updates and Queries |
| `src/options.ts` | Validation, canonicalization, identifiers, and scheduling |
| `src/limits.ts` | Payload, queue, and retention bounds |
| `src/workflows/` | Credential orchestration |
| `demos/credential-supervisor/` | Fake adapters, fake Rotate child, demo worker, and CLI |
| `demos/credential-supervisor/application-binding.example.json` | Metadata-only request accepted through `CREDENTIAL_REQUEST_FILE` |
| `demos/credential-supervisor/connection-examples.ts` | SDK configuration helpers for separate mTLS and API-key connections |
| `test/` | Option and Workflow checks |
| `runbooks/credential-supervisor/` | Operator actions and verification |
| `docs/deployment-guide.md` | Provisioning handoff, rollout gates, migration prerequisites, and pilot acceptance |
| `docs/source-specification.txt` | The supplied specification |

## Temporal references

The implementation follows Temporal's [TypeScript Workflow guide](https://docs.temporal.io/develop/typescript/workflows), [message-passing guidance](https://docs.temporal.io/develop/typescript/workflows/message-passing), [cancellation scopes](https://docs.temporal.io/develop/typescript/workflows/cancellation-scopes), and [testing guidance](https://docs.temporal.io/develop/typescript/best-practices/testing-suite). The [entity Workflow pattern](https://docs.temporal.io/design-patterns/entity-workflow) discusses handler draining and deduplication across Continue-As-New.
