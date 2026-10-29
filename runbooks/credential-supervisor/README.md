# Credential Supervisor runbook

Audience: platform engineers and operators evaluating the example locally. These steps use fake namespace trust and fake clients. No command below deploys Kubernetes resources, changes Temporal Cloud trust, or handles real credential material.

Run commands from the repository root. Keep the local server and demo worker running while using the CLI. The fake adapter keeps external inventory in worker memory; stop all demo Credentials before restarting that worker. Its archive persists separately in `.credential-data/archive.sqlite`, or the file selected by `CREDENTIAL_ARCHIVE_PATH`.

## Prepare and start

1. Install dependencies and check the project with Node.js 22.13 or later.

   ```sh
   node --version
   npm install
   npm test
   ```

   Verify that Node is version 22.13 or later, installation completes, and the test command exits successfully.

2. Start a local Temporal server in one terminal. This requires the [Temporal CLI](https://docs.temporal.io/cli).

   ```sh
   temporal server start-dev
   ```

   Verify that the server reports it is listening on `localhost:7233`. Its development UI is available at [localhost:8233](http://localhost:8233).

3. Start the fake worker in a second terminal.

   ```sh
   npm run worker:demo
   ```

   Verify the `FAKE credential worker` message names namespace `default` and task queue `credentialoperations`. Leave the worker running.

4. In a third terminal, start the Credential and read its state.

   ```sh
   npm run ctl -- start
   npm run ctl -- state
   ```

   Verify the returned Workflow ID is `certificate-dev-platform-demo`. Query again if initialization is still running. The state should reach `ACTIVE`, list `orders-dev`, `payments-dev`, and `tagged-dev`, include a fake Deployment, and report `nextRotationAtMs`. The third namespace comes from the fixture's tag selector; duplicate explicit names appear only once.

## Rotate and change scope

1. Queue a manual rotation with a stable business operation ID.

   ```sh
   npm run ctl -- rotate maintenance-001
   npm run ctl -- state
   ```

   Verify the Update returns `accepted: true`. That response acknowledges the queued request. Query until `rotationHistory` includes `maintenance-001` with `SUCCEEDED`; its steps should order `add-trust`, `rollout-clients`, `remove-old-trust`, then `retire-old`. The parent returns to `ACTIVE` after Continue-As-New.

2. Retry the same operation ID.

   ```sh
   npm run ctl -- rotate maintenance-001
   ```

   Verify `duplicate: true`. The business operation ID remains deduplicated after Continue-As-New, including after it leaves the recent in-memory window. The archive keys IDs by the complete Credential identity, so another Credential can use the same operation ID independently. Use a new ID for a new rotation of this Credential.

3. Exercise scheduling without waiting for days. This example assumes the preceding successful rotation issued fake certificates with a 90 day lifetime.

   ```sh
   npm run ctl -- options 10368000000 8640000000
   npm run ctl -- state
   ```

   This sets the requested lifetime to 120 days and the offset to 100 days. The current 90 day certificates are already inside that offset, so the next decision is immediate. Verify a new `scheduled-...` summary reaches `SUCCEEDED`, with requester `credential-scheduler`. The next recommendation should then be in the future. This tests timer decisions against fake expiry metadata.

4. Replace the complete namespace scope.

   ```sh
   npm run ctl -- scope orders-dev
   npm run ctl -- state
   ```

   Verify `options.temporalNamespaces.names` contains only `orders-dev` and the refreshed namespace inventory follows it. The CLI command also removes the previous tag selector. If a child was active when the Update arrived, inspect `activeRotation.request`: it keeps its prior scope, inventory, and options until it finishes.

## Suspend, resume, and stop

1. Suspend scheduling.

   ```sh
   npm run ctl -- suspend
   npm run ctl -- state
   ```

   The Update waits for an active child to reach a terminal safe boundary. Verify the state is `SUSPENDED` and has no scheduled rotation. A startup `executionOptions.suspend` has the same effect: an explicit activation is required before scheduling begins.

2. Activate the Credential.

   ```sh
   npm run ctl -- activate
   npm run ctl -- state
   ```

   Verify the state reaches `ACTIVE` and the suspension is cleared. The demo CLI supplies the complete fixture activation request, so this command also restores its fixture scope and options. A custom client can supply the desired complete activation input instead.

3. Deactivate the Credential.

   ```sh
   npm run ctl -- deactivate
   ```

   Verify the response contains `stopped: true`. The command waits for the active rotation and the parent to finish; the parent does not Continue-As-New. Already queued requests are not started after deactivation is requested.

## Exercise restore and cancellation

Use this as a separate scenario after deactivating the preceding Credential. Stop the old fake worker, then restart it in a mode that pauses a child after client rollout:

```sh
FAKE_ROTATION_MODE=hold-after-rollout npm run worker:demo
```

Verify the worker starts successfully. Its inventory is a fresh in-memory fixture.

1. Start a separate Credential and wait for an active child.

   ```sh
   export CREDENTIAL_SCOPE=revert-demo
   npm run ctl -- start
   npm run ctl -- rotate restore-001
   npm run ctl -- state
   ```

   Verify the parent is `ROTATING` and `activeRotation.childWorkflowId` identifies `restore-001`. The fake child pauses after `rollout-clients`. The fixture's rotation timeout is 30 seconds, so perform the next step before that timer expires.

2. Request compensation through the Activity/client Update bridge.

   ```sh
   npm run ctl -- restore
   npm run ctl -- state
   ```

   Verify `lastRestoreResponse.response.accepted` is true. Query until the matching summary is `REVERTED` and no active child remains. The parent enters `FAILED` after a reverted rotation and requires activation before accepting another rotation. A restore acceptance is not proof that compensation has already finished. `npm run demo` additionally asserts that the fake inventory returned to its prior trust and client state. A later restore command receives a fresh child Update ID, allowing compensation to be retried after an earlier attempt failed.

3. Resume and start another held rotation, then request native parent cancellation.

   ```sh
   npm run ctl -- activate
   npm run ctl -- rotate cancel-002
   npm run ctl -- state
   temporal workflow cancel --workflow-id certificate-dev-platform-revert-demo
   temporal workflow describe --workflow-id certificate-dev-platform-revert-demo
   ```

   Verify the child was active before cancellation, then inspect the final Workflow status with `describe`. Parent cancellation requests child cancellation and waits for its revert path for up to one `rotationTimeoutMs`. A normal rotation first gets that duration to run, then another equal duration after cancellation is requested. If the second window expires, the state records `REVERT_TIMED_OUT`; that is an unresolved child, and further rotations stay blocked while it remains active. Parent closure uses `REQUEST_CANCEL` so it does not terminate remaining compensation.

4. Clean up the local processes.

   Stop the worker and local server with Ctrl-C after the scenario has completed. Verify both processes exit. Unset `CREDENTIAL_SCOPE` in the CLI terminal before returning to the default fixture identity.

## Inspect failures

Run `npm run ctl -- state` to read status, options, inventories, active snapshot, recent Update outcomes, and recent rotation summaries. Run `npm run ctl -- state --failures` to include attributable error details. Verify that the relevant operation ID and its terminal summary match the failed operation. Check Temporal Event History for the authoritative execution record within the server's retention period.

Ordinary fake rotation failures after a possible mutation run compensation before returning `FAILED`. For example, rotating only `orders-dev` cannot retire an old CA still used by another namespace; the child restores its prior inventory and retains the retirement failure in the summary. If compensation itself fails, the child remains open. Inspect its `getRotationState` Query for the original `failure`, `compensationFailure`, and `compensationStatus`, correct the adapter problem, and issue `restore` again. Keep new rotations blocked until the unresolved child reaches a safe boundary.

An externally owned inventory failure or unsuccessful child puts the entity in `FAILED`. Correct the input or adapter problem, ensure no child remains unresolved, and use a complete `activate` Update to resume. The future production adapters must classify configuration and permission errors, use bounded retries for transient errors, and make mutation retries idempotent.

## Inspect and preserve the archive

```sh
npm run ctl -- archive 20 0
npm run ctl -- archive 20 20
```

The command reads paginated, distinct rotation summaries and diagnostic snapshots for the CLI's Credential identity. Use `nextOffset` for the next page. Each collection has its own 1-100 item page limit with the same offset. State Queries show recent windows; archived rotation summaries and accepted operation IDs remain available after they leave those windows. `historyArchive` reports the latest acknowledged archive reference and durable rotation and operation counts.

The Workflow archives all completed rotation summaries and accepted operation IDs before pruning, then carries at most 512 KiB of JSON into its next run. Recent carryover windows retain 16 rotations, 128 IDs, 64 Update outcomes, and 32 failures, plus IDs for pending work. Update outcomes and nonrotation failures are bounded diagnostic logs, so archive snapshots can omit older entries from those logs. Rollover also occurs after 128 accepted Updates or a server history recommendation while the Credential is suspended or failed.

All workers for these Credentials must use the same SQLite file on the same host. Set `CREDENTIAL_ARCHIVE_PATH` consistently on the worker and CLI when changing its location. Preserve the file and its SQLite sidecar files, use a SQLite-aware backup procedure, and restrict filesystem access. Archive write failures block pruning and rollover. A running Workflow chain supplies its last acknowledged checkpoint on archive reads and writes; a missing checkpoint produces `ArchiveUnavailable`. Restore the correct archive before activating the failed Credential. A new chain without that reference cannot detect an archive lost before it started, so reusing an identity after deleting its archive requires an explicit recovery decision.

Admission limits reject initial requests above 32 KiB, Updates above 8 KiB, and individual inventory responses or rotation summaries above 64 KiB of JSON. Recent rotation history is also capped at 128 KiB after checkpointing. At most 32 manual rotations can wait in the queue; operator reasons are limited to 1,024 characters and requester names to 128. Reduce scope or request size, or wait for queued work to finish, before retrying a rejected request.

## Limits before production use

The fake worker cannot establish live mTLS connectivity or zero downtime. Concrete Temporal Cloud Operations API, PKI, Vault/VSO, and Kubernetes adapters and a real Rotate child are deferred. Production deployment also needs appropriate permissions, Worker Deployment setup for `PINNED`, and a durable shared archive adapter with a defined concurrency and recovery design. The local SQLite archive supplies no availability guarantee across hosts.
