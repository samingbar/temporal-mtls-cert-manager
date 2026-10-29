import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, before, test } from 'node:test';
import { ApplicationFailure } from '@temporalio/common';
import type { WorkflowHandle } from '@temporalio/client';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { bundleWorkflowCode, DefaultLogger, Runtime, Worker } from '@temporalio/worker';
import type { WorkflowBundle } from '@temporalio/worker';
import type { ActivateCertificateRequest, CredentialState } from '../src/contracts';
import type { credentialWorkflow } from '../src/workflows/credential';
import * as messages from '../src/messages';
import { LIMITS, utf8JsonBytes } from '../src/limits';
import { DAY_MS } from '../src/options';
import { createFakeActivities, type FakeActivityOptions } from '../demos/credential-supervisor/fake-activities';
import { startPinnedCredential } from '../demos/credential-supervisor/start-pinned';
import { createCredentialArchiveStore } from '../demos/credential-supervisor/archive-store';

let environment: TestWorkflowEnvironment;
let workflowBundle: WorkflowBundle;
const version = { deploymentName: 'credential-bounds-tests', buildId: 'bounds-v1' };
const actor = { reason: 'Bounded history regression', requester: 'test-suite' };
type Handle = WorkflowHandle<typeof credentialWorkflow>;
type Fake = ReturnType<typeof createFakeActivities>;
type Activities = Fake['activities'];

before(async () => {
  Runtime.install({ logger: new DefaultLogger('ERROR') });
  environment = await TestWorkflowEnvironment.createLocal();
  workflowBundle = await bundleWorkflowCode({
    workflowsPath: require.resolve('../demos/credential-supervisor/fake-workflows'),
    logger: new DefaultLogger('ERROR'),
  });
}, { timeout: 120_000 });
after(async () => { await environment?.teardown(); });

function request(overrides: Partial<ActivateCertificateRequest> = {}): ActivateCertificateRequest {
  return {
    credentialType: 'certificate', environment: 'bounds-test', teamName: 'platform', scopeKey: randomUUID(),
    temporalNamespaceOptions: { names: ['orders-dev', 'payments-dev', 'tagged-dev'] },
    ...overrides,
  };
}

function activation(state: CredentialState): ActivateCertificateRequest {
  return {
    ...state.identity, temporalNamespaceOptions: state.options!.temporalNamespaces,
    credentialOptions: state.options!.credential, kubernetesOptions: state.options!.kubernetes,
    executionOptions: { rotationTimeoutMs: state.options!.execution.rotationTimeoutMs },
  };
}

function gate() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}

async function until(predicate: () => boolean | Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${label}`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

async function stateWhen(handle: Handle, predicate: (state: CredentialState) => boolean): Promise<CredentialState> {
  let state!: CredentialState;
  await until(async () => {
    state = (await handle.query(messages.getState, { includeFailureDetail: true })).state;
    return predicate(state);
  }, 'Credential state');
  return state;
}

async function scenario(
  input: ActivateCertificateRequest,
  configure: (fake: Fake) => Activities | Promise<Activities>,
  body: (handle: Handle, fake: Fake) => Promise<void>,
  fakeOptions: FakeActivityOptions = {},
): Promise<void> {
  const fake = createFakeActivities({ ...fakeOptions, client: environment.client, stepDelayMs: 0 });
  const taskQueue = `bounds-${randomUUID()}`;
  const worker = await Worker.create({
    connection: environment.nativeConnection, workflowBundle, taskQueue, activities: await configure(fake),
    workerDeploymentOptions: { version, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED' },
  });
  try {
    await worker.runUntil(async () => {
      const handle = await startPinnedCredential(environment.client, input, {
        workflowId: `bounds-${randomUUID()}`, taskQueue, version,
      });
      try { await body(handle, fake); }
      finally { await handle.cancel().catch(() => undefined); }
    });
  } finally { fake.close(); }
}

for (const suspended of [false, true]) {
  test(`${suspended ? 'suspended' : 'idle empty-scope'} entity rolls over after operator Updates without a rotation`, { timeout: 60_000 }, async () => {
    const input = request({
      temporalNamespaceOptions: { names: [] },
      ...(suspended ? { executionOptions: { suspend: actor } } : {}),
    });
    await scenario(input, fake => fake.activities, async handle => {
      await stateWhen(handle, state => state.status === (suspended ? 'SUSPENDED' : 'ACTIVE'));
      const originalRun = (await handle.describe()).runId;
      for (let index = 0; index < LIMITS.updatesPerRun; index += 1) {
        await handle.executeUpdate(messages.putOptions, { args: [{}] });
      }
      await until(async () => (await handle.describe()).runId !== originalRun, 'idle history rollover');
      const state = await stateWhen(handle, state => !!state.historyArchive && state.status === (suspended ? 'SUSPENDED' : 'ACTIVE'));
      assert.equal(state.revision, LIMITS.updatesPerRun);
      assert.equal(state.historyArchive!.operationCount, 0);
      assert.deepEqual(state.rotationHistory, []);
      assert.ok(state.updateOutcomes.length <= LIMITS.updateOutcomes);
      assert.ok(utf8JsonBytes(state) < LIMITS.carryoverBytes);
      assert.equal(state.nextRotationAtMs, undefined);
      await handle.executeUpdate(messages.deactivate, { args: [actor] });
      await handle.result();
    });
  });
}

test('archive failure preserves rotation and deduplication state; recovery retries the checkpoint safely', { timeout: 60_000 }, async () => {
  let archiveUnavailable = true;
  let lookupUnavailable = false;
  let archiveAttempts = 0;
  await scenario(request(), fake => ({
    ...fake.activities,
    async archiveCredentialHistory(input) {
      archiveAttempts += 1;
      if (archiveUnavailable) throw ApplicationFailure.nonRetryable('Archive unavailable', 'ArchiveTestFailure');
      return fake.activities.archiveCredentialHistory(input);
    },
    async lookupArchivedOperation(input) {
      if (lookupUnavailable) throw ApplicationFailure.nonRetryable('Deduplication index unavailable', 'LookupTestFailure');
      return fake.activities.lookupArchivedOperation(input);
    },
  }), async (handle, fake) => {
    await stateWhen(handle, state => state.status === 'ACTIVE');
    const originalRun = (await handle.describe()).runId;
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'before-archive-outage' }] });
    const failed = await stateWhen(handle, state => state.status === 'FAILED' && state.failures.some(failure => failure.type === 'ArchiveUnavailable'));
    assert.equal(archiveAttempts, 1);
    assert.equal((await handle.describe()).runId, originalRun);
    assert.deepEqual(failed.rotationHistory.map(summary => summary.operationId), ['before-archive-outage']);
    assert.deepEqual(failed.acceptedOperationIds, ['before-archive-outage']);
    assert.equal(failed.historyArchive, undefined);
    assert.equal((await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'before-archive-outage' }] })).duplicate, true);

    archiveUnavailable = false;
    await handle.executeUpdate(messages.activate, { args: [activation(failed)] });
    await until(async () => (await handle.describe()).runId !== originalRun, 'operator recovery retries the failed checkpoint');
    await stateWhen(handle, state => state.status === 'ACTIVE' && state.historyArchive?.operationCount === 1);
    const recoveryRun = (await handle.describe()).runId;

    // A successful archive write does not permit new work when the durable index cannot be read.
    lookupUnavailable = true;
    await assert.rejects(handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'unverified-operation' }] }));
    const rejected = (await handle.query(messages.getState, {})).state;
    assert.deepEqual(rejected.acceptedOperationIds, ['before-archive-outage']);
    assert.deepEqual(rejected.pendingRotations, []);
    assert.equal(fake.inspect().events.some(event => event.endsWith(': unverified-operation')), false);

    lookupUnavailable = false;
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'after-archive-recovery' }] });
    await until(async () => (await handle.describe()).runId !== recoveryRun, 'checkpoint recovery rollover');
    const recovered = await stateWhen(handle, state => state.status === 'ACTIVE' && state.historyArchive?.operationCount === 2);
    assert.equal(recovered.historyArchive!.rotationCount, 2);
    assert.equal((await fake.activities.lookupArchivedOperation({ identity: recovered.identity, operationId: 'before-archive-outage' })).found, true);
    assert.equal((await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'before-archive-outage' }] })).duplicate, true);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('two concurrent cache misses for one operation admit and issue it once', { timeout: 60_000 }, async () => {
  const lookupGate = gate();
  let lookups = 0;
  try {
    await scenario(request(), fake => ({
      ...fake.activities,
      async lookupArchivedOperation(input) {
        const result = await fake.activities.lookupArchivedOperation(input);
        if (input.operationId === 'concurrent-operation') {
          assert.equal(result.found, false);
          lookups += 1;
          await lookupGate.promise;
        }
        return result;
      },
    }), async (handle, fake) => {
      await stateWhen(handle, state => state.status === 'ACTIVE');
      const first = handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'concurrent-operation' }] });
      const second = handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'concurrent-operation' }] });
      void first.catch(() => undefined);
      void second.catch(() => undefined);
      await until(() => lookups === 2, 'both concurrent archive misses');
      lookupGate.release();
      const results = await Promise.all([first, second]);
      assert.deepEqual(results.map(result => result.duplicate).sort(), [false, true]);
      const state = await stateWhen(handle, state => state.status === 'ACTIVE' && state.rotationHistory.length === 1 && !!state.historyArchive);
      assert.equal(state.historyArchive!.operationCount, 1);
      assert.equal(fake.inspect().events.filter(event => event === 'FAKE issue: concurrent-operation').length, 1);
      assert.deepEqual(state.pendingRotations, []);
      await handle.executeUpdate(messages.deactivate, { args: [actor] });
      await handle.result();
    });
  } finally { lookupGate.release(); }
});

test('an operation retained only in the archive is still a duplicate', { timeout: 60_000 }, async () => {
  const input = request();
  await scenario(input, async fake => {
    await fake.activities.archiveCredentialHistory({
      identity: input, checkpointId: 'seed-old-history', operationIds: ['archived-old-operation'],
      rotationHistory: [], updateOutcomes: [], failures: [],
    });
    return fake.activities;
  }, async (handle, fake) => {
    const initial = await stateWhen(handle, state => state.status === 'ACTIVE');
    assert.deepEqual(initial.acceptedOperationIds, []);
    const duplicate = await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'archived-old-operation' }] });
    assert.deepEqual(duplicate, { operationId: 'archived-old-operation', accepted: true, duplicate: true });
    const state = (await handle.query(messages.getState, {})).state;
    assert.deepEqual(state.acceptedOperationIds, []);
    assert.deepEqual(state.pendingRotations, []);
    assert.deepEqual(state.rotationHistory, []);
    assert.deepEqual(fake.inspect().events, []);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('oversized Update fields are rejected before acceptance and do not mutate state', { timeout: 60_000 }, async () => {
  await scenario(request(), fake => fake.activities, async handle => {
    await stateWhen(handle, state => state.status === 'ACTIVE');
    const acceptedBefore = (await handle.fetchHistory()).events?.filter(event => event.workflowExecutionUpdateAcceptedEventAttributes).length;
    const oversized = { ...actor, operationId: 'oversized-operation', unknownField: 'x'.repeat(LIMITS.updateBytes) };
    await assert.rejects(handle.executeUpdate(messages.rotate, { args: [oversized] }));
    const state = (await handle.query(messages.getState, {})).state;
    assert.deepEqual(state.acceptedOperationIds, []);
    assert.deepEqual(state.pendingRotations, []);
    assert.deepEqual(state.updateOutcomes, []);
    const acceptedAfter = (await handle.fetchHistory()).events?.filter(event => event.workflowExecutionUpdateAcceptedEventAttributes).length;
    assert.equal(acceptedAfter, acceptedBefore);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('checkpoint drains an in-flight deduplication lookup and releases parked Updates without deadlock', { timeout: 60_000 }, async () => {
  const lookupGate = gate();
  let lookupEntered = false;
  let archiveAttempts = 0;
  try {
    await scenario(request(), fake => ({
      ...fake.activities,
      async lookupArchivedOperation(input) {
        const result = await fake.activities.lookupArchivedOperation(input);
        if (input.operationId === 'checkpoint-operation') {
          lookupEntered = true;
          await lookupGate.promise;
        }
        return result;
      },
      async archiveCredentialHistory(input) {
        archiveAttempts += 1;
        return fake.activities.archiveCredentialHistory(input);
      },
    }), async (handle, fake) => {
      await stateWhen(handle, state => state.status === 'ACTIVE');
      const originalRun = (await handle.describe()).runId;
      for (let index = 0; index < LIMITS.updatesPerRun - 1; index += 1) {
        await handle.executeUpdate(messages.putOptions, { args: [{}] });
      }
      const rotation = await handle.startUpdate(messages.rotate, {
        args: [{ ...actor, operationId: 'checkpoint-operation' }], waitForStage: 'ACCEPTED',
      });
      await until(() => lookupEntered, 'in-flight archive lookup');
      // Give the main loop a task boundary to begin draining the active Update body.
      await new Promise(resolve => setTimeout(resolve, 50));
      let parkedFinished = false;
      const parked = await handle.startUpdate(messages.putOptions, {
        args: [{ credentialOptions: { ttlMs: 60 * DAY_MS } }], waitForStage: 'ACCEPTED',
      });
      const parkedResult = parked.result().then(result => { parkedFinished = true; return result; });
      void parkedResult.catch(() => undefined);
      await new Promise(resolve => setTimeout(resolve, 50));
      assert.equal(parkedFinished, false, 'Update body should park outside the checkpoint drain counter');
      assert.equal(archiveAttempts, 0, 'Archive must not race an in-flight deduplication lookup');
      lookupGate.release();
      assert.equal((await rotation.result()).duplicate, false);
      assert.equal((await parkedResult).options.credential.ttlMs, 60 * DAY_MS);
      await until(async () => (await handle.describe()).runId !== originalRun, 'checkpoint rollover with parked Update');
      const state = await stateWhen(handle, state => state.status === 'ACTIVE' && state.rotationHistory.length === 1);
      assert.equal(state.options!.credential.ttlMs, 60 * DAY_MS);
      assert.equal(fake.inspect().events.filter(event => event === 'FAKE issue: checkpoint-operation').length, 1);
      assert.equal((await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'checkpoint-operation' }] })).duplicate, true);
      await handle.executeUpdate(messages.deactivate, { args: [actor] });
      await handle.result();
    });
  } finally { lookupGate.release(); }
});

for (const mode of ['complete', 'fail'] as const) {
  test(`large recent summaries are pruned by bytes only after the complete history is archived (${mode})`, { timeout: 60_000 }, async () => {
    const directory = mkdtempSync(join(tmpdir(), 'credential-history-bounds-'));
    const archivePath = join(directory, 'history.sqlite');
    const initialNotAfterMs = Date.now() + 30 * DAY_MS;
    const namespaces = Array.from({ length: 128 }, (_, index) => ({
      name: `namespace-${String(index).padStart(3, '0')}-${'n'.repeat(186)}`,
      tags: { team: 'bulk' }, trustedCAs: ['FAKE-CA-INITIAL'], clientCA: 'FAKE-CA-INITIAL', notAfterMs: initialNotAfterMs,
    }));
    const input = request({ temporalNamespaceOptions: { names: [], tagSelector: { team: 'bulk' } } });
    try {
      await scenario(input, fake => fake.activities, async (handle, fake) => {
        await stateWhen(handle, state => state.status === 'ACTIVE' && state.namespaceInventory?.namespaces.length === namespaces.length);
        let previousRun = '';
        for (let index = 0; index < 6; index += 1) {
          if (mode === 'fail' && index > 0) {
            const failed = await stateWhen(handle, state => state.status === 'FAILED' && !state.activeRotation);
            await handle.executeUpdate(messages.activate, { args: [activation(failed)] });
            await stateWhen(handle, state => state.status === 'ACTIVE');
          }
          previousRun = (await handle.describe()).runId;
          await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: `large-summary-${index}` }] });
          const expectedStatus = mode === 'fail' ? 'FAILED' : 'ACTIVE';
          await stateWhen(handle, state => state.status === expectedStatus && !state.activeRotation
            && state.rotationHistory.some(summary => summary.operationId === `large-summary-${index}`));
          // Five summaries with these namespace names exceed the byte window. Failed
          // rotations must then roll over independently of the successful-child path.
          if (mode === 'complete' || index >= 4) {
            await until(async () => (await handle.describe()).runId !== previousRun, 'large-summary rollover');
            await stateWhen(handle, state => state.status === expectedStatus && state.historyArchive?.rotationCount === index + 1);
          }
        }
        const state = (await handle.query(messages.getState, {})).state;
        assert.ok(state.rotationHistory.length > 0);
        assert.ok(state.rotationHistory.length < 6, 'The byte budget must prune summaries before the count limit is reached');
        assert.ok(utf8JsonBytes(state.rotationHistory) <= LIMITS.recentHistoryBytes);
        assert.equal(state.historyArchive!.rotationCount, 6);
        assert.equal(state.historyArchive!.operationCount, 6);
        assert.equal(state.rotationHistory.at(-1)!.operationId, 'large-summary-5');
        assert.ok(state.rotationHistory.every(summary => summary.status === (mode === 'fail' ? 'FAILED' : 'SUCCEEDED')));
        if (mode === 'fail') {
          assert.ok(state.rotationHistory.every(summary => summary.compensationStatus === 'SUCCEEDED'));
          assert.equal(fake.inspect().events.filter(event => event.startsWith('FAKE revert:')).length, 6);
          assert.ok(fake.inspect().namespaces.every(namespace => namespace.clientCA === 'FAKE-CA-INITIAL'
            && namespace.trustedCAs.length === 1 && namespace.trustedCAs[0] === 'FAKE-CA-INITIAL'));
        }

        const archive = createCredentialArchiveStore({ path: archivePath });
        try {
          const complete = archive.inspectHistory({ identity: state.identity, limit: 20 }).rotationHistory;
          assert.equal(complete.length, 6);
          assert.deepEqual(complete.map(summary => summary.operationId), Array.from({ length: 6 }, (_, index) => `large-summary-${index}`));
          assert.ok(complete.every(summary => summary.namespaceNames.length === namespaces.length));
          assert.ok(complete.every(summary => summary.status === (mode === 'fail' ? 'FAILED' : 'SUCCEEDED')));
          if (mode === 'fail') assert.ok(complete.every(summary => summary.compensationStatus === 'SUCCEEDED'));
        } finally { archive.close(); }

        const previous = environment.client.workflow.getHandle(handle.workflowId, previousRun);
        const history = await previous.fetchHistory();
        const continued = history.events?.find(event => event.workflowExecutionContinuedAsNewEventAttributes)
          ?.workflowExecutionContinuedAsNewEventAttributes;
        const payload = continued?.input?.payloads?.[0];
        assert.ok(payload?.data, 'History rollover should have a Continue-As-New payload');
        const carriedInput = JSON.parse(Buffer.from(payload.data).toString('utf8')) as ActivateCertificateRequest;
        assert.ok(utf8JsonBytes(carriedInput) <= LIMITS.carryoverBytes);
        assert.equal(carriedInput.carryover!.state.namespaceInventory, undefined);
        assert.equal(carriedInput.carryover!.state.deploymentInventory, undefined);
        await handle.executeUpdate(messages.deactivate, { args: [actor] });
        await handle.result();
      }, { archivePath, namespaces, mode });
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });
}
