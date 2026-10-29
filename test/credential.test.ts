import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { randomUUID } from 'node:crypto';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { bundleWorkflowCode, DefaultLogger, Runtime, Worker } from '@temporalio/worker';
import type { WorkflowBundle } from '@temporalio/worker';
import type { WorkflowHandle } from '@temporalio/client';
import type { ActivateCertificateRequest, CredentialState } from '../src/contracts';
import type { credentialWorkflow } from '../src/workflows/credential';
import * as messages from '../src/messages';
import { createFakeActivities, type FakeActivityOptions } from '../demos/credential-supervisor/fake-activities';
import { DAY_MS } from '../src/options';

let env: TestWorkflowEnvironment;
let workflowBundle: WorkflowBundle;
const TEST_VERSION = { deploymentName: 'credential-tests', buildId: 'test-v1' };
before(async () => {
  Runtime.install({ logger: new DefaultLogger('ERROR') });
  env = await TestWorkflowEnvironment.createLocal();
  workflowBundle = await bundleWorkflowCode({ workflowsPath: require.resolve('../demos/credential-supervisor/fake-workflows'), logger: new DefaultLogger('ERROR') });
}, { timeout: 120_000 });
after(async () => { await env?.teardown(); });

const request = (overrides: Partial<ActivateCertificateRequest> = {}): ActivateCertificateRequest => ({
  credentialType: 'certificate', environment: 'test', teamName: 'platform', scopeKey: 'all',
  temporalNamespaceOptions: { names: ['payments-dev', 'orders-dev', 'orders-dev'], tagSelector: { team: 'platform' } },
  ...overrides,
});
type Handle = WorkflowHandle<typeof credentialWorkflow>;
async function startPinned(input: ActivateCertificateRequest, taskQueue: string, workflowTaskTimeoutMs?: number): Promise<Handle> {
  const workflowId = `test-${randomUUID()}`;
  const deadline = Date.now() + 20_000;
  while (true) {
    try {
      return await env.client.workflow.start<typeof credentialWorkflow>('credentialWorkflow', {
        workflowId, taskQueue, args: [input], versioningOverride: { pinnedTo: TEST_VERSION },
        ...(workflowTaskTimeoutMs ? { workflowTaskTimeout: workflowTaskTimeoutMs } : {}),
      });
    } catch (error) {
      const cause = (error as { cause?: { code?: number; details?: string } }).cause;
      if (Date.now() >= deadline || cause?.code !== 9 || !cause.details?.includes('is not present in task queue')) throw error;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
}
async function waitState(handle: Handle, predicate: (state: CredentialState) => boolean): Promise<CredentialState> {
  const deadline = Date.now() + 20_000;
  let state: CredentialState;
  do {
    state = (await handle.query(messages.getState, { includeFailureDetail: true })).state;
    if (predicate(state)) return state;
    await new Promise(resolve => setTimeout(resolve, 25));
  } while (Date.now() < deadline);
  throw new Error(`State did not converge: ${JSON.stringify(state!)}`);
}
function activation(state: CredentialState): ActivateCertificateRequest {
  return { ...state.identity, temporalNamespaceOptions: state.options!.temporalNamespaces,
    credentialOptions: state.options!.credential, kubernetesOptions: state.options!.kubernetes,
    executionOptions: { rotationTimeoutMs: state.options!.execution.rotationTimeoutMs } };
}
async function scenario(
  input: ActivateCertificateRequest,
  fakeOptions: FakeActivityOptions,
  body: (handle: Handle, fake: ReturnType<typeof createFakeActivities>) => Promise<void>,
): Promise<void> {
  const fake = createFakeActivities({ client: env.client, stepDelayMs: 0, ...fakeOptions });
  const taskQueue = `test-${randomUUID()}`;
  const worker = await Worker.create({ connection: env.nativeConnection, workflowBundle, taskQueue, activities: fake.activities,
    workerDeploymentOptions: { version: TEST_VERSION, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED' } });
  await worker.runUntil(async () => {
    const handle = await startPinned(input, taskQueue);
    try { await body(handle, fake); }
    finally { await handle.cancel().catch(() => undefined); }
  });
}
const actor = { reason: 'Integration test', requester: 'test-suite' };

test('initial inventory resolves explicit + tagged scope, canonicalizes and schedules earliest expiry', async () => {
  const expiry = Date.now() + 30 * DAY_MS;
  await scenario(request(), { initialNotAfterMs: expiry }, async handle => {
    const state = await waitState(handle, state => state.status === 'ACTIVE' && !!state.namespaceInventory);
    assert.deepEqual(state.namespaceInventory!.namespaces.map(item => item.namespace), ['orders-dev', 'payments-dev', 'tagged-dev']);
    assert.equal(state.nextRotationAtMs, expiry - 14 * DAY_MS);
    assert.deepEqual(state.deploymentInventory!.deployments, []);
    assert.equal(state.deploymentInventory!.skipped, true);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('manual rotation preserves trust order, continues as new, and deduplicates a retried operation', async () => {
  await scenario(request(), {}, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    const firstRun = (await handle.describe()).runId;
    const accepted = await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'manual-1' }] });
    assert.equal(accepted.duplicate, false);
    const state = await waitState(handle, state => state.rotationHistory.length === 1 && state.status === 'ACTIVE');
    assert.equal(state.rotationHistory[0].status, 'SUCCEEDED');
    const deadline = Date.now() + 10_000;
    while ((await handle.describe()).runId === firstRun && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 25));
    assert.notEqual((await handle.describe()).runId, firstRun);
    const duplicate = await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'manual-1' }] });
    assert.equal(duplicate.duplicate, true);
    assert.equal((await handle.query(messages.getState, {})).state.rotationHistory.length, 1);
    assert.deepEqual(fake.inspect().events, ['issue', 'add-trust', 'rollout-clients', 'remove-old-trust', 'retire-old'].map(step => `FAKE ${step}: manual-1`));
    for (const namespace of fake.inspect().namespaces) assert.deepEqual(namespace.trustedCAs, [namespace.clientCA]);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('past recommendation triggers a scheduled rotation and persists its summary', async () => {
  await scenario(request(), { initialNotAfterMs: Date.now() + DAY_MS }, async handle => {
    const state = await waitState(handle, state => state.rotationHistory.length > 0 && state.status === 'ACTIVE' && (state.nextRotationAtMs ?? 0) > Date.now());
    assert.equal(state.rotationHistory[0].status, 'SUCCEEDED');
    assert.equal(state.rotationHistory[0].requester, 'credential-scheduler');
    assert.ok(state.nextRotationAtMs! > Date.now());
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('startup suspension exposes inventory, requires activate, and clears stored suspension', async () => {
  await scenario(request({ executionOptions: { suspend: actor } }), {}, async handle => {
    const state = await waitState(handle, state => state.status === 'SUSPENDED' && !!state.namespaceInventory);
    assert.equal(state.nextRotationAtMs, undefined);
    await assert.rejects(handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'inactive-1' }] }));
    await handle.executeUpdate(messages.activate, { args: [activation(state)] });
    const active = await waitState(handle, state => state.status === 'ACTIVE' && !!state.nextRotationAtMs);
    assert.equal(active.options!.execution.suspend, undefined);
    assert.equal(active.suspension, undefined);
    await handle.executeUpdate(messages.suspend, { args: [actor] });
    assert.equal((await handle.query(messages.getState, {})).state.status, 'SUSPENDED');
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('a future scheduled instant fires a durable timer without a manual request', async () => {
  await scenario(request({ credentialOptions: { rotationOffsetMs: 0 } }), { initialNotAfterMs: Date.now() + 4_000 }, async handle => {
    const initial = await waitState(handle, state => state.status === 'ACTIVE' && !!state.namespaceInventory);
    assert.ok(initial.nextRotationAtMs! > Date.now());
    assert.deepEqual(initial.rotationHistory, []);
    const state = await waitState(handle, state => state.rotationHistory.length === 1 && state.status === 'ACTIVE');
    assert.equal(state.rotationHistory[0].status, 'SUCCEEDED');
    assert.equal(state.rotationHistory[0].requester, 'credential-scheduler');
    assert.ok(state.rotationHistory[0].startedAtMs >= initial.nextRotationAtMs!);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('empty certificate scope stays active without a scheduled rotation', async () => {
  await scenario(request({ temporalNamespaceOptions: { names: [] } }), {}, async handle => {
    const state = await waitState(handle, state => state.status === 'ACTIVE' && !!state.namespaceInventory);
    assert.equal(state.nextRotationAtMs, undefined);
    assert.deepEqual(state.namespaceInventory!.namespaces, []);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('scope/options changes preserve active snapshot; restore bridges child Update and restores old trust', async () => {
  await scenario(request(), { mode: 'hold' }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'restore-1' }] });
    await waitState(handle, state => !!state.activeRotation && fake.inspect().events.includes('FAKE add-trust: restore-1'));
    await handle.executeUpdate(messages.putTemporalNamespaces, { args: [{ temporalNamespaceOptions: { names: ['orders-dev'] } }] });
    await handle.executeUpdate(messages.putOptions, { args: [{ credentialOptions: { ttlMs: 60 * DAY_MS } }] });
    const changed = (await handle.query(messages.getState, {})).state;
    assert.equal(changed.options!.credential.ttlMs, 60 * DAY_MS);
    assert.equal(changed.activeRotation!.request.options.credential.ttlMs, 90 * DAY_MS);
    assert.equal(changed.activeRotation!.request.namespaceInventory.namespaces.length, 3);
    const restored = await handle.executeUpdate(messages.restore, { args: [actor] });
    assert.equal(restored.response.accepted, true);
    const failed = await waitState(handle, state => state.rotationHistory.length === 1 && !state.activeRotation);
    assert.equal(failed.rotationHistory[0].status, 'REVERTED');
    assert.equal(failed.status, 'FAILED');
    for (const namespace of fake.inspect().namespaces) assert.deepEqual(namespace.trustedCAs, ['FAKE-CA-INITIAL']);
    await handle.executeUpdate(messages.activate, { args: [activation(failed)] });
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'restore-2' }] });
    const second = await waitState(handle, state => state.activeRotation?.request.operationId === 'restore-2');
    assert.deepEqual(second.activeRotation!.request.namespaceInventory.namespaces.map(item => item.namespace), ['orders-dev']);
    assert.equal(second.activeRotation!.request.options.credential.ttlMs, 60 * DAY_MS);
    await handle.executeUpdate(messages.restore, { args: [actor] });
    await waitState(handle, state => !state.activeRotation);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('rotation timeout cancels child and waits for compensation before allowing recovery', async () => {
  await scenario(request({ executionOptions: { rotationTimeoutMs: 1_000 } }), { mode: 'hold' }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'timeout-1' }] });
    const state = await waitState(handle, state => state.rotationHistory.length > 0);
    assert.equal(state.rotationHistory[0].status, 'TIMED_OUT');
    assert.equal(state.status, 'FAILED');
    assert.equal(state.activeRotation, undefined);
    assert.ok(fake.inspect().events.includes('FAKE revert: timeout-1'));
    for (const namespace of fake.inspect().namespaces) assert.deepEqual(namespace.trustedCAs, ['FAKE-CA-INITIAL']);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('native parent cancellation waits for child compensation', async () => {
  await scenario(request(), { mode: 'hold-after-rollout' }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'cancel-1' }] });
    await waitState(handle, state => !!state.activeRotation && fake.inspect().events.includes('FAKE rollout-clients: cancel-1'));
    await handle.cancel();
    await assert.rejects(handle.result());
    assert.ok(fake.inspect().events.includes('FAKE revert: cancel-1'));
    for (const namespace of fake.inspect().namespaces) {
      assert.equal(namespace.clientCA, 'FAKE-CA-INITIAL');
      assert.deepEqual(namespace.trustedCAs, ['FAKE-CA-INITIAL']);
    }
  });
});

test('bounded revert timeout blocks further rotations until unresolved child reaches terminal state', async () => {
  await scenario(request({ executionOptions: { rotationTimeoutMs: 750 } }), { mode: 'hold', holdCompensation: true }, async handle => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'stuck-1' }] });
    const state = await waitState(handle, state => state.rotationHistory.some(item => item.status === 'REVERT_TIMED_OUT'));
    assert.equal(state.status, 'FAILED');
    assert.ok(state.activeRotation);
    await assert.rejects(handle.executeUpdate(messages.activate, { args: [activation(state)] }));
    await assert.rejects(handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'blocked-1' }] }));
    await env.client.workflow.getHandle(state.activeRotation!.childWorkflowId).terminate('Test cleanup of deliberately unresponsive fake child');
    await waitState(handle, state => !state.activeRotation);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('child failure is recorded; explicit activate recovers entity after compensation', async () => {
  await scenario(request(), { mode: 'fail' }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'fail-1' }] });
    const state = await waitState(handle, state => state.status === 'FAILED');
    assert.equal(state.rotationHistory[0].status, 'FAILED');
    assert.ok(state.failures.length > 0);
    assert.ok(fake.inspect().events.includes('FAKE revert: fail-1'));
    await handle.executeUpdate(messages.activate, { args: [activation(state)] });
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('two queued manual rotations survive Continue-As-New exactly once', async () => {
  await scenario(request(), { stepDelayMs: 150 }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    const originalRun = (await handle.describe()).runId;
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'queued-1' }] });
    await waitState(handle, state => state.activeRotation?.request.operationId === 'queued-1');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'queued-2' }] });
    const queued = await waitState(handle, state => state.pendingRotations.some(item => item.operationId === 'queued-2'));
    assert.equal(queued.activeRotation!.request.operationId, 'queued-1');
    assert.equal((await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'queued-2' }] })).duplicate, true);

    const completed = await waitState(handle, state => state.status === 'ACTIVE' && state.rotationHistory.length === 2);
    assert.deepEqual(completed.rotationHistory.map(item => [item.operationId, item.status]), [
      ['queued-1', 'SUCCEEDED'], ['queued-2', 'SUCCEEDED'],
    ]);
    assert.deepEqual(completed.pendingRotations, []);
    for (const operationId of ['queued-1', 'queued-2']) {
      assert.equal(completed.acceptedOperationIds.filter(id => id === operationId).length, 1);
      assert.equal(fake.inspect().events.filter(event => event === `FAKE issue: ${operationId}`).length, 1);
      assert.equal((await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId }] })).duplicate, true);
    }
    assert.notEqual((await handle.describe()).runId, originalRun);
    assert.equal((await handle.query(messages.getState, {})).state.rotationHistory.length, 2);
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('suspend waits for an active rotation to finish successfully without cancelling it', async () => {
  await scenario(request(), { stepDelayMs: 200 }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'suspend-boundary' }] });
    await waitState(handle, state => !!state.activeRotation && fake.inspect().events.includes('FAKE add-trust: suspend-boundary'));
    let acknowledged = false;
    const suspension = handle.executeUpdate(messages.suspend, { args: [actor] }).then(response => {
      acknowledged = true;
      return response;
    });
    void suspension.catch(() => undefined);
    const suspending = await waitState(handle, state => state.status === 'SUSPENDING');
    assert.equal(acknowledged, false);
    assert.equal(suspending.activeRotation!.request.operationId, 'suspend-boundary');
    assert.equal((await suspension).status, 'SUSPENDED');

    const suspended = await waitState(handle, state => state.status === 'SUSPENDED' && !state.activeRotation);
    assert.equal(suspended.rotationHistory[0].status, 'SUCCEEDED');
    assert.equal(suspended.nextRotationAtMs, undefined);
    assert.equal(fake.inspect().events.includes('FAKE revert: suspend-boundary'), false);
    assert.ok(fake.inspect().events.includes('FAKE retire-old: suspend-boundary'));
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('deactivate waits for an active rotation to finish successfully without cancelling it', async () => {
  await scenario(request(), { stepDelayMs: 200 }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    const originalRun = (await handle.describe()).runId;
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'deactivate-boundary' }] });
    await waitState(handle, state => !!state.activeRotation && fake.inspect().events.includes('FAKE add-trust: deactivate-boundary'));
    let acknowledged = false;
    const deactivation = handle.executeUpdate(messages.deactivate, { args: [actor] }).then(() => { acknowledged = true; });
    void deactivation.catch(() => undefined);
    const deactivating = await waitState(handle, state => state.status === 'DEACTIVATING');
    assert.equal(acknowledged, false);
    assert.equal(deactivating.activeRotation!.request.operationId, 'deactivate-boundary');
    await assert.rejects(handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'after-deactivate' }] }));
    await deactivation;
    await handle.result();

    const stopped = (await handle.query(messages.getState, {})).state;
    assert.equal(stopped.status, 'STOPPED');
    assert.equal(stopped.activeRotation, undefined);
    assert.equal(stopped.rotationHistory[0].status, 'SUCCEEDED');
    assert.equal((await handle.describe()).runId, originalRun);
    assert.equal(fake.inspect().events.includes('FAKE revert: deactivate-boundary'), false);
    assert.ok(fake.inspect().events.includes('FAKE retire-old: deactivate-boundary'));
  });
});

test('scope changed during an inventory Activity discards its stale response before starting a child', async () => {
  const fake = createFakeActivities({ client: env.client, mode: 'hold', stepDelayMs: 0 });
  const readInventory = fake.activities.getTemporalNamespaceCertificates;
  let releaseStaleRead!: () => void;
  const staleReadGate = new Promise<void>(resolve => { releaseStaleRead = resolve; });
  let staleReadEntered = false;
  const gatedActivities = {
    ...fake.activities,
    async getTemporalNamespaceCertificates(input: Parameters<typeof readInventory>[0]) {
      const response = await readInventory(input);
      if (input.temporalNamespaceOptions.names.includes('stale-dev')) {
        staleReadEntered = true;
        await staleReadGate;
      }
      return response;
    },
  };
  const taskQueue = `test-${randomUUID()}`;
  const worker = await Worker.create({ connection: env.nativeConnection, workflowBundle, taskQueue, activities: gatedActivities,
    workerDeploymentOptions: { version: TEST_VERSION, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED' } });
  await worker.runUntil(async () => {
    const handle = await startPinned(request(), taskQueue);
    try {
      await waitState(handle, state => state.status === 'ACTIVE' && !!state.namespaceInventory);
      await handle.executeUpdate(messages.putTemporalNamespaces, { args: [{ temporalNamespaceOptions: { names: ['stale-dev'] } }] });
      await waitState(handle, () => staleReadEntered);
      await handle.executeUpdate(messages.putTemporalNamespaces, { args: [{ temporalNamespaceOptions: { names: ['orders-dev'] } }] });
      await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'fresh-scope' }] });
      releaseStaleRead();

      const rotating = await waitState(handle, state => state.activeRotation?.request.operationId === 'fresh-scope');
      assert.deepEqual(rotating.options!.temporalNamespaces.names, ['orders-dev']);
      assert.deepEqual(rotating.namespaceInventory!.namespaces.map(item => item.namespace), ['orders-dev']);
      assert.deepEqual(rotating.activeRotation!.request.namespaceInventory.namespaces.map(item => item.namespace), ['orders-dev']);
      assert.deepEqual(rotating.activeRotation!.request.options.temporalNamespaces.names, ['orders-dev']);
      await handle.executeUpdate(messages.restore, { args: [actor] });
      await waitState(handle, state => !state.activeRotation);
      await handle.executeUpdate(messages.deactivate, { args: [actor] });
      await handle.result();
    } finally {
      releaseStaleRead();
      await handle.cancel().catch(() => undefined);
    }
  });
});

test('suspend enters SUSPENDED after an active child reverts and requires activate to resume', async () => {
  await scenario(request(), { mode: 'hold' }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'suspend-revert' }] });
    await waitState(handle, state => !!state.activeRotation && fake.inspect().events.includes('FAKE add-trust: suspend-revert'));
    const suspension = handle.executeUpdate(messages.suspend, { args: [actor] });
    void suspension.catch(() => undefined);
    await waitState(handle, state => state.status === 'SUSPENDING');
    assert.equal((await handle.executeUpdate(messages.restore, { args: [actor] })).response.accepted, true);
    assert.equal((await suspension).status, 'SUSPENDED');

    const suspended = await waitState(handle, state => state.status === 'SUSPENDED' && !state.activeRotation);
    assert.equal(suspended.rotationHistory[0].status, 'REVERTED');
    assert.equal(suspended.nextRotationAtMs, undefined);
    assert.equal(suspended.suspension!.requester, actor.requester);
    await assert.rejects(handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'before-resume' }] }));
    await handle.executeUpdate(messages.activate, { args: [activation(suspended)] });
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.deactivate, { args: [actor] });
    await handle.result();
  });
});

test('invalid options accepted during initialization reject the Update without breaking the entity', async () => {
  const fake = createFakeActivities({ client: env.client, stepDelayMs: 0 });
  let releaseMerge!: () => void;
  let mergeStarted!: () => void;
  const mergeGate = new Promise<void>(resolve => { releaseMerge = resolve; });
  const mergeEntered = new Promise<void>(resolve => { mergeStarted = resolve; });
  const gatedActivities = {
    ...fake.activities,
    async mergeOptions(input: Parameters<typeof fake.activities.mergeOptions>[0]) {
      mergeStarted();
      await mergeGate;
      return fake.activities.mergeOptions(input);
    },
  };
  const taskQueue = `test-${randomUUID()}`;
  const worker = await Worker.create({ connection: env.nativeConnection, workflowBundle, taskQueue, activities: gatedActivities,
    workerDeploymentOptions: { version: TEST_VERSION, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED' } });
  await worker.runUntil(async () => {
    const handle = await startPinned(request(), taskQueue, 1_000);
    try {
      await mergeEntered;
      const update = await handle.startUpdate(messages.putOptions, {
        args: [{ credentialOptions: { ttlMs: 0 } }], waitForStage: 'ACCEPTED',
      });
      releaseMerge();
      await assert.rejects(update.result(), error => {
        assert.ok(error instanceof Error && error.cause instanceof Error);
        assert.match(error.cause.message, /ttlMs must be a positive safe integer/);
        return true;
      });
      const state = await waitState(handle, state => state.status === 'ACTIVE' && !!state.namespaceInventory);
      assert.equal(state.options!.credential.ttlMs, 90 * DAY_MS);
      assert.equal(state.revision, 0);
      assert.ok(state.updateOutcomes.some(item => item.name === 'putOptions' && item.outcome === 'REJECTED'));
      assert.ok(state.failures.some(item => item.type === 'InvalidRequest' && item.message.includes('ttlMs')));
      const repaired = await handle.executeUpdate(messages.putOptions, { args: [{ credentialOptions: { ttlMs: 60 * DAY_MS } }] });
      assert.equal(repaired.options.credential.ttlMs, 60 * DAY_MS);
      await handle.executeUpdate(messages.deactivate, { args: [actor] });
      await handle.result();
    } finally {
      releaseMerge();
      await handle.cancel().catch(() => undefined);
    }
  });
});

test('native cancellation during timed-out child cleanup does not restart or extend its deadline', async () => {
  await scenario(request({ executionOptions: { rotationTimeoutMs: 1_000 } }), { mode: 'hold', holdCompensation: true }, async (handle, fake) => {
    await waitState(handle, state => state.status === 'ACTIVE');
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'cancel-during-cleanup' }] });
    const rotating = await waitState(handle, state => !!state.activeRotation && fake.inspect().events.includes('FAKE add-trust: cancel-during-cleanup'));
    const child = env.client.workflow.getHandle(rotating.activeRotation!.childWorkflowId);
    try {
      await handle.executeUpdate(messages.putOptions, { args: [{ executionOptions: { rotationTimeoutMs: 10_000 } }] });
      const changed = (await handle.query(messages.getState, {})).state;
      assert.equal(changed.options!.execution.rotationTimeoutMs, 10_000);
      assert.equal(changed.activeRotation!.request.options.execution.rotationTimeoutMs, 1_000);

      // Observe the timeout's cancellation request before issuing native parent cancellation.
      const deadline = Date.now() + 3_000;
      let cancellationDelivered = false;
      while (!cancellationDelivered && Date.now() < deadline) {
        const history = await child.fetchHistory();
        cancellationDelivered = history.events?.some(event => !!event.workflowExecutionCancelRequestedEventAttributes) ?? false;
        if (!cancellationDelivered) await new Promise(resolve => setTimeout(resolve, 20));
      }
      assert.equal(cancellationDelivered, true);
      assert.equal((await handle.query(messages.getState, {})).state.status, 'ROTATING');
      const cancelledAt = Date.now();
      await handle.cancel();
      let watchdog: ReturnType<typeof setTimeout> | undefined;
      try {
        const result = await Promise.race([
          handle.result().then(() => 'completed' as const, () => 'cancelled' as const),
          new Promise<'deadline'>(resolve => { watchdog = setTimeout(() => resolve('deadline'), 3_500); }),
        ]);
        assert.equal(result, 'cancelled', 'Parent reopened a cleanup window after its original bounded wait');
        assert.ok(Date.now() - cancelledAt < 3_500);
      } finally { if (watchdog) clearTimeout(watchdog); }
    } finally {
      await child.terminate('Cleanup of deliberately unresponsive compensation test child').catch(() => undefined);
    }
  });
});

test('invalid starting identity fails the execution without retrying Workflow Tasks', { timeout: 20_000 }, async () => {
  await scenario(request({ teamName: 'bad/team' }), {}, async handle => {
    await assert.rejects(handle.result(), error => {
      assert.ok(error instanceof Error && error.cause instanceof Error);
      assert.equal((error.cause as Error & { type?: string }).type, 'InvalidIdentity');
      assert.match(error.cause.message, /teamName must be a business-safe identifier/);
      return true;
    });
  });
});
