import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { randomUUID } from 'node:crypto';
import { ApplicationFailure } from '@temporalio/common';
import type { WorkflowHandle } from '@temporalio/client';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { bundleWorkflowCode, DefaultLogger, Runtime, Worker, type WorkflowBundle } from '@temporalio/worker';
import type { ActivateCertificateRequest, CredentialState } from '../src/contracts';
import type { credentialWorkflow } from '../src/workflows/credential';
import * as messages from '../src/messages';
import { createFakeActivities, type FakeActivityOptions } from '../demos/credential-supervisor/fake-activities';
import { DAY_MS } from '../src/options';

let env: TestWorkflowEnvironment;
let workflowBundle: WorkflowBundle;
const VERSION = { deploymentName: 'rotation-regressions', buildId: 'test-v1' };
const actor = { reason: 'Regression test', requester: 'test-suite' };
type Handle = WorkflowHandle<typeof credentialWorkflow>;
type Fake = ReturnType<typeof createFakeActivities>;

before(async () => {
  Runtime.install({ logger: new DefaultLogger('ERROR') });
  env = await TestWorkflowEnvironment.createLocal();
  workflowBundle = await bundleWorkflowCode({ workflowsPath: require.resolve('../demos/credential-supervisor/fake-workflows'), logger: new DefaultLogger('ERROR') });
}, { timeout: 120_000 });
after(async () => { await env?.teardown(); });

function request(scopeKey: string, namespaces: string[], rotationTimeoutMs = 10_000): ActivateCertificateRequest {
  return {
    credentialType: 'certificate', environment: 'test', teamName: 'platform', scopeKey,
    temporalNamespaceOptions: { names: namespaces }, executionOptions: { rotationTimeoutMs },
  };
}
async function start(input: ActivateCertificateRequest, taskQueue: string): Promise<Handle> {
  const workflowId = `rotation-fix-${randomUUID()}`;
  const deadline = Date.now() + 20_000;
  while (true) {
    try {
      return await env.client.workflow.start<typeof credentialWorkflow>('credentialWorkflow', {
        workflowId, taskQueue, args: [input], versioningOverride: { pinnedTo: VERSION },
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
    state = (await handle.query(messages.getState, { includeHistory: true, includeFailureDetail: true })).state;
    if (predicate(state)) return state;
    await new Promise(resolve => setTimeout(resolve, 25));
  } while (Date.now() < deadline);
  throw new Error(`State did not converge: ${JSON.stringify(state!)}`);
}
async function scenario(
  inputs: ActivateCertificateRequest[], options: FakeActivityOptions,
  body: (handles: Handle[], fake: Fake) => Promise<void>,
  decorate?: (fake: Fake) => void,
): Promise<void> {
  const fake = createFakeActivities({ client: env.client, stepDelayMs: 0, ...options });
  decorate?.(fake);
  const taskQueue = `rotation-fixes-${randomUUID()}`;
  const worker = await Worker.create({ connection: env.nativeConnection, workflowBundle, taskQueue, activities: fake.activities,
    workerDeploymentOptions: { version: VERSION, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED' } });
  try { await worker.runUntil(async () => {
    const handles: Handle[] = [];
    try {
      for (const input of inputs) handles.push(await start(input, taskQueue));
      for (const handle of handles) await waitState(handle, state => state.status === 'ACTIVE');
      await body(handles, fake);
      for (const handle of handles) {
        await handle.executeUpdate(messages.deactivate, { args: [actor] });
        await handle.result();
      }
    } finally {
      for (const handle of handles) await handle.cancel().catch(() => undefined);
    }
  }); } finally { fake.close(); }
}

test('different Credential identities can independently complete the same operation ID', async () => {
  const namespaces = ['left', 'right'].map(name => ({ name, tags: {}, trustedCAs: [`OLD-${name}`], clientCA: `OLD-${name}`, notAfterMs: Date.now() + 30 * DAY_MS }));
  await scenario([request('left', ['left']), request('right', ['right'])], { namespaces }, async ([left, right], fake) => {
    for (const handle of [left, right]) await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'maintenance-001' }] });
    for (const handle of [left, right]) {
      const state = await waitState(handle, state => state.rotationHistory.length === 1 && !state.activeRotation);
      assert.equal(state.rotationHistory[0].status, 'SUCCEEDED');
    }
    const inventory = fake.inspect();
    assert.notEqual(inventory.namespaces[0].clientCA, inventory.namespaces[1].clientCA);
    for (const namespace of inventory.namespaces) {
      assert.notEqual(namespace.clientCA, `OLD-${namespace.name}`);
      assert.deepEqual(namespace.trustedCAs, [namespace.clientCA]);
    }
    assert.deepEqual(inventory.retiredCAs, ['OLD-left', 'OLD-right']);
    assert.equal(inventory.events.filter(event => event === 'FAKE rollout-clients: maintenance-001').length, 2);
  });
});

test('retirement failure after a scoped rollout compensates before the child reaches FAILED', async () => {
  await scenario([request('subset', ['orders-dev'])], {}, async ([handle], fake) => {
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'subset-retirement' }] });
    const state = await waitState(handle, state => state.status === 'FAILED' && !state.activeRotation);
    const summary = state.rotationHistory[0];
    assert.equal(summary.status, 'FAILED');
    assert.equal(summary.failure?.type, 'UnsafeFakeRotation');
    assert.equal(summary.compensationStatus, 'SUCCEEDED');
    assert.match(summary.failure!.message, /still referenced by payments-dev/);
    assert.ok(summary.steps.includes('FAKE revert: subset-retirement'));
    assert.deepEqual(fake.inspect().retiredCAs, []);
    for (const namespace of fake.inspect().namespaces) {
      assert.equal(namespace.clientCA, 'FAKE-CA-INITIAL');
      assert.deepEqual(namespace.trustedCAs, ['FAKE-CA-INITIAL']);
    }
  });
});

test('a lost mutation response still triggers compensation once', async () => {
  await scenario([request('ambiguous', ['orders-dev', 'payments-dev', 'tagged-dev'])], {}, async ([handle], fake) => {
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'lost-rollout-response' }] });
    const state = await waitState(handle, state => state.status === 'FAILED' && !state.activeRotation);
    assert.equal(state.rotationHistory[0].failure?.type, 'MutationResponseLost');
    assert.equal(fake.inspect().events.filter(event => event === 'FAKE revert: lost-rollout-response').length, 1);
    for (const namespace of fake.inspect().namespaces) {
      assert.equal(namespace.clientCA, 'FAKE-CA-INITIAL');
      assert.deepEqual(namespace.trustedCAs, ['FAKE-CA-INITIAL']);
    }
  }, fake => {
    const perform = fake.activities.performFakeRotationStep;
    fake.activities.performFakeRotationStep = async input => {
      const response = await perform(input);
      if (input.step === 'rollout-clients') throw ApplicationFailure.nonRetryable('Client rollout committed, but its response was lost', 'MutationResponseLost');
      return response;
    };
  });
});

test('a failed compensation remains queryable and restorable, and its retry is idempotent', async () => {
  await scenario([request('restore-retry', ['orders-dev', 'payments-dev', 'tagged-dev'])], {}, async ([handle], fake) => {
    await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'lost-revert-response' }] });
    const rotating = await waitState(handle, state => !!state.activeRotation);
    const child = env.client.workflow.getHandle(rotating.activeRotation!.childWorkflowId);
    const deadline = Date.now() + 5_000;
    let progress = (await child.query(messages.getRotationState, {})).summary;
    while (!progress.steps.some(step => step.includes('CompensationResponseLost')) && Date.now() < deadline) {
      await new Promise(resolve => setTimeout(resolve, 25));
      progress = (await child.query(messages.getRotationState, {})).summary;
    }
    assert.equal(progress.failure?.type, 'MutationResponseLost');
    assert.equal(progress.compensationFailure?.type, 'CompensationResponseLost');
    assert.equal(progress.compensationStatus, 'UNRESOLVED');
    assert.ok(progress.steps.some(step => step.includes('CompensationResponseLost')));
    assert.ok((await handle.query(messages.getState, {})).state.activeRotation);
    const restored = await handle.executeUpdate(messages.restore, { args: [actor] });
    assert.equal(restored.response.accepted, true);
    const retryDeadline = Date.now() + 5_000;
    do {
      progress = (await child.query(messages.getRotationState, {})).summary;
      if (progress.steps.filter(step => step.includes('CompensationResponseLost')).length >= 2) break;
      await new Promise(resolve => setTimeout(resolve, 25));
    } while (Date.now() < retryDeadline);
    assert.equal(progress.steps.filter(step => step.includes('CompensationResponseLost')).length, 2);
    assert.equal((await handle.executeUpdate(messages.restore, { args: [actor] })).response.accepted, true);
    const state = await waitState(handle, state => state.status === 'FAILED' && !state.activeRotation);
    assert.equal(state.rotationHistory[0].failure?.type, 'MutationResponseLost');
    assert.equal(state.rotationHistory[0].compensationFailure?.type, 'CompensationResponseLost');
    assert.equal(state.rotationHistory[0].compensationStatus, 'SUCCEEDED');
    assert.equal(fake.inspect().events.filter(event => event === 'FAKE revert: lost-revert-response').length, 1);
    for (const namespace of fake.inspect().namespaces) assert.equal(namespace.clientCA, 'FAKE-CA-INITIAL');
  }, fake => {
    const perform = fake.activities.performFakeRotationStep;
    let compensationResponsesLost = 0;
    fake.activities.performFakeRotationStep = async input => {
      const response = await perform(input);
      if (input.step === 'rollout-clients') throw ApplicationFailure.nonRetryable('Committed rollout lost its response', 'MutationResponseLost');
      if (input.step === 'revert' && compensationResponsesLost++ < 2) throw ApplicationFailure.nonRetryable('Compensation committed, but its response was lost', 'CompensationResponseLost');
      return response;
    };
  });
});
