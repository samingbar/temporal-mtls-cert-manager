import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, test } from 'node:test';
import { ApplicationFailure, CancelledFailure } from '@temporalio/common';
import type { WorkflowHandle } from '@temporalio/client';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { bundleWorkflowCode, DefaultLogger, Runtime, Worker } from '@temporalio/worker';
import type { WorkflowBundle } from '@temporalio/worker';
import type { ActivateCertificateRequest, CredentialState } from '../src/contracts';
import type { credentialWorkflow } from '../src/workflows/credential';
import * as messages from '../src/messages';
import { createFakeActivities } from '../demos/credential-supervisor/fake-activities';
import { startPinnedCredential } from '../demos/credential-supervisor/start-pinned';

let environment: TestWorkflowEnvironment;
let workflowBundle: WorkflowBundle;
const version = { deploymentName: 'inventory-revision-tests', buildId: 'revision-v1' };
const actor = { reason: 'Inventory revision regression', requester: 'test-suite' };
type Handle = WorkflowHandle<typeof credentialWorkflow>;
type Fake = ReturnType<typeof createFakeActivities>;

before(async () => {
  Runtime.install({ logger: new DefaultLogger('ERROR') });
  environment = await TestWorkflowEnvironment.createLocal();
  workflowBundle = await bundleWorkflowCode({
    workflowsPath: require.resolve('../demos/credential-supervisor/fake-workflows'),
    logger: new DefaultLogger('ERROR'),
  });
}, { timeout: 120_000 });
after(async () => { await environment?.teardown(); });

function request(): ActivateCertificateRequest {
  return {
    credentialType: 'certificate', environment: 'revision-test', teamName: 'platform', scopeKey: randomUUID(),
    temporalNamespaceOptions: { names: ['tagged-dev'] },
  };
}

function gate() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => { release = resolve; });
  return { promise, release };
}

async function until(predicate: () => boolean | Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 20_000;
  while (!await predicate()) {
    if (Date.now() >= deadline) throw new Error('Timed out waiting for inventory regression state');
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

async function stateWhen(handle: Handle, predicate: (state: CredentialState) => boolean): Promise<CredentialState> {
  let state!: CredentialState;
  await until(async () => {
    state = (await handle.query(messages.getState, { includeFailureDetail: true })).state;
    return predicate(state);
  });
  return state;
}

async function scenario(
  configure: (fake: Fake) => Fake['activities'],
  body: (handle: Handle, fake: Fake) => Promise<void>,
): Promise<void> {
  const fake = createFakeActivities({ client: environment.client, mode: 'hold', stepDelayMs: 0 });
  const taskQueue = `revision-${randomUUID()}`;
  const worker = await Worker.create({
    connection: environment.nativeConnection, workflowBundle, taskQueue, activities: configure(fake),
    workerDeploymentOptions: { version, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED' },
  });
  try {
    await worker.runUntil(async () => {
      const handle = await startPinnedCredential(environment.client, request(), {
        workflowId: `revision-${randomUUID()}`, taskQueue, version,
      });
      try { await body(handle, fake); }
      finally {
        await handle.cancel().catch(() => undefined);
        await handle.result().catch(() => undefined);
      }
    });
  } finally { fake.close(); }
}

for (const failingInventory of ['namespaces', 'deployments'] as const) {
  for (const boundary of ['scope refresh', 'pre-rotation refresh'] as const) {
    test(`obsolete ${failingInventory} failure during ${boundary} does not pause corrected scope`, { timeout: 60_000 }, async () => {
      const staleRead = gate();
      let readCount = 0;
      let readEntered = false;
      const beforeRead = async (kind: typeof failingInventory): Promise<void> => {
        if (kind !== failingInventory || ++readCount !== 2) return;
        readEntered = true;
        await staleRead.promise;
        throw ApplicationFailure.nonRetryable('Old configuration inventory unavailable', 'ObsoleteInventoryFailure');
      };
      try {
        await scenario(fake => ({
          ...fake.activities,
          async getTemporalNamespaceCertificates(input) {
            await beforeRead('namespaces');
            return fake.activities.getTemporalNamespaceCertificates(input);
          },
          async getKubernetesDeployments(input) {
            await beforeRead('deployments');
            return fake.activities.getKubernetesDeployments(input);
          },
        }), async handle => {
          await stateWhen(handle, state => !!state.namespaceInventory && state.status === 'ACTIVE');
          if (boundary === 'scope refresh') {
            await handle.executeUpdate(messages.putTemporalNamespaces, {
              args: [{ temporalNamespaceOptions: { names: ['orders-dev'] } }],
            });
          } else {
            await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'corrected-scope' }] });
          }
          await until(() => readEntered);
          await handle.executeUpdate(messages.putTemporalNamespaces, {
            args: [{ temporalNamespaceOptions: { names: ['payments-dev'] } }],
          });
          if (boundary === 'scope refresh') {
            await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'corrected-scope' }] });
          }
          staleRead.release();

          const state = await stateWhen(handle, state => state.activeRotation?.request.operationId === 'corrected-scope');
          assert.deepEqual(state.failures, []);
          assert.deepEqual(state.namespaceInventory!.namespaces.map(item => item.namespace), ['payments-dev']);
          assert.deepEqual(state.activeRotation!.request.options.temporalNamespaces.names, ['payments-dev']);
          assert.deepEqual(state.activeRotation!.request.namespaceInventory.namespaces.map(item => item.namespace), ['payments-dev']);
          assert.equal(state.pendingRotations.length, 0);
        });
      } finally { staleRead.release(); }
    });
  }
}

for (const boundary of ['initialization', 'pre-rotation refresh'] as const) {
  test(`current configuration inventory failure during ${boundary} pauses safely`, { timeout: 60_000 }, async () => {
    let readCount = 0;
    await scenario(fake => ({
      ...fake.activities,
      async getTemporalNamespaceCertificates(input) {
        if (++readCount === (boundary === 'initialization' ? 1 : 2)) {
          throw ApplicationFailure.nonRetryable('Current configuration inventory unavailable', 'CurrentInventoryFailure');
        }
        return fake.activities.getTemporalNamespaceCertificates(input);
      },
    }), async (handle, fake) => {
      if (boundary === 'pre-rotation refresh') {
        await stateWhen(handle, state => !!state.namespaceInventory && state.status === 'ACTIVE');
        await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'blocked-rotation' }] });
      }
      const state = await stateWhen(handle, state => state.status === 'FAILED');
      assert.equal(state.failures.length, 1);
      assert.equal(state.failures[0].type, 'CurrentInventoryFailure');
      assert.equal(state.failures[0].operationId, boundary === 'initialization' ? undefined : 'blocked-rotation');
      assert.equal(state.activeRotation, undefined);
      assert.deepEqual(fake.inspect().events, []);
      await handle.executeUpdate(messages.deactivate, { args: [actor] });
      await handle.result();
    });
  });
}

test('native cancellation remains terminal while an obsolete inventory read is in flight', { timeout: 60_000 }, async () => {
  const staleRead = gate();
  let readCount = 0;
  let readEntered = false;
  try {
    await scenario(fake => ({
      ...fake.activities,
      async getTemporalNamespaceCertificates(input) {
        if (++readCount === 2) {
          readEntered = true;
          await staleRead.promise;
        }
        return fake.activities.getTemporalNamespaceCertificates(input);
      },
    }), async (handle, fake) => {
      await stateWhen(handle, state => !!state.namespaceInventory && state.status === 'ACTIVE');
      await handle.executeUpdate(messages.rotate, { args: [{ ...actor, operationId: 'cancel-before-rotation' }] });
      await until(() => readEntered);
      await handle.executeUpdate(messages.putTemporalNamespaces, {
        args: [{ temporalNamespaceOptions: { names: ['payments-dev'] } }],
      });
      await handle.cancel();
      await assert.rejects(handle.result(), error => (error as { cause?: unknown }).cause instanceof CancelledFailure);
      staleRead.release();
      assert.equal((await handle.describe()).status.name, 'CANCELLED');
      assert.deepEqual(fake.inspect().events, []);
    });
  } finally { staleRead.release(); }
});
