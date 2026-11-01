import assert from 'node:assert/strict';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { Worker } from '@temporalio/worker';
import type { CredentialState } from '../../src/contracts';
import { activate, deactivate, getState, putTemporalNamespaces, restore, rotate, suspend } from '../../src/messages';
import { credentialWorkflowId } from '../../src/options';
import { DEMO_DEPLOYMENT, DEMO_REQUEST, DEMO_TASK_QUEUE, DEMO_WORKER_DEPLOYMENT_OPTIONS } from './fixtures';
import { createFakeActivities } from './fake-activities';
import { startPinnedCredential } from './start-pinned';

async function eventually<T>(read: () => Promise<T>, ready: (value: T) => boolean, description: string): Promise<T> {
  const deadline = Date.now() + 25_000;
  while (Date.now() < deadline) {
    const value = await read();
    if (ready(value)) return value;
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

export async function runDemo(): Promise<void> {
  console.log('FAKE mTLS CA rollover: in-memory namespaces and clients; no cloud credentials.');
  console.log('Starting an isolated Temporal development server (first run may download its binary).');
  const env = await TestWorkflowEnvironment.createLocal();
  try {
    const fake = createFakeActivities({ client: env.client, mode: 'hold-after-rollout', stepDelayMs: 20 });
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: DEMO_TASK_QUEUE,
      workflowsPath: require.resolve('./fake-workflows'),
      activities: fake.activities,
      workerDeploymentOptions: DEMO_WORKER_DEPLOYMENT_OPTIONS,
    });
    await worker.runUntil(async () => {
      const handle = await startPinnedCredential(env.client, DEMO_REQUEST, {
        workflowId: credentialWorkflowId(DEMO_REQUEST), taskQueue: DEMO_TASK_QUEUE, version: DEMO_DEPLOYMENT,
      });
      const state = async (): Promise<CredentialState> => (await handle.query(getState, {})).state;
      const initial = await eventually(state, value => value.status === 'ACTIVE' && value.namespaceInventory !== undefined && value.nextRotationAtMs !== undefined, 'initial inventory');
      console.log(`\nACTIVE: ${initial.namespaceInventory?.namespaces.map(item => item.namespace).join(', ')}`);
      console.log(`Next scheduled rollover: ${new Date(initial.nextRotationAtMs!).toISOString()}`);

      const first = { operationId: 'demo-rollback-001', reason: 'Demonstrate safe compensation', requester: 'local-demo' };
      await handle.executeUpdate(rotate, { args: [first], updateId: `rotate-${first.operationId}` });
      await eventually(async () => fake.inspect(), snapshot => snapshot.events.some(event => event === `FAKE rollout-clients: ${first.operationId}`), 'fake client rollout');
      const dualTrust = fake.inspect();
      assert(dualTrust.namespaces.every(item => item.trustedCAs.length === 2 && item.trustedCAs.includes(item.clientCA)));
      console.log('\nPAUSED after client rollout: both CAs trusted; all fake clients use the new CA.');
      console.log(JSON.stringify(dualTrust.namespaces.map(({ name, trustedCAs, clientCA }) => ({ name, trustedCAs, clientCA })), null, 2));

      await handle.executeUpdate(putTemporalNamespaces, { args: [{ temporalNamespaceOptions: { names: ['orders-dev'] } }] });
      const changedScope = await state();
      assert.deepEqual(changedScope.options?.temporalNamespaces.names, ['orders-dev']);
      assert.equal(changedScope.activeRotation?.request.namespaceInventory.namespaces.length, 3);
      console.log('Scope changed to orders-dev; the active child keeps its original three-namespace snapshot.');
      const restored = await handle.executeUpdate(restore, { args: [{ reason: 'Exercise the revert bridge', requester: 'local-demo' }] });
      assert(restored.response.accepted);
      await eventually(state, value => value.status === 'FAILED' && value.rotationHistory.some(item => item.operationId === first.operationId && item.status === 'REVERTED'), 'compensation');
      assert(fake.inspect().namespaces.every(item => item.trustedCAs.length === 1 && item.clientCA === 'FAKE-CA-INITIAL'));
      console.log('REVERTED: original trust and clients restored in all three namespaces.');

      // Explicit recovery also rejoins every namespace before retiring their shared original CA.
      await handle.executeUpdate(activate, { args: [DEMO_REQUEST] });
      fake.setRotationMode('complete');
      const second = { operationId: 'demo-success-002', reason: 'Complete the fake rollover', requester: 'local-demo' };
      const beforeSuccessRunId = (await handle.describe()).runId;
      await handle.executeUpdate(rotate, { args: [second], updateId: `rotate-${second.operationId}` });
      const completed = await eventually(state, value => value.status === 'ACTIVE' && value.rotationHistory.some(item => item.operationId === second.operationId && item.status === 'SUCCEEDED'), 'successful rotation and Continue-As-New');
      await eventually(() => handle.describe(), description => description.runId !== beforeSuccessRunId, 'new run after successful rotation');
      const summary = completed.rotationHistory.find(item => item.operationId === second.operationId)!;
      assert.deepEqual(summary.namespaceNames, ['orders-dev', 'payments-dev', 'tagged-dev']);
      assert.deepEqual(summary.steps, ['issue', 'add-trust', 'rollout-clients', 'verify-clients', 'remove-old-trust', 'retire-old'].map(step => `FAKE ${step}: ${second.operationId}`));
      assert.deepEqual(fake.inspect().clientVerifications, [{
        identity: completed.identity, operationId: second.operationId,
        deploymentsConverged: true, newCredentialAccepted: true,
      }]);
      const rotated = fake.inspect().namespaces.find(item => item.name === 'orders-dev')!;
      assert.deepEqual(rotated.trustedCAs, [rotated.clientCA]);
      assert.notEqual(rotated.clientCA, 'FAKE-CA-INITIAL');
      assert.equal(rotated.clientCA, rotated.trustedCAs[0]);
      assert(fake.inspect().namespaces.every(item => item.clientCA === rotated.clientCA && item.trustedCAs.length === 1));
      assert(fake.inspect().retiredCAs.includes('FAKE-CA-INITIAL'));
      console.log('\nSUCCEEDED: full scope restored; simulated Deployment convergence and new-credential acceptance passed before old trust removal and CA retirement.');
      for (const step of summary.steps) console.log(`  ${step}`);
      const duplicate = await handle.executeUpdate(rotate, { args: [second], updateId: 'retry-demo-success-002' });
      assert(duplicate.duplicate);
      console.log('Retrying the same business operation ID is deduplicated across Continue-As-New.');
      await handle.executeUpdate(suspend, { args: [{ reason: 'Demo complete', requester: 'local-demo' }] });
      assert.equal((await state()).status, 'SUSPENDED');
      await handle.executeUpdate(deactivate, { args: [{ reason: 'Stop the local demo', requester: 'local-demo' }] });
      await handle.result();
      console.log('\nDemo assertions passed. Credential suspended, then deactivated cleanly.');
      console.log('This demonstrates orchestration only; it does not establish real mTLS connectivity or availability.');
    }).finally(() => fake.close());
  } finally {
    await env.teardown();
  }
}

if (require.main === module) runDemo().catch(error => { console.error(error); process.exitCode = 1; });
