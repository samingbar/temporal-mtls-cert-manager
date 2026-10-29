import { Client, Connection } from '@temporalio/client';
import { NativeConnection, Worker } from '@temporalio/worker';
import { createFakeActivities, type FakeRotationMode } from './fake-activities';
import { DEMO_TASK_QUEUE, DEMO_WORKER_DEPLOYMENT_OPTIONS } from './fixtures';
import { defaultCredentialArchivePath } from './archive-store';

/** Persistent demo worker for `npm run ctl -- ...`; production adapters remain deferred. */
async function main(): Promise<void> {
  const address = process.env.TEMPORAL_ADDRESS ?? 'localhost:7233';
  const namespace = process.env.TEMPORAL_NAMESPACE ?? 'default';
  const mode = process.env.FAKE_ROTATION_MODE ?? 'complete';
  if (!['complete', 'hold', 'hold-after-rollout', 'fail'].includes(mode)) throw new Error(`Unknown FAKE_ROTATION_MODE: ${mode}`);
  const connection = await Connection.connect({ address });
  const nativeConnection = await NativeConnection.connect({ address });
  try {
    const client = new Client({ connection, namespace });
    const fake = createFakeActivities({ client, mode: mode as FakeRotationMode, stepDelayMs: 500, archivePath: defaultCredentialArchivePath() });
    const worker = await Worker.create({
      connection: nativeConnection, namespace, taskQueue: DEMO_TASK_QUEUE,
      workflowsPath: require.resolve('./fake-workflows'), activities: fake.activities,
      workerDeploymentOptions: DEMO_WORKER_DEPLOYMENT_OPTIONS,
    });
    console.log(`FAKE credential worker: ${address}, namespace ${namespace}, queue ${DEMO_TASK_QUEUE}.`);
    console.log('In-memory demo adapters only. Restarting this worker resets fake external inventory.');
    try { await worker.run(); }
    finally { fake.close(); }
  } finally {
    await nativeConnection.close();
    await connection.close();
  }
}
if (require.main === module) main().catch(error => { console.error(error); process.exitCode = 1; });
