import type { ActivateCertificateRequest } from '../../src/contracts';
import type { WorkerDeploymentVersion } from '@temporalio/common';
import type { WorkerDeploymentOptions } from '@temporalio/worker';

/** Deliberately fake names and fingerprints; this example never creates private keys. */
export const DEMO_REQUEST: ActivateCertificateRequest = {
  credentialType: 'certificate',
  environment: 'dev',
  teamName: 'platform',
  scopeKey: 'demo',
  credentialOptions: { ttlMs: 90 * 24 * 60 * 60 * 1000, rotationOffsetMs: 14 * 24 * 60 * 60 * 1000 },
  temporalNamespaceOptions: {
    names: ['payments-dev', 'orders-dev', 'payments-dev'],
    tagSelector: { team: 'platform', environment: 'dev' },
  },
  kubernetesOptions: {
    enabled: true,
    deploymentSelectors: [{ namespace: 'apps', labelSelector: 'team=platform' }],
  },
  executionOptions: { rotationTimeoutMs: 30_000 },
};

export const DEMO_TASK_QUEUE = 'credentialoperations';
/** Explicit local routing keeps PINNED workflows on the same demo worker version. */
export const DEMO_DEPLOYMENT: WorkerDeploymentVersion = { deploymentName: 'credential-operations-demo', buildId: 'demo-v2' };
export const DEMO_WORKER_DEPLOYMENT_OPTIONS: WorkerDeploymentOptions = {
  version: DEMO_DEPLOYMENT, useWorkerVersioning: true, defaultVersioningBehavior: 'PINNED',
};
