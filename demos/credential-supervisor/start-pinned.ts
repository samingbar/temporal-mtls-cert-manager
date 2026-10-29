import type { Client, WorkflowHandleWithStartDetails } from '@temporalio/client';
import type { WorkerDeploymentVersion } from '@temporalio/common';
import type { ActivateCertificateRequest } from '../../src/contracts';
import type { credentialWorkflow } from '../../src/workflows/credential';

export interface StartPinnedCredentialOptions {
  workflowId: string;
  taskQueue: string;
  version: WorkerDeploymentVersion;
  readinessTimeoutMs?: number;
}

function isPollRegistrationRace(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 8 && current && typeof current === 'object'; depth++) {
    const candidate = current as { code?: unknown; message?: unknown; cause?: unknown };
    if (candidate.code === 9 && typeof candidate.message === 'string'
      && /Pinned version .* is not present in task queue/.test(candidate.message)) return true;
    current = candidate.cause;
  }
  return false;
}

/** runUntil may start its callback just before the server sees the versioned poller. */
export async function startPinnedCredential(
  client: Client,
  request: ActivateCertificateRequest,
  options: StartPinnedCredentialOptions,
): Promise<WorkflowHandleWithStartDetails<typeof credentialWorkflow>> {
  const deadline = Date.now() + (options.readinessTimeoutMs ?? 20_000);
  for (;;) {
    try {
      return await client.workflow.start<typeof credentialWorkflow>('credentialWorkflow', {
        workflowId: options.workflowId, taskQueue: options.taskQueue, args: [request],
        versioningOverride: { pinnedTo: options.version },
      });
    } catch (error) {
      if (!isPollRegistrationRace(error) || Date.now() >= deadline) throw error;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
}
