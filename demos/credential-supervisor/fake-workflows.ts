/** Only this demo bundle registers a Rotate implementation. Production src has its contract only. */
export { credentialWorkflow } from '../../src/workflows/credential';
import {
  allHandlersFinished, ApplicationFailure, CancellationScope, condition, isCancellation,
  proxyActivities, setHandler, setWorkflowOptions, sleep, workflowInfo,
} from '@temporalio/workflow';
import type { FailureDetail, GetRotationStateResponse, RotationSummary, StartRotateRequest } from '../../src/contracts';
import { cancel, getRotationState, revert } from '../../src/messages';
import { boundedText, retainRecent } from '../../src/limits';
import type { FakeRotationStep, FakeWorkflowActivities } from './fake-activities';

const fake = proxyActivities<FakeWorkflowActivities>({
  startToCloseTimeout: '10 seconds',
  retry: { maximumAttempts: 1 },
});

/** Simulates ordering and compensation; it does not issue certificates or contact infrastructure. */
export async function rotateWorkflow(request: StartRotateRequest): Promise<GetRotationStateResponse> {
  let revertRequested = false;
  let cancelRequested = false;
  let holdCompensation = false;
  let revertRevision = 0;
  let workStarted = false;
  const summary: RotationSummary = {
    operationId: request.operationId,
    childWorkflowId: workflowInfo().workflowId,
    reason: request.reason,
    requester: request.requester,
    status: 'SUCCEEDED',
    startedAtMs: Date.now(),
    finishedAtMs: 0,
    namespaceNames: request.namespaceInventory.namespaces.map(item => item.namespace),
    steps: [],
  };
  setHandler(getRotationState, () => ({ summary: {
    ...summary,
    steps: [...summary.steps],
    namespaceNames: [...summary.namespaceNames],
    ...(summary.failure ? { failure: { ...summary.failure } } : {}),
    ...(summary.compensationFailure ? { compensationFailure: { ...summary.compensationFailure } } : {}),
  } }));
  setHandler(revert, request => {
    if (request.operationId !== summary.operationId) return { operationId: request.operationId, accepted: false, message: 'Operation ID does not match this fake rotation' };
    revertRequested = true;
    revertRevision += 1;
    return { operationId: request.operationId, accepted: true, message: 'Fake compensation requested' };
  });
  setHandler(cancel, () => { cancelRequested = true; return { accepted: true }; });

  function appendStep(description: unknown): void {
    summary.steps.push(boundedText(description, 256));
    retainRecent(summary.steps, 16);
  }
  async function step(name: FakeRotationStep) {
    const response = await fake.performFakeRotationStep({ request, step: name });
    appendStep(response.description);
  }
  function failureDetail(error: unknown): FailureDetail {
    let cause = error;
    const seen = new Set<unknown>();
    while (cause instanceof Error && cause.cause instanceof Error && !seen.has(cause.cause)) {
      seen.add(cause);
      cause = cause.cause;
    }
    return {
      operationId: request.operationId,
      message: boundedText(cause instanceof Error ? cause.message : cause, 512),
      type: boundedText(cause instanceof ApplicationFailure ? cause.type ?? cause.name : cause instanceof Error ? cause.name : 'UnknownFailure', 128),
      atMs: Date.now(),
    };
  }
  async function compensate(status: 'REVERTED' | 'CANCELLED' | 'FAILED') {
    await CancellationScope.nonCancellable(async () => {
      // Fault injection deliberately exercises the parent's bounded unresolved-child path.
      if (holdCompensation) await condition(() => false);
      while (true) {
        const attemptedRevision = revertRevision;
        try {
          // Use the immutable active rotation's budget, even if the parent changes options.
          await CancellationScope.withTimeout(request.options.execution.rotationTimeoutMs, () => step('revert'));
          summary.status = status;
          summary.compensationStatus = 'SUCCEEDED';
          return;
        } catch (error) {
          const compensationFailure = failureDetail(error);
          summary.status = 'FAILED';
          summary.compensationStatus = 'UNRESOLVED';
          summary.compensationFailure = compensationFailure;
          // Keep both failures visible and retain ownership until a later restore succeeds.
          appendStep(`FAKE compensation failed: ${compensationFailure.type}: ${compensationFailure.message}`);
          await condition(() => revertRevision > attemptedRevision);
        }
      }
    });
  }

  try {
    const config = await fake.getFakeRotationConfig(request);
    holdCompensation = config.holdCompensation;
    for (const name of ['issue', 'add-trust', 'rollout-clients', 'verify-clients', 'remove-old-trust', 'retire-old'] as const) {
      if (revertRequested || cancelRequested) break;
      // A failed Activity result may follow a committed mutation, so compensate from before dispatch.
      workStarted = true;
      await step(name);
      if (config.stepDelayMs > 0) await sleep(config.stepDelayMs);
      if (name === 'add-trust' && config.mode === 'fail') {
        throw ApplicationFailure.nonRetryable('Deliberate fake rotation failure after dual trust', 'FakeRotationFailure');
      }
      if ((name === 'add-trust' && config.mode === 'hold') || (name === 'rollout-clients' && config.mode === 'hold-after-rollout')) {
        await condition(() => revertRequested || cancelRequested);
      }
    }
    if (revertRequested || cancelRequested) await compensate(revertRequested ? 'REVERTED' : 'CANCELLED');
  } catch (error) {
    if (isCancellation(error)) {
      if (workStarted) await compensate('CANCELLED');
      else summary.status = 'CANCELLED';
    } else {
      summary.failure = failureDetail(error);
      summary.status = 'FAILED';
      if (workStarted) await compensate('FAILED');
    }
  }
  summary.finishedAtMs = Date.now();
  await condition(allHandlersFinished);
  return { summary };
}

setWorkflowOptions({ versioningBehavior: 'PINNED' }, rotateWorkflow);
