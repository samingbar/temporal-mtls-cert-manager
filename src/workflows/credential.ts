import {
  ApplicationFailure,
  CancellationScope,
  CancelledFailure,
  ChildWorkflowCancellationType,
  ParentClosePolicy,
  TemporalFailure,
  allHandlersFinished,
  condition,
  continueAsNew,
  currentUpdateInfo,
  executeChild,
  isCancellation,
  proxyActivities,
  proxyLocalActivities,
  setHandler,
  setWorkflowOptions,
  sleep,
  workflowInfo,
} from '@temporalio/workflow';
import type * as C from '../contracts';
import * as messages from '../messages';
import {
  canonicalizeKubernetes,
  canonicalizeTemporalNamespaces,
  computeNextRotationAt,
  mergeEffectiveOptions,
  rotationWorkflowId,
  validateIdentity,
  validateOperationId,
} from '../options';
import { assertPayloadSize, boundedText, LIMITS, retainRecent, utf8JsonBytes, validateActorInput } from '../limits';

const activities = proxyActivities<Omit<C.CredentialActivities, 'mergeOptions'>>({
  startToCloseTimeout: '30 seconds',
  scheduleToCloseTimeout: '2 minutes',
  retry: { maximumAttempts: 3 },
});
const localActivities = proxyLocalActivities<Pick<C.CredentialActivities, 'mergeOptions'>>({
  startToCloseTimeout: '10 seconds',
  retry: { maximumAttempts: 1 },
});

type ChildResult = { kind: 'result'; response: C.GetRotationStateResponse } | { kind: 'error'; error: unknown };
type ChildWait = ChildResult | { kind: 'timeout' } | { kind: 'cancelled' };
interface RunningChild {
  scope: CancellationScope;
  result: Promise<ChildResult>;
  settled?: ChildResult;
  timedOut: boolean;
}

/** The entity owns scheduling and safe child lifetime; adapters own all external I/O. */
export async function credentialWorkflow(request: C.ActivateCertificateRequest): Promise<void> {
  try { assertPayloadSize(request, request?.carryover ? LIMITS.carryoverBytes + LIMITS.startBytes : LIMITS.startBytes, 'Workflow input'); }
  catch (error) { fail(error instanceof Error ? error.message : String(error), 'InvalidRequest'); }
  const identity = pickIdentity(request);
  try { validateIdentity(identity); }
  catch (error) { fail(error instanceof Error ? error.message : String(error), 'InvalidIdentity'); }
  if (request.carryover && !workflowInfo().continuedFromExecutionRunId) {
    fail('Carryover is reserved for Continue-As-New', 'InvalidRequest');
  }
  let state: C.CredentialState = request.carryover ? copy(request.carryover.state) : {
    identity,
    status: 'INITIALIZING',
    revision: 0,
    pendingRotations: [],
    acceptedOperationIds: [],
    rotationHistory: [],
    updateOutcomes: [],
    failures: [],
  };
  if (!sameIdentity(state.identity, identity)) {
    throw ApplicationFailure.nonRetryable('Carryover identity does not match the request', 'InvalidIdentity');
  }
  let scheduledSequence = request.carryover?.scheduledSequence ?? 0;
  const acceptedIds = new Set(state.acceptedOperationIds);
  let initialized = false;
  let deactivating = state.status === 'DEACTIVATING';
  let suspension = state.suspension;
  let failed = state.status === 'FAILED';
  let refreshNeeded = true;
  let wakeVersion = 0;
  let continueRequested = false;
  let runningChild: RunningChild | undefined;
  let cancellationCleanupDone = false;
  let checkpointing = false;
  let activeUpdateBodies = 0;
  let updatesThisRun = 0;
  let checkpointBlocked = false;
  let checkpointSequence = 0;
  const rootScope = CancellationScope.current();

  const wake = (): void => { wakeVersion += 1; };
  const outcome = (name: string, result: string, operationId?: string): void => {
    state.updateOutcomes.push({ name, atMs: Date.now(), outcome: boundedText(result, 256), ...(operationId ? { operationId } : {}) });
    retainRecent(state.updateOutcomes, LIMITS.updateOutcomes);
  };
  const recordFailure = (error: unknown, operationId?: string, type?: string): C.FailureDetail => {
    let cause = error;
    const seen = new Set<unknown>();
    while (cause instanceof Error && cause.cause instanceof Error && !seen.has(cause.cause)) {
      seen.add(cause);
      cause = cause.cause;
    }
    const detail: C.FailureDetail = {
      message: boundedText(cause instanceof Error ? cause.message : String(cause)),
      type: boundedText(type ?? (cause instanceof ApplicationFailure ? cause.type ?? cause.name : cause instanceof Error ? cause.name : 'UnknownFailure'), 128),
      atMs: Date.now(),
      ...(operationId ? { operationId } : {}),
    };
    state.failures.push(detail);
    retainRecent(state.failures, LIMITS.failures);
    return detail;
  };
  const statusAtBoundary = (): void => {
    state.status = deactivating ? 'DEACTIVATING' : suspension ? 'SUSPENDED' : failed ? 'FAILED' : 'ACTIVE';
  };
  const assertOpen = (): void => {
    if (deactivating || state.status === 'STOPPED') fail('Credential is deactivating', 'CredentialClosing');
  };
  const awaitInitialized = async (): Promise<void> => { await condition(() => initialized); };
  const replaceOptions = (options: C.EffectiveOptions): void => {
    assertPayloadSize(options, LIMITS.startBytes, 'Effective options');
    state.options = copy(options);
    state.revision += 1;
    refreshNeeded = true;
    wake();
  };
  const effectiveRequest = (): C.ActivateCertificateRequest => {
    if (!state.options) fail('Credential options are unavailable; activate with complete input', 'NotInitialized');
    return {
      ...identity,
      credentialOptions: copy(state.options.credential),
      temporalNamespaceOptions: copy(state.options.temporalNamespaces),
      kubernetesOptions: copy(state.options.kubernetes),
      executionOptions: copy(state.options.execution),
    };
  };
  const wasArchived = async (operationId: string): Promise<boolean> => {
    const result = await activities.lookupArchivedOperation({
      identity: copy(identity), operationId,
      ...(state.historyArchive ? { expectedArchiveRef: state.historyArchive.archiveRef } : {}),
    });
    if (typeof result?.found !== 'boolean') fail('Archive lookup returned no definitive deduplication result', 'InvalidArchiveResponse');
    return result.found;
  };
  const assertSameIdentity = (input: C.ActivateCertificateRequest): void => {
    validateIdentity(input);
    if (!sameIdentity(input, identity)) fail('Activation cannot change identity', 'InvalidIdentity');
    if (input.carryover !== undefined) fail('Carryover is reserved for Continue-As-New', 'InvalidRequest');
  };
  const guardUpdate = <Request, Response>(name: string, handler: (input: Request) => Promise<Response>) =>
    async (input: Request): Promise<Response> => {
      // New handlers park outside this count while a checkpoint drains existing bodies.
      await condition(() => initialized && !checkpointing);
      activeUpdateBodies += 1;
      updatesThisRun += 1;
      try { return await handler(input); }
      catch (error) {
        // Validators see an earlier state than a handler that awaited initialization.
        // Invalid combinations discovered here must reject the Update, not fail the Workflow Task.
        if (error instanceof TemporalFailure) throw error;
        const rejection = ApplicationFailure.nonRetryable(error instanceof Error ? error.message : String(error), 'InvalidRequest');
        recordFailure(rejection);
        outcome(name, 'REJECTED');
        throw rejection;
      }
      finally { activeUpdateBodies -= 1; wake(); }
    };

  setHandler(messages.getState, (input = {}) => {
    const snapshot = copy(state);
    if (input.includeHistory === false) {
      snapshot.rotationHistory = [];
      snapshot.updateOutcomes = [];
    }
    if (input.includeFailureDetail !== true) {
      snapshot.failures = [];
      for (const summary of snapshot.rotationHistory) delete summary.failure;
      for (const summary of snapshot.rotationHistory) delete summary.compensationFailure;
    }
    return { state: snapshot };
  });
  setHandler(messages.activate, guardUpdate('activate', async (input: C.ActivateCertificateRequest) => {
    await awaitInitialized();
    assertOpen();
    if (state.activeRotation) fail('An active or unresolved rotation must reach a terminal boundary before activation', 'RotationInProgress');
    const next = mergeEffectiveOptions(input, state.options);
    delete next.execution.suspend;
    suspension = undefined;
    delete state.suspension;
    failed = false;
    if (checkpointBlocked) continueRequested = true;
    checkpointBlocked = false;
    replaceOptions(next);
    statusAtBoundary();
    outcome('activate', 'ACTIVE');
  }), { validator: input => {
    assertPayloadSize(input, LIMITS.updateBytes, 'Activation Update');
    assertSameIdentity(input);
    mergeEffectiveOptions(input, state.options);
  } });
  setHandler(messages.deactivate, guardUpdate('deactivate', async (input: C.DeactivateRequest) => {
    await awaitInitialized();
    deactivating = true;
    checkpointBlocked = false;
    state.status = 'DEACTIVATING';
    delete state.nextRotationAtMs;
    wake();
    await condition(() => !state.activeRotation);
    outcome('deactivate', `STOPPED: ${input.reason}`);
  }), { validator: validateActor });
  setHandler(messages.putTemporalNamespaces, guardUpdate('putTemporalNamespaces', async (input: C.PutTemporalNamespacesRequest) => {
    await awaitInitialized();
    assertOpen();
    if (!state.options) fail('Activate with complete input before changing scope', 'NotInitialized');
    const next = copy(state.options);
    next.temporalNamespaces = canonicalizeTemporalNamespaces(input.temporalNamespaceOptions);
    replaceOptions(next);
    outcome('putTemporalNamespaces', 'APPLIED');
    return { temporalNamespaceOptions: copy(next.temporalNamespaces), revision: state.revision };
  }), { validator: input => { assertPayloadSize(input, LIMITS.updateBytes, 'Scope Update'); canonicalizeTemporalNamespaces(input.temporalNamespaceOptions); } });
  setHandler(messages.putOptions, guardUpdate('putOptions', async (input: C.PutOptionsRequest) => {
    await awaitInitialized();
    assertOpen();
    const next = mergeEffectiveOptions({ ...effectiveRequest(), ...copy(input) }, state.options);
    if (input.executionOptions?.suspend) {
      suspension = copy(input.executionOptions.suspend);
      state.suspension = copy(suspension);
    }
    replaceOptions(next);
    if (!state.activeRotation) statusAtBoundary();
    outcome('putOptions', 'APPLIED');
    return { options: copy(next), revision: state.revision };
  }), { validator: input => {
    assertObject(input, 'PutOptionsRequest');
    assertPayloadSize(input, LIMITS.updateBytes, 'Options Update');
    if (state.options) mergeEffectiveOptions({ ...effectiveRequest(), ...copy(input) }, state.options);
    else {
      if (input.kubernetesOptions) canonicalizeKubernetes(input.kubernetesOptions);
      if (input.executionOptions?.suspend) validateActor(input.executionOptions.suspend);
    }
  } });
  setHandler(messages.rotate, guardUpdate('rotate', async (input: C.RotateRequest) => {
    await awaitInitialized();
    if (acceptedIds.has(input.operationId)) return { operationId: input.operationId, accepted: true, duplicate: true };
    assertOpen();
    if (suspension || failed || checkpointBlocked || !state.options) fail('Credential must be active before accepting rotations', 'CredentialInactive');
    if (state.pendingRotations.length >= LIMITS.pendingRotations) fail('Pending rotation queue is full; retry after work completes', 'RotationQueueFull');
    let historical: boolean;
    try { historical = await wasArchived(input.operationId); }
    catch (error) { recordFailure(error, input.operationId); outcome('rotate', 'REJECTED', input.operationId); throw error; }
    // Other handlers may have admitted the same operation while the lookup was in flight.
    if (historical || acceptedIds.has(input.operationId)) return { operationId: input.operationId, accepted: true, duplicate: true };
    assertOpen();
    if (suspension || failed || checkpointBlocked) fail('Credential became inactive before rotation acceptance', 'CredentialInactive');
    if (state.pendingRotations.length >= LIMITS.pendingRotations) fail('Pending rotation queue is full; retry after work completes', 'RotationQueueFull');
    acceptedIds.add(input.operationId);
    state.acceptedOperationIds.push(input.operationId);
    state.pendingRotations.push({ operationId: input.operationId, reason: input.reason, requester: input.requester });
    outcome('rotate', 'QUEUED', input.operationId);
    wake();
    return { operationId: input.operationId, accepted: true, duplicate: false };
  }), { validator: input => { validateActor(input); validateOperationId(input.operationId); } });
  setHandler(messages.restore, guardUpdate('restore', async (input: C.RestoreRequest) => {
    await awaitInitialized();
    const active = state.activeRotation;
    if (!active) fail('No active rotation is available to restore', 'NoActiveRotation');
    try {
      const result = await activities.requestRotateRevert({
        ...copy(input),
        operationId: active.request.operationId,
        childWorkflowId: active.childWorkflowId,
        revertRequestId: `${workflowInfo().runId}-${currentUpdateInfo()!.id}`,
      });
      assertPayloadSize(result, LIMITS.updateBytes, 'Revert response');
      const response = { operationId: active.request.operationId, response: copy(result.response) };
      state.lastRestoreResponse = response;
      outcome('restore', result.response.accepted ? 'ACCEPTED' : 'REJECTED', active.request.operationId);
      return copy(response);
    } catch (error) {
      recordFailure(error, active.request.operationId);
      outcome('restore', 'FAILED', active.request.operationId);
      throw error;
    }
  }), { validator: validateActor });
  setHandler(messages.suspend, guardUpdate('suspend', async (input: C.SuspendRequest) => {
    await awaitInitialized();
    assertOpen();
    suspension = { reason: input.reason, requester: input.requester };
    state.suspension = copy(suspension);
    if (state.options) state.options.execution.suspend = copy(suspension);
    state.status = state.activeRotation ? 'SUSPENDING' : 'SUSPENDED';
    delete state.nextRotationAtMs;
    wake();
    await condition(() => !state.activeRotation);
    // A concurrent deactivate takes precedence; terminal failures remain recorded while suspended.
    statusAtBoundary();
    outcome('suspend', 'SUSPENDED');
    return { status: 'SUSPENDED' as const };
  }), { validator: validateActor });

  const refreshInventory = async (): Promise<boolean> => {
    const revision = state.revision;
    const options = copy(state.options!);
    const [namespaces, deployments] = await Promise.all([
      activities.getTemporalNamespaceCertificates({ identity: copy(identity), temporalNamespaceOptions: options.temporalNamespaces }),
      activities.getKubernetesDeployments({ identity: copy(identity), kubernetesOptions: options.kubernetes }),
    ]);
    // Scope may change while I/O runs. Never decide or start a child from stale scope.
    if (revision !== state.revision) return false;
    assertPayloadSize(namespaces, LIMITS.inventoryBytes, 'Namespace inventory');
    assertPayloadSize(deployments, LIMITS.inventoryBytes, 'Deployment inventory');
    state.namespaceInventory = canonicalizeNamespaceInventory(namespaces);
    state.deploymentInventory = canonicalizeDeploymentInventory(deployments);
    state.nextRotationAtMs = computeNextRotationAt(state.namespaceInventory, options.credential.rotationOffsetMs, Date.now());
    refreshNeeded = false;
    return true;
  };
  const summarize = (active: C.ActiveRotation, result: ChildResult, forcedStatus?: C.RotationStatus): C.RotationSummary => {
    if (result.kind === 'result') {
      const summary = validateSummary(result.response, active);
      const bounded = copy(summary);
      for (const key of ['failure', 'compensationFailure'] as const) {
        if (bounded[key]) bounded[key] = { ...bounded[key]!, message: boundedText(bounded[key]!.message), type: boundedText(bounded[key]!.type, 128) };
      }
      return { ...bounded, ...(forcedStatus ? { status: forcedStatus } : {}) };
    }
    const detail = recordFailure(result.error, active.request.operationId);
    return {
      operationId: active.request.operationId,
      childWorkflowId: active.childWorkflowId,
      reason: active.request.reason,
      requester: active.request.requester,
      status: forcedStatus ?? (isCancellation(result.error) ? 'CANCELLED' : 'FAILED'),
      startedAtMs: active.startedAtMs,
      finishedAtMs: Date.now(),
      namespaceNames: active.request.namespaceInventory.namespaces.map(item => item.namespace),
      steps: [],
      failure: detail,
    };
  };
  const completeChild = (active: C.ActiveRotation, result: ChildResult, forcedStatus?: C.RotationStatus): boolean => {
    let summary: C.RotationSummary;
    let childProvidedSummary = false;
    try {
      summary = summarize(active, result, forcedStatus);
      childProvidedSummary = result.kind === 'result';
    }
    catch (error) { summary = summarize(active, { kind: 'error', error }); }
    state.rotationHistory.push(summary);
    if (summary.failure && childProvidedSummary) { state.failures.push(copy(summary.failure)); retainRecent(state.failures, LIMITS.failures); }
    if (forcedStatus === 'TIMED_OUT' && !summary.failure) {
      summary.failure = recordFailure(new Error('Rotation exceeded its timeout and was cancelled; the child has now reached its terminal boundary'), active.request.operationId, 'RotationTimeout');
    }
    delete state.activeRotation;
    runningChild = undefined;
    failed = summary.status !== 'SUCCEEDED';
    refreshNeeded = true;
    statusAtBoundary();
    wake();
    return summary.status === 'SUCCEEDED';
  };
  const cancelAndWait = async (child: RunningChild, timeoutMs: number): Promise<ChildResult | undefined> => {
    child.scope.cancel();
    return CancellationScope.nonCancellable(async () => {
      const timerScope = new CancellationScope();
      const timer = timerScope.run(async () => {
        await sleep(timeoutMs);
        return undefined;
      }).catch(() => undefined);
      try { return await Promise.race([child.result, timer]); }
      finally { timerScope.cancel(); }
    });
  };
  const recordUnresolved = (active: C.ActiveRotation): void => {
    failed = true;
    const failure = recordFailure(new Error('Rotation cancellation and revert exceeded the additional rotation timeout; further rotations are blocked until the child terminates'), active.request.operationId, 'RevertTimeout');
    state.rotationHistory.push({
      operationId: active.request.operationId,
      childWorkflowId: active.childWorkflowId,
      reason: active.request.reason,
      requester: active.request.requester,
      status: 'REVERT_TIMED_OUT',
      startedAtMs: active.startedAtMs,
      finishedAtMs: Date.now(),
      namespaceNames: active.request.namespaceInventory.namespaces.map(item => item.namespace),
      steps: [],
      failure,
    });
    state.status = 'FAILED';
    delete state.nextRotationAtMs;
  };
  const runRotation = async (rotation: C.RotateRequest): Promise<void> => {
    const options = copy(state.options!);
    const snapshot: C.StartRotateRequest = {
      identity: copy(identity), options,
      namespaceInventory: copy(state.namespaceInventory!),
      deploymentInventory: copy(state.deploymentInventory!),
      ...copy(rotation),
    };
    const active: C.ActiveRotation = {
      request: snapshot,
      childWorkflowId: rotationWorkflowId(identity, rotation.operationId),
      startedAtMs: Date.now(),
    };
    state.activeRotation = copy(active);
    state.status = 'ROTATING';
    delete state.nextRotationAtMs;
    const childScope = new CancellationScope();
    const child: RunningChild = {
      scope: childScope,
      result: childScope.run(() => executeChild<C.RotateWorkflow>('rotateWorkflow', {
        workflowId: active.childWorkflowId,
        args: [copy(snapshot)],
        cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        parentClosePolicy: ParentClosePolicy.REQUEST_CANCEL,
      })).then<ChildResult, ChildResult>(response => ({ kind: 'result', response }), error => ({ kind: 'error', error })),
      timedOut: false,
    };
    runningChild = child;
    void child.result.then(result => { child.settled = result; wake(); });
    const timerScope = new CancellationScope();
    const timer: Promise<ChildWait> = timerScope.run(async () => {
      await sleep(options.execution.rotationTimeoutMs);
      return { kind: 'timeout' as const };
    }).catch(() => ({ kind: 'cancelled' as const }));
    const cancelled: Promise<ChildWait> = rootScope.cancelRequested.then(
      () => ({ kind: 'cancelled' as const }),
      () => ({ kind: 'cancelled' as const }),
    );
    let result: ChildWait;
    try { result = await Promise.race([child.result, timer, cancelled]); }
    finally { timerScope.cancel(); }
    if (result.kind === 'result' || result.kind === 'error') {
      continueRequested = completeChild(active, result);
      return;
    }
    child.timedOut = result.kind === 'timeout';
    const terminal = await cancelAndWait(child, options.execution.rotationTimeoutMs);
    if (terminal) completeChild(active, terminal, result.kind === 'timeout' ? 'TIMED_OUT' : 'CANCELLED');
    else recordUnresolved(active);
    if (result.kind === 'cancelled' || rootScope.consideredCancelled) {
      cancellationCleanupDone = true;
      throw new CancelledFailure('Credential cancellation completed its bounded child cleanup');
    }
  };

  const rolloverDue = (): boolean => continueRequested || updatesThisRun >= LIMITS.updatesPerRun
    || workflowInfo().continueAsNewSuggested
    || state.acceptedOperationIds.length > LIMITS.recentOperationIds
    || state.rotationHistory.length > LIMITS.recentRotations
    || utf8JsonBytes(state.rotationHistory) > LIMITS.recentHistoryBytes;
  const archiveAndCompact = async (): Promise<boolean> => {
    checkpointing = true;
    try {
      // Accepted handlers arriving now wait outside activeUpdateBodies. Existing
      // async lookups must finish before any IDs can move from memory to storage.
      await condition(() => activeUpdateBodies === 0);
      const archived = await activities.archiveCredentialHistory({
        identity: copy(identity), checkpointId: `${workflowInfo().runId}-${checkpointSequence++}`,
        ...(state.historyArchive ? { expectedArchiveRef: state.historyArchive.archiveRef } : {}),
        operationIds: copy(state.acceptedOperationIds), rotationHistory: copy(state.rotationHistory),
        updateOutcomes: copy(state.updateOutcomes), failures: copy(state.failures),
      });
      assertPayloadSize(archived, LIMITS.updateBytes, 'Archive acknowledgement');
      if (typeof archived.archiveRef !== 'string' || !archived.archiveRef
        || !Number.isSafeInteger(archived.operationCount) || archived.operationCount < 0
        || !Number.isSafeInteger(archived.rotationCount) || archived.rotationCount < 0) {
        fail('Archive did not return a valid durable acknowledgement', 'InvalidArchiveResponse');
      }
      state.historyArchive = copy(archived);
      retainRecent(state.rotationHistory, LIMITS.recentRotations);
      while (state.rotationHistory.length > 1 && utf8JsonBytes(state.rotationHistory) > LIMITS.recentHistoryBytes) state.rotationHistory.shift();
      const pendingIds = state.pendingRotations.map(item => item.operationId);
      state.acceptedOperationIds = [...new Set([...state.acceptedOperationIds.slice(-LIMITS.recentOperationIds), ...pendingIds])];
      acceptedIds.clear();
      for (const id of state.acceptedOperationIds) acceptedIds.add(id);
      retainRecent(state.updateOutcomes, LIMITS.updateOutcomes);
      retainRecent(state.failures, LIMITS.failures);
      return true;
    } catch (error) {
      if (isCancellation(error)) throw error;
      recordFailure(error, undefined, 'ArchiveUnavailable');
      failed = true;
      checkpointBlocked = true;
      continueRequested = false;
      statusAtBoundary();
      return false;
    } finally { checkpointing = false; wake(); }
  };
  const carryoverRequest = (): C.ActivateCertificateRequest => {
    const next = state.options ? effectiveRequest() : { ...copy(request), carryover: undefined };
    const carried = copy(state);
    // Authoritative inventories are re-read at the start of every run.
    delete carried.namespaceInventory;
    delete carried.deploymentInventory;
    delete carried.nextRotationAtMs;
    next.carryover = { state: carried, scheduledSequence };
    // All of these older records were acknowledged by the archive before pruning.
    while (utf8JsonBytes(next) > LIMITS.carryoverBytes) {
      if (carried.rotationHistory.length > 0) carried.rotationHistory.shift();
      else if (carried.updateOutcomes.length > 0) carried.updateOutcomes.shift();
      else if (carried.failures.length > 0) carried.failures.shift();
      else fail('Bounded core carryover exceeds the configured byte budget', 'CarryoverTooLarge');
    }
    return next;
  };

  try {
    // This Local Activity is deliberately the first command in every run, including Continue-As-New.
    try {
      const merged = await localActivities.mergeOptions({ request: copy(request) });
      assertPayloadSize(merged.options, LIMITS.startBytes, 'Effective options');
      state.options = copy(merged.options);
      if (!request.carryover) {
        suspension = merged.options.execution.suspend ? copy(merged.options.execution.suspend) : undefined;
        state.suspension = suspension;
        failed = false;
      }
      statusAtBoundary();
      await refreshInventory();
      if (suspension) delete state.nextRotationAtMs;
    } catch (error) {
      if (isCancellation(error)) throw error;
      recordFailure(error);
      failed = true;
      state.status = 'FAILED';
    } finally { initialized = true; }

    while (true) {
      if (deactivating && !state.activeRotation) {
        if (checkpointBlocked) {
          const observedWake = wakeVersion;
          await condition(() => wakeVersion !== observedWake);
          continue;
        }
        if (!await archiveAndCompact()) continue;
        await condition(allHandlersFinished);
        state.status = 'STOPPED';
        return;
      }
      if (rolloverDue() && !state.activeRotation && !checkpointBlocked) {
        if (!await archiveAndCompact()) continue;
        await condition(allHandlersFinished);
        if (deactivating) continue;
        await continueAsNew<typeof credentialWorkflow>(carryoverRequest());
      }
      if (state.activeRotation && runningChild?.settled) {
        // A child that missed its revert deadline may still reach a safe boundary later.
        // Keep the bounded timeout summary, then record its eventual terminal response.
        completeChild(state.activeRotation, runningChild.settled, 'TIMED_OUT');
        continue;
      }
      if (deactivating || suspension || failed || !state.options || state.activeRotation) {
        const observedWake = wakeVersion;
        await condition(() => wakeVersion !== observedWake);
        continue;
      }
      if (refreshNeeded) {
        try {
          if (!await refreshInventory()) continue;
        } catch (error) {
          if (isCancellation(error)) throw error;
          recordFailure(error);
          failed = true;
          statusAtBoundary();
          continue;
        }
        // An Update may have suspended or deactivated the entity while inventory was loading.
        if (deactivating || suspension || failed) continue;
      }
      const manual = state.pendingRotations[0];
      const scheduled = state.nextRotationAtMs !== undefined && state.nextRotationAtMs <= Date.now();
      if (manual || scheduled) {
        const revision = state.revision;
        try {
          // External inventory is authoritative and is refreshed at the decision boundary.
          if (!await refreshInventory()) continue;
        } catch (error) {
          if (isCancellation(error)) throw error;
          recordFailure(error, manual?.operationId);
          failed = true;
          statusAtBoundary();
          continue;
        }
        if (revision !== state.revision || deactivating || suspension || failed) continue;
        if (!manual && (state.nextRotationAtMs === undefined || state.nextRotationAtMs > Date.now())) continue;
        let nextRotation: C.RotateRequest;
        if (manual) nextRotation = state.pendingRotations.shift()!;
        else {
          let operationId: string;
          try {
            do { operationId = `scheduled-${++scheduledSequence}`; }
            while (acceptedIds.has(operationId) || await wasArchived(operationId));
          } catch (error) {
            if (isCancellation(error)) throw error;
            recordFailure(error);
            failed = true;
            statusAtBoundary();
            continue;
          }
          // Async deduplication must not let a scope Update invalidate this decision snapshot.
          if (revision !== state.revision || deactivating || suspension || failed) continue;
          acceptedIds.add(operationId);
          state.acceptedOperationIds.push(operationId);
          nextRotation = { operationId, reason: 'Earliest managed certificate reached its rotation offset', requester: 'credential-scheduler' };
        }
        await runRotation(nextRotation);
        continue;
      }
      const observedWake = wakeVersion;
      if (state.nextRotationAtMs === undefined) await condition(() => wakeVersion !== observedWake || rolloverDue());
      else await condition(() => wakeVersion !== observedWake || rolloverDue(), Math.max(1, state.nextRotationAtMs - Date.now()));
    }
  } catch (error) {
    if (!isCancellation(error)) throw error;
    if (!cancellationCleanupDone && runningChild && state.activeRotation) {
      const active = state.activeRotation;
      const terminal = await cancelAndWait(runningChild, active.request.options.execution.rotationTimeoutMs);
      if (terminal) completeChild(active, terminal, 'CANCELLED');
      else recordUnresolved(active);
    }
    throw error;
  }
}

setWorkflowOptions({ versioningBehavior: 'PINNED' }, credentialWorkflow);

function copy<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }
function fail(message: string, type: string): never { throw ApplicationFailure.nonRetryable(message, type); }
function assertObject(value: unknown, name: string): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail(`${name} must be an object`, 'InvalidRequest');
}
function validateActor(input: C.RestoreRequest): void {
  assertPayloadSize(input, LIMITS.updateBytes, 'Operator Update');
  validateActorInput(input);
  assertObject(input, 'Request');
  if (typeof input.reason !== 'string' || !input.reason.trim() || typeof input.requester !== 'string' || !input.requester.trim()) {
    fail('reason and requester must be non-empty strings', 'InvalidRequest');
  }
}
function pickIdentity(input: C.CredentialIdentity): C.CredentialIdentity {
  assertObject(input, 'Credential request');
  return { credentialType: input.credentialType, environment: input.environment, teamName: input.teamName, scopeKey: input.scopeKey };
}
function sameIdentity(a: C.CredentialIdentity, b: C.CredentialIdentity): boolean {
  return a.credentialType === b.credentialType && a.environment === b.environment && a.teamName === b.teamName && a.scopeKey === b.scopeKey;
}
function compare(a: string, b: string): number { return a < b ? -1 : a > b ? 1 : 0; }
function canonicalizeNamespaceInventory(input: C.GetTemporalNamespaceCertificatesResponse): C.GetTemporalNamespaceCertificatesResponse {
  if (!input || !Array.isArray(input.namespaces) || !Number.isFinite(input.observedAtMs)) fail('Malformed namespace inventory', 'InvalidInventory');
  const namespaces = new Map<string, C.CertificateInventoryItem>();
  for (const item of input.namespaces) {
    if (!item || typeof item.namespace !== 'string' || !item.namespace || !Array.isArray(item.certificates)) fail('Malformed certificate inventory', 'InvalidInventory');
    const existing = namespaces.get(item.namespace)?.certificates ?? [];
    for (const certificate of item.certificates) {
      if (!certificate || typeof certificate.fingerprint !== 'string' || !Number.isFinite(certificate.notAfterMs)) fail('Malformed certificate expiration', 'InvalidInventory');
    }
    const certs = new Map([...existing, ...item.certificates].map(cert => [`${cert.fingerprint}:${cert.notAfterMs}`, copy(cert)]));
    namespaces.set(item.namespace, { namespace: item.namespace, certificates: [...certs.values()].sort((a, b) => a.notAfterMs - b.notAfterMs || compare(a.fingerprint, b.fingerprint)) });
  }
  return { namespaces: [...namespaces.values()].sort((a, b) => compare(a.namespace, b.namespace)), observedAtMs: input.observedAtMs };
}
function canonicalizeDeploymentInventory(input: C.GetKubernetesDeploymentsResponse): C.GetKubernetesDeploymentsResponse {
  if (!input || !Array.isArray(input.deployments) || !Number.isFinite(input.observedAtMs) || typeof input.skipped !== 'boolean') fail('Malformed deployment inventory', 'InvalidInventory');
  const deployments = new Map<string, C.DeploymentInventoryItem>();
  for (const item of input.deployments) {
    if (!item || typeof item.namespace !== 'string' || typeof item.name !== 'string' || ![item.generation, item.desiredReplicas, item.availableReplicas].every(Number.isFinite)) fail('Malformed deployment', 'InvalidInventory');
    deployments.set(`${item.namespace}/${item.name}`, copy(item));
  }
  return { deployments: [...deployments.values()].sort((a, b) => compare(a.namespace, b.namespace) || compare(a.name, b.name)), observedAtMs: input.observedAtMs, skipped: input.skipped };
}
function validateSummary(response: C.GetRotationStateResponse, active: C.ActiveRotation): C.RotationSummary {
  assertPayloadSize(response, LIMITS.summaryBytes, 'Rotation summary');
  const summary = response?.summary;
  const statuses: C.RotationStatus[] = ['SUCCEEDED', 'REVERTED', 'CANCELLED', 'FAILED', 'TIMED_OUT', 'REVERT_TIMED_OUT'];
  if (!summary || summary.operationId !== active.request.operationId || summary.childWorkflowId !== active.childWorkflowId || !statuses.includes(summary.status) || !Array.isArray(summary.steps) || !Array.isArray(summary.namespaceNames) || !Number.isFinite(summary.startedAtMs) || !Number.isFinite(summary.finishedAtMs)) {
    fail('Rotate child returned an invalid or mismatched terminal summary', 'InvalidRotationResult');
  }
  return summary;
}
