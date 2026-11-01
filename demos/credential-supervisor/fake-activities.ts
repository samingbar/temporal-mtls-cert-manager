import type { Client } from '@temporalio/client';
import { createHash } from 'node:crypto';
import { ApplicationFailure } from '@temporalio/common';
import type {
  CredentialActivities, CredentialIdentity, GetTemporalNamespaceCertificatesRequest, StartRotateRequest,
} from '../../src/contracts';
import { revert } from '../../src/messages';
import { mergeEffectiveOptions } from '../../src/options';
import type { CredentialArchiveActivities } from '../../src/archive-contracts';
import { createCredentialArchiveStore } from './archive-store';

export type FakeRotationMode = 'complete' | 'hold' | 'hold-after-rollout' | 'fail';
export type FakeRotationStep = 'issue' | 'add-trust' | 'rollout-clients' | 'verify-clients' | 'remove-old-trust' | 'retire-old' | 'revert';
export interface FakeRotationConfig { mode: FakeRotationMode; stepDelayMs: number; holdCompensation: boolean }
/** Injected simulation facts; no Deployment is inspected and no authenticated request is sent. */
export interface FakeClientVerification { deploymentsConverged: boolean; newCredentialAccepted: boolean }
export interface FakeClientVerificationRecord extends FakeClientVerification { identity: CredentialIdentity; operationId: string }
export interface FakeRotationStepRequest { request: StartRotateRequest; step: FakeRotationStep }
export interface FakeRotationStepResponse { description: string }
export interface FakeWorkflowActivities {
  getFakeRotationConfig(request: StartRotateRequest): Promise<FakeRotationConfig>;
  performFakeRotationStep(request: FakeRotationStepRequest): Promise<FakeRotationStepResponse>;
}
export interface FakeNamespace {
  name: string;
  tags: Record<string, string>;
  trustedCAs: string[];
  clientCA: string;
  notAfterMs: number;
}
export interface FakeInventorySnapshot {
  adapter: 'FAKE — in-memory only';
  namespaces: FakeNamespace[];
  retiredCAs: string[];
  events: string[];
  clientVerifications: FakeClientVerificationRecord[];
}
export interface FakeActivityOptions {
  client?: Client;
  /** Use a persistent shared path for worker restarts; isolated tests default to memory. */
  archivePath?: string;
  mode?: FakeRotationMode;
  stepDelayMs?: number;
  /** Fault injection only: never reach a safe terminal boundary after cancellation. */
  holdCompensation?: boolean;
  initialNotAfterMs?: number;
  /** Optional fully specified seed inventory for focused scenarios. */
  namespaces?: FakeNamespace[];
  /** Independent simulated readiness and connectivity facts for the verification gate. */
  clientVerification?: FakeClientVerification;
}

const INITIAL_CA = 'FAKE-CA-INITIAL';
const DAY = 24 * 60 * 60 * 1000;

/** Operation IDs are unique within a Credential, never across all Credentials. */
function fakeOperationKey(request: StartRotateRequest): string {
  const { credentialType, environment, teamName, scopeKey } = request.identity;
  return JSON.stringify([credentialType, environment, teamName, scopeKey, request.operationId]);
}

export function fakeCAFor(request: StartRotateRequest): string {
  return `FAKE-CA-${createHash('sha256').update(fakeOperationKey(request)).digest('hex')}`;
}

/** Demo adapter only: no network calls except the explicit child Update bridge. */
export function createFakeActivities(options: FakeActivityOptions = {}) {
  const archive = createCredentialArchiveStore({ path: options.archivePath ?? ':memory:' });
  let config: FakeRotationConfig = { mode: options.mode ?? 'complete', stepDelayMs: options.stepDelayMs ?? 25, holdCompensation: options.holdCompensation ?? false };
  const initialNotAfterMs = options.initialNotAfterMs ?? Date.now() + 30 * DAY;
  const records = new Map<string, FakeNamespace>();
  const retiredCAs = new Set<string>();
  const events: string[] = [];
  const clientVerifications: FakeClientVerificationRecord[] = [];
  const simulatedVerification = structuredClone(options.clientVerification ?? { deploymentsConverged: true, newCredentialAccepted: true });
  const operations = new Map<string, {
    old: Map<string, { trustedCAs: string[]; clientCA: string; notAfterMs: number }>;
    newCA: string;
    completed: Set<FakeRotationStep>;
  }>();

  function ensure(name: string): FakeNamespace {
    let record = records.get(name);
    if (!record) {
      record = { name, tags: {}, trustedCAs: [INITIAL_CA], clientCA: INITIAL_CA, notAfterMs: initialNotAfterMs };
      records.set(name, record);
    }
    return record;
  }
  if (options.namespaces) {
    for (const record of options.namespaces) records.set(record.name, structuredClone(record));
  } else {
    for (const name of ['orders-dev', 'payments-dev', 'tagged-dev']) {
      ensure(name).tags = { environment: 'dev', team: 'platform' };
    }
  }

  function selected(request: GetTemporalNamespaceCertificatesRequest): FakeNamespace[] {
    const names = new Set(request.temporalNamespaceOptions.names);
    const tags = request.temporalNamespaceOptions.tagSelector;
    if (tags && Object.keys(tags).length > 0) {
      for (const record of records.values()) {
        if (Object.entries(tags).every(([key, value]) => record.tags[key] === value)) names.add(record.name);
      }
    }
    return [...names].sort().map(ensure);
  }

  const activities: CredentialActivities & CredentialArchiveActivities & FakeWorkflowActivities = {
    ...archive.activities,
    async mergeOptions({ request }) {
      return { options: mergeEffectiveOptions(request) };
    },
    async getTemporalNamespaceCertificates(request) {
      return {
        namespaces: selected(request).map(record => ({
          namespace: record.name,
          certificates: [...record.trustedCAs].sort().map(fingerprint => ({ fingerprint, notAfterMs: record.notAfterMs })),
        })),
        observedAtMs: Date.now(),
      };
    },
    async getKubernetesDeployments({ kubernetesOptions }) {
      const deployments = new Map<string, {
        namespace: string; name: string; generation: number; desiredReplicas: number; availableReplicas: number;
      }>();
      if (kubernetesOptions.enabled) {
        for (const selector of kubernetesOptions.deploymentSelectors) {
          const namespace = selector.namespace ?? 'apps';
          const name = 'fake-sdk-workers';
          deployments.set(`${namespace}/${name}`, { namespace, name, generation: 1, desiredReplicas: 2, availableReplicas: 2 });
        }
      }
      return { deployments: [...deployments.values()].sort((a, b) => `${a.namespace}/${a.name}`.localeCompare(`${b.namespace}/${b.name}`)), observedAtMs: Date.now(), skipped: !kubernetesOptions.enabled };
    },
    async requestRotateRevert(request) {
      if (!options.client) throw ApplicationFailure.nonRetryable('Fake revert bridge requires a Temporal client', 'MissingFakeClient');
      const response = await options.client.workflow.getHandle(request.childWorkflowId).executeUpdate(revert, {
        args: [{ operationId: request.operationId, reason: request.reason, requester: request.requester }],
        // Retry this Activity with the same Update, but allow a later operator restore to retry compensation.
        updateId: `revert-${request.revertRequestId}`,
      });
      return { response };
    },
    async getFakeRotationConfig() { return { ...config }; },
    async performFakeRotationStep({ request, step }) {
      const operationKey = fakeOperationKey(request);
      let operation = operations.get(operationKey);
      if (!operation) {
        operation = { old: new Map(), newCA: fakeCAFor(request), completed: new Set() };
        for (const { namespace } of request.namespaceInventory.namespaces) {
          const record = ensure(namespace);
          operation.old.set(namespace, { trustedCAs: [...record.trustedCAs], clientCA: record.clientCA, notAfterMs: record.notAfterMs });
        }
        operations.set(operationKey, operation);
      }
      const description = `FAKE ${step}: ${request.operationId}`;
      if (operation.completed.has(step)) return { description };
      const namespaces = [...operation.old.keys()].map(ensure);
      switch (step) {
        case 'issue': break;
        case 'add-trust':
          for (const record of namespaces) record.trustedCAs = [...new Set([...record.trustedCAs, operation.newCA])].sort();
          break;
        case 'rollout-clients':
          for (const record of namespaces) {
            if (!record.trustedCAs.includes(operation.newCA)) throw ApplicationFailure.nonRetryable('Cannot change fake clients before adding trust', 'UnsafeFakeRotation');
            record.clientCA = operation.newCA;
          }
          break;
        case 'verify-clients': {
          // These separate fixture facts model two required checks, not production evidence.
          const verification: FakeClientVerificationRecord = {
            identity: structuredClone(request.identity), operationId: request.operationId,
            deploymentsConverged: simulatedVerification.deploymentsConverged,
            newCredentialAccepted: simulatedVerification.newCredentialAccepted,
          };
          clientVerifications.push(verification);
          if (verification.deploymentsConverged !== true) {
            throw ApplicationFailure.nonRetryable('Simulated Deployment rollout has not converged; old CA trust must remain', 'FakeDeploymentNotConverged');
          }
          if (verification.newCredentialAccepted !== true) {
            throw ApplicationFailure.nonRetryable('Simulated authenticated connection using the new credential was rejected; old CA trust must remain', 'FakeClientConnectivityRejected');
          }
          if (namespaces.some(record => record.clientCA !== operation.newCA || !record.trustedCAs.includes(operation.newCA))) {
            throw ApplicationFailure.nonRetryable('Fake clients do not use a trusted new credential', 'UnsafeFakeRotation');
          }
          break;
        }
        case 'remove-old-trust':
          if (!operation.completed.has('verify-clients')) throw ApplicationFailure.nonRetryable('Cannot remove fake old trust before client verification', 'UnsafeFakeRotation');
          for (const record of namespaces) {
            if (record.clientCA !== operation.newCA) throw ApplicationFailure.nonRetryable('Cannot remove fake old trust before client rollout', 'UnsafeFakeRotation');
            record.trustedCAs = [operation.newCA];
            record.notAfterMs = Date.now() + request.options.credential.ttlMs;
          }
          break;
        case 'retire-old': {
          const oldCAs = new Set([...operation.old.values()].flatMap(old => old.trustedCAs));
          // Real PKI retirement needs complete dependency knowledge beyond the current scope.
          for (const record of records.values()) {
            if (oldCAs.has(record.clientCA) || record.trustedCAs.some(ca => oldCAs.has(ca))) {
              throw ApplicationFailure.nonRetryable(`Cannot retire fake CA still referenced by ${record.name}`, 'UnsafeFakeRotation');
            }
          }
          for (const ca of oldCAs) retiredCAs.add(ca);
          break;
        }
        case 'revert':
          for (const [name, old] of operation.old) {
            Object.assign(ensure(name), structuredClone(old));
            for (const ca of old.trustedCAs) retiredCAs.delete(ca);
          }
          break;
      }
      operation.completed.add(step);
      events.push(description);
      return { description };
    },
  };

  return {
    activities,
    close() { archive.close(); },
    setRotationMode(mode: FakeRotationMode, stepDelayMs = config.stepDelayMs) { config = { ...config, mode, stepDelayMs }; },
    inspect(): FakeInventorySnapshot {
      return structuredClone({ adapter: 'FAKE — in-memory only' as const, namespaces: [...records.values()].sort((a, b) => a.name.localeCompare(b.name)), retiredCAs: [...retiredCAs].sort(), events, clientVerifications });
    },
  };
}
