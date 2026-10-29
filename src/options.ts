import type {
  ActivateCertificateRequest, CredentialIdentity, EffectiveOptions,
  GetTemporalNamespaceCertificatesResponse, KubernetesOptions, TemporalNamespaceOptions,
} from './contracts';
import { assertPayloadSize, LIMITS, validateActorInput } from './limits';

export const DAY_MS = 24 * 60 * 60 * 1000;
export const DEFAULT_OPTIONS: EffectiveOptions = {
  credential: { ttlMs: 90 * DAY_MS, rotationOffsetMs: 14 * DAY_MS },
  temporalNamespaces: { names: [] },
  kubernetes: { enabled: false, deploymentSelectors: [] },
  execution: { rotationTimeoutMs: 30 * 60 * 1000 },
};
const SAFE_IDENTIFIER = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$/;
export function validateIdentity(identity: CredentialIdentity): void {
  if (!identity || !['certificate', 'api-key'].includes(identity.credentialType)) throw new Error('credentialType must be certificate or api-key');
  for (const field of ['environment', 'teamName', 'scopeKey'] as const) {
    if (typeof identity[field] !== 'string' || !SAFE_IDENTIFIER.test(identity[field])) throw new Error(`${field} must be a business-safe identifier (1-64 letters, digits, dot, underscore or hyphen)`);
  }
}
export function validateOperationId(operationId: string): void {
  if (typeof operationId !== 'string' || !SAFE_IDENTIFIER.test(operationId)) throw new Error('operationId must be a business-safe identifier (1-64 letters, digits, dot, underscore or hyphen)');
}
export function credentialWorkflowId(identity: CredentialIdentity): string {
  validateIdentity(identity);
  const value = `${identity.credentialType}-${identity.environment}-${identity.teamName}-${identity.scopeKey}`;
  if (value.length > 180) throw new Error('Credential identity is too long; use shorter business identifiers');
  return value;
}
export function rotationWorkflowId(identity: CredentialIdentity, operationId: string): string {
  validateOperationId(operationId);
  return `rotate-${credentialWorkflowId(identity)}-${operationId}`;
}
export function canonicalizeTemporalNamespaces(options: TemporalNamespaceOptions): TemporalNamespaceOptions {
  if (!options || !Array.isArray(options.names)) throw new Error('Complete temporalNamespaceOptions.names is required');
  const names = options.names.map((name) => {
    if (typeof name !== 'string' || !name.trim() || name !== name.trim()) throw new Error('Namespace names must be nonempty without surrounding whitespace');
    return name;
  });
  let tagSelector: Record<string, string> | undefined;
  if (options.tagSelector !== undefined) {
    if (!options.tagSelector || typeof options.tagSelector !== 'object' || Array.isArray(options.tagSelector)) throw new Error('tagSelector must be a string map');
    tagSelector = {};
    for (const key of Object.keys(options.tagSelector).sort()) {
      const value = options.tagSelector[key];
      if (!key.trim() || typeof value !== 'string' || !value.trim()) throw new Error('Namespace tag keys and values must be nonempty');
      Object.defineProperty(tagSelector, key, { value, enumerable: true, configurable: true, writable: true });
    }
  }
  return { names: [...new Set(names)].sort(), ...(tagSelector && Object.keys(tagSelector).length ? { tagSelector } : {}) };
}
export function canonicalizeKubernetes(options: KubernetesOptions = {}): EffectiveOptions['kubernetes'] {
  if (options.enabled !== undefined && typeof options.enabled !== 'boolean') throw new Error('Kubernetes enabled must be boolean');
  if (options.deploymentSelectors !== undefined && !Array.isArray(options.deploymentSelectors)) throw new Error('deploymentSelectors must be an array');
  const selectors = new Map<string, { namespace?: string; labelSelector: string }>();
  for (const selector of options.deploymentSelectors ?? []) {
    if (!selector || typeof selector.labelSelector !== 'string' || !selector.labelSelector.trim()) throw new Error('Deployment labelSelector must be nonempty');
    if (selector.namespace !== undefined && (typeof selector.namespace !== 'string' || !selector.namespace.trim())) throw new Error('Deployment namespace must be nonempty when supplied');
    const value = { ...(selector.namespace ? { namespace: selector.namespace.trim() } : {}), labelSelector: selector.labelSelector.trim() };
    selectors.set(JSON.stringify([value.namespace ?? '', value.labelSelector]), value);
  }
  return { enabled: options.enabled ?? false, deploymentSelectors: [...selectors.entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([, value]) => value) };
}
function positiveDuration(value: number, name: string, zeroAllowed = false): void {
  if (!Number.isSafeInteger(value) || (zeroAllowed ? value < 0 : value <= 0)) throw new Error(`${name} must be a ${zeroAllowed ? 'nonnegative' : 'positive'} safe integer in milliseconds`);
}
/** Run as a Local Activity. Environment defaults are adapter-owned, outside deterministic code. */
export function mergeEffectiveOptions(request: ActivateCertificateRequest, environmentDefaults: Partial<EffectiveOptions> = {}): EffectiveOptions {
  const { carryover: _carryover, ...input } = request;
  assertPayloadSize(input, LIMITS.startBytes, 'Credential options');
  validateIdentity(request);
  credentialWorkflowId(request);
  if (request.credentialType !== 'certificate') throw new Error('This example implements certificate credentials only');
  const temporalNamespaces = canonicalizeTemporalNamespaces(request.temporalNamespaceOptions);
  const credential = {
    ttlMs: request.credentialOptions?.ttlMs ?? environmentDefaults.credential?.ttlMs ?? DEFAULT_OPTIONS.credential.ttlMs,
    rotationOffsetMs: request.credentialOptions?.rotationOffsetMs ?? environmentDefaults.credential?.rotationOffsetMs ?? DEFAULT_OPTIONS.credential.rotationOffsetMs,
  };
  const suspension = request.executionOptions?.suspend ?? environmentDefaults.execution?.suspend;
  const execution = {
    rotationTimeoutMs: request.executionOptions?.rotationTimeoutMs ?? environmentDefaults.execution?.rotationTimeoutMs ?? DEFAULT_OPTIONS.execution.rotationTimeoutMs,
    ...(suspension ? { suspend: { reason: suspension.reason, requester: suspension.requester } } : {}),
  };
  const kubernetes = canonicalizeKubernetes({ ...DEFAULT_OPTIONS.kubernetes, ...environmentDefaults.kubernetes, ...request.kubernetesOptions });
  positiveDuration(credential.ttlMs, 'ttlMs');
  positiveDuration(credential.rotationOffsetMs, 'rotationOffsetMs', true);
  positiveDuration(execution.rotationTimeoutMs, 'rotationTimeoutMs');
  if (credential.rotationOffsetMs >= credential.ttlMs) throw new Error('rotationOffsetMs must be smaller than ttlMs');
  if (execution.suspend && (!execution.suspend.reason?.trim() || !execution.suspend.requester?.trim())) throw new Error('Startup suspension requires reason and requester');
  if (execution.suspend) validateActorInput(execution.suspend);
  const effective = { credential, temporalNamespaces, kubernetes, execution };
  assertPayloadSize(effective, LIMITS.startBytes, 'Effective options');
  return effective;
}
export function computeNextRotationAt(inventory: GetTemporalNamespaceCertificatesResponse, rotationOffsetMs: number, nowMs: number): number | undefined {
  positiveDuration(rotationOffsetMs, 'rotationOffsetMs', true);
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) throw new Error('nowMs must be an epoch millisecond timestamp');
  const expirations = inventory.namespaces.flatMap((namespace) => namespace.certificates.map((certificate) => certificate.notAfterMs));
  for (const expiration of expirations) positiveDuration(expiration, 'Certificate expiration');
  return expirations.length ? Math.max(nowMs, Math.min(...expirations) - rotationOffsetMs) : undefined;
}
