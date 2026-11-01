import { open } from 'node:fs/promises';
import type { ActivateCertificateRequest } from '../../src/contracts';
import { assertPayloadSize, LIMITS } from '../../src/limits';
import { mergeEffectiveOptions } from '../../src/options';
import { DEMO_REQUEST } from './fixtures';

type IdentityOverrides = Partial<Pick<ActivateCertificateRequest, 'environment' | 'teamName' | 'scopeKey'>>;

function knownKeys(value: unknown, keys: readonly string[], label: string): asserts value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${label} must be an object`);
  for (const key of Object.keys(value)) {
    if (!keys.includes(key)) throw new Error(`${label} contains unsupported field ${key}; supply non-sensitive request metadata only`);
  }
}

/** Proposed application-binding handoff; it is not a customer Terraform schema. */
export function normalizeCredentialRequest(input: unknown, overrides: IdentityOverrides = {}): ActivateCertificateRequest {
  if (input && typeof input === 'object' && Object.hasOwn(input, 'carryover')) {
    throw new Error('carryover is internal workflow state and cannot be supplied by a starting client');
  }
  knownKeys(input, ['credentialType', 'environment', 'teamName', 'scopeKey', 'credentialOptions',
    'temporalNamespaceOptions', 'kubernetesOptions', 'executionOptions'], 'Credential request');
  assertPayloadSize(input, LIMITS.startBytes, 'Credential request');
  if (input.credentialOptions !== undefined) knownKeys(input.credentialOptions, ['ttlMs', 'rotationOffsetMs'], 'credentialOptions');
  knownKeys(input.temporalNamespaceOptions, ['names', 'tagSelector'], 'temporalNamespaceOptions');
  if (input.kubernetesOptions !== undefined) {
    knownKeys(input.kubernetesOptions, ['enabled', 'deploymentSelectors'], 'kubernetesOptions');
    if (input.kubernetesOptions.deploymentSelectors !== undefined) {
      if (!Array.isArray(input.kubernetesOptions.deploymentSelectors)) throw new Error('deploymentSelectors must be an array');
      for (const selector of input.kubernetesOptions.deploymentSelectors) {
        knownKeys(selector, ['namespace', 'labelSelector'], 'Deployment selector');
      }
    }
  }
  if (input.executionOptions !== undefined) {
    knownKeys(input.executionOptions, ['rotationTimeoutMs', 'suspend'], 'executionOptions');
    if (input.executionOptions.suspend !== undefined) knownKeys(input.executionOptions.suspend, ['reason', 'requester'], 'Startup suspension');
  }
  const request = { ...input, ...overrides } as unknown as ActivateCertificateRequest;
  const options = mergeEffectiveOptions(request);
  const normalized: ActivateCertificateRequest = {
    credentialType: request.credentialType,
    environment: request.environment,
    teamName: request.teamName,
    scopeKey: request.scopeKey,
    credentialOptions: options.credential,
    temporalNamespaceOptions: options.temporalNamespaces,
    kubernetesOptions: options.kubernetes,
    executionOptions: options.execution,
  };
  assertPayloadSize(normalized, LIMITS.startBytes, 'Credential request');
  return normalized;
}

async function readBoundedRequest(path: string): Promise<unknown> {
  if (!path.trim()) throw new Error('CREDENTIAL_REQUEST_FILE must name a JSON request file');
  const file = await open(path, 'r');
  try {
    const stat = await file.stat();
    if (!stat.isFile()) throw new Error('CREDENTIAL_REQUEST_FILE must name a regular JSON file');
    const tooLarge = () => new Error(`Credential request file exceeds its ${LIMITS.startBytes} byte limit`);
    if (stat.size > LIMITS.startBytes) throw tooLarge();
    // Read at most the cap plus one byte, including if the file grows after stat.
    const buffer = Buffer.alloc(LIMITS.startBytes + 1);
    let length = 0;
    while (length < buffer.length) {
      const { bytesRead } = await file.read(buffer, length, buffer.length - length, length);
      if (bytesRead === 0) break;
      length += bytesRead;
    }
    if (length > LIMITS.startBytes) throw tooLarge();
    try { return JSON.parse(buffer.toString('utf8', 0, length)); }
    catch { throw new Error('Credential request file must contain valid JSON'); }
  } finally { await file.close(); }
}

/** Every CLI command resolves the same identity, so state and updates target the loaded binding. */
export async function loadCredentialRequest(environment: NodeJS.ProcessEnv = process.env): Promise<ActivateCertificateRequest> {
  const input = environment.CREDENTIAL_REQUEST_FILE === undefined ? structuredClone(DEMO_REQUEST)
    : await readBoundedRequest(environment.CREDENTIAL_REQUEST_FILE);
  return normalizeCredentialRequest(input, {
    ...(environment.CREDENTIAL_ENVIRONMENT !== undefined ? { environment: environment.CREDENTIAL_ENVIRONMENT } : {}),
    ...(environment.CREDENTIAL_TEAM !== undefined ? { teamName: environment.CREDENTIAL_TEAM } : {}),
    ...(environment.CREDENTIAL_SCOPE !== undefined ? { scopeKey: environment.CREDENTIAL_SCOPE } : {}),
  });
}
