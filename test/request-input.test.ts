import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import type { ActivateCertificateRequest } from '../src/contracts';
import { LIMITS } from '../src/limits';
import { DEFAULT_OPTIONS } from '../src/options';
import { DEMO_REQUEST } from '../demos/credential-supervisor/fixtures';
import { loadCredentialRequest, normalizeCredentialRequest } from '../demos/credential-supervisor/request-input';

function binding(): ActivateCertificateRequest {
  return {
    credentialType: 'certificate', environment: 'dev', teamName: 'platform', scopeKey: 'binding-demo',
    temporalNamespaceOptions: { names: ['payments-dev', 'orders-dev', 'payments-dev'], tagSelector: { team: 'platform', environment: 'dev' } },
  };
}

async function withRequestFile(body: string, run: (path: string) => Promise<void>): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), 'credential-request-'));
  try {
    const path = join(directory, 'binding.json');
    await writeFile(path, body);
    await run(path);
  } finally { await rm(directory, { recursive: true, force: true }); }
}

test('a binding request receives complete defaults and canonical scope without mutating its export', () => {
  const input = binding();
  const original = structuredClone(input);
  const request = normalizeCredentialRequest(input);
  assert.deepEqual(request.temporalNamespaceOptions.names, ['orders-dev', 'payments-dev']);
  assert.deepEqual(request.credentialOptions, DEFAULT_OPTIONS.credential);
  assert.deepEqual(request.kubernetesOptions, DEFAULT_OPTIONS.kubernetes);
  assert.deepEqual(request.executionOptions, DEFAULT_OPTIONS.execution);
  assert.deepEqual(input, original);
});

test('the public example file is a complete, accepted application-binding request', async () => {
  const path = join(process.cwd(), 'demos/credential-supervisor/application-binding.example.json');
  const exported = JSON.parse(await readFile(path, 'utf8'));
  const request = await loadCredentialRequest({ CREDENTIAL_REQUEST_FILE: path });
  assert.deepEqual(request, normalizeCredentialRequest(exported));
  assert.equal(request.scopeKey, 'binding-demo');
  assert.deepEqual(request.temporalNamespaceOptions, { names: ['orders-dev', 'payments-dev'], tagSelector: { environment: 'dev', team: 'platform' } });
});

test('without a request file the CLI keeps the existing demo fixture and supports identity overrides', async () => {
  assert.deepEqual(await loadCredentialRequest({}), normalizeCredentialRequest(DEMO_REQUEST));
  const request = await loadCredentialRequest({ CREDENTIAL_ENVIRONMENT: 'stage', CREDENTIAL_TEAM: 'security', CREDENTIAL_SCOPE: 'new-binding' });
  assert.equal(request.environment, 'stage');
  assert.equal(request.teamName, 'security');
  assert.equal(request.scopeKey, 'new-binding');
  assert.deepEqual(request.temporalNamespaceOptions, normalizeCredentialRequest(DEMO_REQUEST).temporalNamespaceOptions);
});

test('identity environment overrides apply after reading a request file', async () => {
  await withRequestFile(JSON.stringify(binding()), async path => {
    const request = await loadCredentialRequest({ CREDENTIAL_REQUEST_FILE: path,
      CREDENTIAL_ENVIRONMENT: 'stage', CREDENTIAL_TEAM: 'security', CREDENTIAL_SCOPE: 'override-binding' });
    assert.equal(request.environment, 'stage');
    assert.equal(request.teamName, 'security');
    assert.equal(request.scopeKey, 'override-binding');
    assert.deepEqual(request.temporalNamespaceOptions.names, ['orders-dev', 'payments-dev']);
    await assert.rejects(loadCredentialRequest({ CREDENTIAL_REQUEST_FILE: path, CREDENTIAL_SCOPE: '' }), /scopeKey/);
  });
});

test('incomplete scopes, invalid identity and invalid option values fail before contacting Temporal', () => {
  const missingScope = { ...binding() } as Partial<ActivateCertificateRequest>;
  delete missingScope.temporalNamespaceOptions;
  assert.throws(() => normalizeCredentialRequest(missingScope), /temporalNamespaceOptions/);
  const missingKey = { ...binding() } as Partial<ActivateCertificateRequest>;
  delete missingKey.scopeKey;
  assert.throws(() => normalizeCredentialRequest(missingKey), /scopeKey/);
  assert.throws(() => normalizeCredentialRequest({ ...binding(), temporalNamespaceOptions: {} }), /names/);
  assert.throws(() => normalizeCredentialRequest({ ...binding(), teamName: 'with spaces' }), /teamName/);
  assert.throws(() => normalizeCredentialRequest({ ...binding(), credentialOptions: { ttlMs: 1, rotationOffsetMs: 1 } }), /smaller than ttlMs/);
  for (const input of [null, [], true, 'request']) assert.throws(() => normalizeCredentialRequest(input), /must be an object/);
});

test('unknown credential and secret fields are rejected at each fixed schema level', () => {
  const unexpected = [
    { ...binding(), privateKey: 'do-not-send' },
    { ...binding(), credentialOptions: { apiKey: 'do-not-send' } },
    { ...binding(), temporalNamespaceOptions: { names: ['orders-dev'], certificate: 'do-not-send' } },
    { ...binding(), kubernetesOptions: { token: 'do-not-send' } },
    { ...binding(), kubernetesOptions: { deploymentSelectors: [{ labelSelector: 'team=platform', password: 'do-not-send' }] } },
    { ...binding(), executionOptions: { credential: 'do-not-send' } },
    { ...binding(), executionOptions: { suspend: { reason: 'pause', requester: 'operator', secret: 'do-not-send' } } },
  ];
  for (const input of unexpected) assert.throws(() => normalizeCredentialRequest(input), /unsupported field/);
  assert.throws(() => normalizeCredentialRequest({ ...binding(), carryover: {} }), /carryover is internal/);
  assert.throws(() => normalizeCredentialRequest({ ...binding(), carryover: null }), /carryover is internal/);
});

test('malformed nested options are rejected rather than silently defaulted', () => {
  for (const field of ['credentialOptions', 'kubernetesOptions', 'executionOptions']) {
    for (const value of [null, [], false]) assert.throws(() => normalizeCredentialRequest({ ...binding(), [field]: value }), /must be an object/);
  }
  assert.throws(() => normalizeCredentialRequest({ ...binding(), kubernetesOptions: { deploymentSelectors: 'apps' } }), /must be an array/);
  assert.throws(() => normalizeCredentialRequest({ ...binding(), temporalNamespaceOptions: { names: ['orders-dev'], tagSelector: { team: 42 } } }), /keys and values must be nonempty/);
});

test('request files reject invalid JSON without echoing potentially sensitive input', async () => {
  await withRequestFile('{"privateKey":"do-not-echo-this', async path => {
    await assert.rejects(loadCredentialRequest({ CREDENTIAL_REQUEST_FILE: path }), error => {
      assert.equal((error as Error).message, 'Credential request file must contain valid JSON');
      return true;
    });
  });
  await assert.rejects(loadCredentialRequest({ CREDENTIAL_REQUEST_FILE: '' }), /must name a JSON request file/);
});

test('raw request file size is bounded even when excess bytes are only whitespace', async () => {
  await withRequestFile(`${JSON.stringify(binding())}${' '.repeat(LIMITS.startBytes)}`, async path => {
    await assert.rejects(loadCredentialRequest({ CREDENTIAL_REQUEST_FILE: path }), /exceeds its 32768 byte limit/);
  });
  assert.throws(() => normalizeCredentialRequest({ ...binding(), temporalNamespaceOptions: { names: ['x'.repeat(LIMITS.startBytes)] } }), /exceeds its 32768 byte JSON limit/);
});
