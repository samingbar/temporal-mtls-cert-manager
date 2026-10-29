import assert from 'node:assert/strict';
import test from 'node:test';
import type {
  ActivateCertificateRequest,
  CredentialIdentity,
  EffectiveOptions,
  GetTemporalNamespaceCertificatesResponse,
} from '../src/contracts';
import {
  canonicalizeKubernetes,
  canonicalizeTemporalNamespaces,
  computeNextRotationAt,
  credentialWorkflowId,
  mergeEffectiveOptions,
  rotationWorkflowId,
  validateIdentity,
  validateOperationId,
} from '../src/options';

const day = 24 * 60 * 60 * 1_000;

function request(overrides: Partial<ActivateCertificateRequest> = {}): ActivateCertificateRequest {
  return {
    credentialType: 'certificate',
    environment: 'dev',
    teamName: 'platform',
    scopeKey: 'applications',
    temporalNamespaceOptions: { names: ['orders', 'payments'] },
    ...overrides,
  };
}

function inventory(expirations: number[][]): GetTemporalNamespaceCertificatesResponse {
  return {
    observedAtMs: 1_000,
    namespaces: expirations.map((notAfterValues, index) => ({
      namespace: `namespace-${index}`,
      certificates: notAfterValues.map((notAfterMs, certificate) => ({
        fingerprint: `fingerprint-${index}-${certificate}`,
        notAfterMs,
      })),
    })),
  };
}

test('default options use a 90 day lifetime, 14 day offset, and 30 minute rotation timeout', () => {
  const options = mergeEffectiveOptions(request());
  assert.deepEqual(options.credential, { ttlMs: 90 * day, rotationOffsetMs: 14 * day });
  assert.equal(options.execution.rotationTimeoutMs, 30 * 60 * 1_000);
});

test('request values overlay environment configuration while retaining unspecified nested values', () => {
  const environmentDefaults: Partial<EffectiveOptions> = {
    credential: { ttlMs: 80 * day, rotationOffsetMs: 6 * day },
    temporalNamespaces: { names: ['environment-only'] },
    kubernetes: {
      enabled: true,
      deploymentSelectors: [{ namespace: 'applications', labelSelector: 'team=platform' }],
    },
    execution: { rotationTimeoutMs: 15 * 60 * 1_000 },
  };
  const options = mergeEffectiveOptions(request({
    credentialOptions: { ttlMs: 60 * day },
    executionOptions: { rotationTimeoutMs: 5 * 60 * 1_000 },
    kubernetesOptions: { enabled: false },
  }), environmentDefaults);

  assert.deepEqual(options.credential, { ttlMs: 60 * day, rotationOffsetMs: 6 * day });
  assert.deepEqual(options.temporalNamespaces.names, ['orders', 'payments']);
  assert.deepEqual(options.kubernetes, {
    enabled: false,
    deploymentSelectors: [{ namespace: 'applications', labelSelector: 'team=platform' }],
  });
  assert.equal(options.execution.rotationTimeoutMs, 5 * 60 * 1_000);
});

test('an explicit zero rotation offset overrides environment configuration', () => {
  const options = mergeEffectiveOptions(request({ credentialOptions: { rotationOffsetMs: 0 } }), {
    credential: { ttlMs: 60 * day, rotationOffsetMs: 6 * day },
  });
  assert.equal(options.credential.rotationOffsetMs, 0);
});

test('startup suspension is preserved in effective execution options', () => {
  const suspend = { reason: 'Await maintenance approval', requester: 'operator' };
  const options = mergeEffectiveOptions(request({ executionOptions: { suspend } }));
  assert.deepEqual(options.execution.suspend, suspend);
});

test('initial requests require their own complete identity and explicit namespace scope', () => {
  for (const property of ['credentialType', 'environment', 'teamName', 'scopeKey', 'temporalNamespaceOptions']) {
    const incomplete = { ...request() };
    delete (incomplete as unknown as Record<string, unknown>)[property];
    assert.throws(() => mergeEffectiveOptions(incomplete), `missing ${property} must be rejected`);
  }

  const missingScope = { ...request(), temporalNamespaceOptions: undefined } as unknown as ActivateCertificateRequest;
  assert.throws(() => mergeEffectiveOptions(missingScope, { temporalNamespaces: { names: ['from-environment'] } }));
});

test('the certificate execution example rejects the otherwise valid api-key contract type', () => {
  const apiKeyRequest = request({ credentialType: 'api-key' });
  assert.doesNotThrow(() => validateIdentity(apiKeyRequest));
  assert.throws(() => mergeEffectiveOptions(apiKeyRequest));
});

test('lifetime and rotation timeout must be finite positive durations', () => {
  for (const value of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    assert.throws(() => mergeEffectiveOptions(request({ credentialOptions: { ttlMs: value } })));
    assert.throws(() => mergeEffectiveOptions(request({ executionOptions: { rotationTimeoutMs: value } })));
  }
});

test('rotation offsets must be finite, nonnegative, and shorter than the credential lifetime', () => {
  for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY, 90 * day, 91 * day]) {
    assert.throws(() => mergeEffectiveOptions(request({ credentialOptions: { rotationOffsetMs: value } })));
  }
  assert.doesNotThrow(() => mergeEffectiveOptions(request({ credentialOptions: { rotationOffsetMs: 0 } })));
});

test('invalid environment defaults are validated after overlay', () => {
  assert.throws(() => mergeEffectiveOptions(request(), {
    kubernetes: { enabled: true, deploymentSelectors: [{ labelSelector: `team=${'x'.repeat(40_000)}` }] },
  }), /Effective options exceeds/);
  assert.throws(() => mergeEffectiveOptions(request(), {
    credential: { ttlMs: 10 * day, rotationOffsetMs: 14 * day },
  }));
  assert.doesNotThrow(() => mergeEffectiveOptions(request({ credentialOptions: { rotationOffsetMs: day } }), {
    credential: { ttlMs: 10 * day, rotationOffsetMs: 14 * day },
  }));
});

test('workflow identity components accept safe business identifiers and reject unsafe values', () => {
  const identity: CredentialIdentity = request();
  assert.doesNotThrow(() => validateIdentity({ ...identity, scopeKey: 'Production_01.example-team' }));
  assert.doesNotThrow(() => validateIdentity({ ...identity, scopeKey: 'a'.repeat(64) }));

  for (const property of ['environment', 'teamName', 'scopeKey'] as const) {
    for (const value of ['', ' ', '-leading', 'with spaces', 'path/value', 'secret@example.com', 'a'.repeat(65)]) {
      assert.throws(() => validateIdentity({ ...identity, [property]: value }), `${property}=${JSON.stringify(value)}`);
    }
  }
  assert.throws(() => validateIdentity({ ...identity, credentialType: 'unsupported' } as unknown as CredentialIdentity));
});

test('rotation operation IDs follow the same safe identifier boundary', () => {
  for (const value of ['manual-rotation.2026_10_29', 'a', 'a'.repeat(64)]) {
    assert.doesNotThrow(() => validateOperationId(value));
  }
  for (const value of ['', ' ', '-leading', 'rotation/1', 'rotation 1', 'a'.repeat(65)]) {
    assert.throws(() => validateOperationId(value));
  }
});

test('credential and rotation workflow IDs are stable and retain the business operation ID', () => {
  const identity = request();
  assert.equal(credentialWorkflowId(identity), 'certificate-dev-platform-applications');
  assert.equal(rotationWorkflowId(identity, 'maintenance-42'), 'rotate-certificate-dev-platform-applications-maintenance-42');
  assert.throws(() => rotationWorkflowId(identity, 'unsafe operation'));
});

test('namespace names and tag keys are canonicalized without changing caller input', () => {
  const input = { names: ['payments', 'orders', 'payments'], tagSelector: { team: 'platform', environment: 'dev' } };
  const original = structuredClone(input);
  const result = canonicalizeTemporalNamespaces(input);
  assert.deepEqual(result.names, ['orders', 'payments']);
  assert.deepEqual(Object.keys(result.tagSelector ?? {}), ['environment', 'team']);
  assert.deepEqual(input, original);
});

test('namespace scopes can be empty or use only a tag selector', () => {
  assert.deepEqual(canonicalizeTemporalNamespaces({ names: [] }).names, []);
  assert.deepEqual(canonicalizeTemporalNamespaces({ names: [], tagSelector: { team: 'platform' } }).tagSelector, { team: 'platform' });
});

test('namespace names cannot contain empty entries', () => {
  for (const value of ['', ' ', '\t']) {
    assert.throws(() => canonicalizeTemporalNamespaces({ names: ['orders', value] }));
  }
});

test('deployment selectors are sorted and deduplicated by namespace and label selector', () => {
  const input = {
    enabled: true,
    deploymentSelectors: [
      { namespace: 'z-apps', labelSelector: 'team=platform' },
      { namespace: 'a-apps', labelSelector: 'service=payments' },
      { namespace: 'a-apps', labelSelector: 'service=orders' },
      { namespace: 'a-apps', labelSelector: 'service=orders' },
    ],
  };
  const original = structuredClone(input);
  assert.deepEqual(canonicalizeKubernetes(input), {
    enabled: true,
    deploymentSelectors: [
      { namespace: 'a-apps', labelSelector: 'service=orders' },
      { namespace: 'a-apps', labelSelector: 'service=payments' },
      { namespace: 'z-apps', labelSelector: 'team=platform' },
    ],
  });
  assert.deepEqual(input, original);
});

test('a deployment label selector is mandatory even when Kubernetes integration is disabled', () => {
  for (const labelSelector of ['', ' ', '\t']) {
    assert.throws(() => canonicalizeKubernetes({ enabled: false, deploymentSelectors: [{ labelSelector }] }));
  }
});

test('rotation scheduling selects the earliest expiration across every managed namespace and certificate', () => {
  assert.equal(computeNextRotationAt(inventory([[9_000, 7_000], [12_000, 5_000]]), 1_000, 2_000), 4_000);
});

test('past and exact-now rotation recommendations are clamped to immediate', () => {
  assert.equal(computeNextRotationAt(inventory([[2_500]]), 1_000, 2_000), 2_000);
  assert.equal(computeNextRotationAt(inventory([[3_000]]), 1_000, 2_000), 2_000);
});

test('no namespace certificates means no scheduled rotation', () => {
  assert.equal(computeNextRotationAt(inventory([]), 1_000, 2_000), undefined);
  assert.equal(computeNextRotationAt(inventory([[], []]), 1_000, 2_000), undefined);
});

test('invalid certificate expirations are rejected even when another certificate has a valid expiration', () => {
  for (const value of [Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY]) {
    assert.throws(() => computeNextRotationAt(inventory([[5_000, value]]), 1_000, 2_000));
  }
});
