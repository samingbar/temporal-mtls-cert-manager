import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import type { ArchiveCredentialHistoryRequest } from '../src/archive-contracts';
import type { CredentialIdentity } from '../src/contracts';
import { createCredentialArchiveStore } from '../demos/credential-supervisor/archive-store';

const identity: CredentialIdentity = { credentialType: 'certificate', environment: 'dev', teamName: 'platform', scopeKey: 'applications' };
function checkpoint(overrides: Partial<ArchiveCredentialHistoryRequest> = {}): ArchiveCredentialHistoryRequest {
  return {
    identity,
    checkpointId: 'run-1-checkpoint-1',
    operationIds: ['maintenance-001'],
    rotationHistory: [{
      operationId: 'maintenance-001', childWorkflowId: 'rotate-1', reason: 'Maintenance', requester: 'operator',
      status: 'SUCCEEDED', startedAtMs: 1_000, finishedAtMs: 2_000, namespaceNames: ['orders-dev'], steps: ['issue', 'rollout'],
    }],
    updateOutcomes: [{ name: 'rotate', atMs: 1_000, outcome: 'accepted', operationId: 'maintenance-001' }],
    failures: [{ message: 'An earlier failure', type: 'ExampleFailure', atMs: 500 }],
    ...overrides,
  };
}

test('archive and operation deduplication survive process-style store close and reopen', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'credential-archive-'));
  const path = join(directory, 'nested', 'history.sqlite');
  try {
    const first = createCredentialArchiveStore({ path });
    const saved = await first.activities.archiveCredentialHistory(checkpoint());
    first.close();
    const reopened = createCredentialArchiveStore({ path });
    try {
      assert.deepEqual(await reopened.activities.lookupArchivedOperation({ identity, operationId: 'maintenance-001', expectedArchiveRef: saved.archiveRef }), { found: true });
      assert.deepEqual(reopened.inspectCheckpoints({ identity }).checkpoints, [checkpoint()]);
      assert.deepEqual(reopened.inspectHistory({ identity }), {
        rotationHistory: checkpoint().rotationHistory, updateOutcomes: checkpoint().updateOutcomes, failures: checkpoint().failures,
      });
      assert.deepEqual(await reopened.activities.archiveCredentialHistory(checkpoint()), saved);
      assert.equal(reopened.inspectCheckpoints({ identity }).checkpoints.length, 1);
    } finally { reopened.close(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('lookup rejects a missing archive checkpoint instead of accepting an old operation again', async () => {
  const original = createCredentialArchiveStore({ path: ':memory:' });
  const wrongFile = createCredentialArchiveStore({ path: ':memory:' });
  try {
    const saved = await original.activities.archiveCredentialHistory(checkpoint());
    await assert.rejects(wrongFile.activities.lookupArchivedOperation({
      identity, operationId: 'maintenance-001', expectedArchiveRef: saved.archiveRef,
    }), { name: 'ArchiveUnavailable' });
    await assert.rejects(original.activities.lookupArchivedOperation({
      identity: { ...identity, scopeKey: 'another' }, operationId: 'maintenance-001', expectedArchiveRef: saved.archiveRef,
    }), { name: 'ArchiveUnavailable' });
    const next = checkpoint({ checkpointId: 'next-checkpoint', expectedArchiveRef: saved.archiveRef });
    await assert.rejects(wrongFile.activities.archiveCredentialHistory(next), { name: 'ArchiveUnavailable' });
    assert.deepEqual(wrongFile.inspectHistory({ identity }), { rotationHistory: [], updateOutcomes: [], failures: [] });
    assert.deepEqual(wrongFile.inspectCheckpoints({ identity }).checkpoints, []);
    const nextSaved = await original.activities.archiveCredentialHistory(next);
    assert.equal(nextSaved.rotationCount, 1);
    assert.equal(nextSaved.operationCount, 1);
    assert.deepEqual(await original.activities.archiveCredentialHistory(next), nextSaved);
    assert.deepEqual(await original.activities.lookupArchivedOperation({
      identity, operationId: 'new-operation', expectedArchiveRef: saved.archiveRef,
    }), { found: false });
  } finally {
    original.close();
    wrongFile.close();
  }
});

test('a completed archive survives worker process exit without an explicit database close', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'credential-archive-exit-'));
  const path = join(directory, 'history.sqlite');
  try {
    execFileSync(process.execPath, ['-e', `
      const { createCredentialArchiveStore } = require(process.env.ARCHIVE_MODULE);
      const store = createCredentialArchiveStore({ path: process.env.ARCHIVE_TEST_PATH });
      store.activities.archiveCredentialHistory(JSON.parse(process.env.ARCHIVE_TEST_CHECKPOINT))
        .then(() => process.exit(0), error => { console.error(error); process.exit(1); });
    `], { env: {
      ...process.env,
      ARCHIVE_MODULE: require.resolve('../demos/credential-supervisor/archive-store'),
      ARCHIVE_TEST_PATH: path,
      ARCHIVE_TEST_CHECKPOINT: JSON.stringify(checkpoint()),
    }, stdio: 'pipe' });
    const restarted = createCredentialArchiveStore({ path });
    try {
      assert.deepEqual(await restarted.activities.lookupArchivedOperation({ identity, operationId: 'maintenance-001' }), { found: true });
      assert.deepEqual(restarted.inspectCheckpoints({ identity }).checkpoints, [checkpoint()]);
    } finally { restarted.close(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('archive keys include every Credential identity field and avoid separator collisions', async () => {
  const store = createCredentialArchiveStore({ path: ':memory:' });
  try {
    await store.activities.archiveCredentialHistory(checkpoint());
    for (const different of [
      { ...identity, credentialType: 'api-key' as const },
      { ...identity, environment: 'prod' },
      { ...identity, teamName: 'other' },
      { ...identity, scopeKey: 'other' },
    ]) {
      assert.deepEqual(await store.activities.lookupArchivedOperation({ identity: different, operationId: 'maintenance-001' }), { found: false });
      await store.activities.archiveCredentialHistory(checkpoint({ identity: different }));
      assert.equal(store.inspectCheckpoints({ identity: different }).checkpoints.length, 1);
    }
    const delimitedA = { ...identity, environment: 'dev-a', teamName: 'b' };
    const delimitedB = { ...identity, environment: 'dev', teamName: 'a-b' };
    await store.activities.archiveCredentialHistory(checkpoint({ identity: delimitedA }));
    assert.deepEqual(await store.activities.lookupArchivedOperation({ identity: delimitedB, operationId: 'maintenance-001' }), { found: false });
    assert.equal(store.inspectCheckpoints({ identity }).checkpoints.length, 1);
  } finally { store.close(); }
});

test('checkpoint retries append history once and conflicting retries cannot partially add operation IDs', async () => {
  const store = createCredentialArchiveStore({ path: ':memory:' });
  try {
    const saved = await store.activities.archiveCredentialHistory(checkpoint());
    const reordered = { ...checkpoint(), identity: { scopeKey: identity.scopeKey, teamName: identity.teamName, environment: identity.environment, credentialType: identity.credentialType } };
    assert.deepEqual(await store.activities.archiveCredentialHistory(reordered), saved);
    await assert.rejects(store.activities.archiveCredentialHistory(checkpoint({ operationIds: ['must-not-be-added'] })), /different contents/);
    assert.deepEqual(await store.activities.lookupArchivedOperation({ identity, operationId: 'must-not-be-added' }), { found: false });
    assert.equal(store.inspectCheckpoints({ identity }).checkpoints.length, 1);
  } finally { store.close(); }
});

test('a database failure rolls back both history and every operation ID in the checkpoint', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'credential-archive-rollback-'));
  const path = join(directory, 'history.sqlite');
  const store = createCredentialArchiveStore({ path });
  const faultInjector = new DatabaseSync(path);
  try {
    faultInjector.exec(`
      CREATE TRIGGER fail_operation_insert BEFORE INSERT ON credential_operations
      WHEN NEW.operation_id = 'rollback-me'
      BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END;
    `);
    const request = checkpoint({ operationIds: ['first-in-transaction', 'rollback-me'] });
    await assert.rejects(store.activities.archiveCredentialHistory(request), /injected storage failure/);
    assert.deepEqual(store.inspectCheckpoints({ identity }).checkpoints, []);
    assert.deepEqual(await store.activities.lookupArchivedOperation({ identity, operationId: 'first-in-transaction' }), { found: false });
    faultInjector.exec('DROP TRIGGER fail_operation_insert');
    await store.activities.archiveCredentialHistory(request);
    assert.deepEqual(await store.activities.lookupArchivedOperation({ identity, operationId: 'first-in-transaction' }), { found: true });
    assert.equal(store.inspectCheckpoints({ identity }).checkpoints.length, 1);
  } finally {
    faultInjector.close();
    store.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test('independent worker connections share deduplication and paginated history', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'credential-archive-workers-'));
  const path = join(directory, 'history.sqlite');
  const first = createCredentialArchiveStore({ path });
  const second = createCredentialArchiveStore({ path });
  try {
    await first.activities.archiveCredentialHistory(checkpoint());
    const next = checkpoint({ checkpointId: 'run-2-checkpoint-1', operationIds: ['maintenance-002'] });
    await second.activities.archiveCredentialHistory(next);
    assert.deepEqual(await first.activities.lookupArchivedOperation({ identity, operationId: 'maintenance-002' }), { found: true });
    const page = second.inspectCheckpoints({ identity, limit: 1 });
    assert.deepEqual(page, { checkpoints: [checkpoint()], nextOffset: 1 });
    assert.deepEqual(second.inspectCheckpoints({ identity, limit: 1, offset: page.nextOffset }), { checkpoints: [next] });
  } finally {
    first.close();
    second.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test('overlapping recent windows archive each distinct summary and diagnostic once', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'credential-archive-overlap-'));
  const path = join(directory, 'history.sqlite');
  try {
    const first = createCredentialArchiveStore({ path });
    const original = await first.activities.archiveCredentialHistory(checkpoint());
    assert.equal(original.rotationCount, 1);
    assert.equal(original.operationCount, 1);
    const overlapping = await first.activities.archiveCredentialHistory(checkpoint({ checkpointId: 'overlapping-window' }));
    assert.equal(overlapping.rotationCount, 1);
    assert.equal(overlapping.operationCount, 1);
    first.close();
    const reopened = createCredentialArchiveStore({ path });
    try {
      assert.deepEqual(reopened.inspectHistory({ identity }), {
        rotationHistory: checkpoint().rotationHistory, updateOutcomes: checkpoint().updateOutcomes, failures: checkpoint().failures,
      });
      const next = checkpoint({
        checkpointId: 'next-window', operationIds: ['maintenance-001', 'maintenance-002'],
        rotationHistory: [...checkpoint().rotationHistory, { ...checkpoint().rotationHistory[0]!, operationId: 'maintenance-002' }],
      });
      const added = await reopened.activities.archiveCredentialHistory(next);
      assert.equal(added.rotationCount, 2);
      assert.equal(added.operationCount, 2);
      const page = reopened.inspectHistory({ identity, limit: 1 });
      assert.equal(page.nextOffset, 1);
      assert.deepEqual(page.rotationHistory, checkpoint().rotationHistory);
      const lastPage = reopened.inspectHistory({ identity, limit: 1, offset: page.nextOffset });
      assert.deepEqual(lastPage, { rotationHistory: [next.rotationHistory[1]], updateOutcomes: [], failures: [] });
    } finally { reopened.close(); }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('archive batches and inspection pages enforce finite limits', async () => {
  const store = createCredentialArchiveStore({ path: ':memory:' });
  try {
    await assert.rejects(store.activities.archiveCredentialHistory(checkpoint({ operationIds: Array.from({ length: 513 }, (_, index) => `operation-${index}`) })), /at most 512/);
    await assert.rejects(store.activities.archiveCredentialHistory(checkpoint({ failures: [{ message: 'x'.repeat(1_000_001), type: 'TooLarge', atMs: 1 }] })), /1,000,000 bytes/);
    assert.equal(store.inspectCheckpoints({ identity }).checkpoints.length, 0);
    for (const limit of [0, 101, Number.POSITIVE_INFINITY]) assert.throws(() => store.inspectHistory({ identity, limit }), /limit/);
    for (const offset of [-1, 0.5, Number.NaN]) assert.throws(() => store.inspectHistory({ identity, offset }), /offset/);
  } finally { store.close(); }
});
