import assert from 'node:assert/strict';
import test from 'node:test';
import {
  assertPayloadSize,
  boundedText,
  LIMITS,
  retainRecent,
  utf8JsonBytes,
  validateActorInput,
} from '../src/limits';

test('JSON payload sizes count multibyte characters, supplementary characters, and escaped surrogates', () => {
  const values: unknown[] = [
    null, true, 42, 'ascii', 'é', '漢字', '😀', '\ud800', '\udc00',
    'quote" slash\\ newline\n', { reason: '更新 🔐', values: ['é', '\ud800', ''] },
  ];
  for (const value of values) {
    assert.equal(utf8JsonBytes(value), Buffer.byteLength(JSON.stringify(value), 'utf8'), JSON.stringify(value));
  }
  assert.equal(utf8JsonBytes('😀'), 6);
  assert.equal(utf8JsonBytes('\ud800'), 8);
  assert.throws(() => utf8JsonBytes(undefined), /JSON representation/);
  assert.throws(() => utf8JsonBytes(1n));
});

test('payload admission accepts the exact limit and rejects oversized unknown fields without mutation', () => {
  const input = { reason: 'Maintenance', requester: 'operator', unexpected: { blob: '漢'.repeat(100) } };
  const original = structuredClone(input);
  const bytes = utf8JsonBytes(input);
  assert.doesNotThrow(() => assertPayloadSize(input, bytes, 'Operator request'));
  assert.throws(() => assertPayloadSize(input, bytes - 1, 'Operator request'), /Operator request exceeds/);
  assert.throws(() => assertPayloadSize({ extra: 'x'.repeat(LIMITS.updateBytes) }, LIMITS.updateBytes, 'Update'), /byte JSON limit/);
  assert.deepEqual(input, original);
});

test('operator metadata has explicit shape and character limits without rewriting supplied text', () => {
  const actor = { reason: ` ${'r'.repeat(1_022)} `, requester: 'u'.repeat(128) };
  const original = { ...actor };
  assert.doesNotThrow(() => validateActorInput(actor));
  assert.deepEqual(actor, original);
  for (const input of [null, [], 'actor', {}, { reason: '', requester: 'user' }, { reason: ' ', requester: 'user' }, { reason: 'reason', requester: 1 }]) {
    assert.throws(() => validateActorInput(input));
  }
  assert.throws(() => validateActorInput({ reason: 'r'.repeat(1_025), requester: 'user' }), /reason must be at most/);
  assert.throws(() => validateActorInput({ reason: 'reason', requester: 'u'.repeat(129) }), /requester must be at most/);
});

test('failure descriptions stay bounded even for errors and values that cannot be serialized', () => {
  assert.equal(boundedText(new Error('x'.repeat(2_000))).length, 1_024);
  assert.equal(boundedText(new Error('Vault failed')), 'Vault failed');
  assert.equal(boundedText({ adapter: 'Vault', failed: true }), '{"adapter":"Vault","failed":true}');
  assert.equal(boundedText(undefined), 'undefined');
  assert.equal(boundedText('a😀b', 2), 'a');
  assert.equal(boundedText('a😀b', 3), 'a😀');
  assert.equal(boundedText('failure', 0), '');
  const circular: { self?: unknown } = {};
  circular.self = circular;
  assert.equal(boundedText(circular), 'Unable to describe failure');
  assert.equal(boundedText({ toJSON() { throw new Error('Broken serialization'); } }, 5), 'Unabl');
});

test('recent retention keeps the newest entries and the original collection reference', () => {
  const items = [{ id: 1 }, { id: 2 }, { id: 3 }];
  const reference = items;
  const newest = items[2];
  retainRecent(items, 2);
  assert.equal(items, reference);
  assert.deepEqual(items.map(item => item.id), [2, 3]);
  assert.equal(items[1], newest);
  retainRecent(items, 5);
  assert.equal(items.length, 2);
  retainRecent(items, 0);
  assert.deepEqual(items, []);
});
