/** Explicit admission and retention limits for the long-lived Credential entity. */
export const LIMITS = Object.freeze({
  startBytes: 32_768,
  updateBytes: 8_192,
  inventoryBytes: 65_536,
  summaryBytes: 65_536,
  recentHistoryBytes: 131_072,
  carryoverBytes: 524_288,
  pendingRotations: 32,
  recentRotations: 16,
  recentOperationIds: 128,
  updateOutcomes: 64,
  failures: 32,
  updatesPerRun: 128,
});

/** Count the bytes of the JSON payload without relying on Node APIs in Workflow code. */
export function utf8JsonBytes(value: unknown): number {
  const json = JSON.stringify(value);
  if (json === undefined) throw new Error('Value must have a JSON representation');
  let bytes = 0;
  for (let index = 0; index < json.length; index += 1) {
    const code = json.charCodeAt(index);
    if (code < 0x80) bytes += 1;
    else if (code < 0x800) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff
      && index + 1 < json.length && json.charCodeAt(index + 1) >= 0xdc00
      && json.charCodeAt(index + 1) <= 0xdfff) {
      bytes += 4;
      index += 1;
    } else bytes += 3;
  }
  return bytes;
}

export function assertPayloadSize(value: unknown, maxBytes: number, label: string): void {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) throw new Error('Payload byte limit must be a nonnegative safe integer');
  const bytes = utf8JsonBytes(value);
  if (bytes > maxBytes) throw new Error(`${label} exceeds its ${maxBytes} byte JSON limit (${bytes} bytes)`);
}

/** Shared by operator Updates and startup suspension options. */
export function validateActorInput(input: unknown): asserts input is { reason: string; requester: string } {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Request must be an object');
  const actor = input as { reason?: unknown; requester?: unknown };
  if (typeof actor.reason !== 'string' || !actor.reason.trim()
    || typeof actor.requester !== 'string' || !actor.requester.trim()) {
    throw new Error('reason and requester must be non-empty strings');
  }
  if (actor.reason.length > 1_024) throw new Error('reason must be at most 1024 characters');
  if (actor.requester.length > 128) throw new Error('requester must be at most 128 characters');
}

/** Failure descriptions are untrusted adapter output and must not grow entity state indefinitely. */
export function boundedText(value: unknown, maxChars = 1_024): string {
  if (!Number.isSafeInteger(maxChars) || maxChars < 0) throw new Error('Text limit must be a nonnegative safe integer');
  let text: string;
  try {
    if (typeof value === 'string') text = value;
    else if (value instanceof Error) text = typeof value.message === 'string' ? value.message : String(value);
    else text = JSON.stringify(value) ?? String(value);
  } catch {
    text = 'Unable to describe failure';
  }
  if (text.length <= maxChars) return text;
  // Preserve a complete surrogate pair when the length boundary lands inside an emoji.
  const last = text.charCodeAt(maxChars - 1);
  const next = text.charCodeAt(maxChars);
  const end = last >= 0xd800 && last <= 0xdbff && next >= 0xdc00 && next <= 0xdfff ? maxChars - 1 : maxChars;
  return text.slice(0, end);
}

/** Trim oldest entries in place so existing references to the bounded collection remain valid. */
export function retainRecent<T>(items: T[], max: number): void {
  if (!Number.isSafeInteger(max) || max < 0) throw new Error('Retention limit must be a nonnegative safe integer');
  if (items.length > max) items.splice(0, items.length - max);
}
