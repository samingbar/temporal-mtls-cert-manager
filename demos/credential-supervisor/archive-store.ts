import { createHash } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import type {
  ArchiveCredentialHistoryRequest, ArchiveCredentialHistoryResponse, CredentialArchiveActivities,
} from '../../src/archive-contracts';
import type { CredentialIdentity, FailureDetail, RotationSummary, UpdateOutcome } from '../../src/contracts';
import { validateIdentity, validateOperationId } from '../../src/options';

const MAX_BATCH_ITEMS = 512;
const MAX_CHECKPOINT_BYTES = 1_000_000;
export interface CredentialArchiveStoreOptions { path?: string }
export interface ArchivedHistoryPage {
  rotationHistory: RotationSummary[];
  updateOutcomes: UpdateOutcome[];
  failures: FailureDetail[];
  nextOffset?: number;
}
export interface ArchivedCheckpointPage {
  checkpoints: ArchiveCredentialHistoryRequest[];
  nextOffset?: number;
}
export class CredentialArchiveUnavailableError extends Error {
  constructor() {
    super('The expected Credential archive checkpoint is missing; restore the correct archive before accepting new operations');
    this.name = 'ArchiveUnavailable';
  }
}

export function defaultCredentialArchivePath(): string {
  return process.env.CREDENTIAL_ARCHIVE_PATH ?? resolve(process.cwd(), '.credential-data', 'archive.sqlite');
}

/** Collision-free identity encoding: delimiter-containing business fields remain distinct. */
function identityKey(identity: CredentialIdentity): string {
  validateIdentity(identity);
  return JSON.stringify([identity.credentialType, identity.environment, identity.teamName, identity.scopeKey]);
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().filter(key => (value as Record<string, unknown>)[key] !== undefined)
      .map(key => `${JSON.stringify(key)}:${stableJson((value as Record<string, unknown>)[key])}`).join(',')}}`;
  }
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new Error('Archive checkpoint must contain JSON values');
  return serialized;
}

function checkpointPayload(request: ArchiveCredentialHistoryRequest): { payload: string; digest: string } {
  if (typeof request.checkpointId !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,199}$/.test(request.checkpointId)) {
    throw new Error('Archive checkpointId must be a nonempty business identifier of at most 200 characters');
  }
  for (const name of ['operationIds', 'rotationHistory', 'updateOutcomes', 'failures'] as const) {
    if (!Array.isArray(request[name]) || request[name].length > MAX_BATCH_ITEMS) {
      throw new Error(`Archive ${name} batch must contain at most ${MAX_BATCH_ITEMS} items`);
    }
  }
  for (const operationId of request.operationIds) validateOperationId(operationId);
  const payload = stableJson(request);
  if (Buffer.byteLength(payload, 'utf8') > MAX_CHECKPOINT_BYTES) throw new Error('Archive checkpoint exceeds 1,000,000 bytes');
  return { payload, digest: createHash('sha256').update(payload).digest('hex') };
}

/**
 * Durable single-host demo archive. All workers must use the same persistent path.
 * A transaction commits history and operation deduplication together; Workflow
 * state may be pruned only after the Activity returns successfully.
 */
export function createCredentialArchiveStore(options: CredentialArchiveStoreOptions = {}) {
  const path = options.path ?? defaultCredentialArchivePath();
  if (path !== ':memory:') mkdirSync(dirname(resolve(path)), { recursive: true });
  const database = new DatabaseSync(path);
  database.exec(`
    PRAGMA busy_timeout = 10000;
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = FULL;
    CREATE TABLE IF NOT EXISTS credential_checkpoints (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      identity TEXT NOT NULL,
      checkpoint_id TEXT NOT NULL,
      payload_digest TEXT NOT NULL,
      payload TEXT NOT NULL,
      UNIQUE (identity, checkpoint_id)
    );
    CREATE TABLE IF NOT EXISTS credential_operations (
      identity TEXT NOT NULL,
      operation_id TEXT NOT NULL,
      PRIMARY KEY (identity, operation_id)
    );
    CREATE TABLE IF NOT EXISTS credential_records (
      sequence INTEGER PRIMARY KEY AUTOINCREMENT,
      identity TEXT NOT NULL,
      kind TEXT NOT NULL,
      record_hash TEXT NOT NULL,
      payload TEXT NOT NULL,
      UNIQUE (identity, kind, record_hash)
    );
  `);
  const findCheckpoint = database.prepare('SELECT payload_digest FROM credential_checkpoints WHERE identity = ? AND checkpoint_id = ?');
  const insertCheckpoint = database.prepare('INSERT INTO credential_checkpoints (identity, checkpoint_id, payload_digest, payload) VALUES (?, ?, ?, ?)');
  const insertOperation = database.prepare('INSERT OR IGNORE INTO credential_operations (identity, operation_id) VALUES (?, ?)');
  const findOperation = database.prepare('SELECT 1 FROM credential_operations WHERE identity = ? AND operation_id = ?');
  const insertRecord = database.prepare('INSERT OR IGNORE INTO credential_records (identity, kind, record_hash, payload) VALUES (?, ?, ?, ?)');
  const recordPage = database.prepare('SELECT payload FROM credential_records WHERE identity = ? AND kind = ? ORDER BY sequence LIMIT ? OFFSET ?');
  const checkpointPage = database.prepare('SELECT payload FROM credential_checkpoints WHERE identity = ? ORDER BY sequence LIMIT ? OFFSET ?');
  const rotationCount = database.prepare("SELECT COUNT(*) AS count FROM credential_records WHERE identity = ? AND kind = 'rotationHistory'");
  const operationCount = database.prepare('SELECT COUNT(*) AS count FROM credential_operations WHERE identity = ?');

  function validatePage(limit: number, offset: number) {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) throw new Error('Archive page limit must be 1-100');
    if (!Number.isSafeInteger(offset) || offset < 0) throw new Error('Archive page offset must be a nonnegative safe integer');
  }

  function archiveRefPrefix(key: string): string {
    return `sqlite:credential-checkpoint:${createHash('sha256').update(key).digest('hex')}:`;
  }

  function assertExpectedArchive(key: string, expectedArchiveRef: string | undefined): void {
    if (expectedArchiveRef === undefined) return;
    const prefix = archiveRefPrefix(key);
    if (typeof expectedArchiveRef !== 'string' || !expectedArchiveRef.startsWith(prefix)
      || !findCheckpoint.get(key, expectedArchiveRef.slice(prefix.length))) {
      throw new CredentialArchiveUnavailableError();
    }
  }

  const activities: CredentialArchiveActivities = {
    async archiveCredentialHistory(request): Promise<ArchiveCredentialHistoryResponse> {
      const key = identityKey(request.identity);
      const { payload, digest } = checkpointPayload(request);
      assertExpectedArchive(key, request.expectedArchiveRef);
      // BEGIN IMMEDIATE serializes writers across worker processes before checking the idempotency key.
      database.exec('BEGIN IMMEDIATE');
      try {
        const previous = findCheckpoint.get(key, request.checkpointId);
        if (previous) {
          if (previous.payload_digest !== digest) throw new Error('Archive checkpointId was reused with different contents');
        } else {
          insertCheckpoint.run(key, request.checkpointId, digest, payload);
          for (const operationId of request.operationIds) insertOperation.run(key, operationId);
          for (const kind of ['rotationHistory', 'updateOutcomes', 'failures'] as const) {
            for (const record of request[kind]) {
              const serialized = stableJson(record);
              insertRecord.run(key, kind, createHash('sha256').update(serialized).digest('hex'), serialized);
            }
          }
        }
        database.exec('COMMIT');
      } catch (error) {
        database.exec('ROLLBACK');
        throw error;
      }
      return {
        archiveRef: `${archiveRefPrefix(key)}${request.checkpointId}`,
        rotationCount: Number(rotationCount.get(key)?.count),
        operationCount: Number(operationCount.get(key)?.count),
      };
    },
    async lookupArchivedOperation({ identity, operationId, expectedArchiveRef }) {
      const key = identityKey(identity);
      validateOperationId(operationId);
      assertExpectedArchive(key, expectedArchiveRef);
      return { found: findOperation.get(key, operationId) !== undefined };
    },
  };

  return {
    activities,
    close() { database.close(); },
    /** Each collection uses this page offset and limit; overlapping checkpoints never repeat records. */
    inspectHistory({ identity, limit = 20, offset = 0 }: { identity: CredentialIdentity; limit?: number; offset?: number }): ArchivedHistoryPage {
      validatePage(limit, offset);
      const key = identityKey(identity);
      const rows = {
        rotationHistory: recordPage.all(key, 'rotationHistory', limit + 1, offset),
        updateOutcomes: recordPage.all(key, 'updateOutcomes', limit + 1, offset),
        failures: recordPage.all(key, 'failures', limit + 1, offset),
      };
      return {
        rotationHistory: rows.rotationHistory.slice(0, limit).map(row => JSON.parse(row.payload as string) as RotationSummary),
        updateOutcomes: rows.updateOutcomes.slice(0, limit).map(row => JSON.parse(row.payload as string) as UpdateOutcome),
        failures: rows.failures.slice(0, limit).map(row => JSON.parse(row.payload as string) as FailureDetail),
        ...(Object.values(rows).some(values => values.length > limit) ? { nextOffset: offset + limit } : {}),
      };
    },
    /** Bounded checkpoint audit pages are separate from deduplicated historical records. */
    inspectCheckpoints({ identity, limit = 20, offset = 0 }: { identity: CredentialIdentity; limit?: number; offset?: number }): ArchivedCheckpointPage {
      validatePage(limit, offset);
      const rows = checkpointPage.all(identityKey(identity), limit + 1, offset);
      const checkpoints = rows.slice(0, limit).map(row => JSON.parse(row.payload as string) as ArchiveCredentialHistoryRequest);
      return { checkpoints, ...(rows.length > limit ? { nextOffset: offset + limit } : {}) };
    },
  };
}
