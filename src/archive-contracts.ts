import type {
  CredentialIdentity, FailureDetail, RotationSummary, UpdateOutcome,
} from './contracts';

/** A bounded checkpoint written outside Workflow history before its contents are pruned. */
export interface ArchiveCredentialHistoryRequest {
  identity: CredentialIdentity;
  /** Unique within this Credential; retries must carry the same contents. */
  checkpointId: string;
  /** Last acknowledged checkpoint, required after a running chain has established its archive. */
  expectedArchiveRef?: string;
  operationIds: string[];
  rotationHistory: RotationSummary[];
  updateOutcomes: UpdateOutcome[];
  failures: FailureDetail[];
}
export interface ArchiveCredentialHistoryResponse {
  archiveRef: string;
  /** Durable identity-scoped totals, including overlapping checkpoints only once. */
  rotationCount: number;
  operationCount: number;
}
export interface LookupArchivedOperationRequest {
  identity: CredentialIdentity;
  operationId: string;
  /** Last acknowledged checkpoint; a missing/mismatched store must reject the lookup. */
  expectedArchiveRef?: string;
}
export interface LookupArchivedOperationResponse { found: boolean }
export interface CredentialArchiveActivities {
  archiveCredentialHistory(request: ArchiveCredentialHistoryRequest): Promise<ArchiveCredentialHistoryResponse>;
  lookupArchivedOperation(request: LookupArchivedOperationRequest): Promise<LookupArchivedOperationResponse>;
}
