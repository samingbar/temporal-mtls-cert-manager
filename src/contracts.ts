import type { CredentialArchiveActivities, ArchiveCredentialHistoryResponse } from './archive-contracts';
/** JSON-only contracts. Durations and timestamps use milliseconds; no private keys enter histories. */
export type CredentialType = 'certificate' | 'api-key';
export interface CredentialIdentity {
  credentialType: CredentialType;
  environment: string;
  teamName: string;
  scopeKey: string;
}
export interface CredentialOptions { ttlMs?: number; rotationOffsetMs?: number }
export interface TemporalNamespaceOptions { names: string[]; tagSelector?: Record<string, string> }
export interface DeploymentSelector { namespace?: string; labelSelector: string }
export interface KubernetesOptions { enabled?: boolean; deploymentSelectors?: DeploymentSelector[] }
export interface SuspendRequest { reason: string; requester: string }
export interface ExecutionOptions { rotationTimeoutMs?: number; suspend?: SuspendRequest }
export interface EffectiveOptions {
  credential: { ttlMs: number; rotationOffsetMs: number };
  temporalNamespaces: TemporalNamespaceOptions;
  kubernetes: { enabled: boolean; deploymentSelectors: DeploymentSelector[] };
  execution: { rotationTimeoutMs: number; suspend?: SuspendRequest };
}
export interface ActivateCertificateRequest extends CredentialIdentity {
  credentialOptions?: CredentialOptions;
  temporalNamespaceOptions: TemporalNamespaceOptions;
  kubernetesOptions?: KubernetesOptions;
  executionOptions?: ExecutionOptions;
  /** Internal use only by Continue-As-New, never supplied by a starting client. */
  carryover?: CredentialCarryover;
}
export interface DeactivateRequest { reason: string; requester: string }
export interface PutTemporalNamespacesRequest { temporalNamespaceOptions: TemporalNamespaceOptions }
export interface PutTemporalNamespacesResponse { temporalNamespaceOptions: TemporalNamespaceOptions; revision: number }
export interface PutOptionsRequest {
  credentialOptions?: CredentialOptions;
  kubernetesOptions?: KubernetesOptions;
  executionOptions?: ExecutionOptions;
}
export interface PutOptionsResponse { options: EffectiveOptions; revision: number }
export interface RotateRequest { operationId: string; reason: string; requester: string }
export interface RotateResponse { operationId: string; accepted: boolean; duplicate: boolean }
export interface RestoreRequest { reason: string; requester: string }
export interface RestoreResponse { operationId: string; response: RevertRotateResponse }
export interface SuspendResponse { status: 'SUSPENDED' }
export interface GetStateRequest { includeHistory?: boolean; includeFailureDetail?: boolean }
export interface GetStateResponse { state: CredentialState }
export interface CertificateInventoryItem {
  namespace: string;
  certificates: { fingerprint: string; notAfterMs: number }[];
}
export interface GetTemporalNamespaceCertificatesRequest {
  identity: CredentialIdentity;
  temporalNamespaceOptions: TemporalNamespaceOptions;
}
export interface GetTemporalNamespaceCertificatesResponse {
  namespaces: CertificateInventoryItem[];
  observedAtMs: number;
}
export interface DeploymentInventoryItem { namespace: string; name: string; generation: number; desiredReplicas: number; availableReplicas: number }
export interface GetKubernetesDeploymentsRequest { identity: CredentialIdentity; kubernetesOptions: EffectiveOptions['kubernetes'] }
export interface GetKubernetesDeploymentsResponse { deployments: DeploymentInventoryItem[]; observedAtMs: number; skipped: boolean }
export interface MergeOptionsRequest { request: ActivateCertificateRequest }
export interface MergeOptionsResponse { options: EffectiveOptions }
export interface RequestRotateRevertRequest extends RestoreRequest { childWorkflowId: string; operationId: string; revertRequestId: string }
export interface RequestRotateRevertResponse { response: RevertRotateResponse }
export interface CredentialActivities extends CredentialArchiveActivities {
  mergeOptions(request: MergeOptionsRequest): Promise<MergeOptionsResponse>;
  getTemporalNamespaceCertificates(request: GetTemporalNamespaceCertificatesRequest): Promise<GetTemporalNamespaceCertificatesResponse>;
  getKubernetesDeployments(request: GetKubernetesDeploymentsRequest): Promise<GetKubernetesDeploymentsResponse>;
  requestRotateRevert(request: RequestRotateRevertRequest): Promise<RequestRotateRevertResponse>;
}
export interface StartRotateRequest {
  identity: CredentialIdentity;
  options: EffectiveOptions;
  namespaceInventory: GetTemporalNamespaceCertificatesResponse;
  deploymentInventory: GetKubernetesDeploymentsResponse;
  operationId: string;
  reason: string;
  requester: string;
}
export type RotationStatus = 'SUCCEEDED' | 'REVERTED' | 'CANCELLED' | 'FAILED' | 'TIMED_OUT' | 'REVERT_TIMED_OUT';
export interface FailureDetail { message: string; type: string; atMs: number; operationId?: string }
export interface RotationSummary {
  operationId: string;
  childWorkflowId: string;
  reason: string;
  requester: string;
  status: RotationStatus;
  startedAtMs: number;
  finishedAtMs: number;
  namespaceNames: string[];
  steps: string[];
  failure?: FailureDetail;
  compensationFailure?: FailureDetail;
  compensationStatus?: 'SUCCEEDED' | 'UNRESOLVED';
}
export interface GetRotationStateRequest { includeFailureDetail?: boolean }
export interface GetRotationStateResponse { summary: RotationSummary }
export interface RevertRotateRequest extends RestoreRequest { operationId: string }
export interface RevertRotateResponse { operationId: string; accepted: boolean; message: string }
export interface CancelRotateRequest { reason: string; requester: string }
export interface CancelRotateResponse { accepted: boolean }
/** External Rotate implementation must export this workflow type; it is not implemented in src. */
export type RotateWorkflow = (request: StartRotateRequest) => Promise<GetRotationStateResponse>;
export type CredentialStatus = 'INITIALIZING' | 'ACTIVE' | 'ROTATING' | 'SUSPENDING' | 'SUSPENDED' | 'FAILED' | 'DEACTIVATING' | 'STOPPED';
export interface ActiveRotation { request: StartRotateRequest; childWorkflowId: string; startedAtMs: number }
export interface UpdateOutcome { name: string; atMs: number; outcome: string; operationId?: string }
export interface CredentialState {
  identity: CredentialIdentity;
  status: CredentialStatus;
  options?: EffectiveOptions;
  revision: number;
  namespaceInventory?: GetTemporalNamespaceCertificatesResponse;
  deploymentInventory?: GetKubernetesDeploymentsResponse;
  nextRotationAtMs?: number;
  activeRotation?: ActiveRotation;
  pendingRotations: RotateRequest[];
  acceptedOperationIds: string[];
  rotationHistory: RotationSummary[];
  updateOutcomes: UpdateOutcome[];
  failures: FailureDetail[];
  lastRestoreResponse?: RestoreResponse;
  suspension?: SuspendRequest;
  /** Queries expose a bounded recent window; older records live in this durable archive. */
  historyArchive?: ArchiveCredentialHistoryResponse;
}
export interface CredentialCarryover {
  state: CredentialState;
  /** Persist scheduling sequence even when no child has completed. */
  scheduledSequence: number;
}
