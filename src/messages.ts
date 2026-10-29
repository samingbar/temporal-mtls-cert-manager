import { defineQuery, defineUpdate } from '@temporalio/workflow';
import type * as C from './contracts';

export const activate = defineUpdate<void, [C.ActivateCertificateRequest]>('activate');
export const deactivate = defineUpdate<void, [C.DeactivateRequest]>('deactivate');
export const putTemporalNamespaces = defineUpdate<C.PutTemporalNamespacesResponse, [C.PutTemporalNamespacesRequest]>('putTemporalNamespaces');
export const putOptions = defineUpdate<C.PutOptionsResponse, [C.PutOptionsRequest]>('putOptions');
export const rotate = defineUpdate<C.RotateResponse, [C.RotateRequest]>('rotate');
export const restore = defineUpdate<C.RestoreResponse, [C.RestoreRequest]>('restore');
export const suspend = defineUpdate<C.SuspendResponse, [C.SuspendRequest]>('suspend');
export const getState = defineQuery<C.GetStateResponse, [C.GetStateRequest]>('getState');
export const revert = defineUpdate<C.RevertRotateResponse, [C.RevertRotateRequest]>('revert');
export const cancel = defineUpdate<C.CancelRotateResponse, [C.CancelRotateRequest]>('cancel');
export const getRotationState = defineQuery<C.GetRotationStateResponse, [C.GetRotationStateRequest]>('getState');
