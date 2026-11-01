import { readFile } from 'node:fs/promises';
import type { ConnectionOptions } from '@temporalio/client';
import type { NativeConnectionOptions } from '@temporalio/worker';

/** SDK configuration examples, never Workflow input or Activity return values. */
type SharedConnectionOptions = Pick<ConnectionOptions & NativeConnectionOptions, 'address' | 'tls' | 'apiKey'>;

export async function mtlsConnectionOptions(input: {
  address: string;
  certificatePath: string;
  privateKeyPath: string;
}): Promise<SharedConnectionOptions> {
  const [crt, key] = await Promise.all([
    readFile(input.certificatePath), readFile(input.privateKeyPath),
  ]);
  return { address: input.address, tls: { clientCertPair: { crt, key } } };
}

/** Use a separately authenticated connection when checking the API-key path. */
export function apiKeyConnectionOptions(input: { address: string; apiKey: string }): SharedConnectionOptions {
  if (!input.apiKey.trim()) throw new Error('An API key is required');
  return { address: input.address, tls: true, apiKey: input.apiKey };
}
