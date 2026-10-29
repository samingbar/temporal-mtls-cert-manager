import { Client, Connection } from '@temporalio/client';
import type { ActivateCertificateRequest, PutOptionsRequest } from '../../src/contracts';
import { activate, deactivate, getState, putOptions, putTemporalNamespaces, restore, rotate, suspend } from '../../src/messages';
import { credentialWorkflowId } from '../../src/options';
import { DEMO_DEPLOYMENT, DEMO_REQUEST, DEMO_TASK_QUEUE } from './fixtures';
import { startPinnedCredential } from './start-pinned';
import { createCredentialArchiveStore } from './archive-store';

const usage = `Usage: npm run ctl -- <command> [arguments]
  start                         Start the Credential with complete demo input
  state [--failures]            Show inventory, options, history and optional errors
  archive [limit] [offset]       Read a page of durable history for this Credential
  rotate <operation-id>         Queue one manual rollover (reuse ID when retrying)
  suspend                       Wait for safe boundary, then pause scheduling
  activate                      Resume a suspended or failed Credential
  deactivate                    Finish active work and stop the Credential
  scope <name,name,...>          Replace namespace scope with explicit names
  restore                       Ask active fake rotation to compensate
  options <ttl-ms> <offset-ms>   Change next rotation's certificate options

Environment: TEMPORAL_ADDRESS=localhost:7233 TEMPORAL_NAMESPACE=default
  CREDENTIAL_ENVIRONMENT=dev CREDENTIAL_TEAM=platform CREDENTIAL_SCOPE=demo
Requires the fake worker and a local Temporal server. No real PKI or cloud calls.`;

function positiveNumber(value: string | undefined, name: string, allowZero = false): number {
  const number = Number(value);
  if (!value || !Number.isFinite(number) || (allowZero ? number < 0 : number <= 0)) throw new Error(`${name} must be ${allowZero ? 'nonnegative' : 'positive'} milliseconds`);
  return number;
}

async function main(): Promise<void> {
  const [command, ...args] = process.argv.slice(2);
  if (!command || command === 'help' || command === '--help') { console.log(usage); return; }
  const request: ActivateCertificateRequest = {
    ...structuredClone(DEMO_REQUEST),
    environment: process.env.CREDENTIAL_ENVIRONMENT ?? DEMO_REQUEST.environment,
    teamName: process.env.CREDENTIAL_TEAM ?? DEMO_REQUEST.teamName,
    scopeKey: process.env.CREDENTIAL_SCOPE ?? DEMO_REQUEST.scopeKey,
  };
  if (command === 'archive') {
    const archive = createCredentialArchiveStore();
    try {
      console.log(JSON.stringify(archive.inspectHistory({ identity: request,
        limit: args[0] === undefined ? 20 : Number(args[0]), offset: args[1] === undefined ? 0 : Number(args[1]) }), null, 2));
    } finally { archive.close(); }
    return;
  }
  const connection = await Connection.connect({ address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233' });
  try {
    const client = new Client({ connection, namespace: process.env.TEMPORAL_NAMESPACE ?? 'default' });
    const workflowId = credentialWorkflowId(request);
    const handle = client.workflow.getHandle(workflowId);
    const operator = { reason: 'Local demo operator request', requester: 'demo-cli' };
    let result: unknown;
    switch (command) {
      case 'start':
        await startPinnedCredential(client, request, { workflowId, taskQueue: DEMO_TASK_QUEUE, version: DEMO_DEPLOYMENT });
        result = { workflowId, started: true };
        break;
      case 'state': result = await handle.query(getState, { includeFailureDetail: args.includes('--failures') }); break;
      case 'rotate': {
        if (!args[0]) throw new Error('rotate requires a stable operation ID; reuse it for retries');
        result = await handle.executeUpdate(rotate, { args: [{ ...operator, operationId: args[0] }] });
        break;
      }
      case 'suspend': result = await handle.executeUpdate(suspend, { args: [operator] }); break;
      case 'activate': result = await handle.executeUpdate(activate, { args: [request] }); break;
      case 'deactivate':
        await handle.executeUpdate(deactivate, { args: [operator] });
        await handle.result();
        result = { workflowId, stopped: true };
        break;
      case 'scope':
        if (!args[0]) throw new Error('scope requires comma-separated namespace names');
        result = await handle.executeUpdate(putTemporalNamespaces, { args: [{ temporalNamespaceOptions: { names: args[0].split(',').map(name => name.trim()).filter(Boolean) } }] });
        break;
      case 'restore': result = await handle.executeUpdate(restore, { args: [operator] }); break;
      case 'options': {
        const options: PutOptionsRequest = { credentialOptions: { ttlMs: positiveNumber(args[0], 'ttl'), rotationOffsetMs: positiveNumber(args[1], 'offset', true) } };
        result = await handle.executeUpdate(putOptions, { args: [options] });
        break;
      }
      default: throw new Error(usage);
    }
    console.log(JSON.stringify(result ?? { accepted: true }, null, 2));
  } finally { await connection.close(); }
}
if (require.main === module) main().catch(error => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
