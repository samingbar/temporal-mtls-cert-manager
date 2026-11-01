# Adopting mTLS without disrupting deployments

This example supplies a rotation coordinator and an executable simulation. Use it to establish the control flow, then connect the Activity contracts to existing provisioning, PKI, and deployment systems. The sections below define that handoff and the evidence a real pilot needs. They do not imply that cloud access or a production issuer is configured.

## Keep three lifecycles separate

| Lifecycle | Owner and action | Coverage here |
| --- | --- | --- |
| Application onboarding | Provision namespace access, select the workload identity, and register its certificate destination and rollout targets | A proposed configuration-file handoff; no infrastructure is provisioned |
| Routine end-entity certificate renewal | Existing PKI issues a replacement client certificate; deliver it to the workload and reload or replace the process | Integration guidance only; the fake `issue` step creates no certificates |
| CA rollover | Add new namespace trust, reissue affected client certificates, roll out and verify every client, then remove old trust | Credential scheduling and simulated Rotate child |

Renewing a client certificate under an unchanged trusted CA does not require a CA rollover. The scheduler in this repository reads namespace CA expiration metadata; it does **not** schedule per-workload leaf renewal. Keep the existing issuer's renewal mechanism, or build that lifecycle separately. The fixture's 90-day lifetime is a demo setting, not a recommended CA policy.

These are client authentication certificates. Temporal Cloud's server TLS certificate is a separate concern. A workload presents its end-entity certificate and private key; the namespace trusts the issuing CA. End-entity expiry must precede CA expiry. A shared CA also needs an explicit namespace authorization design, such as certificate filters; issuing a certificate does not automatically reproduce API-key user or group permissions. See [Temporal's certificate requirements and authorization guidance](https://docs.temporal.io/cloud/certificates).

Human Cloud UI access, groups, and service identities also need separate treatment. This workflow does not enroll human users or replace the account's identity and access management.

## Fit the coordinator into an existing application binding

Keep the existing provisioning module as the source of desired namespaces, groups, service identities, and workload bindings. A proposed binding registry should associate a stable public identity with its namespace scope, issuer policy, Secret destination, and all consuming workloads. The existing module's actual schema remains an integration decision.

Export only the coordinator's complete `ActivateCertificateRequest` shape, illustrated by [application-binding.example.json](../demos/credential-supervisor/application-binding.example.json). Namespace names and Deployment selectors enter Workflow history; private keys, API keys, Secret contents, and issuer credentials must stay in the issuer or secret store. The CLI rejects unknown request fields and externally supplied carryover state. That validates the shape; the exporting system must still ensure that allowed text fields contain only public metadata.

| Existing binding information | Coordinator handoff | Production adapter responsibility |
| --- | --- | --- |
| Environment, owning team, shared CA scope | `environment`, `teamName`, `scopeKey` | Resolve the binding registry using the complete identity |
| Desired namespace membership | `temporalNamespaceOptions` | Read actual Cloud trust and expand any tag selector |
| Workload membership | `kubernetesOptions.deploymentSelectors` | Resolve every consuming Deployment and credential destination |
| Issuer role and Secret destination | Remain in adapter-owned configuration | Issue distinct workload certificates and deliver them through the chosen secret system |
| User and service permissions | Remain in provisioning and authorization policy | Confirm that the mTLS identity has the intended access |

One Credential coordinates a shared CA scope, not one human user. Give each CA scope one owner. Do not run independent rotations against overlapping namespace trust. Removing a namespace from the coordinator's current scope also does not prove it stopped using the old CA; retirement needs complete dependency knowledge.

### Run the configuration-file handoff locally

Follow the [runbook](../runbooks/credential-supervisor/README.md#prepare-and-start) to install dependencies, build, and start the local server and fake worker. In the CLI terminal:

```sh
export CREDENTIAL_REQUEST_FILE=./demos/credential-supervisor/application-binding.example.json
npm run ctl -- start
npm run ctl -- state
npm run ctl -- rotate binding-pilot-001
npm run ctl -- state
npm run ctl -- deactivate
```

Wait for `ACTIVE` before rotating, then query until the operation is `SUCCEEDED` before deactivating. The example uses the fake inventory's full shared-CA scope. All commands use the file's identity; `activate` reapplies that file's complete configuration. Explicit `CREDENTIAL_ENVIRONMENT`, `CREDENTIAL_TEAM`, and `CREDENTIAL_SCOPE` values override its identity. The server and worker still use fake integrations. This handoff does not read Terraform state or implement a Terraform provider.

For automation, have the provisioning pipeline reconcile the exported desired scope through a Temporal client. `putTemporalNamespaces` replaces the whole prior scope. `putOptions` changes the next decision. An active child keeps its original snapshot. Read the resulting state and retain a stable operation ID when retrying a rotation; an acceptance response is not completion.

## Give namespace trust one writer

Proposed ownership: Terraform manages namespace creation and desired access policy; the rotation adapter manages the accepted CA bundle at runtime. Avoid a later Terraform apply restoring an earlier CA bundle.

The provider documents `accepted_client_ca` as the namespace CA bundle field. If runtime ownership is selected, the existing namespace resource can ignore subsequent changes to that field:

```hcl
# Within the existing temporalcloud_namespace resource, after agreeing ownership:
lifecycle {
  ignore_changes = [accepted_client_ca]
}
```

Keep bootstrap trust configuration for initial creation, and establish how recreation and recovery obtain the current bundle. Review a plan after rollover: it must not restore retired trust. `ignore_changes` delegates reconciliation; it is not a backup or a way to protect certificate filters from unrelated changes. Check the schema for the provider version pinned in the existing infrastructure repository. See the [Temporal Cloud provider namespace reference](https://registry.terraform.io/providers/temporalio/temporalcloud/latest/docs/resources/namespace).

## Confirm the API-key/mTLS transition before deployment

API-key/mTLS coexistence and old/new CA coexistence are different mechanisms. The demo exercises the latter. Its `api-key` contract literal does not implement API-key rotation or enable mixed authentication.

As checked on November 1, 2026, Temporal's public namespace documentation describes mixed authentication as pre-release, requires contacting Support, excludes High Availability, and states that existing API-key namespaces cannot enable both methods. A feature request or two Terraform authentication fields is not evidence that an overlap period is available. Confirm eligibility and the migration path for each target namespace before committing to an overlap rollout. See [accessing Cloud namespaces](https://docs.temporal.io/cloud/namespaces#how-to-access-a-namespace-in-temporal-cloud).

For a namespace where both methods are confirmed, test the two paths with separate connections and the endpoints specified for each method. The same documentation requires the regional endpoint for API-key clients on a mixed-authentication namespace. Retain the working old path until all intended clients are verified on the new path. If the namespace is ineligible, agree a supported migration plan with Temporal before changing authentication.

## Load credentials in the SDK process

[connection-examples.ts](../demos/credential-supervisor/connection-examples.ts) contains configuration helpers checked against the pinned TypeScript SDK. Use the namespace's verified mTLS endpoint and the end-entity certificate mounted for that workload:

```ts
import { NativeConnection } from '@temporalio/worker';
import { mtlsConnectionOptions } from './connection-examples';

const options = await mtlsConnectionOptions({
  address: namespaceEndpoint,
  certificatePath: '/var/run/temporal/tls.crt',
  privateKeyPath: '/var/run/temporal/tls.key',
});
const connection = await NativeConnection.connect(options);
// Supply connection to Worker.create; close it after the Worker has stopped.
```

The same options work with `Connection.connect` for an SDK client. `apiKeyConnectionOptions` supplies a separate TLS connection authenticated with an API key. Neither helper enables a namespace authentication method or loads control-plane credentials into a Workflow. See the [TypeScript connection options](https://typescript.temporal.io/api/interfaces/client.ConnectionOptions).

The helper reads files once when called. Replacing a mounted Secret alone does not recreate that connection. Use a tested process reload strategy or a rolling Worker replacement with graceful shutdown. Keep the coordinator's own credentials valid independently of the workloads it is rotating.

Vault/VSO is one possible integration, not a requirement. Its `VaultPKISecret` supports a destination Secret and rollout restart targets; if selected, let that controller own issuance and restart triggers and have the rotation adapter verify the outcome. Avoid two controllers independently restarting the same workloads. See [VSO's resource reference](https://developer.hashicorp.com/vault/docs/deploy/kubernetes/vso/api-reference).

## Require evidence before removing old trust

The simulated sequence is `issue → add-trust → rollout-clients → verify-clients → remove-old-trust → retire-old`. Its verification step checks two independent fixture facts: Deployment convergence and authenticated acceptance of the new credential. Either failure prevents old-trust removal and triggers compensation. Those booleans are test inputs, not live checks.

For the real Rotate adapter, use bounded, retry-safe Activities that record public evidence tied to the operation ID:

1. Read the current bundle, filters, intended bindings, and all CA dependencies. Identify the exact managed CA, preserve unrelated trust and filters, and use the Cloud resource's version or equivalent concurrency protection. Add the new managed CA and read back the applied configuration.
2. Issue and deliver the new leaf certificates through the existing PKI and secret store. Record fingerprints and issuer references, never key bytes.
3. Replace or reload every affected client process. Verify the expected Deployment generation and updated/available replicas, and that old consumers are gone. A generic Pod-ready signal alone is insufficient. See [Kubernetes Deployment status](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#deployment-status).
4. Establish fresh authenticated connections using only the new credential. Exercise representative SDK work on the intended task queues and check workload-side credential fingerprints. A probe's successful connection alone does not prove that every Worker has switched. Keep representative work running through the rollout and an agreed observation window.
5. Remove only the old managed CA after all gates pass, preserving independent trust entries and filters. Check new connections and workload progress again. Retire old issuer material only after checking dependencies beyond this snapshot. Stop for operator intervention when inventory or evidence is incomplete.

The fake inventory models one managed trust set and replaces it wholesale. A production adapter must not copy that simplified mutation for a namespace that has independent trust entries.

Before old trust is removed, compensation can restore the previous usable configuration. After a real key is revoked, destroyed, or expired, it may be impossible to restore it. The fake adapter's ability to undo retirement is not a production recovery strategy; define a forward-recovery path and point of no return with the PKI owner.

## Pilot acceptance

| Exercise | Required evidence |
| --- | --- |
| Bind one nonproduction application | Desired namespace, service identity, Secret, consumers, and owners match the existing provisioning module |
| Renew a leaf under the same CA | New certificate is delivered and used without changing namespace trust |
| Roll over a CA under representative traffic | Old and new clients work during dual trust; all consumers switch; work continues after old trust is removed |
| Fail readiness or new-credential authentication | Old trust is preserved; the failure is visible; recovery follows the defined safe path |
| Retry after an ambiguous mutation response or Worker restart | The external operation is reconciled using a durable idempotency record; no duplicate issuance or destructive action |
| Reapply infrastructure | The plan does not restore stale trust or revoke still-needed access |
| Expand or shrink scope during rotation | Active work retains its snapshot; the next decision uses current scope; obsolete read failures do not pause corrected scope |
| Lose archive access or a dependency inventory | New destructive work stops; state and operation IDs remain recoverable |

Run the local tests and demo first. Real Cloud, issuer, secret-delivery, connectivity, and restart evidence is still required before expanding beyond the pilot. Establish actual role owners, renewal policy, namespace authentication eligibility, and a shared durable archive before treating this as a production service.
