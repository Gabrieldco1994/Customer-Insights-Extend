# Renegociação — DEMO backend (Python 3.11 / Functions v2)

Fictitious SQL source, independently usable by Power Pages and Copilot Studio.
**Not legally binding, not a bank integration.** No cloud deployment, Dataverse
schema mutation or live native-process write is performed by the local tests.
Everything in this directory is self-contained.

## Public API contract

`POST /api/renegociacao/{action}`, JSON, `Authorization: Bearer <portal JWT>`.
Every action verifies exactly the selected `AUTH_MODE` trust boundary:
`portal` (default) uses the trusted portal public key and contact-GUID `sub`;
`entra` uses the pinned tenant signing keys and a delegated access token, then
resolves verified `oid` + issuer to the contact. Contact/CPF/user headers never
authorize. Both clients must pass the **end customer's selected-mode token**,
not an agent/service identity, arbitrary contact ID or function key. There is
no fallback between modes, including when the portal reports `PortalSTS0018`.
This backend does not change portal login, implicit-grant certificates or SSO.

Successful envelope (HTTP 200 or 202):

```json
{"status":"ok","data":{},"demonstration":true}
```

`status` is **only** `ok` or `processing`. HTTP 202 / `processing` means native
reconciliation is pending (possibly the server's BPF-creation flow). It is not
completion. `data.status` is the source-facing journey state:
`draft`, `review`, `processing`, `completed`, or `not_started`.
`completed` requires durable source formalization **and** verified native
projection/BPF completion. All public errors are non-2xx:

```json
{"error":{"code":"INVALID_IDENTITY","message":"The portal identity is invalid or expired."},"demonstration":true}
```

| Action | Exact input | `data` shape |
|---|---|---|
| `profile` | `{}` or `{"cpf":"000.000.000-00"}` | `{contactId,name,cpfVerified}` |
| `contracts` | `{}` | `{contracts:[{id,number,vehicle,debtCents,eligible}]}` |
| `offers` | `{contractId}` | `{offers:[OFFER]}` |
| `start` | `{contractId,channel:"page"\|"chat"}` | `{requestId,status}` |
| `review` | `{requestId,offerId,version}` | `{requestId,reviewHash,offer:OFFER}` |
| `confirm` | `{requestId,reviewHash,confirmed:true}` | `{requestId,status,protocol,offer:OFFER}` |
| `status` | `{requestId}` **or** `{contractId}` | `{requestId,status,protocol,offer:OFFER}` |

### Authenticated chat transport

`POST /api/renegociacao/chat-token` accepts `{}` with the same delegated Entra
identity and an exact `Origin: https://portalatendimento01.powerappsportals.com`.
The server first resolves an active, unique contact binding, then uses
`DIRECTLINE_SECRET` from encrypted Function app settings to mint a short-lived,
origin-bound Direct Line token. Its unwrapped response is
`{token,conversationId,userId,expires_in}` with `Cache-Control: no-store`.
The secret never reaches page code, source files or logs. This token authorizes
the transport only: the agent's financial tools still require the customer's
delegated API token and never authorize by Direct Line user ID or CPF.

### Isolated WhatsApp simulation

`POST /api/whatsapp-demo/{action}` is a separate, function-key-protected demo
surface. It requires `WHATSAPP_DEMO_ENABLED=1`; the default is disabled.
The Copilot Studio companion uses a maker-owned managed connector connection,
not an end-customer token. Its key must stay in that encrypted connection.
The exact request wrapper is `{"payload":"<JSON action input>"}`.

This route fixes the customer server-side to the dedicated fictitious
`whatsapp-demo@renegociacao.invalid` contact and permits only `WHATS-DEMO-001`
and `WHATS-DEMO-002`, with checked source IDs and product anchors. It refuses
an active identity binding, a changed contact marker, customer selection,
phone numbers, access tokens, and contracts or requests outside that set.
Responses additionally contain `simulationOnly:true`.

The companion's numeric-code question is a **simulation**, not authentication:
no SMS is sent, and no Microsoft/phone identity is established. It must state
that distinction explicitly. The existing portal, authenticated agent, delegated
JWT route, original customer mappings and prior agreements remain unchanged.
Native Cases and BPFs are recorded against only the dedicated synthetic customer.
Use `scripts/renegociacao_whatsapp_seed.py` to prepare its native fixtures and
the guarded bootstrap launcher to append its source contracts; never reopen
completed demo agreements.

### Native MCP tools

The same Function app also exposes the native Azure Functions MCP extension at
`/runtime/webhooks/mcp` (Streamable HTTP). The extension requires its dedicated
`mcp_extension` **system key**; anonymous webhook authorization is not enabled.
The key belongs only in the encrypted Power Platform connection.

The discoverable tools are exactly `listar_contratos_demo`,
`consultar_ofertas_demo`, and `consultar_andamento_demo`. All reuse the fixed
synthetic-customer guard and refuse arbitrary customer/contract selectors.
MCP dispatch explicitly disallows writes, even for an authenticated service.
Returned offers include an exact BRL `resumo` formatted from integer cents,
without asking the language model to calculate amounts.

`scripts/renegociacao_mcp.py probe` verifies unauthorized access fails closed,
performs a real MCP initialization/discovery and queries the protected dataset.
It never displays the system key. The regular delegated JWT API and the
function-key-only transactional demo connector remain distinct trust boundaries.

`OFFER` has exactly:

```json
{"id":"55555555-5555-4555-8555-555555555555","version":1,"entryCents":1001,"installments":6,"paymentCents":1500,"totalCents":10001,"validUntil":"2026-11-01T00:00:00Z"}
```

All money is integer BRL cents: `entryCents + installments * paymentCents ==
totalCents`. The seed moves cent remainders into the entry, never rounds away
debt or calculates arbitrary discounts. `protocol` and `offer` are nullable
before review/formalization. Status for an owned contract without a request is
`{requestId:null,status:"not_started",protocol:null,offer:null}`. A pending
formalization response includes its durable `DEMO-...` protocol and saved offer
even when native reconciliation failed.

**Both clients must** display the exact returned review (amounts, count, total,
expiry and demo disclaimer), ask for explicit confirmation and then pass its
`reviewHash` with literal boolean `true`. Do not have an LLM calculate money,
invent offers, auto-confirm, or treat an HTTP 202 as success. Expired/changed
offers require review again. A hash from an old/different request is rejected.

## Configuration

| Environment variable | Purpose |
|---|---|
| `SQL_SERVER` | Full `name.database.windows.net` hostname (not an IP/private alias). |
| `SQL_DATABASE` | Dedicated demo database. |
| `DATAVERSE_URL` | `https://org88957f9a.crm.dynamics.com` |
| `AUTH_MODE` | `portal` (default) or `entra`; exact, case-sensitive selection, never fallback. |
| `PORTAL_PUBLIC_KEY_PEM` | Portal mode only: trusted RSA public PEM, >=2048 bits; real newlines or escaped `\n`. Never a private key. |
| `PORTAL_ISSUER`, `PORTAL_AUDIENCE` | Portal mode only: exact portal JWT issuer and backend audience. |
| `ENTRA_AUDIENCE` | Entra mode only: canonical lowercase client-ID GUID of the **dedicated API application**, not Graph, portal, frontend or MI client ID. |
| `CPF_FIELD` | Optional; only `fsi_cpf` is accepted. CPF checks the resolved contact, never selects one. |
| `REASON_CODE_FIELD` | Existing `fsi_motivo` text field containing `CRE-REN`; required, no guessed column. |
| `ANCHOR_CONTACT_FIELD` | Optional existing anchor-to-contact lookup logical name; otherwise discover the unique contact relationship. Fail closed if ambiguous. |
| `BPF_WORKFLOW_ID` | Exact deployed `fsi_processorenegociacao` workflow GUID; required before projecting the case reason. Backend validates its unique name and active state. |
| `BPF_STAGE_MAP` | JSON mapping the five slugs below to real `processstageid` GUIDs. Missing map = explicit pending, never an invented stage. |
| `ODATA_PUBLIC_ROOT` | HTTPS origin/path, e.g. `https://<function>.azurewebsites.net/api/odata`, no query/key. |
| `MANAGED_IDENTITY_CLIENT_ID` | Optional **client ID** for user-assigned MI; otherwise system-assigned MI. |
| `SQL_CA_FILE` | Optional trusted CA bundle file. Default certifi bundle; TLS and hostname verification cannot be disabled. |
| `BOOTSTRAP_ENABLED` | Default disabled. Set exactly `1` only during initial admin bootstrap, then remove/disable. |
| `DIRECTLINE_SECRET` | Server-side secret of the dedicated bot. Required only for authenticated `chat-token`; never include its value in source files, browser settings or logs. |
| `AZURE_CREDENTIAL_MODE` | Default `managed_identity`. Only local operations may use `azure_cli` with `ALLOW_LOCAL_AZURE_CLI=1`. Hosted Functions reject CLI mode. Independent of inbound `AUTH_MODE`. |

**Migration:** older `AUTH_MODE=managed_identity` / `azure_cli` values are now
explicit configuration errors. Move that choice to `AZURE_CREDENTIAL_MODE` and
select `AUTH_MODE=portal` or `entra`. Never alias or auto-detect token modes.

The application user is not System Administrator. Besides the explicitly scoped
record privileges, the SDK needs `prvReadEntity`, `prvReadAttribute` and
`prvReadRelationship` at the metadata scope supported by Dataverse. Case
projection also required `prvReadAsyncOperation` at Basic depth. A successful
`$metadata` request alone does not prove access to `EntityDefinitions`; verify
both under the application user's permissions.

### Entra trust and contact binding

Pinned, non-overridable tenant:
`3a3c8be3-6a50-455f-a303-f9df0c768430`.

- Issuer: `https://login.microsoftonline.com/3a3c8be3-6a50-455f-a303-f9df0c768430/v2.0`
- JWKS: `https://login.microsoftonline.com/3a3c8be3-6a50-455f-a303-f9df0c768430/discovery/v2.0/keys`
- Algorithm: RS256 only, RSA >=2048 bits; `exp`, `iss`, `aud`, `oid`, `tid`
  required. Issuer, tenant and single audience must match exactly.
- A delegated `scp` containing the exact word `Renegotiate` is required.
  App-only tokens (including `idtyp=app` or roles without `scp`) are rejected.
- For the separately provisioned API app, request v2 access tokens and expose
  delegated scope `api://<API-client-id>/Renegotiate` (or the app's explicitly
  configured Application ID URI plus `/Renegotiate`). Consent and client token
  acquisition are external; do not send an ID token, Graph token or MI token.
- PyJWT validates signatures/rotation using a five-minute JWKS-set cache.
  HTTPS certificate validation is mandatory; redirects and token-supplied key
  URLs cannot change the pinned fetch destination. Private-network deployment
  must allow outbound HTTPS to the pinned Microsoft endpoint. Key-fetch
  failure returns 503, not portal fallback or unauthenticated access.

Native contact mapping (provisioned/populated **only by trusted server/admin
operations**, never by these customer endpoints):

| Table/field | Type | Stored value |
|---|---|---|
| `contact.fsi_authobjectid` | Text(36), not required | Canonical verified user `oid` GUID. |
| `contact.fsi_authissuer` | Text(200), not required | The exact pinned issuer above, no trailing slash. |
| `contact.fsi_cpf` | Existing text | CPF verification only, after binding. |

The read query uses both mapping fields and `top=2`, then checks exact returned
values and active state. Missing, inactive, ambiguous or duplicate bindings
return the same 403 without enumeration. Missing metadata/backend availability
returns 503. No email, `sub`, CPF, request body, portal-contact header or
user-created binding fallback exists. Enforce column-level write restrictions
and preferably a unique composite key externally; the fields need not be
globally required. No code here creates or updates these mappings.

`BPF_STAGE_MAP` keys: `identificacao`, `elegibilidade`, `simulacao`,
`confirmacao`, `formalizacao`. Validate them against the deployed active
`fsi_processorenegociacao` definition. Runtime uses the server-created incident
BPF: draft advances to elegibilidade, review to confirmacao, formalized to
formalizacao, traversing adjacent stages. **Never creates a guessed BPF
instance.** An inactive, unexpected or ahead-of-source stage fails reconciliation.
The case reason lets the existing "Assunto do caso define a jornada" flow create
the process instance. Legacy `incident.processid` and `stageid` can remain null;
the dedicated BPF instance table is authoritative. Reconciliation never
rewrites the existing case reason or operator title: doing so could retrigger
the flow and reset process progress. Customer, product, reason and line bindings
must still match. Missing or invalid workflow configuration leaves source
reconciliation pending before the case/reason is written. No deterministic workflow GUID is invented, and no
SDK `flush_cache` call or metadata mutation is performed.
The native request explicitly retains `fsi_etapa=aguardando_bpf` and a
`BPF_PENDING` diagnostic until the actual BPF can be verified.
Only a confirmed external result may end this BPF. No code changes
`fsi_vinculonegocio`, macro journey, cobrança, standard-column metadata, or case
closure. A reason or stage mismatch remains pending for operator correction.

Runtime record CRUD uses `DataverseClient`, MI, and the closed attribution
`app=dataverse-skills/unknown;skill=dv-data;agent=copilot`. Only CSDL retrieval
uses verified HTTPS requests because SDK 1.1.0 has no public CSDL endpoint;
lookup casing/entity sets are verified before SDK `@odata.bind` operations.
The local auth helper elsewhere in the repository is not modified or used.
`fsi_snapshot` is compatible with the supplied **Memo10000** field.

Configure Functions platform CORS separately for exact allowed page origins;
it is not authentication. Do not expose OData/bootstrap keys to browser code.
Do not enable HTTP-body/Authorization/CPF logging in APIM, proxies or telemetry.
JWTs/CPF and integration exception bodies are not logged by this backend.

## SQL bootstrap and least privilege

Provisioning is external to this project. MI needs an Entra application user
with scoped Dataverse read/create/update privileges for the named entities;
contacts and anchors should be read-only. SQL tokens use only
`https://database.windows.net/.default`; no passwords, ODBC or custom auth.
`python-tds` requires pyOpenSSL for its TLS path (included in requirements).

Temporarily authorize the Functions MI as SQL Entra administrator. Invoke
`POST /api/bootstrap` with **that function's own key** in `x-functions-key`,
`Content-Type: application/json`, and `BOOTSTRAP_ENABLED=1`:

```json
{
  "fixtures": [
    {
      "contactId":"11111111-1111-4111-8111-111111111111",
      "anchorId":"44444444-4444-4444-8444-444444444444",
      "contractId":"33333333-3333-4333-8333-333333333333",
      "number":"DEMO-001",
      "vehicle":"Veículo fictício",
      "debtCents":10001,
      "eligible":true
    }
  ],
  "managedIdentityName":"your-function-mi",
  "managedIdentityClientId":"66666666-6666-4666-8666-666666666666"
}
```

The IDs above are placeholders: use **existing authorized demo contacts and
native product-client anchors**, plus new source contract IDs. Do not import
real CPF/financial records. Optional MI fields must both be supplied; the SQL SID is
the Entra service principal **client ID**, not its principal object ID. Azure SQL's documented
contained Entra-user form avoids directory lookup:

```sql
-- Execute as temporary SQL Entra admin. Client-id GUID -> SQL binary SID:
SELECT CONVERT(varchar(34),
    CONVERT(varbinary(16), CONVERT(uniqueidentifier, '<MI-client-id-GUID>')), 1);
-- Paste the exact 0x... result, not the string literal '<...>':
CREATE USER [your-function-mi] WITH SID = 0x..., TYPE = E;
ALTER ROLE demo_service ADD MEMBER [your-function-mi];
```

`schema.sql` creates schema/tables/indexes/constraints and the role idempotently.
Bootstrap serializes initialization, inserts deterministic fixtures plus at
least **2,000 synthetic installment rows across the submitted fixtures**, and
two 30-day fixed offers per contract. Same input never resets eligibility,
rewrites agreements, or renews offers. Changed immutable fixture values cause
409. No drop/truncate is used. If the database does not support `WITH SID,
TYPE=E`, have the SQL administrator use the supported
`CREATE USER [your-function-mi] FROM EXTERNAL PROVIDER` instead, then rerun
without optional MI fields and grant role membership manually. The backend
does not silently substitute unsupported DDL.

**Immediately afterwards:** set `BOOTSTRAP_ENABLED=0`, revoke its function key,
remove temporary MI server-admin privileges, and retain only `demo_service`
membership (not `db_owner`, `db_ddladmin` or administrator). The role grants
schema-scoped SELECT/INSERT/UPDATE/DELETE/EXECUTE, with UPDATE/DELETE explicitly
denied on immutable Agreements. Its trigger also prevents mutation. Test a
fresh MI connection after removing administrator rights. An existing SQL user
with a mismatched SID is an explicit conflict, not silently rebound.

## Transaction/reconciliation behavior

Contract `UPDLOCK,HOLDLOCK`, consistent contract-before-request lock order,
one unique request per contract, and unique agreement request/contract/protocol
protect across workers/processes. Channels share the same durable request;
channel records its original entry point. Confirmation atomically creates the
immutable Agreement, saves protocol/snapshot, and closes contract eligibility.
Retries return the existing agreement, including after its offer expires.

Reconciliation takes a source lock, rereads current state, projects deterministic
`incidentid == requestId` and `fsi_renegociacaoid == requestId`, verifies native
readback, and advances the BPF only when appropriate. Known transient API
429/503/504 responses have bounded retries; other errors are not disguised.
Only confirmed duplicate-key error codes permit duplicate recovery, with
ownership checks. CRM failure never undoes a committed external agreement or
creates a replacement: HTTP 202, then `status` retries reconciliation.
SQL commit/network ambiguity returns an explicit error instructing a status
check, not an automatic retry or false success. Clients must continue polling
`status` (bounded backoff, then operator intervention); no background queue is
claimed. Server metadata is cached per worker; restart workers after schema
changes.

## Genuine read-only OData v4 source

`GET /api/odata/`, `$metadata`, `Contracts`, `Installments`, and
`Contracts(<GUID>)` / `Installments(<GUID>)`. The entire route, including
metadata and keys, has `AuthLevel.FUNCTION`, unlike the host-anonymous JWT
customer route. Use a separate **ExternalSourceOData function-level key**, not
host/master/bootstrap keys. The host enforces keys; direct Python invocation
does not simulate host key authorization. Prefer `x-functions-key` header.

OData JSON/CSDL publish the exact requested names/types: `ClientRef` is the
contact GUID as Edm.String; `ProductRef` is the native anchor GUID as
Edm.String; `ContractRef` is a source contract GUID as Edm.String. Public
customer operations never expose global OData data. Contact/contract lookups
have source indexes; public operations may read SQL directly, while VTs
independently read this OData source.

Supported query options: `$filter` (typed `eq` joined by lowercase `and`),
`$select` (explicit known columns), `$top`, `$skip`. The stable `Id` is always
included, including for single-row reads. Example:

```text
Contracts?$filter=ClientRef eq '11111111-1111-4111-8111-111111111111' and Eligible eq true
Installments?$filter=ContractRef eq '33333333-3333-4333-8333-333333333333'&$top=500
```

GUID keys use unquoted v4 literals; String GUID refs use single quotes. Strings
escape apostrophes as `''`. Default page 100, maximum explicit window 1,000, stable Id order,
`@odata.nextLink` preserves filter and remaining top/skip, never function keys.
Clients must resend the header key when following links. `$top=0` is supported.
The built-in Dataverse provider did not propagate the source continuation when
an explicit 500-row query was capped at 200. Explicit windows now return the
requested size up to 1,000; larger `$top` requests fail with `ODATA_QUERY_LIMIT`
instead of silently truncating. Use indexed contract filters and explicit skip
windows for larger reads. This demonstration does not certify unrestricted bulk
virtual-table exports.
Unsupported operators/functions/options (`or`, ranges, `$expand`, `$orderby`,
`$count`, etc.), duplicate query parameters and invalid types fail
explicitly; no filter is silently ignored. No POST/PATCH/DELETE OData endpoints.
Offsets are deterministic for an unchanged demo fixture set; don't bootstrap
new fixtures while a virtual-table client is paginating.

## Deployment preparation only (no deployment performed)

`deployment.settings.example.json` is the exact **backend app-settings**
template for Entra mode. Replace every `<...>` value using the independently
provisioned API app, SQL/Functions resources and deployed BPF. `BPF_STAGE_MAP`
is a JSON-encoded string, not an embedded settings object. The workflow ID is
the deployed deterministic helper result supplied by the schema owner; do not
derive or guess it here. Portal settings are neither required nor used in
Entra mode.

The template assumes system-assigned MI. For user-assigned MI add
`MANAGED_IDENTITY_CLIENT_ID=<MI clientId>`; optional ambiguous anchor lookup
selection uses `ANCHOR_CONTACT_FIELD=<confirmed logical name>`. Do not put
client secrets, passwords, access tokens or function keys in the template.
Keep `BOOTSTRAP_ENABLED=0` except during the guarded initialization window.

Flex infrastructure must already specify Python **3.11**, the deployment
container/storage authentication and host-storage connection via the chosen
platform-managed MI configuration. These are provisioning concerns, not
hardcoded here. Use Flex's supported remote-build option for dependency
installation; do **not** copy a macOS `.venv`, or set legacy
`WEBSITE_RUN_FROM_PACKAGE`, `SCM_DO_BUILD_DURING_DEPLOYMENT`,
`ENABLE_ORYX_BUILD`, `FUNCTIONS_WORKER_RUNTIME` or
`FUNCTIONS_EXTENSION_VERSION` settings on Flex.

Create the source ZIP locally (no deployment side effect):

```bash
cd services/renegociacao
.venv/bin/python build_zip.py
```

`renegociacao.zip` has this exact runtime manifest, with `host.json` at ZIP root:

```text
function_app.py
host.json
requirements.txt
schema.sql
renegociacao/__init__.py
renegociacao/bootstrap.py
renegociacao/core.py
renegociacao/dataverse.py
renegociacao/directline_token.py
renegociacao/odata.py
renegociacao/security.py
renegociacao/sql.py
```

Do not include tests, settings, fixture JSON, keys, `.venv`, `.python_packages`,
README or the ZIP builder. Bootstrap needs `schema.sql` at that exact root.
The builder assigns Unix regular-file mode `0644` to every entry. Do not use
`ZipFile.writestr()` defaults (`0600`): the Flex runtime can then fail with
`Access to the path '/home/site/wwwroot/host.json' is denied`, even when
upload and remote build succeed. The packaging test checks these permissions.
`python-tds` 1.17 uses `X509.get_extension()` for SQL certificate hostname
validation; pyOpenSSL 26 removed it. Keep pyOpenSSL below 26 until the driver
supports the new API. The TLS compatibility test checks a matching and a
mismatched hostname; never disable TLS validation to bypass this incompatibility.
For the deployment operator **later**, Azure CLI's Flex-aware ZIP/OneDeploy
invocation is below; it has **not** been run by this task:

```bash
az functionapp deployment source config-zip \
  --resource-group '<resource-group>' --name '<function-app>' \
  --src services/renegociacao/renegociacao.zip --build-remote true
```

Run that example from the repository root, after configuring the prepared
settings in the provisioning workflow. There is no deploy command in the ZIP
builder or tests. Azure host/function keys must still be enforced by Functions;
cloud networking, remote build, native schema and actual issuer-issued tokens
require separate runtime acceptance.

### Bootstrap body and MI grant/read-model names

`bootstrap.example.json` is the exact fixture-body example: root `fixtures`
array with `{contactId,anchorId,contractId,number,vehicle,debtCents,eligible}`,
plus optional paired `managedIdentityName`/`managedIdentityClientId`. Replace the
synthetic contact/anchor IDs with existing authorized demo native IDs, source
contract IDs with fresh GUIDs, and MI values with the Function identity:

- `managedIdentityName`: SQL contained-user name / MI display name.
- `managedIdentityClientId`: the managed identity **clientId**, converted to SQL
  binary SID internally. Server administrator assignment separately uses
  **principalId / object ID**; these IDs are not interchangeable.
- `MANAGED_IDENTITY_CLIENT_ID`: optional user-assigned MI **clientId** for Azure
  credential selection, **not** the API application's `ENTRA_AUDIENCE`.
- The SSO **user** `oid` is a fourth, distinct ID stored on the native contact;
  do not bind the Function MI principal to a customer contact.

Use the bootstrap function's own `x-functions-key` header and explicit enabled
flag. The body does not contain an access token, Entra `oid`, CPF or SQL password.
Do not infer a customer from the identity of the bootstrap caller.

SQL runtime grant objects are `SCHEMA::demo` and role `demo_service`; tables
are `demo.Contracts`, `demo.Installments`, `demo.Offers`, `demo.Requests`,
`demo.Agreements`. The role's schema DML/EXECUTE grant and Agreement
UPDATE/DELETE denial are in `schema.sql`. **No db_owner grant is used.**
Contained-user conflict checks read `sys.database_principals.name`, `.sid`
(binary) and `.type` (`E` for Entra service principal). Administrator grant
verification may read `sys.database_role_members.role_principal_id` /
`.member_principal_id` joined to `sys.database_principals.principal_id`.
No SQL grant is needed for the inbound SSO user; SQL always uses the service MI.

Exact read-model mapping (public OData property → physical source column):

| Entity set | OData properties → SQL columns |
|---|---|
| `Contracts` | `Id→Id`, `Name→Number`, `ClientRef→ContactId`, `ProductRef→AnchorId`, `Number→Number`, `Vehicle→Vehicle`, `DebtCents→DebtCents`, `Eligible→Eligible`, `PositionDate→PositionDate` |
| `Installments` | `Id→Id`, `Name→Number` (string representation), `ContractRef→ContractId`, `ClientRef→ContactId`, `Number→Number`, `AmountCents→AmountCents`, `DueDate→DueDate`, `Status→Status` |

`ClientRef`, `ProductRef`, `ContractRef` are **OData Edm.String GUIDs**, not
physical SQL column names. Source `ContactId` / `AnchorId` / `ContractId` are
SQL uniqueidentifiers. The backend MI role scopes to `demo`, not guessed
`ClientRef` SQL columns or Dataverse tables.

## Local verification

```bash
cd services/renegociacao
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m py_compile function_app.py build_zip.py renegociacao/*.py tests/*.py
```

Unit tests use generated RSA keys and in-memory source/CRM models only, including
concurrent replay, exact cents, authorization, expired/changed reviews, pending
native BPF, OData parsing/paging, function bindings, bootstrap count/idempotence,
Entra/portal isolation, pinned JWKS, scope/app-only rejection, exact unique
contact binding, case-reason replay safety, and the 12-file ZIP manifest.
Fresh isolated Python subprocess tests also import the real dependencies,
serialize/index all three Functions, verify trigger/Python parameter names,
and prohibit network I/O. Import/indexing needs no portal, SQL or Dataverse
configuration; clients and credentials are initialized only on requests.
They do **not** prove live SQL locking/commit behavior, contained-user syntax and
post-admin MI grants, Azure private DNS/routing/firewall, platform key enforcement,
actual portal/Entra JWT issuance and consent, server-managed user-contact
mapping, Dataverse privileges/CSDL/required fields, server
reason-to-BPF flow/stage transitions, or VT provider compatibility. These require
runtime acceptance after the separately managed deployment. No real environment
data or credentials are used by this suite.

## Deployment acceptance — 2026-10-26

- The existing customer portal page and the published conversational agent each
  completed a separate fictitious renegotiation and displayed its real `DEMO-`
  protocol. Neither channel required the other to confirm the offer.
- Dataverse readback confirmed exact cent totals, `Concluido`, no integration
  error, and a completed five-stage BPF for both requests.
- The external SQL source contains 2,000 synthetic installments. Dataverse
  virtual-table reads returned a complete, contract-filtered 500-row window and
  reflected the source eligibility change after formalization.
- Bootstrap is disabled and its temporary key revoked; the SQL administrator
  was restored. Runtime uses the dedicated managed identity and limited role,
  not System Administrator. Chat credentials remain server-side.

These checks validate the demonstration, not a production bank integration,
legal signature, payment, unrestricted bulk export, or production load SLO.
