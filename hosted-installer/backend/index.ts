import { gunzipSync } from 'node:zlib';
import { error, json, router } from '@appdeploy/sdk';
import { payloadGzipBase64 } from './payload';

type Body = Record<string, unknown>;
type PackageFile = { path: string; content: string };
type PackageItem = {
    type: string;
    displayName: string;
    files: PackageFile[];
};
type InstallerPackage = {
    solution: string;
    items: Record<string, PackageItem>;
};
type FabricCapacity = {
    id: string;
    displayName: string;
    sku: string;
    region: string;
    state: string;
};
type FabricWorkspace = {
    id: string;
    displayName: string;
    description?: string;
    type: string;
    capacityId?: string;
};
type WorkspaceRoleAssignment = {
    principal: { id: string; type: string };
    role: string;
};
type DataverseShortcut = {
    name: string;
    target?: {
        type?: string;
        dataverse?: {
            connectionId?: string;
            deltaLakeFolder?: string;
            environmentDomain?: string;
            tableName?: string;
        };
    };
};

const CLIENT_ID = 'ed676c8d-6fbb-43d0-a4f7-5fb228538ab9';
const FABRIC = 'https://api.fabric.microsoft.com/v1';
const SOLUTION_NAME = 'FabricCIJourneyDashboardandAgents';
const SOLUTION_VERSION = '1.0.0.2';
const MARKER = 'Managed by CustomerInsightsJourney package 1.0.0.2';
const TARGET_WORKSPACE = 'CI-JOURNEY-Integration';
const WORKSPACE_MARKER = 'Customer Insights Journey Dataverse Link and deployment workspace';
const JOURNEY_SHORTCUT_TABLES = [
    'EmailSent',
    'EmailDelivered',
    'EmailOpened',
    'CustomSent',
    'CustomNotSent',
];
const REQUIRED_JOURNEY_SHORTCUT_TABLES = JOURNEY_SHORTCUT_TABLES.slice(0, 3);
const REQUIRED_LINK_TABLES = [
    'contact',
    'lead',
    'opportunity',
    'msdynmkt_journey',
    'msdynmkt_email',
    'msdynmkt_segmentusage',
    'crfc2_journeychannelcost',
];
const SOURCE = {
    workspace: '00000000-0000-0000-0000-000000000000',
    lakehouseId: '11111111-1111-4111-8111-111111111111',
    lakehouseName: 'SOURCE_LAKEHOUSE',
    server: 'SOURCE_SQL_ENDPOINT',
    databaseId: '33333333-3333-4333-8333-333333333333',
    semanticId: '22222222-2222-4222-8222-222222222222',
};

const installerPackage = JSON.parse(
    gunzipSync(Buffer.from(payloadGzipBase64, 'base64')).toString('utf8'),
) as InstallerPackage;

function value(body: Body, name: string): string {
    const result = body[name];
    if (typeof result !== 'string' || !result.trim()) {
        throw new Error(`${name} is required.`);
    }
    return result.trim();
}

function environmentUrl(input: string): string {
    const url = new URL(input);
    if (
        url.protocol !== 'https:' ||
        !/^[a-z0-9-]+\.crm\d*\.dynamics\.com$/i.test(url.hostname)
    ) {
        throw new Error('Enter a valid public-cloud Dataverse environment URL.');
    }
    return url.origin;
}

function tokenObjectId(token: string): string {
    try {
        const claims = JSON.parse(
            Buffer.from(token.split('.')[1], 'base64url').toString('utf8'),
        ) as { oid?: string };
        return uuid(claims.oid || '', 'token object ID');
    } catch {
        throw new Error('The Fabric token does not contain a valid user object ID.');
    }
}

async function managedWorkspace(token: string, workspaceId: string) {
    const workspace = await apiGet(
        `${FABRIC}/workspaces/${encodeURIComponent(workspaceId)}`,
        token,
    ) as FabricWorkspace;
    if (
        workspace.displayName !== TARGET_WORKSPACE ||
        workspace.description !== WORKSPACE_MARKER
    ) {
        throw new Error(`${TARGET_WORKSPACE} is not managed by this installer.`);
    }
    const assignments = await fabricCollection(
        `/workspaces/${encodeURIComponent(workspace.id)}/roleAssignments`,
        token,
    ) as WorkspaceRoleAssignment[];
    const objectId = tokenObjectId(token);
    const currentUserIsAdmin = assignments.some(
        (assignment) =>
            assignment.principal.id.toLowerCase() === objectId.toLowerCase() &&
            assignment.principal.type === 'User' &&
            assignment.role === 'Admin',
    );
    const hasOtherPrincipals = assignments.some(
        (assignment) => assignment.principal.id.toLowerCase() !== objectId.toLowerCase(),
    );
    if (!currentUserIsAdmin || hasOtherPrincipals) {
        throw new Error(
            `${TARGET_WORKSPACE} cannot be reused safely. ` +
            'The connected user must be its only assigned principal and an Admin.',
        );
    }
    return workspace;
}

function normalizedEnvironment(input?: string) {
    try {
        return new URL(input || '').origin.toLowerCase();
    } catch {
        return '';
    }
}

async function responseJson(response: Response): Promise<unknown> {
    const text = await response.text();
    let data: unknown = null;
    if (text) {
        try {
            data = JSON.parse(text);
        } catch {
            data = text;
        }
    }
    if (!response.ok) {
        throw new Error(microsoftError(response, data));
    }
    return data;
}

function microsoftError(response: Response, data: unknown): string {
    const detail = data && typeof data === 'object'
        ? data as {
            error?: string | { code?: string; message?: string };
            error_description?: string;
            code?: string;
            message?: string;
        }
        : {};
    const nested = typeof detail.error === 'object' ? detail.error : undefined;
    const code = nested?.code || detail.code || (
        typeof detail.error === 'string' ? detail.error : ''
    );
    const message =
        nested?.message ||
        detail.error_description ||
        detail.message ||
        (typeof data === 'string' ? data : '');
    const requestId =
        response.headers.get('x-ms-request-id') ||
        response.headers.get('request-id') ||
        response.headers.get('x-ms-activity-id');
    return [
        `Microsoft API ${response.status}${response.statusText ? ` ${response.statusText}` : ''}`,
        code,
        message,
        requestId ? `Request ID: ${requestId}` : '',
    ].filter(Boolean).join(' — ');
}

async function deviceStart(resource: string, environment?: string) {
    // ponytail: device-code auth avoids popup and redirect configuration.
    const scope =
        resource === 'fabric'
            ? 'https://api.fabric.microsoft.com/Item.ReadWrite.All https://api.fabric.microsoft.com/Workspace.ReadWrite.All https://api.fabric.microsoft.com/Capacity.ReadWrite.All https://api.fabric.microsoft.com/OneLake.ReadWrite.All'
            : resource === 'azure'
              ? 'https://management.azure.com/user_impersonation'
            : `${environmentUrl(environment || '')}/user_impersonation`;
    const response = await fetch(
        'https://login.microsoftonline.com/organizations/oauth2/v2.0/devicecode?mkt=en-US',
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ client_id: CLIENT_ID, scope }),
        },
    );
    return responseJson(response);
}

async function devicePoll(deviceCode: string) {
    const response = await fetch(
        'https://login.microsoftonline.com/organizations/oauth2/v2.0/token',
        {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({
                client_id: CLIENT_ID,
                device_code: deviceCode,
                grant_type: 'urn:ietf:params:oauth:grant-type:device_code',
            }),
        },
    );
    const data = (await response.json()) as {
        access_token?: string;
        error?: string;
        error_description?: string;
    };
    if (response.ok) return { status: 'succeeded', accessToken: data.access_token };
    if (data.error === 'authorization_pending' || data.error === 'slow_down') {
        return { status: 'pending' };
    }
    throw new Error(microsoftError(response, data));
}

function bearerHeaders(token: string, jsonBody = false): Record<string, string> {
    return {
        Authorization: 'Bearer ' + token,
        Accept: 'application/json',
        ...(jsonBody ? { 'Content-Type': 'application/json' } : {}),
    };
}

async function apiGet(url: string, token: string) {
    return responseJson(
        await fetch(url, { headers: bearerHeaders(token) }),
    );
}

async function fabricCollection(path: string, token: string) {
    const values: unknown[] = [];
    let next: string | null = `${FABRIC}${path}`;
    while (next) {
        const page = (await apiGet(next, token)) as {
            value?: unknown[];
            continuationUri?: string;
        };
        values.push(...(page.value || []));
        next = page.continuationUri || null;
    }
    return values;
}

async function fabricCapacities(token: string): Promise<FabricCapacity[]> {
    return await fabricCollection('/capacities', token) as FabricCapacity[];
}

async function context(body: Body) {
    const environment = environmentUrl(value(body, 'environmentUrl'));
    const dataverseToken = value(body, 'dataverseToken');
    const fabricToken = value(body, 'fabricToken');
    const headers = bearerHeaders(dataverseToken);
    const who = (await responseJson(
        await fetch(`${environment}/api/data/v9.2/WhoAmI`, { headers }),
    )) as { UserId: string };
    const user = (await responseJson(
        await fetch(
            `${environment}/api/data/v9.2/systemusers(${who.UserId})` +
            '?$select=fullname,internalemailaddress' +
            '&$expand=systemuserroles_association($select=name)',
            { headers },
        ),
    )) as {
        fullname?: string;
        internalemailaddress?: string;
        systemuserroles_association?: Array<{ name?: string }>;
    };
    const systemAdministrator = Boolean(
        user.systemuserroles_association?.some(
            (role) => role.name === 'System Administrator',
        ),
    );
    const journeyCheck = await fetch(
        `${environment}/api/data/v9.2/EntityDefinitions(LogicalName='msdynmkt_journey')?$select=LogicalName`,
        { headers },
    );
    if (!journeyCheck.ok && journeyCheck.status !== 404) {
        await responseJson(journeyCheck);
    }
    const solutions = (await apiGet(
        `${environment}/api/data/v9.2/solutions` +
        `?$select=version,ismanaged&$filter=uniquename eq '${SOLUTION_NAME}'`,
        dataverseToken,
    )) as { value?: Array<{ version?: string; ismanaged?: boolean }> };
    const solution = solutions.value?.find(
        (item) => item.ismanaged && item.version === SOLUTION_VERSION,
    );
    const workspaces = (await fabricCollection('/workspaces', fabricToken)) as Array<{
        id: string;
        displayName: string;
        type: string;
    }>;
    const capacities = await fabricCapacities(fabricToken);
    return {
        user: {
            name: user.fullname || 'Microsoft user',
            email: user.internalemailaddress || '',
        },
        systemAdministrator,
        customerInsightsInstalled: journeyCheck.ok,
        solutionInstalled: Boolean(solution),
        solutionVersion: solution?.version || '',
        capacities,
        workspaces: workspaces
            .filter((workspace) => workspace.type === 'Workspace')
            .map(({ id, displayName }) => ({ id, displayName })),
    };
}

async function capacities(body: Body) {
    return { capacities: await fabricCapacities(value(body, 'fabricToken')) };
}

function qualifyingCapacity(capacity: FabricCapacity) {
    return /^(F|P)\d+$/i.test(capacity.sku) && capacity.state === 'Active';
}

async function ensureWorkspace(body: Body) {
    const token = value(body, 'fabricToken');
    const requestedCapacityId = value(body, 'capacityId');
    const capacities = await fabricCapacities(token);
    const requestedCapacity = capacities.find(
        (capacity) => capacity.id === requestedCapacityId && qualifyingCapacity(capacity),
    );
    if (!requestedCapacity) throw new Error('Select an active Fabric F/P capacity.');

    const workspaces = await fabricCollection('/workspaces', token) as FabricWorkspace[];
    const matches = workspaces.filter(
        (workspace) => workspace.type === 'Workspace' && workspace.displayName === TARGET_WORKSPACE,
    );
    if (matches.length > 1) throw new Error(`Multiple ${TARGET_WORKSPACE} workspaces exist.`);

    let workspace = matches[0];
    let created = false;
    if (!workspace) {
        workspace = await responseJson(await fetch(`${FABRIC}/workspaces`, {
            method: 'POST',
            headers: bearerHeaders(token, true),
            body: JSON.stringify({
                displayName: TARGET_WORKSPACE,
                description: WORKSPACE_MARKER,
            }),
        })) as FabricWorkspace;
        created = true;
    } else if (workspace.description !== WORKSPACE_MARKER) {
        throw new Error(`${TARGET_WORKSPACE} already exists and is not managed by this installer.`);
    } else {
        workspace = await managedWorkspace(token, workspace.id);
    }

    const currentCapacity = capacities.find(
        (capacity) => capacity.id === workspace.capacityId && qualifyingCapacity(capacity),
    );
    if (!currentCapacity) {
        await responseJson(await fetch(
            `${FABRIC}/workspaces/${encodeURIComponent(workspace.id)}/assignToCapacity`,
            {
                method: 'POST',
                headers: bearerHeaders(token, true),
                body: JSON.stringify({ capacityId: requestedCapacity.id }),
            },
        ));
    }
    return {
        workspace: { id: workspace.id, displayName: workspace.displayName },
        capacityId: currentCapacity?.id || requestedCapacity.id,
        created,
    };
}

async function workspaceCheck(body: Body) {
    const token = value(body, 'fabricToken');
    const dataverseToken = value(body, 'dataverseToken');
    const environment = environmentUrl(value(body, 'environmentUrl'));
    const workspaceId = value(body, 'workspaceId');
    const workspace = await managedWorkspace(token, workspaceId);
    const items = (await fabricCollection(
        `/workspaces/${encodeURIComponent(workspaceId)}/items`,
        token,
    )) as Array<{ id: string; type: string; displayName: string }>;
    const lakehouseItems = items.filter((item) => item.type === 'Lakehouse');
    const linkedLakehouses: Array<{
        id: string;
        displayName: string;
        shortcuts: DataverseShortcut[];
    }> = [];
    for (const lakehouse of lakehouseItems) {
        const shortcutPath =
            `/workspaces/${encodeURIComponent(workspaceId)}` +
            `/items/${encodeURIComponent(lakehouse.id)}/shortcuts`;
        const shortcuts = await fabricCollection(shortcutPath, token) as DataverseShortcut[];
        const environmentShortcuts = shortcuts.filter(
            (shortcut) =>
                shortcut.target?.type === 'Dataverse' &&
                normalizedEnvironment(shortcut.target.dataverse?.environmentDomain) ===
                    environment.toLowerCase(),
        );
        if (environmentShortcuts.length) {
            linkedLakehouses.push({ ...lakehouse, shortcuts: environmentShortcuts });
        }
    }
    if (linkedLakehouses.length > 1) {
        throw new Error('Multiple Dataverse-linked Lakehouses exist in the installer workspace.');
    }
    const linkState = await dataverseFabricLinkState(environment, dataverseToken);
    const journeyShortcuts = linkedLakehouses.length === 1
        ? inspectJourneyShortcuts(linkedLakehouses[0].shortcuts)
        : { ready: false, missing: [...JOURNEY_SHORTCUT_TABLES] };
    const shortcutTables = new Set(
        (linkedLakehouses[0]?.shortcuts || []).map(
            (shortcut) =>
                (shortcut.target?.dataverse?.tableName || shortcut.name).toLowerCase(),
        ),
    );
    const missingLinkTables = REQUIRED_LINK_TABLES.filter(
        (table) => !shortcutTables.has(table.toLowerCase()),
    );
    return {
        workspace: { id: workspace.id, displayName: workspace.displayName },
        lakehouses: linkedLakehouses.map(({ id, displayName }) => ({ id, displayName })),
        dataverseLinkHydrated: linkState.hydrated,
        dataverseLinkSyncState: linkState.syncState,
        dataverseTablesReady: missingLinkTables.length === 0,
        missingLinkTables,
        journeyShortcutsReady: journeyShortcuts.ready,
        missingJourneyShortcuts: journeyShortcuts.missing,
    };
}

async function dataverseFabricLinkState(environment: string, token: string) {
    const result = await apiGet(
        `${environment}/api/data/v9.2/synapselinkprofiles` +
        '?$select=name,extendedproperties&$filter=statecode eq 0',
        token,
    ) as { value?: Array<{ name?: string; extendedproperties?: string }> };
    const profile = (result.value || []).find((item) => {
        if (!item.extendedproperties) return false;
        const properties = JSON.parse(item.extendedproperties) as { LinkedToFabric?: boolean };
        return properties.LinkedToFabric === true;
    });
    if (!profile?.extendedproperties) {
        return { hydrated: false, syncState: 'Fabric link not found' };
    }
    const properties = JSON.parse(profile.extendedproperties) as {
        DestinationInitialSyncState?: string;
        CDS3?: { IsCDS3Hydrated?: boolean };
    };
    return {
        hydrated:
            properties.DestinationInitialSyncState === 'Completed' &&
            properties.CDS3?.IsCDS3Hydrated === true,
        syncState: properties.DestinationInitialSyncState || 'Unknown',
    };
}

function isJourneyFolder(folder: string) {
    return /\/(?:CustomerInsightsJourneys(?:\/deltalake)?|msdynmkt_cio_analytics_v\d+)\/?$/i.test(folder);
}

function inspectJourneyShortcuts(shortcuts: DataverseShortcut[]) {
    const missing: string[] = [];
    for (const tableName of JOURNEY_SHORTCUT_TABLES) {
        const existing = shortcuts.find(
            (shortcut) => shortcut.name.toLowerCase() === tableName.toLowerCase(),
        );
        if (!existing) {
            missing.push(tableName);
            continue;
        }
        const target = existing.target?.dataverse;
        if (
            target?.tableName?.toLowerCase() !== tableName.toLowerCase() ||
            !isJourneyFolder(target.deltaLakeFolder || '')
        ) {
            throw new Error(`${tableName} already exists and is not the expected Journey shortcut.`);
        }
    }
    return {
        ready: !missing.some((table) => REQUIRED_JOURNEY_SHORTCUT_TABLES.includes(table)),
        missing,
    };
}

function uuid(input: string, name: string): string {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(input)) {
        throw new Error(`${name} must be a valid UUID.`);
    }
    return input;
}

async function azureSubscriptions(body: Body) {
    const token = value(body, 'azureToken');
    const result = await apiGet(
        'https://management.azure.com/subscriptions?api-version=2022-12-01',
        token,
    ) as { value?: Array<{ subscriptionId: string; displayName: string; state: string }> };
    return {
        subscriptions: (result.value || [])
            .filter((item) => item.state === 'Enabled')
            .map(({ subscriptionId, displayName }) => ({ id: subscriptionId, displayName })),
    };
}

async function azureResourceGroups(body: Body) {
    const token = value(body, 'azureToken');
    const subscriptionId = uuid(value(body, 'subscriptionId'), 'subscriptionId');
    const result = await apiGet(
        `https://management.azure.com/subscriptions/${subscriptionId}/resourcegroups?api-version=2021-04-01`,
        token,
    ) as { value?: Array<{ name: string; location: string }> };
    return { resourceGroups: (result.value || []).map(({ name, location }) => ({ name, location })) };
}

async function azureQuota(body: Body) {
    const token = value(body, 'azureToken');
    const subscriptionId = uuid(value(body, 'subscriptionId'), 'subscriptionId');
    const location = value(body, 'location').toLowerCase().replace(/\s+/g, '');
    if (!/^[a-z0-9]{3,30}$/.test(location)) throw new Error('Invalid Azure region.');
    const result = await apiGet(
        `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.Fabric/locations/${location}/usages?api-version=2023-11-01`,
        token,
    ) as { value?: Array<{ currentValue?: number; limit?: number }> };
    const usage = result.value?.[0];
    const current = usage?.currentValue || 0;
    const limit = usage?.limit || 0;
    return { current, limit, available: Math.max(limit - current, 0), location };
}

function capacityInputs(body: Body) {
    const subscriptionId = uuid(value(body, 'subscriptionId'), 'subscriptionId');
    const resourceGroup = value(body, 'resourceGroup');
    const capacityName = value(body, 'capacityName').toLowerCase();
    const location = value(body, 'location').toLowerCase().replace(/\s+/g, '');
    const adminEmail = value(body, 'adminEmail');
    if (!/^[\w().-]{1,90}$/.test(resourceGroup)) throw new Error('Invalid Azure resource group name.');
    if (!/^[a-z][a-z0-9]{2,62}$/.test(capacityName)) {
        throw new Error('Capacity name must be 3-63 lowercase letters or numbers and start with a letter.');
    }
    if (!/^[a-z0-9]{3,30}$/.test(location)) throw new Error('Invalid Azure region.');
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(adminEmail)) throw new Error('Invalid capacity administrator email.');
    return { subscriptionId, resourceGroup, capacityName, location, adminEmail };
}

function capacityUrl(inputs: ReturnType<typeof capacityInputs>) {
    return `https://management.azure.com/subscriptions/${inputs.subscriptionId}/resourceGroups/${encodeURIComponent(inputs.resourceGroup)}/providers/Microsoft.Fabric/capacities/${inputs.capacityName}?api-version=2023-11-01`;
}

function providerUrl(subscriptionId: string, register = false) {
    return `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.Fabric${register ? '/register' : ''}?api-version=2021-04-01`;
}

async function fabricProviderState(token: string, subscriptionId: string) {
    const provider = await apiGet(providerUrl(subscriptionId), token) as {
        registrationState?: string;
    };
    return provider.registrationState || 'NotRegistered';
}

async function createAzureCapacity(body: Body) {
    if (body.confirmedCost !== true) throw new Error('Azure billing confirmation is required.');
    const token = value(body, 'azureToken');
    const inputs = capacityInputs(body);
    const providerState = await fabricProviderState(token, inputs.subscriptionId);
    if (providerState !== 'Registered') {
        await responseJson(await fetch(providerUrl(inputs.subscriptionId, true), {
            method: 'POST',
            headers: bearerHeaders(token),
        }));
        return { status: 'registering', state: 'Registering Microsoft.Fabric' };
    }
    const response = await fetch(capacityUrl(inputs), {
        method: 'PUT',
        headers: bearerHeaders(token, true),
        body: JSON.stringify({
            location: inputs.location,
            sku: { name: 'F2', tier: 'Fabric' },
            properties: { administration: { members: [inputs.adminEmail] } },
            tags: { managedBy: 'CustomerInsightsJourneyInstaller' },
        }),
    });
    const capacity = await responseJson(response) as {
        properties?: { provisioningState?: string; state?: string };
    };
    return {
        status: capacity.properties?.provisioningState === 'Succeeded' ? 'succeeded' : 'running',
        state: capacity.properties?.state || 'Provisioning',
    };
}

async function azureProviderStatus(body: Body) {
    const token = value(body, 'azureToken');
    const subscriptionId = uuid(value(body, 'subscriptionId'), 'subscriptionId');
    const state = await fabricProviderState(token, subscriptionId);
    return { status: state === 'Registered' ? 'succeeded' : 'running', state };
}

async function azureCapacityStatus(body: Body) {
    const token = value(body, 'azureToken');
    const capacity = await apiGet(capacityUrl(capacityInputs(body)), token) as {
        properties?: { provisioningState?: string; state?: string };
    };
    const provisioning = capacity.properties?.provisioningState || '';
    return {
        status: provisioning === 'Succeeded' ? 'succeeded' : provisioning === 'Failed' ? 'failed' : 'running',
        state: capacity.properties?.state || provisioning || 'Provisioning',
    };
}

async function startDataverseImport(body: Body) {
    const environment = environmentUrl(value(body, 'environmentUrl'));
    const token = value(body, 'dataverseToken');
    const importJobId = crypto.randomUUID();
    const response = await fetch(
        `${environment}/api/data/v9.2/ImportSolutionAsync`,
        {
            method: 'POST',
            headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
                Accept: 'application/json',
            },
            body: JSON.stringify({
                OverwriteUnmanagedCustomizations: false,
                PublishWorkflows: true,
                CustomizationFile: installerPackage.solution,
                ImportJobId: importJobId,
                SkipProductUpdateDependencies: false,
                HoldingSolution: false,
            }),
        },
    );
    const result = (await responseJson(response)) as {
        AsyncOperationId?: string;
        ImportJobKey?: string;
    };
    return {
        asyncOperationId: result.AsyncOperationId,
        importJobId,
        importJobKey: result.ImportJobKey,
    };
}

async function dataverseImportStatus(body: Body) {
    const environment = environmentUrl(value(body, 'environmentUrl'));
    const token = value(body, 'dataverseToken');
    const id = value(body, 'asyncOperationId');
    const operation = (await apiGet(
        `${environment}/api/data/v9.2/asyncoperations(${encodeURIComponent(id)})?$select=statecode,statuscode,message,friendlymessage`,
        token,
    )) as {
        statecode: number;
        statuscode: number;
        message?: string;
        friendlymessage?: string;
    };
    return {
        status:
            operation.statecode !== 3
                ? 'running'
                : operation.statuscode === 30
                  ? 'succeeded'
                  : 'failed',
        message: operation.friendlymessage || operation.message || '',
    };
}

function replaceAll(text: string, replacements: Record<string, string>) {
    let result = text;
    for (const [from, to] of Object.entries(replacements)) {
        result = result.split(from).join(to);
    }
    return result;
}

function definition(
    item: PackageItem,
    replacements: Record<string, string>,
    workspaceName: string,
    semanticId?: string,
) {
    return {
        parts: item.files.map((file) => {
            let path = replaceAll(file.path, replacements);
            let text = Buffer.from(file.content, 'base64').toString('utf8');
            text = replaceAll(text, replacements);
            if (path === '.platform') {
                const platform = JSON.parse(text) as {
                    config: { logicalId: string };
                };
                platform.config.logicalId = '00000000-0000-0000-0000-000000000000';
                text = JSON.stringify(platform);
            }
            if (path === 'definition.pbir' && semanticId) {
                const report = JSON.parse(text) as Record<string, unknown>;
                report.datasetReference = {
                    byConnection: {
                        connectionString:
                            `Data Source=powerbi://api.powerbi.com/v1.0/myorg/${workspaceName};` +
                            `initial catalog=Visão_MKT;integrated security=ClaimsToken;semanticmodelid=${semanticId}`,
                    },
                };
                text = JSON.stringify(report);
            }
            path = path.replaceAll('\\', '/');
            return {
                path,
                payload: Buffer.from(text, 'utf8').toString('base64'),
                payloadType: 'InlineBase64',
            };
        }),
    };
}

async function startFabricItem(body: Body) {
    const token = value(body, 'fabricToken');
    const workspaceId = value(body, 'workspaceId');
    const lakehouseId = value(body, 'lakehouseId');
    const readiness = await workspaceCheck(body);
    if (
        !readiness.dataverseTablesReady ||
        !readiness.journeyShortcutsReady ||
        !readiness.lakehouses.some((lakehouse) => lakehouse.id === lakehouseId)
    ) {
        throw new Error('Dataverse Link prerequisites changed. Refresh and verify them again.');
    }
    const workspaceName = readiness.workspace.displayName;
    const kind = value(body, 'kind');
    const item = installerPackage.items[kind];
    if (!item || !['semantic', 'report', 'agent'].includes(kind)) {
        throw new Error('Unsupported deployment item.');
    }

    const lakehouse = (await apiGet(
        `${FABRIC}/workspaces/${workspaceId}/lakehouses/${lakehouseId}`,
        token,
    )) as {
        id: string;
        displayName: string;
        properties: {
            sqlEndpointProperties: {
                connectionString: string;
                id: string;
                provisioningStatus: string;
            };
        };
    };
    if (lakehouse.properties.sqlEndpointProperties.provisioningStatus !== 'Success') {
        throw new Error('The Lakehouse SQL endpoint is not ready.');
    }

    const workspaceItems = (await fabricCollection(
        `/workspaces/${workspaceId}/items`,
        token,
    )) as Array<{
        id: string;
        type: string;
        displayName: string;
        description?: string;
    }>;
    const semantic = workspaceItems.find(
        (candidate) =>
            candidate.type === 'SemanticModel' && candidate.displayName === 'Visão_MKT',
    );
    if (kind === 'report' && !semantic) throw new Error('Deploy the semantic model first.');
    if (kind === 'agent' && !semantic) throw new Error('Deploy the semantic model first.');

    const replacements: Record<string, string> = {
        [`"workspaceId": "${SOURCE.workspace}"`]: `"workspaceId": "${workspaceId}"`,
        [SOURCE.lakehouseId]: lakehouse.id,
        [SOURCE.lakehouseName]: lakehouse.displayName,
        [SOURCE.server]: lakehouse.properties.sqlEndpointProperties.connectionString,
        [SOURCE.databaseId]: lakehouse.properties.sqlEndpointProperties.id,
        [SOURCE.semanticId]: semantic?.id || SOURCE.semanticId,
    };
    const itemDefinition = definition(
        item,
        replacements,
        workspaceName,
        kind === 'report' ? semantic?.id : undefined,
    );
    const matches = workspaceItems.filter(
        (candidate) =>
            candidate.type === item.type && candidate.displayName === item.displayName,
    );
    if (matches.length > 1) throw new Error(`Multiple ${item.displayName} items exist.`);

    let endpoint = `${FABRIC}/workspaces/${workspaceId}/items`;
    let requestBody: unknown = {
        displayName: item.displayName,
        description: MARKER,
        type: item.type,
        definition: itemDefinition,
    };
    if (matches.length === 1) {
        const existing = (await apiGet(
            `${FABRIC}/workspaces/${workspaceId}/items/${matches[0].id}`,
            token,
        )) as { description?: string };
        if (existing.description !== MARKER) {
            throw new Error(`${item.displayName} already exists and is not managed by this installer.`);
        }
        endpoint += `/${matches[0].id}/updateDefinition?updateMetadata=true`;
        requestBody = { definition: itemDefinition };
    }

    const response = await fetch(endpoint, {
        method: 'POST',
        headers: bearerHeaders(token, true),
        body: JSON.stringify(requestBody),
    });
    if (response.status === 202) {
        return {
            status: 'running',
            operationId: response.headers.get('x-ms-operation-id'),
        };
    }
    try {
        await responseJson(response);
    } catch (cause) {
        if (response.status === 403 && kind === 'agent' && cause instanceof Error) {
            throw new Error(`${cause.message} — A Fabric administrator must open Fabric Admin Portal > Tenant settings and enable the Fabric data agent and Copilot tenant settings.`);
        }
        throw cause;
    }
    return { status: 'succeeded' };
}

async function fabricOperationStatus(body: Body) {
    const token = value(body, 'fabricToken');
    const operationId = value(body, 'operationId');
    const operation = (await apiGet(
        `${FABRIC}/operations/${encodeURIComponent(operationId)}`,
        token,
    )) as {
        status: string;
        error?: {
            errorCode?: string;
            message?: string;
            moreDetails?: Array<{ errorCode?: string; message?: string }>;
        };
    };
    const details = operation.error?.moreDetails
        ?.map((detail) => [detail.errorCode, detail.message].filter(Boolean).join(': '))
        .filter(Boolean)
        .join(' — ');
    return {
        status: operation.status.toLowerCase(),
        message: [
            operation.error?.errorCode,
            operation.error?.message,
            details,
        ].filter(Boolean).join(' — '),
    };
}

function handle(handler: (body: Body) => Promise<unknown>) {
    return async ({ body }: { body: unknown }) => {
        try {
            return json(await handler((body || {}) as Body));
        } catch (cause) {
            return error(cause instanceof Error ? cause.message : 'Unexpected error.', 400);
        }
    };
}

export const handler = router({
    'GET /api/_healthcheck': [async () => json({ message: 'Success' })],
    'POST /api/auth/start': [handle(async (body) => deviceStart(
        value(body, 'resource'),
        typeof body.environmentUrl === 'string' ? body.environmentUrl : undefined,
    ))],
    'POST /api/auth/poll': [handle(async (body) => devicePoll(value(body, 'deviceCode')))],
    'POST /api/context': [handle(context)],
    'POST /api/capacities': [handle(capacities)],
    'POST /api/workspace/ensure': [handle(ensureWorkspace)],
    'POST /api/workspace': [handle(workspaceCheck)],
    'POST /api/azure/subscriptions': [handle(azureSubscriptions)],
    'POST /api/azure/resource-groups': [handle(azureResourceGroups)],
    'POST /api/azure/quota': [handle(azureQuota)],
    'POST /api/azure/capacity/start': [handle(createAzureCapacity)],
    'POST /api/azure/provider/status': [handle(azureProviderStatus)],
    'POST /api/azure/capacity/status': [handle(azureCapacityStatus)],
    'POST /api/install/dataverse/start': [handle(startDataverseImport)],
    'POST /api/install/dataverse/status': [handle(dataverseImportStatus)],
    'POST /api/install/fabric/start': [handle(startFabricItem)],
    'POST /api/install/fabric/status': [handle(fabricOperationStatus)],
});
