import { useMemo, useRef, useState } from 'react';
import { api } from '@appdeploy/client';
import {
    ArrowUpRight,
    Check,
    CircleAlert,
    Cloud,
    Database,
    ExternalLink,
    LoaderCircle,
    LockKeyhole,
    Rocket,
    ShieldCheck,
} from 'lucide-react';

type Prompt = {
    device_code: string;
    user_code: string;
    verification_uri: string;
    expires_in: number;
    interval: number;
    message: string;
};
type Workspace = { id: string; displayName: string };
type Lakehouse = { id: string; displayName: string };
type Capacity = { id: string; displayName: string; sku: string; region: string; state: string };
type Subscription = { id: string; displayName: string };
type ResourceGroup = { name: string; location: string };
type Quota = { current: number; limit: number; available: number; location: string };
type Context = {
    user: { name: string; email: string };
    systemAdministrator: boolean;
    customerInsightsInstalled: boolean;
    solutionInstalled: boolean;
    solutionVersion: string;
    capacities: Capacity[];
    workspaces: Workspace[];
};

const wait = (seconds: number) =>
    new Promise((resolve) => window.setTimeout(resolve, seconds * 1000));

const isQualifyingCapacity = (capacity: Capacity) =>
    /^(F|P)\d+$/i.test(capacity.sku) && capacity.state === 'Active';

const requiredLinkTables = [
    'contact',
    'lead',
    'opportunity',
    'msdynmkt_journey',
    'msdynmkt_email',
    'msdynmkt_segmentusage',
    'crfc2_journeychannelcost',
];

const journeyShortcutTables = [
    'EmailSent',
    'EmailDelivered',
    'EmailOpened',
    'CustomSent',
    'CustomNotSent',
];

function failureMessage(cause: unknown, fallback: string) {
    if (!cause || typeof cause !== 'object') return fallback;
    const failure = cause as {
        message?: string;
        response?: { status?: number; data?: unknown };
    };
    const data = failure.response?.data;
    if (typeof data === 'string' && data) return data;
    if (data && typeof data === 'object') {
        const detail = data as {
            error?: string | { message?: string };
            message?: string;
        };
        const apiMessage =
            typeof detail.error === 'string' ? detail.error : detail.error?.message;
        if (apiMessage || detail.message) return apiMessage || detail.message || fallback;
    }
    return failure.message || (
        failure.response?.status ? `${fallback} (HTTP ${failure.response.status})` : fallback
    );
}

function App() {
    const [environmentUrl, setEnvironmentUrl] = useState('');
    const [fabricToken, setFabricToken] = useState('');
    const [dataverseToken, setDataverseToken] = useState('');
    const [prompt, setPrompt] = useState<Prompt | null>(null);
    const [authResource, setAuthResource] = useState<'fabric' | 'dataverse' | 'azure'>('fabric');
    const [context, setContext] = useState<Context | null>(null);
    const [workspaceId, setWorkspaceId] = useState('');
    const [lakehouses, setLakehouses] = useState<Lakehouse[]>([]);
    const [lakehouseId, setLakehouseId] = useState('');
    const [linkTablesReady, setLinkTablesReady] = useState(false);
    const [journeyShortcutsReady, setJourneyShortcutsReady] = useState(false);
    const [linkMessage, setLinkMessage] = useState('');
    const [linkError, setLinkError] = useState('');
    const [azureToken, setAzureToken] = useState('');
    const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
    const [subscriptionId, setSubscriptionId] = useState('');
    const [resourceGroups, setResourceGroups] = useState<ResourceGroup[]>([]);
    const [resourceGroup, setResourceGroup] = useState('');
    const [capacityName, setCapacityName] = useState('cijourneyf2');
    const [location, setLocation] = useState('');
    const [quota, setQuota] = useState<Quota | null>(null);
    const [capacityAdminEmail, setCapacityAdminEmail] = useState('');
    const [costConfirmed, setCostConfirmed] = useState(false);
    const [capacityCreating, setCapacityCreating] = useState(false);
    const [busy, setBusy] = useState(false);
    const [status, setStatus] = useState('Enter your Dataverse environment to begin.');
    const [error, setError] = useState('');
    const [dataverseInstalled, setDataverseInstalled] = useState(false);
    const [installed, setInstalled] = useState(false);
    const cancelled = useRef(false);

    const workspace = useMemo(
        () => context?.workspaces.find((item) => item.id === workspaceId),
        [context, workspaceId],
    );
    const linkReady =
        dataverseInstalled && lakehouses.length > 0 && linkTablesReady && journeyShortcutsReady;
    const capacityReady = Boolean(context?.capacities.some(isQualifyingCapacity));
    const environmentHost = environmentUrl
        .replace(/^https?:\/\//i, '')
        .replace(/\/.*$/, '');
    const dataverseReady = Boolean(
        context?.customerInsightsInstalled &&
        context.systemAdministrator &&
        dataverseToken,
    );
    const fabricReady =
        Boolean(
            dataverseInstalled &&
                workspace &&
                lakehouseId &&
                linkTablesReady &&
                journeyShortcutsReady &&
                fabricToken,
        );

    async function authenticate(resource: 'fabric' | 'dataverse' | 'azure') {
        const label = resource === 'fabric' ? 'Microsoft Fabric' : resource === 'azure' ? 'Azure' : 'Dataverse';
        setAuthResource(resource);
        setStatus(`Waiting for ${label} sign-in…`);
        const start = await api.post('/api/auth/start', {
            resource,
            environmentUrl,
        });
        const authPrompt = start.data as Prompt;
        setPrompt(authPrompt);
        window.requestAnimationFrame(() =>
            document.getElementById(`${resource}-sign-in`)?.scrollIntoView({
                behavior: 'smooth',
                block: 'center',
            }),
        );
        const deadline = Date.now() + authPrompt.expires_in * 1000;
        while (Date.now() < deadline && !cancelled.current) {
            await wait(Math.max(authPrompt.interval, 2));
            const poll = await api.post('/api/auth/poll', {
                deviceCode: authPrompt.device_code,
            });
            if (poll.data.status === 'succeeded') {
                setPrompt(null);
                return poll.data.accessToken as string;
            }
        }
        throw new Error('Microsoft sign-in expired. Start again.');
    }

    async function connectAzure() {
        setBusy(true);
        setError('');
        cancelled.current = false;
        try {
            const token = await authenticate('azure');
            setAzureToken(token);
            setStatus('Checking Azure subscriptions…');
            const result = await api.post('/api/azure/subscriptions', { azureToken: token });
            const found = result.data.subscriptions as Subscription[];
            setSubscriptions(found);
            setStatus(found.length ? 'Select where to create the Fabric F2 capacity.' : 'No enabled Azure subscription is accessible to this account.');
        } catch (cause) {
            setError(failureMessage(cause, 'Azure access check failed.'));
        } finally {
            setBusy(false);
            setPrompt(null);
        }
    }

    async function loadResourceGroups(id: string) {
        setSubscriptionId(id);
        setResourceGroups([]);
        setResourceGroup('');
        setQuota(null);
        if (!id) return;
        setBusy(true);
        setError('');
        try {
            const result = await api.post('/api/azure/resource-groups', {
                azureToken,
                subscriptionId: id,
            });
            setResourceGroups(result.data.resourceGroups as ResourceGroup[]);
        } catch (cause) {
            setError(failureMessage(cause, 'Azure resource group check failed.'));
        } finally {
            setBusy(false);
        }
    }

    async function checkQuota(region = location) {
        setQuota(null);
        if (!subscriptionId || !region) return;
        setBusy(true);
        setError('');
        try {
            const result = await api.post('/api/azure/quota', {
                azureToken,
                subscriptionId,
                location: region,
            });
            setQuota(result.data as Quota);
        } catch (cause) {
            setError(failureMessage(cause, 'Fabric quota check failed.'));
        } finally {
            setBusy(false);
        }
    }

    async function createCapacity() {
        setBusy(true);
        setCapacityCreating(true);
        setError('');
        const request = {
            azureToken,
            subscriptionId,
            resourceGroup,
            capacityName,
            location,
            adminEmail: capacityAdminEmail,
            confirmedCost: costConfirmed,
        };
        try {
            setStatus('Creating the billable Fabric F2 capacity in Azure…');
            let start = await api.post('/api/azure/capacity/start', request);
            if (start.data.status === 'registering') {
                setStatus('Registering the Microsoft.Fabric provider in your Azure subscription…');
                for (let attempt = 0; attempt < 60; attempt += 1) {
                    await wait(5);
                    const provider = await api.post('/api/azure/provider/status', {
                        azureToken,
                        subscriptionId,
                    });
                    if (provider.data.status === 'succeeded') break;
                    if (attempt === 59) throw new Error('Microsoft.Fabric provider registration exceeded 5 minutes.');
                }
                setStatus('Provider registered. Creating the billable Fabric F2 capacity…');
                start = await api.post('/api/azure/capacity/start', request);
            }
            for (let attempt = 0; start.data.status !== 'succeeded' && attempt < 120; attempt += 1) {
                await wait(10);
                const result = await api.post('/api/azure/capacity/status', request);
                if (result.data.status === 'failed') throw new Error(`Azure capacity provisioning failed: ${result.data.state}`);
                if (result.data.status === 'succeeded') break;
                if (attempt === 119) throw new Error('Azure capacity provisioning exceeded 20 minutes.');
            }
            const result = await api.post('/api/capacities', { fabricToken });
            setContext((current) => current && ({ ...current, capacities: result.data.capacities as Capacity[] }));
            setStatus('Fabric F2 capacity created. It can take a few minutes to appear in Power Apps.');
        } catch (cause) {
            setError(failureMessage(cause, 'Fabric capacity creation failed.'));
        } finally {
            setBusy(false);
            setCapacityCreating(false);
        }
    }

    async function connect() {
        setBusy(true);
        setError('');
        setDataverseInstalled(false);
        setInstalled(false);
        cancelled.current = false;
        try {
            setStatus('Connecting to Microsoft Fabric…');
            const nextFabricToken = await authenticate('fabric');
            setFabricToken(nextFabricToken);
            setStatus('Connecting to Dataverse…');
            const nextDataverseToken = await authenticate('dataverse');
            setDataverseToken(nextDataverseToken);
            setStatus('Checking your environment…');
            const result = await api.post('/api/context', {
                environmentUrl,
                fabricToken: nextFabricToken,
                dataverseToken: nextDataverseToken,
            });
            let nextContext = result.data as Context;
            setDataverseInstalled(nextContext.solutionInstalled);
            const activeCapacity = nextContext.capacities.find(isQualifyingCapacity);
            if (activeCapacity) {
                setStatus('Preparing the CI-JOURNEY-Integration workspace…');
                const ensured = await api.post('/api/workspace/ensure', {
                    fabricToken: nextFabricToken,
                    capacityId: activeCapacity.id,
                });
                const ensuredWorkspace = ensured.data.workspace as Workspace;
                nextContext = {
                    ...nextContext,
                    workspaces: [
                        ensuredWorkspace,
                        ...nextContext.workspaces.filter((item) => item.id !== ensuredWorkspace.id),
                    ],
                };
                setWorkspaceId(ensuredWorkspace.id);
                await checkWorkspace(ensuredWorkspace.id, nextFabricToken, nextDataverseToken);
            }
            setContext(nextContext);
            setCapacityAdminEmail(nextContext.user.email);
            if (!activeCapacity && nextContext.workspaces.length === 1) {
                setWorkspaceId(nextContext.workspaces[0].id);
                await checkWorkspace(nextContext.workspaces[0].id, nextFabricToken, nextDataverseToken);
            }
            setStatus(
                nextContext.solutionInstalled
                    ? `Managed solution ${nextContext.solutionVersion} detected. Phase 1 was skipped.`
                    : 'Microsoft connection complete. Review the prerequisites.',
            );
        } catch (cause) {
            setError(failureMessage(cause, 'Connection failed.'));
            setStatus('Connection stopped.');
        } finally {
            setBusy(false);
            setPrompt(null);
        }
    }

    async function checkWorkspace(
        id: string,
        token = fabricToken,
        dvToken = dataverseToken,
    ) {
        setWorkspaceId(id);
        setLakehouses([]);
        setLakehouseId('');
        setLinkTablesReady(false);
        setJourneyShortcutsReady(false);
        setLinkMessage('Checking the Dataverse Link and Journey shortcuts…');
        setLinkError('');
        if (!id) return;
        setBusy(true);
        setError('');
        try {
            const result = await api.post('/api/workspace', {
                workspaceId: id,
                fabricToken: token,
                dataverseToken: dvToken,
                environmentUrl,
            });
            const found = result.data.lakehouses as Lakehouse[];
            const linkHydrated = Boolean(result.data.dataverseLinkHydrated);
            const tablesReady = Boolean(result.data.dataverseTablesReady);
            const missingTables = result.data.missingLinkTables as string[];
            const shortcutsReady = Boolean(result.data.journeyShortcutsReady);
            const missing = result.data.missingJourneyShortcuts as string[];
            setLakehouses(found);
            setLinkTablesReady(tablesReady);
            setJourneyShortcutsReady(shortcutsReady);
            if (found.length === 1) setLakehouseId(found[0].id);
            setStatus(
                found.length && !tablesReady
                    ? `Dataverse Link is missing required tables: ${missingTables.join(', ')}.`
                    : shortcutsReady
                    ? missing.length
                        ? `Required Journey shortcuts detected. Add when available: ${missing.join(', ')}.`
                        : 'Dataverse Link and Customer Insights Journeys shortcuts detected.'
                    : found.length && !linkHydrated
                    ? 'Dataverse Fabric Link found. Add the Journey shortcuts currently available.'
                    : found.length
                      ? `Dataverse Link detected. Add these Journey shortcuts: ${missing.join(', ')}.`
                      : 'No Dataverse-linked Lakehouse was found in this workspace.',
            );
            setLinkMessage(
                found.length && !tablesReady
                    ? `Link found. In Power Apps, add these Required Link tables: ${missingTables.join(', ')}.`
                    : shortcutsReady
                    ? missing.length
                        ? `Ready. ${missing.join(', ')} can be added later after those interaction types generate data.`
                        : 'Ready. All Customer Insights Journeys shortcuts use a dedicated Dataverse connection.'
                    : found.length && !linkHydrated
                    ? 'Link found. Microsoft reports hydration as incomplete, but you can continue after adding EmailSent, EmailDelivered, and EmailOpened.'
                    : found.length
                      ? `Link found. In the Lakehouse, add: ${missing.join(', ')}.`
                      : 'No Dataverse-linked Lakehouse was found in this workspace.',
            );
        } catch (cause) {
            const message = failureMessage(cause, 'Workspace check failed.');
            setError(message);
            setLinkError(message);
            setLinkMessage('');
        } finally {
            setBusy(false);
        }
    }

    async function pollDataverse(asyncOperationId: string) {
        for (let attempt = 0; attempt < 180; attempt += 1) {
            await wait(5);
            const result = await api.post('/api/install/dataverse/status', {
                environmentUrl,
                dataverseToken,
                asyncOperationId,
            });
            if (result.data.status === 'succeeded') return;
            if (result.data.status === 'failed') {
                throw new Error(result.data.message || 'Dataverse solution import failed.');
            }
        }
        throw new Error('Dataverse import exceeded 15 minutes.');
    }

    async function deployFabricItem(kind: string, label: string) {
        setStatus(`Deploying ${label}…`);
        const start = await api.post('/api/install/fabric/start', {
            kind,
            fabricToken,
            dataverseToken,
            environmentUrl,
            workspaceId,
            workspaceName: workspace?.displayName,
            lakehouseId,
        });
        if (start.data.status === 'succeeded') return;
        const operationId = start.data.operationId as string;
        for (let attempt = 0; attempt < 180; attempt += 1) {
            await wait(3);
            const result = await api.post('/api/install/fabric/status', {
                fabricToken,
                operationId,
            });
            if (result.data.status === 'succeeded') return;
            if (result.data.status === 'failed') {
                throw new Error(result.data.message || `${label} deployment failed.`);
            }
        }
        throw new Error(`${label} deployment exceeded 9 minutes.`);
    }

    async function installDataverse() {
        if (!dataverseReady || dataverseInstalled) return;
        setBusy(true);
        setError('');
        setDataverseInstalled(false);
        setInstalled(false);
        try {
            setStatus('Importing the managed Dataverse solution…');
            const importStart = await api.post('/api/install/dataverse/start', {
                environmentUrl,
                dataverseToken,
            });
            await pollDataverse(importStart.data.asyncOperationId as string);
            setDataverseInstalled(true);
            setStatus('Phase 1 complete. Create the Fabric link and select the required tables.');
        } catch (cause) {
            setError(failureMessage(cause, 'Dataverse solution installation failed.'));
            setStatus('Phase 1 stopped. It is safe to retry.');
        } finally {
            setBusy(false);
        }
    }

    async function installFabric() {
        if (!fabricReady) return;
        setBusy(true);
        setError('');
        setInstalled(false);
        try {
            await deployFabricItem('semantic', 'semantic model');
            await deployFabricItem('report', 'Marketing Report');
            setInstalled(true);
            try {
                await deployFabricItem('agent', 'Data Agent');
                setStatus('Phase 2 complete. Installation finished successfully.');
            } catch (cause) {
                setStatus(
                    `Semantic model and report deployed. Data Agent can be retried after its tenant settings are enabled: ${
                        failureMessage(cause, 'Data Agent deployment failed.')
                    }`,
                );
            }
        } catch (cause) {
            setError(failureMessage(cause, 'Fabric deployment failed.'));
            setStatus('Phase 2 stopped. Completed steps are safe to rerun.');
        } finally {
            setBusy(false);
        }
    }

    return (
        <main>
            <header className="hero">
                <div className="brand"><Cloud size={20} /> Microsoft solution installer</div>
                <h1>Customer Insights Journey</h1>
                <p>Install the Dataverse solution and every required Microsoft Fabric item from one guided page.</p>
                <div className="security"><LockKeyhole size={16} /> This installer does not persist passwords or tokens.</div>
            </header>

            <section className="prerequisites" aria-labelledby="prerequisites-title">
                <div className="prerequisiteTitle">
                    <ShieldCheck size={22} />
                    <div>
                        <h2 id="prerequisites-title">Before you begin</h2>
                        <p>Azure CLI is not required. An Azure subscription is needed only when creating a new Fabric capacity.</p>
                    </div>
                </div>
                <ul>
                    <li>A Microsoft work or school account in the target tenant.</li>
                    <li>Dataverse System Administrator access, or equivalent solution import and customization permissions.</li>
                    <li>A Power BI Premium license or active Fabric capacity in the same Azure geography as Dataverse.</li>
                    <li>Power BI workspace Administrator access; Capacity Administrator access if the link wizard creates the workspace.</li>
                    <li>Customer Insights - Journeys must be installed. This installer imports its managed solution before you create the Fabric link.</li>
                    <li>Fabric tenant settings must allow creating Fabric items, workspaces, and external OneLake access.</li>
                    <li>A Fabric administrator must configure the <a href="https://learn.microsoft.com/fabric/data-science/data-agent-tenant-settings" target="_blank" rel="noreferrer">Fabric data agent and Copilot tenant settings</a>.</li>
                    <li>Creating an F2 requires an Azure subscription, Contributor access to its resource group, provider <code>Microsoft.Fabric</code>, and acceptance of pay-as-you-go billing.</li>
                    <li>Tenant policies must allow delegated consent for Dataverse, Fabric, and optional Azure management permissions. Administrator approval may be required.</li>
                </ul>
                <p className="errorDisclosure">If Microsoft denies access, the validation panel shows the original HTTP status, error code/message, and Request ID when provided.</p>
            </section>

            <section className="panel identity">
                <div className="stepIcon"><span>1</span></div>
                <div className="content">
                    <div className="titleRow">
                        <div>
                            <h2>Connect your Microsoft environment</h2>
                            <p>Use a Dataverse and Fabric administrator account from the target tenant.</p>
                        </div>
                        {context && <span className="badge success"><Check size={14} /> {context.user.name}</span>}
                    </div>
                    <label>
                        Dataverse environment URL
                        <input
                            type="url"
                            value={environmentUrl}
                            onChange={(event) => setEnvironmentUrl(event.target.value)}
                            placeholder="https://yourorg.crm.dynamics.com"
                            disabled={busy}
                        />
                    </label>
                    <button className="primary" onClick={connect} disabled={busy || !environmentUrl}>
                        {busy && !context ? <LoaderCircle className="spin" size={18} /> : <LockKeyhole size={18} />}
                        Connect to Microsoft
                    </button>
                    <small>Sign in to Fabric first; the Dataverse code appears after Fabric sign-in completes.</small>
                    <DevicePrompt
                        prompt={prompt}
                        resource={authResource}
                        label={authResource === 'fabric'
                            ? 'Sign-in 1 of 2: Microsoft Fabric'
                            : authResource === 'dataverse'
                              ? 'Sign-in 2 of 2: Dataverse'
                              : 'Optional sign-in: Azure capacity management'}
                    />
                </div>
            </section>

            <section className="panel">
                <div className="stepIcon"><span>2</span></div>
                <div className="content">
                    <div className="titleRow">
                        <div>
                            <h2>Fabric or Power BI Premium capacity</h2>
                            <p>Link to Fabric needs an active qualifying capacity in the same geography as Dataverse.</p>
                        </div>
                        <RequirementStatus ready={context ? capacityReady : undefined} />
                    </div>
                    {Boolean(context?.capacities.length) && (
                        <div className="capacityList">
                            {context?.capacities.map((item) => (
                                <span key={item.id} className={`capacityItem ${isQualifyingCapacity(item) ? '' : 'inactive'}`}>
                                    <strong>{item.displayName}</strong> {item.sku} · {item.region} · {item.state}
                                    {!isQualifyingCapacity(item) && (
                                        <small>{item.state !== 'Active' ? 'Resume required' : 'Premium Per User is not a Fabric capacity'}</small>
                                    )}
                                </span>
                            ))}
                        </div>
                    )}
                    {context && !capacityReady && (
                        <div className="capacitySetup">
                            <p>No active qualifying capacity was detected. Resume an F/P capacity, start a trial, purchase capacity, or create an Azure F2 below.</p>
                            <p className="providerNote">If required, the installer registers the Microsoft.Fabric resource provider and waits for it before creating the capacity.</p>
                            {!azureToken ? (
                                <button className="primary" onClick={connectAzure} disabled={busy}>
                                    <Cloud size={18} /> Check Azure and create F2
                                </button>
                            ) : (
                                <>
                                    <div className="grid">
                                        <label>
                                            Azure subscription
                                            <select value={subscriptionId} onChange={(event) => loadResourceGroups(event.target.value)} disabled={busy}>
                                                <option value="">Select a subscription</option>
                                                {subscriptions.map((item) => <option key={item.id} value={item.id}>{item.displayName}</option>)}
                                            </select>
                                        </label>
                                        <label>
                                            Resource group
                                            <select value={resourceGroup} onChange={(event) => {
                                                const name = event.target.value;
                                                setResourceGroup(name);
                                                const group = resourceGroups.find((item) => item.name === name);
                                                if (group) {
                                                    setLocation(group.location);
                                                    void checkQuota(group.location);
                                                }
                                            }} disabled={!resourceGroups.length || busy}>
                                                <option value="">Select a resource group</option>
                                                {resourceGroups.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}
                                            </select>
                                        </label>
                                        <label>
                                            Azure region
                                            <input
                                                value={location}
                                                onChange={(event) => {
                                                    setLocation(event.target.value);
                                                    setQuota(null);
                                                }}
                                                onBlur={() => void checkQuota()}
                                                placeholder="eastus"
                                                disabled={busy}
                                            />
                                        </label>
                                        <label>
                                            Capacity name
                                            <input value={capacityName} onChange={(event) => setCapacityName(event.target.value)} disabled={busy} />
                                        </label>
                                        <label>
                                            Capacity administrator
                                            <input type="email" value={capacityAdminEmail} onChange={(event) => setCapacityAdminEmail(event.target.value)} disabled={busy} />
                                        </label>
                                    </div>
                                    {quota && (
                                        <div className={`quota ${quota.available >= 2 ? 'available' : 'unavailable'}`}>
                                            <strong>{quota.location}: {quota.available} CU available</strong>
                                            <span>{quota.current} used of {quota.limit}. F2 requires 2 CU.</span>
                                            {quota.available < 2 && <span>Choose a region in the Dataverse geography with quota, or request a quota increase.</span>}
                                        </div>
                                    )}
                                    <label className="confirmation">
                                        <input type="checkbox" checked={costConfirmed} onChange={(event) => setCostConfirmed(event.target.checked)} />
                                        I confirm that F2 is pay-as-you-go and billed by Azure while active.
                                    </label>
                                    <div className={`status ${error ? 'failed' : ''}`}>
                                        {error ? <CircleAlert size={18} /> : capacityCreating ? <LoaderCircle className="spin" size={18} /> : <Cloud size={18} />}
                                        <span>{error || status}</span>
                                    </div>
                                    <button className="primary" onClick={createCapacity} disabled={busy || !subscriptionId || !resourceGroup || !location || !capacityName || !capacityAdminEmail || !costConfirmed || !quota || quota.available < 2}>
                                        {capacityCreating ? <LoaderCircle className="spin" size={18} /> : <Cloud size={18} />}
                                        {capacityCreating ? 'Creating F2…' : 'Create billable F2 capacity'}
                                    </button>
                                </>
                            )}
                        </div>
                    )}
                    <div className="links">
                        <a href="https://app.fabric.microsoft.com" target="_blank" rel="noreferrer">Start a Fabric trial <ArrowUpRight size={15} /></a>
                        <a href="https://portal.azure.com/#view/HubsExtension/BrowseResource/resourceType/Microsoft.Fabric%2Fcapacities" target="_blank" rel="noreferrer">Resume a paused capacity <ArrowUpRight size={15} /></a>
                        <a href="https://azure.microsoft.com/pricing/details/microsoft-fabric/" target="_blank" rel="noreferrer">Fabric pricing <ArrowUpRight size={15} /></a>
                        <a href="https://portal.azure.com/#view/Microsoft_Azure_Capacity/QuotaMenuBlade/~/myQuotas" target="_blank" rel="noreferrer">View or request Fabric quota <ArrowUpRight size={15} /></a>
                        <a href="https://learn.microsoft.com/en-us/power-apps/maker/data-platform/fabric-link-to-data-platform" target="_blank" rel="noreferrer">Licensing prerequisites <ArrowUpRight size={15} /></a>
                    </div>
                </div>
            </section>

            <section className="panel">
                <div className="stepIcon"><span>3</span></div>
                <div className="content">
                    <div className="titleRow">
                        <div>
                            <h2>Customer Insights - Journeys and Dataverse access</h2>
                            <p>The Dynamics 365 app must be installed and the connected account must have the System Administrator role.</p>
                        </div>
                        <RequirementStatus ready={context ? context.customerInsightsInstalled && context.systemAdministrator : undefined} />
                    </div>
                    <div className="links">
                        <a href="https://admin.powerplatform.microsoft.com/environments" target="_blank" rel="noreferrer">
                            Open Power Platform admin center <ArrowUpRight size={15} />
                        </a>
                        <a href="https://learn.microsoft.com/en-us/dynamics365/customer-insights/journeys/setup" target="_blank" rel="noreferrer">
                            Installation instructions <ArrowUpRight size={15} />
                        </a>
                    </div>
                    {context && !context.systemAdministrator && (
                        <small className="footnote">The connected Dataverse account does not have the System Administrator role required by Microsoft for managed-lake shortcuts.</small>
                    )}
                </div>
            </section>

            <section className="panel">
                <div className="stepIcon"><span>4</span></div>
                <div className="content">
                    <div className="titleRow">
                        <div>
                            <h2>Phase 1 — Install the Dataverse solution</h2>
                            <p>Creates the custom table and columns before Link to Fabric lists its available tables.</p>
                        </div>
                        <RequirementStatus ready={dataverseInstalled || undefined} />
                    </div>
                    <div className={`status ${error ? 'failed' : dataverseInstalled ? 'complete' : ''}`}>
                        {error ? <CircleAlert size={18} /> : dataverseInstalled ? <Check size={18} /> : busy ? <LoaderCircle className="spin" size={18} /> : <Database size={18} />}
                        <span>{error || status}</span>
                    </div>
                    <button className="primary install" onClick={installDataverse} disabled={!dataverseReady || dataverseInstalled || busy}>
                        <Database size={18} /> {dataverseInstalled ? 'Solution already installed' : 'Install Dataverse solution'}
                    </button>
                    {!dataverseReady && <small className="footnote">Connect Microsoft, confirm Customer Insights - Journeys, and use a Dataverse System Administrator before Phase 1.</small>}
                </div>
            </section>

            <section className="panel">
                <div className="stepIcon"><span>5</span></div>
                <div className="content">
                    <div className="titleRow">
                        <div>
                            <h2>Phase 2 — Link Dataverse tables to Fabric</h2>
                            <p>Open Power Apps only after Phase 1. Select CI-JOURNEY-Integration and every table listed below.</p>
                        </div>
                        <RequirementStatus ready={linkReady} />
                    </div>
                    <p><strong>Required Link tables</strong></p>
                    <div className="capacityList">
                        {requiredLinkTables.map((table) => (
                            <span className="capacityItem" key={table}><code>{table}</code></span>
                        ))}
                    </div>
                    <small className="footnote">Select these CDS3 tables in Power Apps. Tables with Track changes enabled are selected by default.</small>
                    <p><strong>Add from the linked Lakehouse</strong></p>
                    <div className="capacityList">
                        {journeyShortcutTables.map((table) => (
                            <span className="capacityItem" key={table}><code>{table}</code></span>
                        ))}
                    </div>
                    <div className="manualSteps">
                        <strong>Manual step — exact instructions</strong>
                        <ol>
                            <li>In Power Apps, select the target environment, open <strong>Link data</strong>, create a Fabric link in <strong>CI-JOURNEY-Integration</strong>, select every Required Link table above, and finish the wizard.</li>
                            <li>Return here and click <strong>Refresh Link and verify Journey shortcuts</strong> so the Dataverse Lakehouse is detected.</li>
                            <li>Open that Lakehouse, choose <strong>Get data → New shortcut → Dataverse</strong>, then choose <strong>Create new connection</strong>.</li>
                            <li>Enter only <code>{environmentHost || 'your-org.crm.dynamics.com'}</code> — without <code>https://</code> or a trailing slash. Select <strong>Organizational account</strong> and sign in with a Dataverse System Administrator.</li>
                            <li>Do not reuse the connection created by Link to Fabric. Open <strong>Customer Insights Journeys</strong>, select the interaction tables currently available, and choose <strong>Create</strong>.</li>
                            <li>Return here and refresh again. <code>EmailSent</code>, <code>EmailDelivered</code>, and <code>EmailOpened</code> are required; the two Custom tables can be added later.</li>
                        </ol>
                        <small><strong>404 “environment not configured”:</strong> in Power Apps → Link data, select the Fabric link and choose <strong>Refresh Fabric links</strong>. Wait until its initial synchronization finishes.</small>
                        <small>A new tenant shows an interaction type only after it has generated data. If one is missing, run a test journey that produces it and allow up to three hours for Microsoft to publish it.</small>
                    </div>
                    <div className="grid">
                        <label>
                            Fabric workspace
                            <select
                                value={workspaceId}
                                onChange={(event) => checkWorkspace(event.target.value)}
                                disabled={!context || !dataverseInstalled || busy}
                            >
                                <option value="">Select a workspace</option>
                                {context?.workspaces.map((item) => (
                                    <option key={item.id} value={item.id}>{item.displayName}</option>
                                ))}
                            </select>
                        </label>
                        <label>
                            Dataverse Lakehouse
                            <select
                                value={lakehouseId}
                                onChange={(event) => setLakehouseId(event.target.value)}
                                disabled={!dataverseInstalled || !lakehouses.length || busy}
                            >
                                <option value="">Select a Lakehouse</option>
                                {lakehouses.map((item) => (
                                    <option key={item.id} value={item.id}>{item.displayName}</option>
                                ))}
                            </select>
                        </label>
                    </div>
                    {dataverseInstalled ? (
                        <>
                            <div className="links">
                                <a href="https://make.powerapps.com" target="_blank" rel="noreferrer">
                                    Open Power Apps and create the link <ArrowUpRight size={15} />
                                </a>
                                {lakehouseId && (
                                    <a href={`https://app.fabric.microsoft.com/groups/${workspaceId}/lakehouses/${lakehouseId}`} target="_blank" rel="noreferrer">
                                        Open Lakehouse and add Journey shortcuts <ArrowUpRight size={15} />
                                    </a>
                                )}
                                <a href="https://learn.microsoft.com/en-us/power-apps/maker/data-platform/fabric-link-to-data-platform" target="_blank" rel="noreferrer">
                                    Link to Fabric instructions <ArrowUpRight size={15} />
                                </a>
                            </div>
                            <button className="primary" onClick={() => checkWorkspace(workspaceId)} disabled={!workspaceId || busy}>
                                <Database size={18} /> Refresh Link and verify Journey shortcuts
                            </button>
                            {(linkMessage || linkError) && (
                                <div className={`status ${linkError ? 'failed' : linkReady ? 'complete' : ''}`}>
                                    {linkError ? <CircleAlert size={18} /> : linkReady ? <Check size={18} /> : <LoaderCircle className="spin" size={18} />}
                                    <span>{linkError || linkMessage}</span>
                                </div>
                            )}
                        </>
                    ) : <small className="footnote">Complete Phase 1 before creating the Fabric link.</small>}
                </div>
            </section>

            <section className="panel deploy">
                <div className="stepIcon"><span>6</span></div>
                <div className="content">
                    <h2>Complete Phase 2 — Deploy Fabric items</h2>
                    <p>This creates or updates the semantic model, report, and Lakehouse-backed Data Agent.</p>
                    <div className={`status ${error ? 'failed' : installed ? 'complete' : ''}`}>
                        {error ? <CircleAlert size={18} /> : installed ? <Check size={18} /> : busy ? <LoaderCircle className="spin" size={18} /> : <Database size={18} />}
                        <span>{error || status}</span>
                    </div>
                    <button className="primary install" onClick={installFabric} disabled={!fabricReady || busy}>
                        <Rocket size={18} /> Deploy Fabric items
                    </button>
                    {!fabricReady && <small className="footnote">Complete Phase 1, detect the Dataverse Lakehouse, and add the Journey shortcuts before deploying Fabric items.</small>}
                </div>
            </section>
        </main>
    );
}

function RequirementStatus({ ready }: { ready?: boolean }) {
    if (ready === undefined) return <span className="badge">Not checked</span>;
    return ready
        ? <span className="badge success"><Check size={14} /> Ready</span>
        : <span className="badge warning"><CircleAlert size={14} /> Action required</span>;
}

function DevicePrompt({
    prompt,
    resource,
    label,
}: {
    prompt: Prompt | null;
    resource: 'fabric' | 'dataverse' | 'azure';
    label: string;
}) {
    if (!prompt) return null;
    return (
        <div className="device" id={`${resource}-sign-in`}>
            <div>
                <small>{label}</small>
                <strong>{prompt.user_code}</strong>
                <small>The same Microsoft browser session is reused, but each service requires its own code.</small>
            </div>
            <a href={prompt.verification_uri} target="_blank" rel="noreferrer">
                Open Microsoft sign-in <ExternalLink size={15} />
            </a>
        </div>
    );
}

export default App;
