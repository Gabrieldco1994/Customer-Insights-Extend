# Hosted installer

Live app:
https://customer-insights-journey-installer-gxmwv9.v2.appdeploy.ai/

The installer uses the multi-tenant public Entra application
`ed676c8d-6fbb-43d0-a4f7-5fb228538ab9` and delegated Device Code authentication.
It requests:

- Microsoft Fabric: `Item.ReadWrite.All`, `Workspace.ReadWrite.All`, `Capacity.ReadWrite.All`, `OneLake.ReadWrite.All`
- Dataverse: `user_impersonation`
- Optional Azure capacity creation: Azure Service Management `user_impersonation`

The backend exchanges device codes for short-lived delegated tokens. The app
keeps tokens only in browser memory for the current installation and does not
persist them.

The optional Azure path creates an F2 pay-as-you-go capacity through the native
Azure Resource Manager REST API. It requires an enabled subscription,
Contributor access to the selected resource group, and explicit billing
confirmation. It does not create or modify capacity without that confirmation.

After Fabric authentication, the installer idempotently creates or reuses the
marked `CI-JOURNEY-Integration` workspace and assigns it to an active qualifying
capacity. Dataverse Link still creates the Lakehouse and shortcuts in Power Apps.
The flow imports the managed Dataverse solution first so its custom table and
columns are available in Link to Fabric. If the same managed solution version is
already installed, Phase 1 is marked complete without importing it again.
The second phase lists every table that must be selected, refreshes the resulting
Lakehouse, and deploys Fabric items. Customer Insights Journeys interaction
shortcuts must be created from the Lakehouse's **New shortcut > Dataverse**
experience with a dedicated organizational connection. The CDS connection made
by Link to Fabric doesn't authorize the Journeys managed lake. The installer
detects the linked Lakehouse by its Dataverse shortcuts rather than its generated
name, verifies the connected account has the Dataverse `System Administrator`
role, verifies all required interaction shortcuts, and rejects accidental CDS
connection reuse before enabling Fabric deployment. On new tenants, Microsoft
lists an interaction type only after it has generated data; run representative
test journeys and allow up to three hours before creating and verifying every
required shortcut. The installer requires every listed Dataverse Link table.
It also displays the active `synapselinkprofile` hydration state as diagnostic
information because Microsoft can report stale hydration metadata after the
required shortcuts are already available.

Run `generate-payload.ps1` after changing the managed Dataverse solution or any
Fabric definition, then redeploy `backend/payload.ts`.
