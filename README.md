# Journey Cost Management and Marketing Data Agent

An accelerator that brings journey cost management, an ROI dashboard, and an
AI agent to Dynamics 365 Customer Insights - Journeys, running on Microsoft
Fabric.

**Live installer:**

https://customer-insights-journey-installer-gxmwv9.v2.appdeploy.ai/

## What it ships

- Solution with journey cost management and dashboard
- Fabric Lakehouse, Direct Lake semantic layer — relationships and cost/ROI
  measures ready to go
- Business ontology (curated source instructions) that gives the Marketing
  Data Agent context on journeys, costs, and metrics
- Marketing Data Agent — ask in natural language, get answers grounded in the
  real data
- Marketing ROI and engagement dashboard, ready to use

## Architecture

```
Dataverse (CI - Journeys)
   │
   ▼
Link to Fabric  ──▶  Lakehouse + shortcuts
                         │ (CIJ Entities + Dataverse Entities)
                         ▼
                 Marketing Ontology
             ┌───────────┴───────────┐
   Direct Lake Semantic Model   Marketing Data Agent
             │
             ▼
   ROI & Engagement Dashboard
```

The installer is rerunnable. It reuses compatible Fabric resources and updates
existing items instead of creating duplicates.

## Prerequisites

- Dynamics 365 Customer Insights - Journeys
- Dataverse System Administrator access
- An active Microsoft Fabric capacity in the Dataverse geography
- Link to Microsoft Fabric configured for the Dataverse environment
- Journey shortcuts for `EmailSent`, `EmailDelivered`, and `EmailOpened`
- Fabric Data Agent tenant support if the optional agent is required

## Installation

1. Open the [live installer](https://customer-insights-journey-installer-gxmwv9.v2.appdeploy.ai/).
2. Enter the Dataverse environment URL.
3. Complete the sequential Microsoft Fabric and Dataverse device-code sign-ins.
4. Install the managed Dataverse solution.
5. Follow the page instructions to configure Link to Fabric and Journey shortcuts.
6. Deploy the Fabric items.

The application uses delegated Microsoft permissions. Passwords and tokens are
not stored by the installer.

## Repository layout

| Path | Purpose |
| --- | --- |
| `MKTCI-DATA-AGENT/` | Fabric report, semantic model, Data Agent, and managed Dataverse solution |
| `hosted-installer/` | Source overlay deployed to AppDeploy |

After changing a Fabric definition or the managed solution, regenerate the
hosted payload:

```powershell
pwsh ./hosted-installer/generate-payload.ps1
```

## Important behavior

- Journey interaction tables are managed by Microsoft and cannot be populated
  safely by writing directly to OneLake.
- `CustomSent` and `CustomNotSent` remain optional until Customer Insights
  materializes their Delta tables.
- The portable semantic model exposes only fields used by its relationships,
  measures, and report visuals.
- Data Agent deployment is non-blocking because availability depends on tenant
  settings, capacity, and region.

## Security

Never commit `.appdeploy`, access tokens, tenant credentials, or generated
deployment logs. Review the delegated permissions shown during Microsoft
consent before continuing.

## License

[MIT](LICENSE)
