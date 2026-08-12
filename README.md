# Customer Insights Journey Extension

Community installer for a Microsoft Fabric marketing analytics solution built
on Dynamics 365 Customer Insights - Journeys and Dataverse.

**Live installer:**

https://customer-insights-journey-installer-gxmwv9.v2.appdeploy.ai/

## What it installs

- Managed Dataverse solution with journey cost fields
- Direct Lake semantic model
- Marketing ROI and engagement report
- Optional Fabric Data Agent with curated source instructions

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
