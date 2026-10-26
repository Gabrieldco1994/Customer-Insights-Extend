"""Configure the approved private endpoints without enabling public storage."""
import argparse
import json
import subprocess

SUBSCRIPTION = "61bf88ee-aab5-42d6-a457-97df75c9f47f"
GROUP = "rg-fsi-reneg-demo"
NETWORK = "vnet-fsi-reneg-demo"
STORAGE = "fsireneg88957f9a"
SERVER = "fsi-reneg-88957f9a"
BASE = f"/subscriptions/{SUBSCRIPTION}/resourceGroups/{GROUP}/providers"
NETWORK_ID = f"{BASE}/Microsoft.Network/virtualNetworks/{NETWORK}"


def az(*args):
    result = subprocess.run(
        ["az", *args, "--subscription", SUBSCRIPTION, "-o", "json"],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout) if result.stdout.strip() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    specs = [
        ("blob", "privatelink.blob.core.windows.net",
         f"{BASE}/Microsoft.Storage/storageAccounts/{STORAGE}", "blob"),
        ("queue", "privatelink.queue.core.windows.net",
         f"{BASE}/Microsoft.Storage/storageAccounts/{STORAGE}", "queue"),
        ("table", "privatelink.table.core.windows.net",
         f"{BASE}/Microsoft.Storage/storageAccounts/{STORAGE}", "table"),
        ("sql", "privatelink.database.windows.net",
         f"{BASE}/Microsoft.Sql/servers/{SERVER}", "sqlServer"),
    ]
    if not args.apply:
        print(json.dumps({"subscription": SUBSCRIPTION, "privateEndpoints": specs}, indent=2))
        return
    storage = az("storage", "account", "show", "-g", GROUP, "-n", STORAGE)
    if storage["publicNetworkAccess"] != "Disabled":
        raise RuntimeError("Expected storage public network access to remain Disabled")
    endpoints = {e["name"] for e in az("network", "private-endpoint", "list", "-g", GROUP)}
    zones = {e["name"] for e in az("network", "private-dns", "zone", "list", "-g", GROUP)}
    for kind, zone, target, group in specs:
        endpoint = f"pe-fsi-reneg-{kind}"
        if zone not in zones:
            az("network", "private-dns", "zone", "create", "-g", GROUP, "-n", zone)
        links = az("network", "private-dns", "link", "vnet", "list", "-g", GROUP, "-z", zone)
        if not any(link["name"] == "fsi-reneg" for link in links):
            az(
                "network", "private-dns", "link", "vnet", "create",
                "-g", GROUP, "-z", zone, "-n", "fsi-reneg",
                "--virtual-network", NETWORK_ID, "--registration-enabled", "false",
            )
        if endpoint not in endpoints:
            az(
                "network", "private-endpoint", "create", "-g", GROUP, "-n", endpoint,
                "--location", "brazilsouth", "--vnet-name", NETWORK,
                "--subnet", "private-endpoints", "--private-connection-resource-id", target,
                "--group-id", group, "--connection-name", f"conn-fsi-reneg-{kind}",
            )
        dns_groups = az(
            "network", "private-endpoint", "dns-zone-group", "list",
            "-g", GROUP, "--endpoint-name", endpoint,
        )
        if not any(item["name"] == "default" for item in dns_groups):
            az(
                "network", "private-endpoint", "dns-zone-group", "create",
                "-g", GROUP, "--endpoint-name", endpoint, "-n", "default",
                "--private-dns-zone", f"{BASE}/Microsoft.Network/privateDnsZones/{zone}",
                "--zone-name", kind,
            )
        result = az("network", "private-endpoint", "show", "-g", GROUP, "-n", endpoint)
        statuses = [
            item["privateLinkServiceConnectionState"]["status"]
            for item in result["privateLinkServiceConnections"]
        ]
        if not statuses or any(status != "Approved" for status in statuses):
            raise RuntimeError(f"Private endpoint {endpoint} is not approved: {statuses}")
        print(endpoint, result["provisioningState"], statuses, flush=True)
    az(
        "sql", "server", "update", "-g", GROUP, "-n", SERVER,
        "--enable-public-network", "false",
    )
    print("Storage and SQL public network access are disabled.", flush=True)


if __name__ == "__main__":
    main()
