"""Initialize the approved synthetic source, always revoking temporary access."""
import argparse
import json
import subprocess

import requests

GROUP = "rg-fsi-reneg-demo"
APP = "fsi-reneg-88957f9a"
IDENTITY = "7e4e5b32-2744-4d80-93bf-da9a6f886274"
CLIENT = "dcb909a8-b7e5-4f62-bbbc-4b0daee3ddcc"
KEY = "bootstrap-once"


def az(*args):
    result = subprocess.run(
        ["az", *args, "-o", "json"], check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout) if result.stdout.strip() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.apply:
        parser.error("Explicit --apply is required for the authorized source bootstrap")
    with open(args.fixtures, encoding="utf-8") as source:
        fixtures = json.load(source)
    if fixtures.get("managedIdentityClientId") != CLIENT:
        raise RuntimeError("Fixtures do not target the approved managed identity")
    previous = az("sql", "server", "ad-admin", "list", "-g", GROUP, "--server-name", APP)
    if len(previous) != 1 or previous[0]["sid"] == IDENTITY:
        raise RuntimeError("Expected an existing, different SQL administrator to restore")
    key_args = ["-g", GROUP, "-n", APP, "--function-name", "BootstrapDemoSource"]
    try:
        az("sql", "server", "ad-admin", "create", "-g", GROUP, "--server-name", APP,
           "--display-name", "id-fsi-reneg-demo", "--object-id", IDENTITY)
        az("functionapp", "config", "appsettings", "set", "-g", GROUP, "-n", APP,
           "--settings", "BOOTSTRAP_ENABLED=1")
        az("functionapp", "function", "keys", "set", *key_args, "--key-name", KEY)
        key = az("functionapp", "function", "keys", "list", *key_args).get(KEY)
        if not key:
            raise RuntimeError("The bootstrap key was not stored")
        response = requests.post(
            f"https://{APP}.azurewebsites.net/api/bootstrap",
            json=fixtures, headers={"x-functions-key": key}, timeout=240,
        )
        print("Bootstrap HTTP:", response.status_code, flush=True)
        if not response.ok:
            raise RuntimeError(f"Source bootstrap failed: {response.text[:1000]}")
        data = response.json()
        if data.get("demonstration") is not True or data.get("data", {}).get("installmentRows", 0) < 2000:
            raise RuntimeError("Source bootstrap did not confirm at least 2000 synthetic installments")
        print(json.dumps(data, ensure_ascii=False), flush=True)
    finally:
        errors = []
        cleanup = [
            ["functionapp", "config", "appsettings", "set", "-g", GROUP, "-n", APP,
             "--settings", "BOOTSTRAP_ENABLED=0"],
            ["functionapp", "function", "keys", "delete", *key_args, "--key-name", KEY],
            ["sql", "server", "ad-admin", "create", "-g", GROUP, "--server-name", APP,
             "--display-name", previous[0]["login"], "--object-id", previous[0]["sid"]],
        ]
        for command in cleanup:
            try:
                az(*command)
            except subprocess.CalledProcessError as error:
                errors.append((command[:4], error.returncode))
        if errors:
            raise RuntimeError(f"Temporary privilege cleanup failed: {errors}")
        print("Bootstrap disabled, key revoked and previous SQL administrator restored.", flush=True)


if __name__ == "__main__":
    main()
