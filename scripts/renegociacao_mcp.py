"""Probe the protected native MCP endpoint or create its encrypted connection."""
import argparse
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import requests
from azure.identity import AzureCliCredential

ENVIRONMENT = "b3638821-b73d-e1f4-93d0-798d3720e915"
ROOT = Path(__file__).resolve().parents[1]
URL = "https://fsi-reneg-88957f9a.azurewebsites.net/runtime/webhooks/mcp"


def system_key():
    result = subprocess.run(
        ["az", "functionapp", "keys", "list", "-g", "rg-fsi-reneg-demo",
         "-n", "fsi-reneg-88957f9a", "-o", "json"],
        capture_output=True, text=True, check=True, timeout=60,
    )
    key = json.loads(result.stdout).get("systemKeys", {}).get("mcp_extension")
    if not key:
        raise RuntimeError("The native MCP extension key is not available")
    return key


def rpc(session, headers, method, params=None, request_id=1):
    response = session.post(URL, headers=headers, json={
        "jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {},
    }, timeout=90)
    if response.status_code != 200:
        raise RuntimeError(f"MCP {method} failed: HTTP {response.status_code}")
    session_id = response.headers.get("Mcp-Session-Id")
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    if response.headers.get("Content-Type", "").startswith("text/event-stream"):
        messages = [json.loads(line[5:].strip()) for line in response.text.splitlines()
                    if line.startswith("data:")]
        result = next((message for message in messages if message.get("id") == request_id), None)
    else:
        result = response.json()
    if not isinstance(result, dict) or result.get("error"):
        raise RuntimeError(f"MCP {method} returned a protocol error")
    return result["result"]


def probe():
    denied = requests.post(URL, json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, timeout=30)
    if denied.status_code != 401:
        raise RuntimeError("MCP access without its system key did not fail closed")
    headers = {"x-functions-key": system_key(), "Accept": "application/json, text/event-stream"}
    with requests.Session() as session:
        initialized = rpc(session, headers, "initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "fsi-acceptance", "version": "1.0"},
        })
        headers["MCP-Protocol-Version"] = initialized["protocolVersion"]
        ready = session.post(URL, headers=headers, json={
            "jsonrpc": "2.0", "method": "notifications/initialized",
        }, timeout=30)
        if ready.status_code not in (200, 202, 204):
            raise RuntimeError(f"MCP initialization notification failed: {ready.status_code}")
        tools = rpc(session, headers, "tools/list", request_id=2)["tools"]
        expected = {"listar_contratos_demo", "consultar_ofertas_demo", "consultar_andamento_demo"}
        if {tool["name"] for tool in tools} != expected:
            raise RuntimeError("MCP discovery does not match the three read-only tools")
        for tool in tools:
            if set(tool["inputSchema"].get("properties", {})) - {"contractId"}:
                raise RuntimeError("MCP exposes unexpected customer selection parameters")
        print("Protected MCP discovery:", sorted(expected))
        contracts = rpc(session, headers, "tools/call", {
            "name": "listar_contratos_demo", "arguments": {},
        }, request_id=3)
        content = json.loads(next(item["text"] for item in contracts["content"] if item["type"] == "text"))
        if content.get("simulationOnly") is not True:
            raise RuntimeError("MCP did not confirm the isolated dataset")
        print("MCP contracts:", json.dumps(content, ensure_ascii=False))
        offers = rpc(session, headers, "tools/call", {
            "name": "consultar_ofertas_demo",
            "arguments": {"contractId": "38d4b302-39ed-575a-948f-beced3fae3e0"},
        }, request_id=4)
        offers_data = json.loads(next(item["text"] for item in offers["content"] if item["type"] == "text"))
        if offers_data.get("simulationOnly") is not True or "offers" not in offers_data.get("data", {}):
            raise RuntimeError("MCP offers were not confirmed by the synthetic source")
        for offer in offers_data["data"]["offers"]:
            if not isinstance(offer.get("resumo"), str) or not offer["resumo"].startswith("Entrada: R$ "):
                raise RuntimeError("MCP offers do not contain the exact formatted source summary")
        print("MCP eligible offers:", len(offers_data["data"]["offers"]))
        denied = rpc(session, headers, "tools/call", {
            "name": "consultar_ofertas_demo",
            "arguments": {"contractId": "9fbf8f2a-2ebe-5186-b634-41f735b76e02"},
        }, request_id=5)
        denied_data = json.loads(next(item["text"] for item in denied["content"] if item["type"] == "text"))
        if denied_data.get("error", {}).get("code") != "DEMO_CONTRACT_DENIED" or "data" in denied_data:
            raise RuntimeError("MCP did not reject a contract outside the fixed simulation")
        print("MCP real-customer contract rejection: OK")


def connect(connector):
    destination = ROOT / "connectors/renegociacao-mcp/managed-connection.json"
    if destination.exists():
        raise RuntimeError("Connection metadata already exists; refusing a duplicate connection")
    key = system_key()
    token = AzureCliCredential().get_token("https://service.powerapps.com/.default").token
    connection = "fsi-reneg-mcp-" + uuid4().hex
    resource = f"/providers/Microsoft.PowerApps/apis/{connector}/connections/{connection}"
    url = ("https://api.powerapps.com" + resource + "?api-version=2016-11-01"
           "&$filter=environment%20eq%20%27" + ENVIRONMENT + "%27")
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = requests.put(url, headers=headers, json={"properties": {
        "displayName": "FSI Renegociacao MCP - somente dados ficticios",
        "environment": {"id": "/providers/Microsoft.PowerApps/environments/" + ENVIRONMENT,
                        "name": ENVIRONMENT},
        "connectionParameters": {"api_key": key},
    }}, timeout=90)
    if response.status_code not in (200, 201, 202):
        raise RuntimeError(f"MCP connection creation failed: HTTP {response.status_code}; credentials suppressed")
    checked = requests.get(url, headers=headers, timeout=60)
    if checked.status_code != 200:
        raise RuntimeError(f"MCP connection readback failed: HTTP {checked.status_code}")
    data = checked.json()
    statuses = [{"status": x.get("status")} for x in data.get("properties", {}).get("statuses", [])]
    safe = {"connectorId": connector, "connectionId": data.get("name", connection),
            "resourceId": data.get("id", resource), "environment": ENVIRONMENT, "statuses": statuses}
    destination.write_text(json.dumps(safe, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(safe))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["probe", "connect"])
    parser.add_argument("--connector-id")
    args = parser.parse_args()
    if args.command == "connect":
        if not args.connector_id:
            parser.error("--connector-id is required")
        connect(args.connector_id)
    else:
        probe()
