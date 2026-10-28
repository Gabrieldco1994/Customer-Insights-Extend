"""Power Automate emails the approved demo; no email is sent by the portal."""
import argparse
import json
from pathlib import Path
import subprocess

import requests
from azure.identity import AzureCliCredential

from renegociacao_schema import Deployment, ORG, ROOT, SOLUTION
from renegociacao_seed import ensure, identifier

ENV = "b3638821-b73d-e1f4-93d0-798d3720e915"
OUT = ROOT / "connectors" / "renegociacao-assistido"
FLOW_ID = identifier("assisted-approval-email-flow")
ADMIN = "32e53ec9-de28-f111-8341-6045bd0149e9"
MAIL_REFERENCE = "fsi_assisted_automation"
DATAVERSE_REFERENCE = "fsi_assisted_dataverse"


def connector_files():
    OUT.mkdir(parents=True, exist_ok=True)
    spec = {
        "swagger": "2.0", "info": {"title": "FSI Portal Assistido", "version": "1.0",
                                  "description": "Preparar email somente apos aprovacao humana CRM."},
        "host": "fsi-reneg-88957f9a.azurewebsites.net", "basePath": "/api/assisted-automation",
        "schemes": ["https"],
        "securityDefinitions": {"api_key": {"type": "apiKey", "name": "x-functions-key", "in": "header"}},
        "security": [{"api_key": []}],
        "paths": {"/prepare-mail": {"post": {
            "operationId": "PrepareAcceptanceMail", "summary": "Preparar aceite aprovado",
            "consumes": ["application/json"], "produces": ["application/json"],
            "parameters": [{"name": "body", "in": "body", "required": True, "schema": {
                "type": "object", "required": ["requestId"], "additionalProperties": False,
                "properties": {"requestId": {"type": "string"}},
            }}],
            "responses": {"200": {"description": "Email aprovado; manter entradas e saidas protegidas.",
                                   "schema": {"type": "object", "properties": {
                                       key: {"type": "string"} for key in
                                       ["emailId", "recipient", "subject", "requestId"]
                                   }}}, "default": {"description": "Falha explicita; nao enviar email."}},
        }}},
    }
    properties = {"properties": {"connectionParameters": {"api_key": {
        "type": "securestring", "uiDefinition": {
            "displayName": "Chave exclusiva da automacao assistida",
            "description": "Somente na conexao criptografada. Nunca no portal ou no fluxo.",
            "tooltip": "AssistedAutomation function key",
            "constraints": {"required": "true", "clearText": False},
        }}}, "iconBrandColor": "#9e1732", "publisher": "FSIInsights", "stackOwner": "FSIInsights"}}
    for name, value in [("apiDefinition.json", spec), ("apiProperties.json", properties)]:
        (OUT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def connect(connector):
    destination = OUT / "managed-connection.json"
    if destination.exists():
        return json.loads(destination.read_text())
    keys = json.loads(subprocess.run([
        "az", "functionapp", "function", "keys", "list", "-g", "rg-fsi-reneg-demo",
        "-n", "fsi-reneg-88957f9a", "--function-name", "AssistedAutomation", "-o", "json",
    ], check=True, capture_output=True, text=True).stdout)
    key = keys.get("power-automate-assisted")
    if not key:
        raise RuntimeError("The dedicated automation key is not available")
    from uuid import uuid4
    connection = "fsi-assisted-" + uuid4().hex
    resource = f"/providers/Microsoft.PowerApps/apis/{connector}/connections/{connection}"
    url = f"https://api.powerapps.com{resource}?api-version=2016-11-01&$filter=environment%20eq%20%27{ENV}%27"
    token = AzureCliCredential().get_token("https://service.powerapps.com/.default").token
    headers = {"Authorization": "Bearer " + token}
    created = requests.put(url, headers=headers, json={"properties": {
        "displayName": "FSI Portal Assistido - aceite aprovado",
        "environment": {"name": ENV, "id": "/providers/Microsoft.PowerApps/environments/" + ENV},
        "connectionParameters": {"api_key": key},
    }}, timeout=90)
    if created.status_code not in (200, 201, 202):
        raise RuntimeError(f"Automation connection HTTP {created.status_code}; credentials suppressed")
    saved = requests.get(url, headers=headers, timeout=60)
    if saved.status_code != 200:
        raise RuntimeError(f"Automation connection readback HTTP {saved.status_code}")
    safe = {"connectorId": connector, "connectionId": connection, "resourceId": resource, "environment": ENV}
    destination.write_text(json.dumps(safe, indent=2) + "\n", encoding="utf-8")
    return safe


def action(operation, parameters, after=None, secure=False, connector="shared_commondataserviceforapps"):
    value = {"type": "OpenApiConnection", "runAfter": after or {}, "inputs": {
        "host": {"connectionName": connector, "operationId": operation,
                 "apiId": "/providers/Microsoft.PowerApps/apis/" + connector},
        "parameters": parameters, "authentication": "@parameters('$authentication')",
    }}
    if secure:
        value["runtimeConfiguration"] = {"secureData": {"properties": ["inputs", "outputs"]}}
    return value


def definition(connector):
    email_id = "@body('Preparar_aceite')?['emailId']"
    actions = {
        "Preparar_aceite": action("PrepareAcceptanceMail", {
            "body/requestId": "@triggerOutputs()?['body/fsi_renegociacaoid']",
        }, secure=True, connector=connector),
        "Consultar_email": action("GetItem", {
            "entityName": "emails", "recordId": email_id, "$select": "activityid,statuscode",
        }, {"Preparar_aceite": ["Succeeded"]}),
        "Enviar_somente_rascunho": {
            "type": "If", "runAfter": {"Consultar_email": ["Succeeded"]},
            "expression": {"equals": ["@body('Consultar_email')?['statuscode']", 1]},
            "actions": {"Enviar_email": action("PerformBoundAction", {
                "entityName": "emails", "recordId": email_id,
                "actionName": "Microsoft.Dynamics.CRM.SendEmail",
                "item/IssueSend": True, "item/TrackingToken": "",
            })}, "else": {"actions": {}},
        },
        "Registrar_envio": action("UpdateRecord", {
            "entityName": "fsi_renegociacaos", "recordId": "@triggerOutputs()?['body/fsi_renegociacaoid']",
            "item/fsi_emailaceitestatus": "Solicitado pelo Power Automate - consultar atividade de email",
        }, {"Enviar_somente_rascunho": ["Succeeded"]}),
    }
    return {
        "$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
        "contentVersion": "1.0.0.0",
        "parameters": {"$connections": {"defaultValue": {}, "type": "Object"},
                       "$authentication": {"defaultValue": {}, "type": "SecureObject"}},
        "triggers": {"Decisao_aprovada_no_CRM": {
            "type": "OpenApiConnectionWebhook",
            "inputs": {"host": {"connectionName": "shared_commondataserviceforapps",
                                "operationId": "SubscribeWebhookTrigger",
                                "apiId": "/providers/Microsoft.PowerApps/apis/shared_commondataserviceforapps"},
                       "parameters": {
                           "subscriptionRequest/message": 3, "subscriptionRequest/entityname": "fsi_renegociacao",
                           "subscriptionRequest/scope": 4,
                           "subscriptionRequest/filteringattributes": "fsi_decisaoalcada",
                           "subscriptionRequest/filterexpression": "fsi_decisaoalcada eq 712400001 and fsi_pedidoalcada ne null",
                       }, "authentication": "@parameters('$authentication')"},
            "runtimeConfiguration": {"concurrency": {"runs": 1}},
        }},
        "actions": actions, "outputs": {},
    }


def deploy(d, managed):
    connector = managed["connectorId"]
    native_connector = d.first("connector", ["connectorid"], f"connectorinternalid eq '{connector}'")
    ref = d.client.records.list("connectionreference", select=["connectionreferenceid"],
                                filter=f"connectionreferencelogicalname eq '{MAIL_REFERENCE}'", top=2)
    ref_payload = {
        "connectionreferencelogicalname": MAIL_REFERENCE, "connectionreferencedisplayname": "FSI Portal Assistido - email",
        "connectorid": "/providers/Microsoft.PowerApps/apis/" + connector,
        "connectionid": managed["connectionId"],
        "CustomConnectorId@odata.bind": f"/connectors({native_connector['connectorid']})",
    }
    if not ref:
        d.call("POST", "/connectionreferences", ref_payload)
    elif len(ref) == 1:
        d.client.records.update("connectionreference", ref[0]["connectionreferenceid"], ref_payload)
    else:
        raise RuntimeError("Duplicate assisted automation references")
    dataverse_ref = d.client.records.list(
        "connectionreference", select=["connectionreferenceid"],
        filter=f"connectionreferencelogicalname eq '{DATAVERSE_REFERENCE}'", top=2,
    )
    if not dataverse_ref:
        token = d.credential.get_token("https://service.powerapps.com/.default").token
        url = ("https://api.powerapps.com/providers/Microsoft.PowerApps/connections"
               f"?api-version=2016-11-01&$filter=environment%20eq%20%27{ENV}%27")
        response = requests.get(url, headers={"Authorization": "Bearer " + token}, timeout=60)
        response.raise_for_status()
        connections = [r for r in response.json().get("value", [])
                       if r.get("properties", {}).get("apiId", "").endswith("/shared_commondataserviceforapps")
                       and any(s.get("status") == "Connected" for s in r["properties"].get("statuses", []))]
        if len(connections) != 1:
            raise RuntimeError("Select exactly one connected Dataverse connection for the approval flow")
        d.call("POST", "/connectionreferences", {
            "connectionreferencelogicalname": DATAVERSE_REFERENCE,
            "connectionreferencedisplayname": "FSI Portal Assistido - CRM e email",
            "connectorid": "/providers/Microsoft.PowerApps/apis/shared_commondataserviceforapps",
            "connectionid": connections[0]["name"],
        })
    elif len(dataverse_ref) > 1:
        raise RuntimeError("Duplicate assisted Dataverse references")
    refs = {
        "shared_commondataserviceforapps": {
            "runtimeSource": "embedded", "connection": {"connectionReferenceLogicalName": DATAVERSE_REFERENCE},
            "api": {"name": "shared_commondataserviceforapps"},
        },
        connector: {"runtimeSource": "embedded", "connection": {"connectionReferenceLogicalName": MAIL_REFERENCE},
                    "api": {"name": connector}},
    }
    flow = definition(connector)
    data = {"properties": {"connectionReferences": refs, "definition": flow, "templateName": ""},
            "schemaVersion": "1.0.0.0"}
    (ROOT / "deployment" / "renegociacao").mkdir(parents=True, exist_ok=True)
    (ROOT / "deployment" / "renegociacao" / "assisted-email-flow.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    existing = d.client.records.retrieve("workflow", FLOW_ID, select=["workflowid", "statecode", "clientdata"])
    payload = {"name": "FSI - Alcada aprovada envia aceite por email", "clientdata": json.dumps(data)}
    def matches(saved):
        properties = saved.get("properties", {})
        return (properties.get("definition") == flow
                and all(properties.get("connectionReferences", {}).get(name, {}).get("connection")
                        == value["connection"] for name, value in refs.items()))
    if existing:
        d.snapshot("assisted-email-flow-before.json", dict(existing))
        if not matches(json.loads(existing["clientdata"])):
            if existing["statecode"] == 1:
                d.client.records.update("workflow", FLOW_ID, {"statecode": 0, "statuscode": 1})
            d.client.records.update("workflow", FLOW_ID, payload)
    else:
        d.call("POST", "/workflows", {
            "workflowid": FLOW_ID, "category": 5, "type": 1, "scope": 4,
            "primaryentity": "none", **payload,
        })
    d.call("POST", "/AddSolutionComponent", {
        "ComponentId": FLOW_ID, "ComponentType": 29, "SolutionUniqueName": SOLUTION, "AddRequiredComponents": False,
    })
    state = d.client.records.retrieve("workflow", FLOW_ID, select=["statecode"])["statecode"]
    if state != 1:
        d.client.records.update("workflow", FLOW_ID, {"statecode": 1, "statuscode": 2})
    saved = d.client.records.retrieve("workflow", FLOW_ID, select=["statecode", "clientdata", "name"])
    saved_data = json.loads(saved["clientdata"])
    if saved["statecode"] != 1 or not matches(saved_data):
        raise RuntimeError("The email flow did not match its saved definition")
    print("POWER_AUTOMATE_FLOW", FLOW_ID, "Activated; runtime email delivery still requires acceptance verification.")


def approve_admin_mailbox(d):
    mailbox = "4de53ec9-de28-f111-8341-6045bd0149e9"
    fields = ["emailaddress", "emailrouteraccessapproval", "testemailconfigurationscheduled",
              "enabledforoutgoingemail", "outgoingemailstatus"]
    before = d.client.records.retrieve("mailbox", mailbox, select=fields)
    if not before or before["emailaddress"] != "admin@diax24492505.onmicrosoft.com":
        raise RuntimeError("The authorized admin mailbox no longer matches")
    d.snapshot("admin-mailbox-before-approved-test.json", dict(before))
    for field in ["emailrouteraccessapproval", "testemailconfigurationscheduled"]:
        _, metadata, _ = d.call("GET", f"/EntityDefinitions(LogicalName='mailbox')/Attributes(LogicalName='{field}')"
                               "?$select=IsValidForUpdate", sol=False)
        if metadata["IsValidForUpdate"] is not True:
            raise RuntimeError("Native mailbox scheduling is not writable")
    d.client.records.update("mailbox", mailbox, {
        "emailrouteraccessapproval": 1, "testemailconfigurationscheduled": True,
    })
    saved = d.client.records.retrieve("mailbox", mailbox, select=fields)
    if saved["emailrouteraccessapproval"] != 1:
        raise RuntimeError("Mailbox approval readback failed")
    print("Admin mailbox approved; native delivery test scheduled, not presumed successful.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["generate", "deploy"])
    parser.add_argument("--connector-id")
    parser.add_argument("--backup-dir", type=Path)
    parser.add_argument("--approve-admin-mailbox", action="store_true",
                        help="Requires explicit approval to approve/test only the fixed admin mailbox")
    args = parser.parse_args()
    connector_files()
    if args.phase == "deploy":
        if not args.connector_id or not args.backup_dir:
            parser.error("--connector-id and --backup-dir are required")
        managed = connect(args.connector_id)
        import dnsfix
        dnsfix.install()
        deployment = Deployment(args.backup_dir)
        try:
            deployment.preflight()
            if args.approve_admin_mailbox:
                approve_admin_mailbox(deployment)
            deploy(deployment, managed)
        finally:
            deployment.close()
