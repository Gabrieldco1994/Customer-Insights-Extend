"""Project the private synthetic SQL source through the installed OData provider."""
import argparse
import json
import subprocess
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from renegociacao_schema import Deployment, ORG, SOLUTION, b

APP = "fsi-reneg-88957f9a"
ROOT = f"https://{APP}.azurewebsites.net/api/odata"
DATA_SOURCE = str(uuid5(NAMESPACE_URL, "fsi-renegociacao-demo/odata-source"))


def text(name, label, external, length=160):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
        "SchemaName": name, "DisplayName": b.L(label), "MaxLength": length,
        "RequiredLevel": {"Value": "None"}, "ExternalName": external,
    }


def integer(name, label, external):
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
        "SchemaName": name, "DisplayName": b.L(label), "ExternalName": external,
        "MinValue": 0, "MaxValue": 2147483647, "Format": "None",
    }


def table_spec(name, label, external, attributes):
    primary = text("fsi_name", "Nome", "Name", 200)
    primary["IsPrimaryName"] = True
    return {
        "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
        "SchemaName": name, "DisplayName": b.L(label),
        "DisplayCollectionName": b.L(label),
        "OwnershipType": "OrganizationOwned", "HasActivities": False, "HasNotes": False,
        "CanChangeTrackingBeEnabled": {"Value": False}, "ChangeTrackingEnabled": False,
        "IsAuditEnabled": {"Value": False},
        "CanCreateCharts": {"Value": False}, "IsEnabledForCharts": False,
        "IsDuplicateDetectionEnabled": {"Value": False},
        "IsValidForQueue": {"Value": False},
        "CanTriggerWorkflow": False, "IsBusinessProcessEnabled": False,
        "ExternalName": external[:-1], "ExternalCollectionName": external,
        "Attributes": [primary, *attributes],
    }


def specs():
    return [
        table_spec("fsi_contratofinanceirovirtual", "Contratos financeiros externos", "Contracts", [
            text("fsi_clienteexternoref", "Cliente (referência externa)", "ClientRef", 36),
            text("fsi_produtoexternoref", "Produto do cliente (referência)", "ProductRef", 36),
            text("fsi_numerocontrato", "Número do contrato", "Number", 80),
            text("fsi_veiculo", "Veículo fictício", "Vehicle", 160),
            integer("fsi_dividacentavos", "Dívida atual (centavos)", "DebtCents"),
            {"@odata.type": "Microsoft.Dynamics.CRM.BooleanAttributeMetadata",
             "SchemaName": "fsi_elegivel", "ExternalName": "Eligible",
             "DisplayName": b.L("Elegível"),
             "OptionSet": {"TrueOption": {"Value": 1, "Label": b.L("Sim")},
                           "FalseOption": {"Value": 0, "Label": b.L("Não")}}},
            {"@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
             "SchemaName": "fsi_dataposicao", "ExternalName": "PositionDate",
             "DisplayName": b.L("Data da posição"),
             "Format": "DateAndTime", "DateTimeBehavior": {"Value": "UserLocal"}},
        ]),
        table_spec("fsi_parcelafinanceiravirtual", "Parcelas financeiras externas", "Installments", [
            text("fsi_clienteexternoref", "Cliente (referência externa)", "ClientRef", 36),
            text("fsi_contratoexternoref", "Contrato (referência externa)", "ContractRef", 36),
            integer("fsi_numero", "Parcela", "Number"),
            integer("fsi_valorcentavos", "Valor (centavos)", "AmountCents"),
            text("fsi_situacao", "Situação", "Status", 32),
            {"@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
             "SchemaName": "fsi_vencimento", "ExternalName": "DueDate",
             "DisplayName": b.L("Vencimento"),
             "Format": "DateAndTime", "DateTimeBehavior": {"Value": "UserLocal"}},
        ]),
    ]


def az(*args):
    result = subprocess.run(["az", *args, "-o", "json"], check=True, capture_output=True, text=True)
    return json.loads(result.stdout) if result.stdout.strip() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG:
        parser.error("Only the approved environment is allowed")
    if not args.apply:
        print(json.dumps(specs(), ensure_ascii=False, indent=2))
        return
    import dnsfix
    dnsfix.install()
    deployment = Deployment(args.backup_dir)
    try:
        deployment.preflight()
        provider = deployment.first(
            "entitydataprovider", ["entitydataproviderid"], "name eq 'OData v4 Data Provider'"
        )["entitydataproviderid"]
        existing = deployment.client.records.retrieve("msdyn_odatav4ds", DATA_SOURCE)
        if existing is None:
            key_args = [
                "-g", "rg-fsi-reneg-demo", "-n", APP,
                "--function-name", "ExternalSourceOData",
            ]
            keys = az("functionapp", "function", "keys", "list", *key_args)
            key = keys.get("dataverse-virtual")
            if not key:
                az("functionapp", "function", "keys", "set", *key_args,
                   "--key-name", "dataverse-virtual")
                key = az("functionapp", "function", "keys", "list", *key_args).get("dataverse-virtual")
            if not key:
                raise RuntimeError("The protected OData function key was not created")
            deployment.client.records.create("msdyn_odatav4ds", {
                "msdyn_odatav4dsid": DATA_SOURCE,
                "msdyn_name": "FSI Renegociacao - fonte SQL ficticia",
                "msdyn_uri": ROOT + "/", "msdyn_paginationtype": 1,
                "msdyn_paginationmode": True, "msdyn_timeout": 30,
                "msdyn_parameter1name": "x-functions-key",
                "msdyn_parameter1value": key, "msdyn_isparameter1header": True,
                "msdyn_returninlinecount": False,
            })
        for body in specs():
            name = body["SchemaName"]
            metadata = deployment.client.tables.get(name)
            if metadata is None:
                deployment.call("POST", "/EntityDefinitions", {
                    **body, "DataProviderId": provider, "DataSourceId": DATA_SOURCE,
                })
                _, attribute, _ = deployment.call(
                    "GET", f"/EntityDefinitions(LogicalName='{name}')/"
                    f"Attributes(LogicalName='{name}id')", sol=False,
                )
                attribute["ExternalName"] = "Id"
                deployment.call(
                    "PUT", f"/EntityDefinitions(LogicalName='{name}')/"
                    f"Attributes(LogicalName='{name}id')", attribute,
                )
            deployment.add_table_component(name)
            print("Virtual projection prepared:", name, flush=True)
        deployment.call("POST", "/PublishXml", {
            "ParameterXml": "<importexportxml><entities>" + "".join(
                f"<entity>{s['SchemaName']}</entity>" for s in specs()
            ) + "</entities></importexportxml>",
        }, sol=False)
        print("External data stays in SQL; source credentials were not written to the repository.")
        print("No Power Pages table permissions or public Web API were enabled for these projections.")
    finally:
        deployment.close()


if __name__ == "__main__":
    main()
