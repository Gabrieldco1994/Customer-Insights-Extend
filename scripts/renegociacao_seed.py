"""Create synthetic vehicle-financing anchors and an external-source fixture file."""
import argparse
import json
from datetime import date
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from renegociacao_schema import Deployment, ORG

CONTACT = "ad883c81-2ccf-f111-a05c-000d3a343144"
OBJECT = "4e9974ab-2251-4cfa-a98a-4be41612f6e8"
ISSUER = "https://login.microsoftonline.com/3a3c8be3-6a50-455f-a303-f9df0c768430/v2.0"
MI_CLIENT = "dcb909a8-b7e5-4f62-bbbc-4b0daee3ddcc"


def identifier(name):
    return str(uuid5(NAMESPACE_URL, "fsi-renegociacao-demo/" + name))


def cpf(base):
    digits = list(map(int, base))
    for size in (9, 10):
        value = sum(number * weight for number, weight in zip(digits, range(size + 1, 1, -1)))
        digits.append(max(0, 11 - value % 11) if value % 11 >= 2 else 0)
    return "".join(map(str, digits))


def ensure(deployment, table, key, body):
    current = deployment.client.records.retrieve(table, key)
    if current is not None:
        print("Existing demo record:", table, key)
        return key
    primary = deployment.client.tables.get(table)["primary_id_attribute"]
    deployment.client.records.create(table, {primary: key, **body})
    print("Created demo record:", table, key)
    return key


def choice(deployment, table, field, label):
    _, result, _ = deployment.call(
        "GET", f"/EntityDefinitions(LogicalName='{table}')/Attributes(LogicalName='{field}')/"
        "Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet,GlobalOptionSet",
        sol=False,
    )
    options = (result.get("GlobalOptionSet") or result.get("OptionSet") or {})["Options"]
    values = [
        option["Value"] for option in options
        if label in [text["Label"] for text in option["Label"]["LocalizedLabels"]]
    ]
    if len(values) != 1:
        raise RuntimeError(f"Expected one choice {table}.{field} with label {label}")
    return values[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG or not args.apply:
        parser.error("Explicit --apply and the approved CCaaS environment are required")
    import dnsfix
    dnsfix.install()
    deployment = Deployment(args.backup_dir)
    try:
        deployment.preflight()
        contact = deployment.client.records.retrieve(
            "contact", CONTACT,
            select=["fsi_cpf", "fsi_authobjectid", "fsi_authissuer"],
        )
        if contact is None:
            raise RuntimeError("The confirmed portal test contact no longer exists")
        for field, expected in [("fsi_authobjectid", OBJECT), ("fsi_authissuer", ISSUER)]:
            if contact.get(field) not in (None, "", expected):
                raise RuntimeError("Portal test identity mapping changed; refusing to overwrite")
        deployment.snapshot("portal-contact-before.json", dict(contact))
        update = {"fsi_authobjectid": OBJECT, "fsi_authissuer": ISSUER}
        if not contact.get("fsi_cpf"):
            update["fsi_cpf"] = cpf("800000001")
        deployment.client.records.update("contact", CONTACT, update)
        other = ensure(deployment, "contact", identifier("contact-b"), {
            "firstname": "Cliente", "lastname": "Ficticio B - Renegociacao",
            "emailaddress1": "cliente-b@renegociacao.invalid", "fsi_cpf": cpf("800000002"),
        })
        line = deployment.first(
            "fsi_linhanegocio", ["fsi_linhanegocioid"], "fsi_codigo eq 'CRE'"
        )["fsi_linhanegocioid"]
        stage = deployment.first(
            "fsi_etapajornada", ["fsi_etapajornadaid"], "fsi_name eq 'Retenção e risco'"
        )["fsi_etapajornadaid"]
        segment = choice(deployment, "fsi_vinculonegocio", "fsi_segmento", "Varejo PF")
        active = choice(deployment, "fsi_vinculonegocio", "fsi_statusvinculo", "Ativo")
        retail = choice(deployment, "fsi_vinculonegocio", "fsi_macrosegmento", "Varejo")
        product_type = choice(deployment, "fsi_produtocliente", "fsi_tipoproduto", "Crédito")
        product_active = choice(deployment, "fsi_produtocliente", "fsi_statusproduto", "Ativo")
        origin = choice(deployment, "fsi_produtocliente", "fsi_sistemaorigem", "Core Bancário")
        movement_origin = choice(deployment, "fsi_movimentoetapa", "fsi_origem", "Regra")
        movement_type = choice(deployment, "fsi_movimentoetapa", "fsi_tipomovimento", "Avanço")
        reference = deployment.first(
            "product", ["_defaultuomid_value", "_defaultuomscheduleid_value"],
            "name eq 'Crédito Pessoal Bradesco'",
        )
        product = ensure(deployment, "product", identifier("catalog"), {
            "name": "Financiamento de Veiculos - Demonstracao FSI",
            "productnumber": "FSI-REN-DEMO", "description": "Somente dados ficticios; sem operacao bancaria.",
            "quantitydecimal": 0,
            "defaultuomid@odata.bind": f"/uoms({reference['_defaultuomid_value']})",
            "defaultuomscheduleid@odata.bind":
                f"/uomschedules({reference['_defaultuomscheduleid_value']})",
        })
        fixtures = []
        definitions = [
            (CONTACT, "001", 1_800_125, True),
            (CONTACT, "002", 2_400_240, True),
            (CONTACT, "003", 3_000_345, False),
            (other, "004", 1_200_456, True),
        ]
        for customer, suffix, debt, eligible in definitions:
            link = ensure(deployment, "fsi_vinculonegocio", identifier("link/" + customer), {
                "fsi_name": "Credito veicular ficticio - Renegociacao",
                "fsi_chaveintegracao": "REN-DEMO-" + customer,
                "fsi_statusvinculo": active, "fsi_segmento": segment,
                "fsi_macrosegmento": retail, "fsi_emcobranca": True,
                "fsi_datainicio": date.today().isoformat(),
                "fsi_dataentradaetapa": date.today().isoformat(), "fsi_diasnaetapa": 0,
                "fsi_clienteid_contact@odata.bind": f"/contacts({customer})",
                "fsi_linhanegocioid@odata.bind": f"/fsi_linhanegocios({line})",
                "fsi_etapaatualid@odata.bind": f"/fsi_etapajornadas({stage})",
            })
            ensure(deployment, "fsi_movimentoetapa", identifier("movement/" + customer), {
                "fsi_name": "Entrada demonstrativa - Retencao e risco",
                "fsi_dataentrada": date.today().isoformat(), "fsi_duracaodias": 0,
                "fsi_origem": movement_origin, "fsi_tipomovimento": movement_type,
                "fsi_versaocatalogo": "1.0",
                "fsi_vinculoid@odata.bind": f"/fsi_vinculonegocios({link})",
                "fsi_etapadestinoid@odata.bind": f"/fsi_etapajornadas({stage})",
            })
            anchor = ensure(deployment, "fsi_produtocliente", identifier("anchor/" + suffix), {
                "fsi_name": "Financiamento veicular ficticio " + suffix,
                "fsi_numero": "REN-DEMO-" + suffix, "fsi_tipoproduto": product_type,
                "fsi_sistemaorigem": origin, "fsi_statusproduto": product_active,
                "fsi_segmento": segment, "fsi_iniciovigencia": date.today().isoformat(),
                "fsi_clienteprincipalid_contact@odata.bind": f"/contacts({customer})",
                "fsi_linhanegocioid@odata.bind": f"/fsi_linhanegocios({line})",
                "fsi_vinculoid@odata.bind": f"/fsi_vinculonegocios({link})",
                "fsi_productid@odata.bind": f"/products({product})",
            })
            fixtures.append({
                "contactId": customer, "anchorId": anchor,
                "contractId": identifier("contract/" + suffix), "number": "REN-DEMO-" + suffix,
                "vehicle": "Veiculo ficticio " + suffix, "debtCents": debt, "eligible": eligible,
            })
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({
            "fixtures": fixtures, "managedIdentityName": "fsi-reneg-demo-runtime",
            "managedIdentityClientId": MI_CLIENT,
        }, indent=2), encoding="utf-8")
        print("External source fixtures prepared:", len(fixtures), args.output)
    finally:
        deployment.close()


if __name__ == "__main__":
    main()
