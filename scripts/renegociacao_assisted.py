"""Prepare assisted-demo metadata and fixtures in the existing FSI solution."""
import argparse
from datetime import date
from enum import IntEnum
import json
from pathlib import Path
import sys
from xml.etree import ElementTree as ET

from renegociacao_schema import Deployment, ORG, ROOT, SOLUTION, b
from renegociacao_seed import MI_CLIENT, choice, cpf, ensure, identifier

sys.path.insert(0, str(ROOT / "services" / "renegociacao"))
from renegociacao import assisted
import forms


class Decision(IntEnum):
    Pendente = assisted.PENDING
    Aprovado = assisted.APPROVED
    Recusado = assisted.REJECTED


def metadata(d):
    table = "fsi_renegociacao"
    fields = {
        "fsi_decisaoalcada": {
            "@odata.type": "Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            "SchemaName": "fsi_decisaoalcada", "DisplayName": b.L("Decisao de alcada"),
            "RequiredLevel": {"Value": "None"},
            "OptionSet": {"IsGlobal": False, "OptionSetType": "Picklist", "Options": [
                {"Value": int(value), "Label": b.L(value.name)} for value in Decision
            ]},
        },
        "fsi_pedidoalcada": {
            "@odata.type": "Microsoft.Dynamics.CRM.MemoAttributeMetadata",
            "SchemaName": "fsi_pedidoalcada", "DisplayName": b.L("Condicoes solicitadas (somente leitura)"),
            "RequiredLevel": {"Value": "None"}, "MaxLength": 10000,
        },
        "fsi_justificativaalcada": {
            "@odata.type": "Microsoft.Dynamics.CRM.MemoAttributeMetadata",
            "SchemaName": "fsi_justificativaalcada", "DisplayName": b.L("Justificativa da decisao"),
            "RequiredLevel": {"Value": "None"}, "MaxLength": 2000,
        },
        "fsi_emailaceitestatus": b.A("s", "fsi_emailaceitestatus", "Email de aceite", len=200),
    }
    existing = {c["LogicalName"] for c in d.client.tables.list_columns(table)}
    missing = {name: spec for name, spec in fields.items() if name not in existing}
    for spec in missing.values():
        d.call("POST", f"/EntityDefinitions(LogicalName='{table}')/Attributes", spec)
    b.call = d.call
    meta, attributes, labels = forms.meta(table)
    editable = {"fsi_decisaoalcada", "fsi_justificativaalcada"}
    spec = {"tabs": [{"name": "fsi_alcada", "label": "Portal Assistido - Alcada", "sections": [
        {"name": "contexto", "label": "Ocorrencia e cliente ficticio", "columns": 2,
         "fields": ["fsi_name", "fsi_clienteid", "fsi_casoid", "fsi_produtoclienteid"]},
        {"name": "pedido", "label": "Pedido do operador - termos imutaveis",
         "fields": ["fsi_pedidoalcada"]},
        {"name": "decisao", "label": "Decisao humana - nao representa aceite do cliente",
         "fields": ["fsi_decisaoalcada", "fsi_justificativaalcada"]},
        {"name": "aceite", "label": "Email e aceite final do cliente",
         "fields": ["fsi_emailaceitestatus", "fsi_status", "fsi_protocoloexterno"]},
    ]}], "header": ["fsi_decisaoalcada", "fsi_status"]}
    tree = ET.fromstring(forms.build_formxml(table, spec, attributes, labels))
    for control in tree.findall(".//control"):
        if control.get("datafieldname") not in editable:
            control.set("disabled", "true")
    name = "FSI - Alcada do Portal Assistido"
    matches = d.client.records.list("systemform", select=["formid", "formxml"],
                                   filter=f"objecttypecode eq '{table}' and name eq '{name}' and type eq 2", top=2)
    payload = {"name": name, "formxml": ET.tostring(tree, encoding="unicode")}
    if len(matches) > 1:
        raise RuntimeError("Duplicate assisted approval form")
    if matches:
        form_id = matches[0]["formid"]
        d.snapshot("assisted-approval-form-before.json", dict(matches[0]))
        d.client.records.update("systemform", form_id, payload)
    else:
        form_id = d.client.records.create("systemform", {
            **payload, "objecttypecode": table, "type": 2,
            "description": "Aprovacao humana de desconto e parcelas; aceite final somente pelo link enviado ao cliente.",
        })
    fetch, layout = forms.build_view(table, {
        "name": name, "columns": [("fsi_name", 220), ("fsi_clienteid", 200), ("fsi_decisaoalcada", 150),
                                  ("fsi_emailaceitestatus", 240), ("createdon", 160)],
        "filters": [("fsi_pedidoalcada", "not-null", "")], "order": ("createdon", "desc"),
    }, meta, attributes)
    existing_view = d.client.records.list("savedquery", select=["savedqueryid"],
                                         filter=f"returnedtypecode eq '{table}' and name eq '{name}'", top=2)
    if len(existing_view) > 1:
        raise RuntimeError("Duplicate assisted approval view")
    view_payload = {"name": name, "fetchxml": fetch, "layoutxml": layout}
    if existing_view:
        view_id = existing_view[0]["savedqueryid"]
        d.client.records.update("savedquery", view_id, view_payload)
    else:
        view_id = d.client.records.create("savedquery", {**view_payload, "returnedtypecode": table, "querytype": 0})
    app = d.first("appmodule", ["appmoduleid"], "uniquename eq 'fsi_FSICustomerInsights'")
    d.call("POST", "/AddAppComponents", {"AppId": app["appmoduleid"], "Components": [
        {"@odata.type": "Microsoft.Dynamics.CRM.systemform", "formid": form_id},
        {"@odata.type": "Microsoft.Dynamics.CRM.savedquery", "savedqueryid": view_id},
    ]})
    d.call("POST", "/PublishXml", {
        "ParameterXml": "<importexportxml><entities><entity>fsi_renegociacao</entity></entities></importexportxml>",
    }, sol=False)
    print("APPROVAL_FORM", form_id, "VIEW", view_id)
    return form_id


def seed(d, previous, output):
    customer = assisted.CUSTOMER
    ensure(d, "contact", customer, {
        "firstname": "Cliente", "lastname": "Ficticio Portal Assistido",
        "emailaddress1": "assistido-demo@renegociacao.invalid", "fsi_cpf": cpf("800000004"),
    })
    saved = d.client.records.retrieve("contact", customer, select=[
        "firstname", "lastname", "emailaddress1", "fsi_authobjectid", "fsi_authissuer", "statecode",
    ])
    if (saved["firstname"] != "Cliente" or saved["lastname"] != "Ficticio Portal Assistido"
            or saved["emailaddress1"] != "assistido-demo@renegociacao.invalid"
            or saved["statecode"] != 0 or saved.get("fsi_authobjectid") or saved.get("fsi_authissuer")):
        raise RuntimeError("The assisted fixture contact is no longer isolated")
    line = d.first("fsi_linhanegocio", ["fsi_linhanegocioid"], "fsi_codigo eq 'CRE'")["fsi_linhanegocioid"]
    stage = d.first("fsi_etapajornada", ["fsi_etapajornadaid"], "fsi_name eq 'Retenção e risco'")["fsi_etapajornadaid"]
    segment = choice(d, "fsi_vinculonegocio", "fsi_segmento", "Varejo PF")
    today = date.today().isoformat()
    link = ensure(d, "fsi_vinculonegocio", identifier("assisted-link"), {
        "fsi_name": "ASSIST-DEMO - Credito ficticio", "fsi_chaveintegracao": "ASSIST-DEMO-" + customer,
        "fsi_statusvinculo": choice(d, "fsi_vinculonegocio", "fsi_statusvinculo", "Ativo"),
        "fsi_segmento": segment, "fsi_macrosegmento": choice(d, "fsi_vinculonegocio", "fsi_macrosegmento", "Varejo"),
        "fsi_emcobranca": True, "fsi_datainicio": today, "fsi_dataentradaetapa": today, "fsi_diasnaetapa": 0,
        "fsi_clienteid_contact@odata.bind": f"/contacts({customer})",
        "fsi_linhanegocioid@odata.bind": f"/fsi_linhanegocios({line})",
        "fsi_etapaatualid@odata.bind": f"/fsi_etapajornadas({stage})",
    })
    ensure(d, "fsi_movimentoetapa", identifier("assisted-movement"), {
        "fsi_name": "ASSIST-DEMO - Entrada em retencao", "fsi_dataentrada": today, "fsi_duracaodias": 0,
        "fsi_origem": choice(d, "fsi_movimentoetapa", "fsi_origem", "Regra"),
        "fsi_tipomovimento": choice(d, "fsi_movimentoetapa", "fsi_tipomovimento", "Avanço"),
        "fsi_versaocatalogo": "1.0", "fsi_vinculoid@odata.bind": f"/fsi_vinculonegocios({link})",
        "fsi_etapadestinoid@odata.bind": f"/fsi_etapajornadas({stage})",
    })
    fixtures = json.loads(previous.read_text(encoding="utf-8"))["fixtures"]
    for index, (contract_id, (number, anchor)) in enumerate(assisted.CONTRACTS.items(), 1):
        ensure(d, "fsi_produtocliente", anchor, {
            "fsi_name": "Portal Assistido - Financiamento ficticio " + str(index),
            "fsi_numero": number, "fsi_tipoproduto": choice(d, "fsi_produtocliente", "fsi_tipoproduto", "Crédito"),
            "fsi_sistemaorigem": choice(d, "fsi_produtocliente", "fsi_sistemaorigem", "Core Bancário"),
            "fsi_statusproduto": choice(d, "fsi_produtocliente", "fsi_statusproduto", "Ativo"),
            "fsi_segmento": segment, "fsi_iniciovigencia": today,
            "fsi_clienteprincipalid_contact@odata.bind": f"/contacts({customer})",
            "fsi_linhanegocioid@odata.bind": f"/fsi_linhanegocios({line})",
            "fsi_vinculoid@odata.bind": f"/fsi_vinculonegocios({link})",
            "fsi_productid@odata.bind": f"/products({identifier('catalog')})",
        })
        if not any(f["contractId"] == contract_id for f in fixtures):
            fixtures.append({"contactId": customer, "anchorId": anchor, "contractId": contract_id,
                             "number": number, "vehicle": "Veiculo ficticio assistido " + str(index),
                             "debtCents": 1_800_120 + index * 100_000, "eligible": True})
    output.write_text(json.dumps({"fixtures": fixtures, "managedIdentityClientId": MI_CLIENT,
                                 "managedIdentityName": "fsi-reneg-demo-runtime"}, indent=2), encoding="utf-8")
    print("Assisted fixtures ready; previous completed contracts were not changed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--existing-fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG or not args.apply:
        parser.error("Use --apply only for the approved CCaaS environment")
    import dnsfix
    dnsfix.install()
    deployment = Deployment(args.backup_dir)
    try:
        deployment.preflight()
        metadata(deployment)
        seed(deployment, args.existing_fixtures, args.output)
    finally:
        deployment.close()
