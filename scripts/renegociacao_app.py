"""Add renegotiation operations and the approved Pages route to the existing FSI app."""
import argparse
import base64
import json
from pathlib import Path
from xml.etree import ElementTree

from renegociacao_schema import Deployment, ORG, PROCESS, ROOT, b
import forms

PORTAL_TYPE = 712400003
PORTAL = "https://portalatendimento01.powerappsportals.com/renegociacao-financiamento/"
ENTITY = "fsi_renegociacao"
FORM_NAME = "FSI - Operacao da renegociacao"


def apply(deployment):
    b.call = deployment.call
    metadata, attributes, labels = forms.meta(ENTITY)
    fields = [
        "fsi_name", "fsi_status", "fsi_etapa", "fsi_canal", "fsi_clienteid",
        "fsi_produtoclienteid", "fsi_casoid", "fsi_referenciaexterior",
        "fsi_ofertaref", "fsi_versaoferta", "fsi_entradacentavos",
        "fsi_quantidadeparcelas", "fsi_parcelacentavos", "fsi_totalcentavos",
        "fsi_aceitoem", "fsi_protocoloexterno", "fsi_errointegracao", "fsi_snapshot",
    ]
    if set(fields) - attributes.keys():
        raise RuntimeError("Operational schema is incomplete")
    spec = {
        "tabs": [{"name": "fsi_renegociacao", "label": "Renegociação",
                  "sections": [
                      {"name": "contexto", "label": "Atendimento e contrato", "columns": 2,
                       "fields": fields[:8]},
                      {"name": "termos", "label": "Termos aceitos (valores em centavos)", "columns": 2,
                       "fields": fields[8:14]},
                      {"name": "resultado", "label": "Formalização demonstrativa e integração",
                       "fields": fields[14:]},
                  ]}],
        "header": ["fsi_status", "fsi_etapa", "fsi_canal"],
    }
    tree = ElementTree.fromstring(forms.build_formxml(ENTITY, spec, attributes, labels))
    for control in tree.findall(".//control"):
        control.set("disabled", "true")
    xml = ElementTree.tostring(tree, encoding="unicode")
    existing = deployment.client.records.list(
        "systemform", select=["formid", "formxml", "name"],
        filter=f"objecttypecode eq '{ENTITY}' and name eq '{FORM_NAME}' and type eq 2", top=2,
    )
    if len(existing) > 1:
        raise RuntimeError("More than one managed operational form exists")
    if existing:
        form_id = existing[0]["formid"]
        deployment.snapshot("operational-form-before.json", dict(existing[0]))
        deployment.client.records.update("systemform", form_id, {"formxml": xml})
    else:
        form_id = deployment.client.records.create("systemform", {
            "name": FORM_NAME, "objecttypecode": ENTITY, "type": 2, "formxml": xml,
            "description": "Leitura operacional; termos financeiros sao atualizados pelo servico.",
        })
    columns = [
        ("fsi_name", 230), ("fsi_status", 130), ("fsi_etapa", 130),
        ("fsi_clienteid", 180), ("fsi_produtoclienteid", 180),
        ("fsi_canal", 90), ("fsi_protocoloexterno", 230), ("fsi_errointegracao", 230),
    ]
    views = [
        ("FSI - Renegociacoes", []),
        ("FSI - Renegociacoes concluidas", [("fsi_status", "eq", "Concluido")]),
        ("FSI - Renegociacoes com pendencia", [("fsi_errointegracao", "not-null", "")]),
    ]
    components = [{"@odata.type": "Microsoft.Dynamics.CRM.systemform", "formid": form_id}]
    for name, filters in views:
        fetch, layout = forms.build_view(
            ENTITY, {"name": name, "columns": columns, "filters": filters,
                     "order": ("createdon", "desc")}, metadata, attributes,
        )
        rows = deployment.client.records.list(
            "savedquery", select=["savedqueryid", "name", "fetchxml", "layoutxml"],
            filter=f"returnedtypecode eq '{ENTITY}' and name eq '{name}' and querytype eq 0", top=2,
        )
        payload = {"name": name, "fetchxml": fetch, "layoutxml": layout}
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate managed view: {name}")
        if rows:
            identifier = rows[0]["savedqueryid"]
            deployment.snapshot(f"view-{identifier}-before.json", dict(rows[0]))
            deployment.client.records.update("savedquery", identifier, payload)
        else:
            identifier = deployment.client.records.create(
                "savedquery", {**payload, "returnedtypecode": ENTITY, "querytype": 0}
            )
        components.append({
            "@odata.type": "Microsoft.Dynamics.CRM.savedquery", "savedqueryid": identifier,
        })
    _, options, _ = deployment.call(
        "GET", "/GlobalOptionSetDefinitions(Name='fsi_tipotela')", sol=False,
    )
    occupied = next((option for option in options["Options"] if option["Value"] == PORTAL_TYPE), None)
    if occupied:
        if not any(label["Label"] == "Power Pages (autosserviço)"
                   for label in occupied["Label"]["LocalizedLabels"]):
            raise RuntimeError("The publisher value for Pages is already assigned to another type")
    else:
        deployment.call("POST", "/InsertOptionValue", {
            "OptionSetName": "fsi_tipotela", "Value": PORTAL_TYPE,
            "Label": b.L("Power Pages (autosserviço)"),
        })
    resource = deployment.first(
        "webresource", ["webresourceid", "name", "content"],
        "name eq 'fsi_/js/servicotransacional.js'",
    )
    deployment.snapshot("service-resolver-before.json", dict(resource))
    source = ROOT / ".github/skills/dataverse-service-by-stage/scripts/servicotransacional.js"
    deployment.client.records.update("webresource", resource["webresourceid"], {
        "content": base64.b64encode(source.read_bytes()).decode("ascii"),
    })
    reason = deployment.first(
        "fsi_motivo", ["fsi_motivoid"], "fsi_codigo eq 'CRE-REN'"
    )["fsi_motivoid"]
    for slug, title in [
        ("identificacao", "Identificação"), ("elegibilidade", "Elegibilidade"),
        ("simulacao", "Escolha da oferta"), ("confirmacao", "Confirmação"),
        ("formalizacao", "Formalização e acompanhamento"),
    ]:
        code = "SRV-REN-" + slug.upper()
        rows = deployment.client.records.list(
            "fsi_servicotransacional", select=["fsi_servicotransacionalid"],
            filter=f"fsi_codigo eq '{code}'", top=2,
        )
        body = {
            "fsi_codigo": code, "fsi_name": "Renegociação - " + title,
            "fsi_entidade": 712400000, "fsi_processo": PROCESS, "fsi_etapa": slug,
            "fsi_tipotela": PORTAL_TYPE, "fsi_identificador": "renegociacao-financiamento",
            "fsi_parametros": json.dumps({"url": PORTAL}),
            "fsi_descricao": "Autosservico autenticado. O acesso e sempre limitado ao cliente conectado.",
            "fsi_motivoid@odata.bind": f"/fsi_motivos({reason})",
        }
        if len(rows) > 1:
            raise RuntimeError(f"Duplicate managed service: {code}")
        if rows:
            deployment.client.records.update(
                "fsi_servicotransacional", rows[0]["fsi_servicotransacionalid"], body
            )
        else:
            deployment.client.records.create("fsi_servicotransacional", body)
    app = deployment.first(
        "appmodule", ["appmoduleid"], "uniquename eq 'fsi_FSICustomerInsights'"
    )
    deployment.call("POST", "/AddAppComponents", {
        "AppId": app["appmoduleid"], "Components": components,
    })
    deployment.call("POST", "/PublishXml", {
        "ParameterXml": "<importexportxml><entities><entity>fsi_renegociacao</entity>"
        "<entity>fsi_servicotransacional</entity></entities><optionsets><optionset>fsi_tipotela"
        "</optionset></optionsets><webresources><webresource>" + resource["webresourceid"]
        + "</webresource></webresources></importexportxml>",
    }, sol=False)
    print("Operational form:", form_id)
    print("Managed views:", len(views), "Stage services:", 5, "Portal:", PORTAL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG or not args.apply:
        parser.error("Explicit --apply and the approved CCaaS environment are required")
    import dnsfix
    dnsfix.install()
    deployment = Deployment(args.backup_dir)
    try:
        deployment.preflight()
        apply(deployment)
    finally:
        deployment.close()


if __name__ == "__main__":
    main()
