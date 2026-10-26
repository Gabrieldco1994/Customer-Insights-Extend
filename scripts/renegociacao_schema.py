"""Deploy the FSI renegotiation extension without replacing existing processes.

The native phase prepares the schema and BPF. The existing CRE-REN reason is
only connected by the separate wire phase, after the service is validated.
"""
import argparse
import json
import sys
import time
from pathlib import Path
from urllib.parse import quote
import xml.etree.ElementTree as ET

import requests
from azure.identity import AzureCliCredential

ROOT = Path(__file__).resolve().parents[1]
SKILLS = ROOT / ".github" / "skills"
for folder in (
    "dataverse-schema-builder",
    "dataverse-forms-views",
    "dataverse-business-process-flows",
):
    sys.path.insert(0, str(SKILLS / folder / "scripts"))
sys.path.insert(0, str(ROOT / "scripts"))

import dnsfix
from auth import _build_operation_context, get_plugin_headers
from PowerPlatform.Dataverse.client import DataverseClient
import build as b
import bpf

ORG = "https://org88957f9a.crm.dynamics.com"
SOLUTION = "FSIInsights"
PROCESS = "fsi_processorenegociacao"


def process_spec():
    definitions = [
        ("Identificação", "identificacao", 4, "customerid", "Cliente"),
        ("Elegibilidade", "elegibilidade", 5, "fsi_produtoclienteid", "Financiamento"),
        ("Simulação e escolha", "simulacao", 2, "title", "Atendimento"),
        ("Confirmação", "confirmacao", 2, "fsi_motivoid", "Assunto"),
        ("Formalização e acompanhamento", "formalizacao", 6, "description", "Resultado"),
    ]
    return {
        "entity": "incident",
        "uniquename": PROCESS,
        "name": "Processo de Renegociação de Financiamento",
        "description": "Autosserviço demonstrativo integrado ao Caso e ao vínculo FSI.",
        "out": str(ROOT / "deployment" / "renegociacao" / "bpf"),
        "stages": [
            {
                "name": name,
                "slug": slug,
                "category": category,
                "steps": [{"field": field, "label": label, "required": False}],
            }
            for name, slug, category, field, label in definitions
        ],
    }


def configure_schema():
    a = b.A
    b.GLOBAL_SETS = {}
    b.TABLES = {
        "fsi_renegociacao": (
            "Renegociação de Financiamento",
            "Renegociações de Financiamento",
            "UserOwned",
            False,
            True,
            [
                a("s", "fsi_referenciaexterior", "Referência externa", req=1, len=36),
                a("s", "fsi_status", "Situação", req=1, len=40),
                a("s", "fsi_etapa", "Etapa do serviço", len=32),
                a("s", "fsi_ofertaref", "Referência da oferta", len=36),
                a("i", "fsi_versaoferta", "Versão da oferta"),
                a("i", "fsi_entradacentavos", "Entrada (centavos)"),
                a("i", "fsi_parcelacentavos", "Parcela (centavos)"),
                a("i", "fsi_totalcentavos", "Total (centavos)"),
                a("i", "fsi_quantidadeparcelas", "Quantidade de parcelas"),
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.MemoAttributeMetadata",
                    "SchemaName": "fsi_snapshot",
                    "DisplayName": b.L("Termos aceitos (JSON)"),
                    "RequiredLevel": {"Value": "None"},
                    "MaxLength": 10000,
                },
                a("dh", "fsi_aceitoem", "Aceito em"),
                a("s", "fsi_protocoloexterno", "Protocolo demonstrativo", len=60),
                a("s", "fsi_errointegracao", "Erro de integração", len=1000),
                a("s", "fsi_canal", "Canal de entrada", len=20),
            ],
        )
    }
    b.EXT = {
        "contact": [
            a("s", "fsi_authobjectid", "Objeto da identidade autenticada", len=36),
            a("s", "fsi_authissuer", "Emissor da identidade autenticada", len=200),
        ]
    }
    b.LOOKUPS = [
        ("fsi_renegociacao", "fsi_clienteid", "Cliente", "contact", 1, 0),
        ("fsi_renegociacao", "fsi_produtoclienteid", "Financiamento", "fsi_produtocliente", 1, 0),
        ("fsi_renegociacao", "fsi_casoid", "Caso", "incident", 1, 0),
    ]
    b.KEYS = [
        ("fsi_renegociacao", "fsi_referenciaexteriorchave", ["fsi_referenciaexterior"]),
        ("contact", "fsi_identidadeautenticada", ["fsi_authobjectid", "fsi_authissuer"]),
    ]


class Deployment:
    def __init__(self, backup):
        self.backup_dir = backup
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.credential = AzureCliCredential()
        self.session = requests.Session()
        self.client = DataverseClient(
            base_url=ORG,
            credential=self.credential,
            context=_build_operation_context("dv-metadata"),
        )

    def call(self, method, path, body=None, sol=True, tries=6):
        if path.startswith("http"):
            raise ValueError("Only relative Dataverse paths are allowed")
        url = ORG + "/api/data/v9.2" + quote(path, safe="/?$=&,()'%@:;.-_~*")
        for attempt in range(tries):
            token = self.credential.get_token(ORG + "/.default").token
            headers = {
                **get_plugin_headers("dv-metadata", token),
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            if sol:
                headers["MSCRM.SolutionUniqueName"] = SOLUTION
            response = self.session.request(
                method, url, json=body, headers=headers, timeout=180
            )
            if response.status_code in (429, 503, 504) and attempt + 1 < tries:
                retry = response.headers.get("Retry-After", "")
                time.sleep(min(int(retry), 120) if retry.isdigit() else 5 * (attempt + 1))
                continue
            if not response.ok:
                raise RuntimeError(
                    f"Dataverse {method} {path}: HTTP {response.status_code}: "
                    f"{response.text[:1200]}"
                )
            return (
                response.status_code,
                response.json() if response.content else {},
                response.headers,
            )
        raise RuntimeError(f"Transient retries exhausted for {method} {path}")

    def first(self, table, fields, filter):
        rows = self.client.records.list(table, select=fields, filter=filter, top=2)
        if len(rows) != 1:
            raise RuntimeError(f"Expected exactly one {table}: {filter}; found {len(rows)}")
        return rows[0]

    def snapshot(self, name, value):
        target = self.backup_dir / name
        if not target.exists():
            target.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")

    def preflight(self):
        solution = self.first(
            "solution", ["solutionid", "uniquename", "_publisherid_value"],
            "uniquename eq 'FSIInsights'",
        )
        publisher = self.client.records.retrieve(
            "publisher", solution["_publisherid_value"],
            select=["customizationprefix", "uniquename"],
        )
        if publisher["customizationprefix"] != "fsi":
            raise RuntimeError("The confirmed publisher prefix no longer matches fsi")
        reason = self.first(
            "fsi_motivo", ["fsi_motivoid", "fsi_codigo", "fsi_processo"],
            "fsi_codigo eq 'CRE-REN'",
        )
        self.snapshot("reason-before.json", dict(reason))
        print("Target confirmed:", ORG, SOLUTION, publisher["customizationprefix"])

    def native(self):
        configure_schema()
        if self.client.tables.get("fsi_renegociacao") is None:
            b.stage_tables()
        self.client.close()
        for attempt in range(12):
            self.client = DataverseClient(
                base_url=ORG, credential=self.credential,
                context=_build_operation_context("dv-metadata"),
            )
            if self.client.tables.get("fsi_renegociacao") is not None:
                break
            self.client.close()
            time.sleep(5)
        else:
            raise RuntimeError("New renegotiation metadata did not become available")
        columns = {
            column["LogicalName"]
            for column in self.client.tables.list_columns("fsi_renegociacao")
        }
        b.LOOKUPS = [lookup for lookup in b.LOOKUPS if lookup[1] not in columns]
        b.stage_lookups()
        contact_columns = {
            column["LogicalName"] for column in self.client.tables.list_columns("contact")
        }
        b.EXT["contact"] = [
            column for column in b.EXT["contact"]
            if column["SchemaName"] not in contact_columns
        ]
        b.stage_ext()
        for key_spec in list(b.KEYS):
            entity, _, attributes = key_spec
            _, keys, _ = self.call(
                "GET", f"/EntityDefinitions(LogicalName='{entity}')/Keys"
                "?$select=SchemaName,KeyAttributes", sol=False
            )
            if not any(set(key["KeyAttributes"]) == set(attributes) for key in keys["value"]):
                b.KEYS = [key_spec]
                b.stage_keys()
        self.add_table_component("fsi_renegociacao")
        spec = process_spec()
        spec["_out"] = Path(spec["out"])
        bpf.apply(spec)
        self.add_table_component(PROCESS)
        self.call(
            "POST", "/PublishXml",
            {"ParameterXml": "<importexportxml><entities><entity>fsi_renegociacao</entity>"
                             "</entities></importexportxml>"},
            sol=False,
        )
        _, stages, _ = self.call(
            "GET",
            "/processstages?$select=stagename,processstageid&$filter="
            f"_processid_value eq {bpf.RUN(PROCESS, 'workflow')}",
            sol=False,
        )
        stage_map = {r["stagename"]: r["processstageid"] for r in stages["value"]}
        expected = {s["slug"] for s in process_spec()["stages"]}
        if set(stage_map) != expected:
            raise RuntimeError(f"Unexpected BPF stages: {stage_map}")
        self.snapshot("bpf-stage-map.json", stage_map)
        print("BPF_STAGE_MAP", json.dumps(stage_map))
        print("CRE-REN was NOT connected yet; run wire only after service validation.")

    def add_table_component(self, name):
        _, meta, _ = self.call(
            "GET", f"/EntityDefinitions(LogicalName='{name}')?$select=MetadataId",
            sol=False,
        )
        self.call("POST", "/AddSolutionComponent", {
            "ComponentId": meta["MetadataId"],
            "ComponentType": 1,
            "SolutionUniqueName": SOLUTION,
            "AddRequiredComponents": False,
        })

    def backup(self):
        solution = self.first("solution", ["solutionid"], "uniquename eq 'FSIInsights'")
        components = self.client.records.list(
            "solutioncomponent",
            select=["objectid", "componenttype"],
            filter=f"_solutionid_value eq {solution['solutionid']}",
            top=1000,
        )
        self.snapshot("solution-components-before.json", [dict(c) for c in components])
        tables = {c["objectid"] for c in components if c["componenttype"] == 1}
        # An existing BPF without its generated table prevents solution export.
        for name in [
            "fsi_processocontestacao", "fsi_processoatendimento",
            "fsi_processorenovacao", "fsi_processosinistro",
        ]:
            meta = self.client.tables.get(name)
            if meta is None:
                raise RuntimeError(f"Existing BPF table is missing: {name}")
            if meta["metadata_id"] not in tables:
                self.add_table_component(name)
                print("Included existing generated BPF table for export:", name)

    def wire(self):
        reason = self.first(
            "fsi_motivo", ["fsi_motivoid", "fsi_processo"], "fsi_codigo eq 'CRE-REN'"
        )
        if reason.get("fsi_processo") not in (None, "", PROCESS):
            raise RuntimeError("CRE-REN was changed by another operation; refusing to overwrite")
        app = self.first(
            "appmodule", ["appmoduleid"], "uniquename eq 'fsi_FSICustomerInsights'"
        )
        workflow = self.first(
            "workflow", ["workflowid", "statecode"],
            f"uniquename eq '{PROCESS}' and category eq 4",
        )
        if workflow["statecode"] != 1:
            raise RuntimeError("The renegotiation process is not active")
        _, meta, _ = self.call(
            "GET", "/EntityDefinitions(LogicalName='fsi_renegociacao')?$select=MetadataId",
            sol=False,
        )
        self.call("POST", "/AddAppComponents", {
            "AppId": app["appmoduleid"],
            "Components": [
                {"@odata.type": "Microsoft.Dynamics.CRM.entity", "entityid": meta["MetadataId"]},
                {"@odata.type": "Microsoft.Dynamics.CRM.workflow",
                 "workflowid": workflow["workflowid"]},
            ],
        })
        sitemap = self.first(
            "sitemap", ["sitemapid", "sitemapxml"],
            "sitemapnameunique eq 'fsi_FSICustomerInsights'",
        )
        self.snapshot("sitemap-before.json", dict(sitemap))
        tree = ET.fromstring(sitemap["sitemapxml"])
        if tree.find(".//SubArea[@Id='sub_fsi_renegociacao']") is None:
            group = tree.find(".//Group[@Id='grp_ate']")
            if group is None:
                raise RuntimeError("Existing Atendimento sitemap group was not found")
            sub = ET.SubElement(group, "SubArea", {
                "Id": "sub_fsi_renegociacao", "Entity": "fsi_renegociacao"
            })
            titles = ET.SubElement(sub, "Titles")
            ET.SubElement(titles, "Title", {"LCID": "1046", "Title": "Renegociações"})
            self.client.records.update(
                "sitemap", sitemap["sitemapid"],
                {"sitemapxml": ET.tostring(tree, encoding="unicode")},
            )
        self.client.records.update(
            "fsi_motivo", reason["fsi_motivoid"], {"fsi_processo": PROCESS}
        )
        self.call("POST", "/PublishAllXml", {}, sol=False)
        readback = self.client.records.retrieve(
            "fsi_motivo", reason["fsi_motivoid"], select=["fsi_processo"]
        )
        if readback["fsi_processo"] != PROCESS:
            raise RuntimeError("CRE-REN readback did not match")
        print("CRE-REN and existing FSI app connected to", PROCESS)

    def close(self):
        self.client.close()
        self.session.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["backup", "native", "wire"])
    parser.add_argument("--environment", required=True)
    parser.add_argument("--solution", required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG or args.solution != SOLUTION:
        parser.error("This demo deployer only targets the explicitly approved CCaaS/FSIInsights")
    if not args.apply:
        print(json.dumps(process_spec(), ensure_ascii=False, indent=2))
        print("Dry run: no environment changes. Add --apply to execute the selected phase.")
        return
    dnsfix.install()
    b.ORG, b.API, b.SOL = ORG, ORG + "/api/data/v9.2", SOLUTION
    deployment = Deployment(args.backup_dir)
    b.call = deployment.call
    try:
        deployment.preflight()
        getattr(deployment, args.phase)()
    finally:
        deployment.close()


if __name__ == "__main__":
    main()
