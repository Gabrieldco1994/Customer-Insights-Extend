"""Prepare only the dedicated synthetic customer for WhatsApp simulation."""
import argparse
import json
from datetime import date
from pathlib import Path

from renegociacao_schema import Deployment, ORG
from renegociacao_seed import choice, cpf, ensure, identifier, MI_CLIENT

CUSTOMER = identifier("contact-whatsapp-demo")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--backup-dir", type=Path, required=True)
    parser.add_argument("--existing-fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.environment.rstrip("/") != ORG or not args.apply:
        parser.error("Explicit --apply and the approved environment are required")
    import dnsfix
    dnsfix.install()
    previous = json.loads(args.existing_fixtures.read_text(encoding="utf-8"))
    d = Deployment(args.backup_dir)
    try:
        d.preflight()
        ensure(d, "contact", CUSTOMER, {
            "firstname": "Cliente", "lastname": "Ficticio WhatsApp - Renegociacao",
            "emailaddress1": "whatsapp-demo@renegociacao.invalid", "fsi_cpf": cpf("800000003"),
        })
        customer = d.client.records.retrieve(
            "contact", CUSTOMER,
            select=["firstname", "lastname", "emailaddress1", "fsi_authobjectid", "fsi_authissuer"],
        )
        if (customer.get("firstname") != "Cliente"
                or customer.get("lastname") != "Ficticio WhatsApp - Renegociacao"
                or customer.get("emailaddress1") != "whatsapp-demo@renegociacao.invalid"
                or customer.get("fsi_authobjectid") or customer.get("fsi_authissuer")):
            raise RuntimeError("The fixed demo customer is not isolated; refusing to alter it")
        line = d.first("fsi_linhanegocio", ["fsi_linhanegocioid"], "fsi_codigo eq 'CRE'")["fsi_linhanegocioid"]
        stage = d.first("fsi_etapajornada", ["fsi_etapajornadaid"], "fsi_name eq 'Retenção e risco'")["fsi_etapajornadaid"]
        segment = choice(d, "fsi_vinculonegocio", "fsi_segmento", "Varejo PF")
        link = ensure(d, "fsi_vinculonegocio", identifier("whatsapp-demo-link"), {
            "fsi_name": "WHATS-DEMO - Credito ficticio",
            "fsi_chaveintegracao": "WHATS-DEMO-" + CUSTOMER,
            "fsi_statusvinculo": choice(d, "fsi_vinculonegocio", "fsi_statusvinculo", "Ativo"),
            "fsi_segmento": segment,
            "fsi_macrosegmento": choice(d, "fsi_vinculonegocio", "fsi_macrosegmento", "Varejo"),
            "fsi_emcobranca": True, "fsi_datainicio": date.today().isoformat(),
            "fsi_dataentradaetapa": date.today().isoformat(), "fsi_diasnaetapa": 0,
            "fsi_clienteid_contact@odata.bind": f"/contacts({CUSTOMER})",
            "fsi_linhanegocioid@odata.bind": f"/fsi_linhanegocios({line})",
            "fsi_etapaatualid@odata.bind": f"/fsi_etapajornadas({stage})",
        })
        ensure(d, "fsi_movimentoetapa", identifier("whatsapp-demo-movement"), {
            "fsi_name": "WHATS-DEMO - Entrada em retencao",
            "fsi_dataentrada": date.today().isoformat(), "fsi_duracaodias": 0,
            "fsi_origem": choice(d, "fsi_movimentoetapa", "fsi_origem", "Regra"),
            "fsi_tipomovimento": choice(d, "fsi_movimentoetapa", "fsi_tipomovimento", "Avanço"),
            "fsi_versaocatalogo": "1.0", "fsi_vinculoid@odata.bind": f"/fsi_vinculonegocios({link})",
            "fsi_etapadestinoid@odata.bind": f"/fsi_etapajornadas({stage})",
        })
        product = d.client.records.retrieve("product", identifier("catalog"), select=["productid"])
        if not product:
            raise RuntimeError("Existing demonstration product is missing")
        fixtures = list(previous["fixtures"])
        existing_ids = {f["contractId"] for f in fixtures}
        for suffix, amount in [("001", 1_500_120), ("002", 2_100_240)]:
            anchor = ensure(d, "fsi_produtocliente", identifier("whatsapp-anchor-" + suffix), {
                "fsi_name": "WHATS-DEMO - Financiamento ficticio " + suffix,
                "fsi_numero": "WHATS-DEMO-" + suffix,
                "fsi_tipoproduto": choice(d, "fsi_produtocliente", "fsi_tipoproduto", "Crédito"),
                "fsi_sistemaorigem": choice(d, "fsi_produtocliente", "fsi_sistemaorigem", "Core Bancário"),
                "fsi_statusproduto": choice(d, "fsi_produtocliente", "fsi_statusproduto", "Ativo"),
                "fsi_segmento": segment, "fsi_iniciovigencia": date.today().isoformat(),
                "fsi_clienteprincipalid_contact@odata.bind": f"/contacts({CUSTOMER})",
                "fsi_linhanegocioid@odata.bind": f"/fsi_linhanegocios({line})",
                "fsi_vinculoid@odata.bind": f"/fsi_vinculonegocios({link})",
                "fsi_productid@odata.bind": f"/products({product['productid']})",
            })
            contract = identifier("whatsapp-contract-" + suffix)
            if contract not in existing_ids:
                fixtures.append({
                    "contactId": CUSTOMER, "anchorId": anchor, "contractId": contract,
                    "number": "WHATS-DEMO-" + suffix, "vehicle": "Veiculo ficticio WhatsApp " + suffix,
                    "debtCents": amount, "eligible": True,
                })
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({
            "fixtures": fixtures, "managedIdentityName": "fsi-reneg-demo-runtime",
            "managedIdentityClientId": MI_CLIENT,
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Dedicated customer:", CUSTOMER, "Total fixture contracts:", len(fixtures))
        print("Real portal identity and completed agreements were not changed.")
    finally:
        d.close()


if __name__ == "__main__":
    main()
