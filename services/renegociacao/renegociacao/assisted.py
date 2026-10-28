"""Assisted demo: CRM decides; only the emailed capability can accept."""
from datetime import timedelta
import hmac
import html
import json
import os
from uuid import UUID, uuid4, uuid5

import jwt

from .core import (
    ApiError, canonical, cpf_digits, guid, instant, integer, public_contract, public_offer,
    review_hash, stamp, utcnow,
)
from .security import required
from .sql import row
from .whatsapp_demo import money_brl, offer_summary

CUSTOMER = "f0e4d7de-c5cf-5b6c-b1d2-c19376c9fedc"
CONTRACTS = {
    "66e3ecc2-e403-5f94-bd28-e4030c1874b4": ("ASSIST-DEMO-001", "2100d155-c675-5733-b205-8ff5fe652e67"),
    "d60c1dd0-1c18-509e-b382-27d2717558cd": ("ASSIST-DEMO-002", "36cdbb9f-4ec6-5943-923f-ab5b2c2da31e"),
}
PENDING, APPROVED, REJECTED = 712400000, 712400001, 712400002
TOKEN_AUDIENCE = "fsi-assisted-customer-acceptance"
PORTAL = "https://portalatendimento01.powerappsportals.com"
ADMIN_EMAIL = "admin@diax24492505.onmicrosoft.com"
CRM_FIELDS = ["fsi_decisaoalcada", "fsi_pedidoalcada", "fsi_justificativaalcada",
              "fsi_emailaceitestatus", "_modifiedby_value"]


def enabled():
    if os.environ.get("ASSISTED_DEMO_ENABLED") != "1":
        raise ApiError("ASSISTED_DISABLED", "O Portal Assistido ainda nao esta habilitado.", 403)


def operator(backend, identity):
    enabled()
    allowed = {guid(v.strip()) for v in required("ASSISTED_OPERATOR_OIDS").split(",") if v.strip()}
    if identity.mode != "entra" or identity.object_id not in allowed:
        raise ApiError("OPERATOR_DENIED", "Sua conta nao esta autorizada para o Portal Assistido.", 403)
    return backend.crm.resolve_entra_contact(identity.object_id, identity.issuer)


def fixed_customer(backend, supplied_cpf=None):
    contact = backend.crm.client.records.retrieve(
        "contact", CUSTOMER, select=["firstname", "lastname", "emailaddress1", "statecode",
                                    "fsi_authobjectid", "fsi_authissuer", "fsi_cpf"],
    )
    if (not contact or contact.get("statecode") != 0
            or contact.get("firstname") != "Cliente"
            or contact.get("lastname") != "Ficticio Portal Assistido"
            or contact.get("emailaddress1") != "assistido-demo@renegociacao.invalid"
            or contact.get("fsi_authobjectid") or contact.get("fsi_authissuer")):
        raise ApiError("ASSISTED_CUSTOMER_INVALID", "O cliente ficticio dedicado nao esta isolado.", 503)
    if supplied_cpf is not None:
        supplied = cpf_digits(supplied_cpf)
        if not isinstance(contact.get("fsi_cpf"), str) or not hmac.compare_digest(
                supplied, cpf_digits(contact["fsi_cpf"])):
            raise ApiError("ASSISTED_CUSTOMER_NOT_FOUND",
                           "CPF nao encontrado na carteira ficticia autorizada.", 404)
    return contact


def allowed_contracts(rows):
    return [r for r in rows if str(r["Id"]).lower() in CONTRACTS
            and (r["Number"], str(r["AnchorId"]).lower()) == CONTRACTS[str(r["Id"]).lower()]]


def fixed_contracts(backend):
    fixed_customer(backend)
    return allowed_contracts(backend.repository.contracts(CUSTOMER))


def contract(backend, identifier):
    identifier = guid(identifier)
    result = next((r for r in fixed_contracts(backend) if str(r["Id"]).lower() == identifier), None)
    if result is None:
        raise ApiError("ASSISTED_CONTRACT_DENIED", "Contrato fora da demonstracao assistida.", 403)
    return result


def proposal(base, debt, body):
    integer(body["discountCents"], 0, debt - 1)
    count = integer(body["installments"], 1, 360)
    total = debt - body["discountCents"]
    requested_entry = integer(body["entryCents"], 0, total - 1)
    payment, remainder = divmod(total - requested_entry, count)
    if payment < 1:
        raise ApiError("INVALID_PROPOSAL", "O valor de cada parcela deve ser ao menos um centavo.")
    reason = body["reason"]
    if not isinstance(reason, str) or not 10 <= len(reason.strip()) <= 1000:
        raise ApiError("INVALID_REASON", "Descreva a solicitacao em 10 a 1000 caracteres.")
    return {
        "contractId": guid(body["contractId"]), "baseOfferId": base["id"],
        "baseVersion": base["version"], "discountCents": body["discountCents"],
        "requestedEntryCents": requested_entry, "entryCents": requested_entry + remainder,
        "roundingCents": remainder, "installments": count, "paymentCents": payment,
        "totalCents": total, "reason": reason.strip(),
    }


def proposal_text(terms):
    return (
        "SOLICITACAO DE RENEGOCIACAO FICTICIA - A DECIDIR NO CRM\n"
        f"Contrato: {CONTRACTS[terms['contractId']][0]}\n"
        f"Desconto solicitado: {money_brl(terms['discountCents'])}\n"
        f"Entrada: {money_brl(terms['entryCents'])}\n"
        f"Parcelas: {terms['installments']} de {money_brl(terms['paymentCents'])}\n"
        f"Total: {money_brl(terms['totalCents'])}\n"
        f"Ajuste de centavos na entrada: {money_brl(terms['roundingCents'])}\n"
        f"Justificativa do operador: {terms['reason']}\n"
        "Aprovacao interna nao e aceite. O cliente deve revisar e confirmar pelo email."
    )


class Store:
    def __init__(self, repository):
        self.repository = repository

    def get(self, request_id, operator_contact=None):
        with self.repository.transaction() as cursor:
            cursor.execute("SELECT * FROM demo.AssistedRequests WHERE RequestId=%s", (guid(request_id),))
            result = row(cursor)
        if not result or (operator_contact and result["OperatorContactId"] != operator_contact):
            raise ApiError("ASSISTED_REQUEST_DENIED", "Solicitacao nao disponivel para este operador.", 403)
        return result

    def portfolio(self, operator_contact):
        with self.repository.transaction() as cursor:
            cursor.execute(
                "SELECT c.*,a.RequestId AS AssistedRequestId FROM demo.Contracts c "
                "LEFT JOIN demo.Requests r ON r.ContractId=c.Id AND r.ContactId=c.ContactId "
                "LEFT JOIN demo.AssistedRequests a ON a.RequestId=r.Id AND a.OperatorContactId=%s "
                "WHERE c.ContactId=%s ORDER BY c.Number",
                (operator_contact, CUSTOMER),
            )
            return list(cursor.fetchall())

    def save(self, request_id, operator_contact, terms):
        with self.repository.transaction() as cursor:
            cursor.execute("SELECT * FROM demo.AssistedRequests WITH (UPDLOCK,HOLDLOCK) WHERE RequestId=%s",
                           (request_id,))
            existing = row(cursor)
            if existing:
                if (existing["OperatorContactId"] != operator_contact
                        or existing["Proposal"] != canonical(terms)):
                    raise ApiError("ASSISTED_REQUEST_EXISTS", "Ja existe uma solicitacao com outros termos.", 409)
            else:
                cursor.execute(
                    "INSERT INTO demo.AssistedRequests(RequestId,OperatorContactId,Proposal,Nonce) VALUES(%s,%s,%s,%s)",
                    (request_id, operator_contact, canonical(terms), str(uuid4())),
                )
        return self.get(request_id, operator_contact)

    def approved_offer(self, current, approver):
        request_id = current["RequestId"]
        terms = json.loads(current["Proposal"])
        offer_id = str(uuid5(UUID(request_id), "assisted-approved-offer"))
        with self.repository.transaction() as cursor:
            cursor.execute("SELECT * FROM demo.AssistedRequests WITH (UPDLOCK,HOLDLOCK) WHERE RequestId=%s",
                           (request_id,))
            saved = row(cursor)
            if not saved["ApprovedAt"]:
                approved_at = utcnow().replace(microsecond=0)
                cursor.execute(
                    "UPDATE demo.AssistedRequests SET ApprovedAt=%s,ApproverId=%s WHERE RequestId=%s",
                    (approved_at, approver, request_id),
                )
                cursor.execute(
                    "INSERT INTO demo.Offers(Id,ContractId,Version,DownPaymentCents,InstallmentCents,"
                    "FinalInstallmentCents,InstallmentCount,TotalCents,ValidUntil) VALUES(%s,%s,1,%s,%s,%s,%s,%s,%s)",
                    (offer_id, terms["contractId"], terms["entryCents"], terms["paymentCents"],
                     terms["paymentCents"], terms["installments"], terms["totalCents"],
                     approved_at + timedelta(hours=48)),
                )
        return self.get(request_id), offer_id


def native_request(backend, current):
    terms = json.loads(current["Proposal"])
    contract(backend, terms["contractId"])
    record = backend.crm.client.records.retrieve("fsi_renegociacao", current["RequestId"], select=CRM_FIELDS)
    if not record or record.get("fsi_pedidoalcada") != proposal_text(terms):
        raise ApiError("ASSISTED_TERMS_CHANGED", "Os termos no CRM nao correspondem a solicitacao original.", 409)
    return record


def execute(backend, operator_contact, action, body):
    shapes = {
        "contracts": {"cpf"}, "offers": {"contractId"},
        "preview": {"contractId", "offerId", "version", "discountCents", "entryCents", "installments", "reason"},
        "request": {"contractId", "offerId", "version", "discountCents", "entryCents", "installments", "reason", "previewHash"},
        "status": {"requestId"},
    }
    if action not in shapes or not isinstance(body, dict) or set(body) != shapes[action]:
        raise ApiError("INVALID_ASSISTED_BODY", "Os campos nao correspondem a etapa do Portal Assistido.")
    if action == "contracts":
        supplied_cpf = cpf_digits(body["cpf"])
        fixed_customer(backend, supplied_cpf)
        return {
            "customer": {"name": "Cliente Ficticio Portal Assistido"},
            "contracts": [{**public_contract(c),
                           "assistedRequestId": str(c["AssistedRequestId"]) if c["AssistedRequestId"] else None}
                          for c in allowed_contracts(Store(backend.repository).portfolio(operator_contact))],
        }
    store = Store(backend.repository)
    if action == "status":
        current = store.get(body["requestId"], operator_contact)
        native = native_request(backend, current)
        result, _ = backend.execute("status", CUSTOMER, {"requestId": current["RequestId"]})
        email_status = native.get("fsi_emailaceitestatus") or "Nao enviado"
        if current["ApprovedAt"]:
            email = backend.crm.client.records.retrieve(
                "email", str(uuid5(UUID(current["RequestId"]), "assisted-acceptance-email")),
                select=["activityid", "statuscode"],
            )
            if email:
                email_status = {
                    1: "Rascunho preparado; envio ainda nao confirmado", 3: "Enviado",
                    6: "Pendente de envio pelo servidor", 7: "Envio em andamento", 8: "Falha no envio",
                }.get(email.get("statuscode"), "Consultar atividade de email no CRM")
        return {**result["data"], "decision": native.get("fsi_decisaoalcada", PENDING),
                "decisionNote": native.get("fsi_justificativaalcada") or "",
                "emailStatus": email_status,
                "proposal": json.loads(current["Proposal"]), "caseId": current["RequestId"],
                "caseUrl": crm_url(current["RequestId"])}
    selected = contract(backend, body["contractId"])
    offers = [public_offer(v) for v in backend.repository.offers(CUSTOMER, str(selected["Id"]))]
    if action == "offers":
        return {"offers": offers}
    integer(body["version"])
    base = next((o for o in offers if o["id"] == guid(body["offerId"]) and o["version"] == body["version"]), None)
    if not base:
        raise ApiError("OFFER_CHANGED", "Consulte novamente as ofertas vigentes.", 409)
    if not selected["Eligible"]:
        raise ApiError("NOT_ELIGIBLE", "Contrato nao elegivel.", 409)
    terms = proposal(base, selected["DebtCents"], body)
    digest = review_hash(terms)
    if action == "preview":
        return {"proposal": terms, "previewHash": digest, "requiresApproval": True}
    if not isinstance(body["previewHash"], str) or not hmac.compare_digest(digest, body["previewHash"]):
        raise ApiError("PREVIEW_CHANGED", "Revise os valores antes de enviar a solicitacao.", 409)
    started, _ = backend.execute("start", CUSTOMER, {"contractId": str(selected["Id"]), "channel": "page"})
    request_id = started["data"]["requestId"]
    current = store.save(request_id, operator_contact, terms)
    existing = backend.crm.client.records.retrieve("fsi_renegociacao", request_id, select=CRM_FIELDS)
    if not existing:
        raise ApiError("NATIVE_PENDING", "A ocorrencia esta sendo preparada. Consulte novamente.", 503)
    presentation = proposal_text(terms)
    if existing.get("fsi_pedidoalcada") not in (None, "", presentation):
        raise ApiError("ASSISTED_TERMS_CHANGED", "Existe outra solicitacao no atendimento.", 409)
    if not existing.get("fsi_pedidoalcada"):
        backend.crm.client.records.update("fsi_renegociacao", request_id, {
            "fsi_pedidoalcada": presentation, "fsi_decisaoalcada": PENDING,
            "fsi_emailaceitestatus": "Aguardando decisao no CRM",
        })
    return {"requestId": request_id, "caseId": request_id, "caseUrl": crm_url(request_id),
            "status": "awaiting_approval", "proposal": terms}


def crm_url(request_id):
    return ("https://org88957f9a.crm.dynamics.com/main.aspx?appid=ae892729-b9c8-f111-aaaf-000d3a343144"
            "&pagetype=entityrecord&etn=fsi_renegociacao&id=" + guid(request_id)
            + "&formid=" + guid(required("ASSISTED_APPROVAL_FORM_ID")))


def signing_key():
    value = required("ASSISTED_ACCEPTANCE_SECRET")
    if len(value) < 43:
        raise ApiError("CONFIGURATION_ERROR", "A chave de aceite nao atende ao tamanho minimo.", 503)
    return value


def prepare_mail(backend, body):
    enabled()
    if not isinstance(body, dict) or set(body) != {"requestId"}:
        raise ApiError("INVALID_MAIL_REQUEST", "Informe apenas a solicitacao de alcada.")
    store = Store(backend.repository)
    current = store.get(body["requestId"])
    record = native_request(backend, current)
    if record.get("fsi_decisaoalcada") != APPROVED:
        raise ApiError("APPROVAL_REQUIRED", "Somente a decisao aprovada no CRM permite enviar o aceite.", 409)
    if not current["ApprovedAt"]:
        approver = backend.crm.client.records.retrieve(
            "systemuser", record["_modifiedby_value"], select=["systemuserid", "isdisabled", "applicationid"],
        )
        if not approver or approver.get("isdisabled") or approver.get("applicationid"):
            raise ApiError("HUMAN_APPROVAL_REQUIRED", "A decisao deve ser gravada por um aprovador humano no CRM.", 403)
    current, offer_id = store.approved_offer(current, record["_modifiedby_value"])
    source = backend.repository.status(CUSTOMER, "requestId", current["RequestId"])
    if source["SourceState"] == "finalized":
        raise ApiError("ALREADY_FORMALIZED", "O acordo ja foi aceito; nao envie outra solicitacao.", 409)
    if source["OfferId"] is None:
        backend.execute("review", CUSTOMER, {"requestId": current["RequestId"], "offerId": offer_id, "version": 1})
    elif str(source["OfferId"]) != offer_id:
        raise ApiError("ASSISTED_OFFER_CONFLICT", "Outra oferta esta vinculada a esta solicitacao.", 409)
    token = jwt.encode({
        "aud": TOKEN_AUDIENCE, "iss": TOKEN_AUDIENCE, "sub": current["RequestId"],
        "nonce": current["Nonce"], "proposal": review_hash(json.loads(current["Proposal"])),
        "iat": int(instant(current["ApprovedAt"]).timestamp()),
        "exp": int((instant(current["ApprovedAt"]) + timedelta(hours=48)).timestamp()),
    }, signing_key(), algorithm="HS256")
    link = PORTAL + "/aceite-renegociacao/#token=" + token
    terms = json.loads(current["Proposal"])
    summary = offer_summary({**terms, "id": offer_id, "version": 1,
                             "validUntil": stamp(instant(current["ApprovedAt"]) + timedelta(hours=48))})
    mail = {
        "emailId": str(uuid5(UUID(current["RequestId"]), "assisted-acceptance-email")),
        "recipient": ADMIN_EMAIL, "subject": "DEMO - Revise e aceite sua renegociacao",
        "body": "<p>Renegociacao ficticia aprovada para sua revisao. Nenhum acordo foi aceito ainda.</p><p>"
                + html.escape(summary) + '</p><p><a href="' + html.escape(link, quote=True)
                + '">Revisar os termos e decidir</a></p><p>Abrir o link nao confirma a contratacao. '
                  "O aceite deve ser dado na pagina. Link valido por 48 horas.</p>",
        "requestId": current["RequestId"],
    }
    existing = backend.crm.client.records.retrieve("email", mail["emailId"], select=["activityid", "statuscode"])
    if not existing:
        backend.crm.client.records.create("email", {
            "activityid": mail["emailId"], "subject": mail["subject"], "description": mail["body"],
            "directioncode": True,
            "regardingobjectid_incident_email@odata.bind": f"/incidents({current['RequestId']})",
            "email_activity_parties": [
                {"participationtypemask": 1,
                 "partyid_systemuser@odata.bind": "/systemusers(32e53ec9-de28-f111-8341-6045bd0149e9)"},
                {"participationtypemask": 2, "addressused": ADMIN_EMAIL,
                 "partyid_systemuser@odata.bind": "/systemusers(32e53ec9-de28-f111-8341-6045bd0149e9)"},
            ],
        })
    return {key: value for key, value in mail.items() if key != "body"}


def acceptance(backend, action, body):
    enabled()
    fields = {"token"} if action == "review" else {"token", "reviewHash", "confirmed"}
    if action not in ("review", "confirm") or not isinstance(body, dict) or set(body) != fields:
        raise ApiError("INVALID_ACCEPTANCE_BODY", "Use a etapa de revisao ou confirmacao explicita.")
    if not isinstance(body["token"], str) or len(body["token"]) > 4096:
        raise ApiError("INVALID_ACCEPTANCE_LINK", "O link de aceite nao e valido.", 401)
    try:
        claims = jwt.decode(body["token"], signing_key(), algorithms=["HS256"],
                            audience=TOKEN_AUDIENCE, issuer=TOKEN_AUDIENCE,
                            options={"require": ["sub", "exp", "iat", "aud", "iss", "nonce", "proposal"]})
    except jwt.InvalidTokenError:
        raise ApiError("INVALID_ACCEPTANCE_LINK", "O link de aceite expirou ou nao e valido.", 401) from None
    current = Store(backend.repository).get(claims["sub"])
    if claims["nonce"] != current["Nonce"] or claims["proposal"] != review_hash(json.loads(current["Proposal"])):
        raise ApiError("ACCEPTANCE_LINK_CHANGED", "Os termos deste link nao correspondem a solicitacao.", 409)
    native = native_request(backend, current)
    if native.get("fsi_decisaoalcada") != APPROVED or not current["ApprovedAt"]:
        raise ApiError("APPROVAL_REVOKED", "O CRM nao confirma a aprovacao desta proposta.", 409)
    source = backend.repository.status(CUSTOMER, "requestId", current["RequestId"])
    if not source or not source["Snapshot"]:
        raise ApiError("REVIEW_REQUIRED", "Os termos aprovados ainda nao estao disponiveis.", 409)
    offer = public_offer(json.loads(source["Snapshot"]))
    if action == "review":
        state = "awaiting_acceptance"
        if source["SourceState"] == "finalized":
            result, _ = backend.execute("status", CUSTOMER, {"requestId": current["RequestId"]})
            state = result["data"]["status"]
        return {"requestId": current["RequestId"], "offer": offer, "reviewHash": source["ReviewHash"],
                "status": state, "protocol": source.get("Protocol")}
    result, status = backend.execute("confirm", CUSTOMER, {
        "requestId": current["RequestId"], "reviewHash": body["reviewHash"], "confirmed": body["confirmed"],
    })
    return {**result["data"], "integrationPending": status == 202}
