"""Service-authorized simulation, never customer authentication."""
import json

from .core import ApiError, guid

CONTACT = "5ffbdb6c-10a0-5fa8-bc3e-266c675b59ad"
EMAIL = "whatsapp-demo@renegociacao.invalid"
CONTRACTS = {
    "38d4b302-39ed-575a-948f-beced3fae3e0":
        ("WHATS-DEMO-001", "e39416e5-3eb2-5f44-a27e-86f97da11658"),
    "e45cdf23-c6b5-5b50-8704-82c6b7ae6641":
        ("WHATS-DEMO-002", "f115e34d-5978-5722-b7f1-030c0ff1af5b"),
}
ACTIONS = frozenset({"profile", "contracts", "offers", "start", "review", "confirm", "status"})


def money_brl(cents):
    if type(cents) is not int or cents < 0:
        raise ApiError("INVALID_OFFER", "The source offer contains invalid amounts.", 503)
    whole, fraction = divmod(cents, 100)
    return "R$ " + f"{whole:,}".replace(",", ".") + f",{fraction:02d}"


def offer_summary(offer):
    return (f"Entrada: {money_brl(offer['entryCents'])}; "
            f"{offer['installments']} parcelas de {money_brl(offer['paymentCents'])}; "
            f"total: {money_brl(offer['totalCents'])}; validade: {offer['validUntil']}; "
            f"oferta: {offer['id']}; versao: {offer['version']}.")


def decode(body):
    if not isinstance(body, dict) or set(body) != {"payload"}:
        raise ApiError("INVALID_BODY", "Use only a serialized JSON payload.")
    value = body["payload"]
    if not isinstance(value, str) or len(value) > 65536:
        raise ApiError("INVALID_BODY", "The simulation payload must be bounded JSON text.")
    try:
        result = json.loads(value)
    except ValueError:
        raise ApiError("INVALID_JSON", "The simulation payload must contain valid JSON.") from None
    if not isinstance(result, dict):
        raise ApiError("INVALID_BODY", "The simulation payload must be a JSON object.")
    return result


def execute(backend, action, body):
    if action not in ACTIONS:
        raise ApiError("UNKNOWN_ACTION", "This simulation action is not supported.", 404)
    payload = decode(body)
    if payload.keys() & {"contactId", "customerId", "userId", "phone", "code", "accessToken"}:
        raise ApiError("DEMO_CUSTOMER_FIXED", "The simulated customer cannot be selected by the caller.", 403)
    record = backend.crm.client.records.retrieve(
        "contact", CONTACT,
        select=["firstname", "lastname", "emailaddress1", "statecode",
                "fsi_authobjectid", "fsi_authissuer"],
    )
    if (not record or record.get("statecode") != 0
            or record.get("firstname") != "Cliente"
            or record.get("lastname") != "Ficticio WhatsApp - Renegociacao"
            or record.get("emailaddress1") != EMAIL
            or record.get("fsi_authobjectid") or record.get("fsi_authissuer")):
        raise ApiError("DEMO_CUSTOMER_NOT_ISOLATED", "The dedicated synthetic customer is not available.", 503)
    rows = backend.repository.contracts(CONTACT)
    allowed = {
        str(row["Id"]).lower() for row in rows
        if str(row["Id"]).lower() in CONTRACTS
        and (row["Number"], str(row["AnchorId"]).lower()) == CONTRACTS[str(row["Id"]).lower()]
    }
    if "contractId" in payload and guid(payload["contractId"]) not in allowed:
        raise ApiError("DEMO_CONTRACT_DENIED", "Only the fixed synthetic contracts are available.", 403)
    if "requestId" in payload:
        request = backend.repository.status(CONTACT, "requestId", guid(payload["requestId"]))
        if not request or str(request["ContractId"]).lower() not in allowed:
            raise ApiError("DEMO_REQUEST_DENIED", "This request is not part of the isolated simulation.", 403)
    result, status = backend.execute(action, CONTACT, payload)
    if action == "contracts":
        result["data"]["contracts"] = [
            row for row in result["data"]["contracts"] if row["id"] in allowed
        ]
    result["simulationOnly"] = True
    return result, status
