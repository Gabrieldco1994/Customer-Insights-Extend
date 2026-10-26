"""Exact-money domain rules shared by durable SQL and offline repositories."""

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
from uuid import UUID


class ApiError(Exception):
    def __init__(self, code: str, message: str, http: int = 400):
        super().__init__(code)
        self.code, self.message, self.http = code, message, http


class ProjectionError(Exception):
    """Expected integration failure; never include service bodies or identity data."""


def guid(value: object) -> str:
    if not isinstance(value, str):
        raise ApiError("INVALID_ID", "A GUID identifier is required.")
    try:
        parsed = UUID(value)
    except ValueError:
        raise ApiError("INVALID_ID", "A GUID identifier is required.") from None
    if parsed.int == 0:
        raise ApiError("INVALID_ID", "A non-empty GUID is required.")
    return str(parsed)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def instant(value: datetime | str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    return result.replace(tzinfo=timezone.utc) if result.tzinfo is None else result.astimezone(timezone.utc)


def stamp(value: datetime | str) -> str:
    return instant(value).isoformat(timespec="seconds").replace("+00:00", "Z")


def canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def review_hash(snapshot: dict) -> str:
    return hashlib.sha256(canonical(snapshot).encode("utf-8")).hexdigest()


def integer(value: object, minimum: int = 1, maximum: int = 2_147_483_647) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ApiError("INVALID_INTEGER", "An integer within the supported range is required.")
    return value


def cpf_digits(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9.\- ]{11,18}", value):
        raise ApiError("INVALID_CPF", "Provide an eleven-digit CPF.")
    result = re.sub(r"[^0-9]", "", value)
    if len(result) != 11:
        raise ApiError("INVALID_CPF", "Provide an eleven-digit CPF.")
    return result


def authorize_owner(row: dict | None, contact: str) -> dict:
    if row is None or str(row["ContactId"]).lower() != contact:
        raise ApiError("ACCESS_DENIED", "The resource is not available for this identity.", 403)
    return row


def make_snapshot(request: dict, offer: dict, now: datetime) -> dict:
    if offer["ContractId"] != request["ContractId"]:
        raise ApiError("INVALID_OFFER", "The offer does not belong to this contract.", 409)
    if instant(offer["ValidUntil"]) <= now:
        raise ApiError("OFFER_EXPIRED", "Request a current offer before continuing.", 409)
    return {
        "requestId": str(request["Id"]), "contractId": str(request["ContractId"]),
        "offerId": str(offer["Id"]), "version": offer["Version"],
        "currency": "BRL", "downPaymentCents": offer["DownPaymentCents"],
        "installmentCents": offer["InstallmentCents"],
        "finalInstallmentCents": offer["FinalInstallmentCents"],
        "installmentCount": offer["InstallmentCount"], "totalCents": offer["TotalCents"],
        "validUntil": stamp(offer["ValidUntil"]),
        "notice": "DEMO fictícia. Sem efeito jurídico ou integração bancária real.",
    }


def validate_confirmation(request: dict, offer: dict | None, supplied_hash: object,
                          confirmed: object, now: datetime) -> None:
    if confirmed is not True:
        raise ApiError("EXPLICIT_CONFIRMATION_REQUIRED", "Review the offer and explicitly confirm.", 409)
    if not isinstance(supplied_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", supplied_hash):
        raise ApiError("INVALID_REVIEW_HASH", "The saved review hash is required.", 409)
    if not request.get("Snapshot"):
        raise ApiError("REVIEW_REQUIRED", "Review an authoritative offer first.", 409)
    saved = json.loads(request["Snapshot"])
    if not hmac.compare_digest(review_hash(saved), supplied_hash):
        raise ApiError("REVIEW_MISMATCH", "Review the current offer again.", 409)
    # A retried confirmation returns the immutable existing agreement even after expiry.
    if request["SourceState"] == "finalized":
        return
    if offer is None or offer["Version"] != saved["version"]:
        raise ApiError("OFFER_CHANGED", "Review the current offer again.", 409)
    current = make_snapshot(request, offer, now)
    if not hmac.compare_digest(review_hash(current), supplied_hash):
        raise ApiError("OFFER_CHANGED", "Review the current offer again.", 409)


def public_contract(row: dict) -> dict:
    return {"id": str(row["Id"]), "number": row["Number"], "vehicle": row["Vehicle"],
            "debtCents": row["DebtCents"], "eligible": bool(row["Eligible"])}


def public_offer(snapshot: dict) -> dict:
    return {
        "id": snapshot["offerId"], "version": snapshot["version"],
        "entryCents": snapshot["downPaymentCents"], "installments": snapshot["installmentCount"],
        "paymentCents": snapshot["installmentCents"], "totalCents": snapshot["totalCents"],
        "validUntil": snapshot["validUntil"],
    }


def public_request(row: dict) -> dict:
    finalized = row["SourceState"] == "finalized"
    complete = finalized and row["ProjectionState"] == "synced"
    state = "completed" if complete else "processing" if finalized else row["SourceState"]
    return {
        "requestId": str(row["Id"]), "contractId": str(row["ContractId"]),
        "channel": row["Channel"], "state": state, "sourceState": row["SourceState"],
        "nativeProjection": row["ProjectionState"], "nativeBpf": row.get("BpfState", "pending"),
        "review": json.loads(row["Snapshot"]) if row.get("Snapshot") else None,
        "reviewHash": row.get("ReviewHash"), "externalProtocol": row.get("Protocol"),
        "acceptedAt": stamp(row["AcceptedAt"]) if row.get("AcceptedAt") else None,
        "integrationError": row.get("IntegrationError"),
        "notice": "DEMO fictícia. Sem efeito jurídico ou integração bancária real.",
    }


@dataclass
class Service:
    repository: object
    crm: object

    def execute(self, action: str, contact: str, body: dict) -> tuple[dict, int]:
        shapes = {
            "profile": (set(), {"cpf"}), "contracts": (set(), set()),
            "offers": ({"contractId"}, set()), "start": ({"contractId", "channel"}, set()),
            "review": ({"requestId", "offerId", "version"}, set()),
            "confirm": ({"requestId", "reviewHash", "confirmed"}, set()),
            "status": (set(), {"requestId", "contractId"}),
        }
        if action not in shapes:
            raise ApiError("UNKNOWN_ACTION", "This action is not supported.", 404)
        required, optional = shapes[action]
        if not isinstance(body, dict) or not required <= body.keys() or body.keys() - required - optional:
            raise ApiError("INVALID_BODY", "The action body does not match its schema.")
        if action == "profile" and "cpf" in body:
            cpf_digits(body["cpf"])
        # Confirm the signed subject exists before reading any SQL data.
        profile = self.crm.profile(contact, body.get("cpf") if action == "profile" else None)
        code = 200
        if action == "profile":
            data = profile
        elif action == "contracts":
            data = {"contracts": [public_contract(c) for c in self.repository.contracts(contact)]}
        elif action == "offers":
            contract = guid(body["contractId"])
            data = {"offers": [public_offer(offer) for offer in self.repository.offers(contact, contract)]}
        else:
            if action == "start":
                if body["channel"] not in ("page", "chat"):
                    raise ApiError("INVALID_CHANNEL", "Use page or chat.")
                row = self.repository.start(contact, guid(body["contractId"]), body["channel"])
            elif action == "review":
                row = self.repository.review(contact, guid(body["requestId"]), guid(body["offerId"]),
                                             integer(body["version"]))
            elif action == "confirm":
                row = self.repository.confirm(contact, guid(body["requestId"]), body["reviewHash"],
                                              body["confirmed"])
            else:
                if len(body) != 1:
                    raise ApiError("INVALID_SELECTOR", "Provide exactly one requestId or contractId.")
                key = next(iter(body))
                row = self.repository.status(contact, key, guid(body[key]))
                if row is None:
                    return {"status": "ok", "data": {
                        "requestId": None, "status": "not_started", "protocol": None, "offer": None},
                        "demonstration": True}, 200
            row = self.repository.reconcile(contact, str(row["Id"]), self.crm.project)
            result = public_request(row)
            code = 202 if row["ProjectionState"] != "synced" else 200
            if action == "start":
                data = {"requestId": result["requestId"], "status": result["state"]}
            elif action == "review":
                data = {"requestId": result["requestId"], "reviewHash": result["reviewHash"],
                        "offer": public_offer(result["review"])}
            else:
                data = {"requestId": result["requestId"], "status": result["state"],
                        "protocol": result["externalProtocol"],
                        "offer": public_offer(result["review"]) if result["review"] else None}
        return {"status": "processing" if code == 202 else "ok", "data": data, "demonstration": True}, code
