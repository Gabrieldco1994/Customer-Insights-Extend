"""Executable transaction model. Real cross-process SQL locking needs live verification."""

from copy import deepcopy
from datetime import timedelta
from threading import RLock
from uuid import uuid4

from renegociacao.core import (ApiError, ProjectionError, authorize_owner, canonical, cpf_digits,
                               make_snapshot, review_hash, utcnow, validate_confirmation)


CONTACT = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
CONTRACT = "33333333-3333-4333-8333-333333333333"
ANCHOR = "44444444-4444-4444-8444-444444444444"
OFFER = "55555555-5555-4555-8555-555555555555"


class MemoryRepository:
    def __init__(self):
        self.lock = RLock()
        self.contract = {"Id": CONTRACT, "ContactId": CONTACT, "AnchorId": ANCHOR,
                         "Number": "DEMO-001", "Vehicle": "Veículo fictício",
                         "DebtCents": 10001, "Eligible": True, "PositionDate": utcnow()}
        self.offer = {"Id": OFFER, "ContractId": CONTRACT, "Version": 1,
                      "DownPaymentCents": 1001, "InstallmentCents": 1500,
                      "FinalInstallmentCents": 1500, "InstallmentCount": 6,
                      "TotalCents": 10001, "ValidUntil": utcnow() + timedelta(hours=1)}
        self.request = None
        self.agreements = {}

    def contracts(self, contact):
        return [deepcopy(self.contract)] if contact == CONTACT else []

    def offers(self, contact, contract_id):
        authorize_owner(self.contract if contract_id == CONTRACT else None, contact)
        if not self.contract["Eligible"]:
            return []
        snapshot = make_snapshot({"Id": CONTRACT, "ContractId": CONTRACT}, self.offer, utcnow())
        snapshot.pop("requestId")
        return [snapshot]

    def start(self, contact, contract_id, channel):
        with self.lock:
            authorize_owner(self.contract if contract_id == CONTRACT else None, contact)
            if self.request:
                return deepcopy(self.request)
            if not self.contract["Eligible"]:
                raise ApiError("NOT_ELIGIBLE", "Not eligible.", 409)
            self.request = {"Id": str(uuid4()), "ContractId": CONTRACT, "ContactId": CONTACT,
                            "AnchorId": ANCHOR, "Channel": channel, "SourceState": "draft",
                            "ProjectionState": "pending", "BpfState": "pending", "Snapshot": None,
                            "ReviewHash": None, "Protocol": None, "AcceptedAt": None,
                            "IntegrationError": None, "OfferId": None}
            return deepcopy(self.request)

    def _request(self, contact, request_id):
        return authorize_owner(self.request if self.request and self.request["Id"] == request_id else None, contact)

    def review(self, contact, request_id, offer_id, version):
        with self.lock:
            request = self._request(contact, request_id)
            if request["SourceState"] == "finalized":
                raise ApiError("ALREADY_FORMALIZED", "Already finalized.", 409)
            if offer_id != OFFER:
                raise ApiError("INVALID_OFFER", "Invalid offer.", 409)
            if self.offer["Version"] != version:
                raise ApiError("OFFER_CHANGED", "Changed.", 409)
            snapshot = make_snapshot(request, self.offer, utcnow())
            request.update({"OfferId": OFFER, "Snapshot": canonical(snapshot),
                            "ReviewHash": review_hash(snapshot), "SourceState": "review",
                            "ProjectionState": "pending", "IntegrationError": None})
            return deepcopy(request)

    def confirm(self, contact, request_id, supplied_hash, confirmed):
        with self.lock:
            request = self._request(contact, request_id)
            validate_confirmation(request, self.offer, supplied_hash, confirmed, utcnow())
            if request["SourceState"] != "finalized":
                if not self.contract["Eligible"]:
                    raise ApiError("NOT_ELIGIBLE", "Not eligible.", 409)
                request.update({"SourceState": "finalized", "Protocol": "DEMO-" + request_id,
                                "AcceptedAt": utcnow(), "ProjectionState": "pending"})
                self.agreements[request_id] = deepcopy(request)
                self.contract["Eligible"] = False
            return deepcopy(request)

    def status(self, contact, selector, identifier):
        with self.lock:
            if selector == "contractId":
                authorize_owner(self.contract if identifier == CONTRACT else None, contact)
                return deepcopy(self.request)
            return deepcopy(self._request(contact, identifier))

    def reconcile(self, contact, request_id, project):
        with self.lock:
            request = self._request(contact, request_id)
            if request["ProjectionState"] == "synced":
                return deepcopy(request)
            error = None
            try:
                bpf = project(deepcopy(request))
            except ProjectionError:
                bpf, error = "pending", "NATIVE_RECONCILIATION_FAILED"
            request.update({"BpfState": bpf, "IntegrationError": error,
                            "ProjectionState": "synced" if bpf in ("active", "completed") and not error else "pending"})
            return deepcopy(request)


class FakeCrm:
    def __init__(self):
        self.fail = False
        self.pending = False
        self.calls = []

    def profile(self, contact, supplied_cpf):
        if contact != CONTACT:
            raise ApiError("ACCESS_DENIED", "Not authorized.", 403)
        if supplied_cpf is not None and cpf_digits(supplied_cpf) != "00000000000":
            raise ApiError("CPF_MISMATCH", "Does not match.", 403)
        return {"contactId": contact, "name": "Pessoa fictícia", "cpfVerified": supplied_cpf is not None}

    def project(self, request):
        self.calls.append(deepcopy(request))
        if self.fail:
            raise ProjectionError("UNAVAILABLE")
        if self.pending:
            return "pending"
        return "completed" if request["SourceState"] == "finalized" else "active"
