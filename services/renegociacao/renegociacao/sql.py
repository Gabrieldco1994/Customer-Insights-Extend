"""SQL is the transaction boundary, not a per-worker Python mutex."""

from contextlib import contextmanager
import os
import re
from uuid import UUID, uuid4

import certifi
import pytds

from .core import (ApiError, ProjectionError, authorize_owner, canonical, make_snapshot,
                   review_hash, utcnow, validate_confirmation)
from .security import required


def row(cursor) -> dict | None:
    result = cursor.fetchone()
    if result is None:
        return None
    return {key: str(value) if isinstance(value, UUID) else value for key, value in result.items()}


class SqlRepository:
    def __init__(self, credential):
        self.credential = credential

    @contextmanager
    def transaction(self):
        server = required("SQL_SERVER")
        if not re.fullmatch(r"[a-zA-Z0-9.-]+\.database\.windows\.net", server):
            raise ApiError("CONFIGURATION_ERROR", "Use the Azure SQL server FQDN, not an IP.", 503)
        connection = pytds.connect(
            server=server, database=required("SQL_DATABASE"), port=1433,
            access_token_callable=lambda: self.credential.get_token(
                "https://database.windows.net/.default").token,
            cafile=os.environ.get("SQL_CA_FILE") or certifi.where(),
            validate_host=True, enc_login_only=False, autocommit=False,
            as_dict=True, login_timeout=15, timeout=30,
            appname="renegociacao-demo", disable_connect_retry=True,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET XACT_ABORT ON; SET LOCK_TIMEOUT 30000;")
                yield cursor
            connection.commit()
        finally:
            # On any exception close rolls back. Never replay an ambiguous commit automatically.
            connection.close()

    @staticmethod
    def _contract(cursor, contact: str, contract_id: str) -> dict:
        cursor.execute("SELECT * FROM demo.Contracts WITH (UPDLOCK,HOLDLOCK) WHERE Id=%s",
                       (contract_id,))
        return authorize_owner(row(cursor), contact)

    def _request(self, cursor, contact: str, request_id: str) -> dict:
        # All mutation/reconciliation paths acquire contract, then request, in the same order.
        cursor.execute("SELECT ContractId,ContactId FROM demo.Requests WHERE Id=%s", (request_id,))
        reference = authorize_owner(row(cursor), contact)
        contract = self._contract(cursor, contact, reference["ContractId"])
        cursor.execute("SELECT * FROM demo.Requests WITH (UPDLOCK,HOLDLOCK) WHERE Id=%s", (request_id,))
        result = authorize_owner(row(cursor), contact)
        result["AnchorId"] = contract["AnchorId"]
        return result

    @staticmethod
    def _offer(cursor, offer_id: str) -> dict | None:
        cursor.execute("SELECT * FROM demo.Offers WITH (HOLDLOCK) WHERE Id=%s", (offer_id,))
        return row(cursor)

    def contracts(self, contact: str) -> list[dict]:
        with self.transaction() as cursor:
            cursor.execute("SELECT * FROM demo.Contracts WHERE ContactId=%s ORDER BY Id", (contact,))
            return list(cursor.fetchall())

    def offers(self, contact: str, contract_id: str) -> list[dict]:
        with self.transaction() as cursor:
            contract = self._contract(cursor, contact, contract_id)
            if not contract["Eligible"]:
                return []
            cursor.execute("SELECT * FROM demo.Offers WHERE ContractId=%s AND ValidUntil>%s ORDER BY Id",
                           (contract_id, utcnow()))
            results = []
            for offer in cursor.fetchall():
                snapshot = make_snapshot({"Id": contract_id, "ContractId": contract_id},
                                         {**offer, "Id": str(offer["Id"]), "ContractId": str(offer["ContractId"])},
                                         utcnow())
                snapshot.pop("requestId")
                results.append(snapshot)
            return results

    def start(self, contact: str, contract_id: str, channel: str) -> dict:
        with self.transaction() as cursor:
            contract = self._contract(cursor, contact, contract_id)
            cursor.execute("SELECT * FROM demo.Requests WITH (UPDLOCK,HOLDLOCK) WHERE ContractId=%s",
                           (contract_id,))
            existing = row(cursor)
            if existing:
                return authorize_owner(existing, contact)
            if not contract["Eligible"]:
                raise ApiError("NOT_ELIGIBLE", "This contract is not eligible for the demonstration.", 409)
            request_id = str(uuid4())
            cursor.execute(
                "INSERT INTO demo.Requests(Id,ContractId,ContactId,Channel) VALUES(%s,%s,%s,%s)",
                (request_id, contract_id, contact, channel))
            cursor.execute("SELECT * FROM demo.Requests WHERE Id=%s", (request_id,))
            return row(cursor)

    def review(self, contact: str, request_id: str, offer_id: str, version: int) -> dict:
        with self.transaction() as cursor:
            request = self._request(cursor, contact, request_id)
            if request["SourceState"] == "finalized":
                raise ApiError("ALREADY_FORMALIZED", "The source already contains an agreement.", 409)
            offer = self._offer(cursor, offer_id)
            if offer is None or offer["ContractId"] != request["ContractId"]:
                raise ApiError("INVALID_OFFER", "The offer is not available for this request.", 409)
            if offer["Version"] != version:
                raise ApiError("OFFER_CHANGED", "Review the current offer version.", 409)
            snapshot = make_snapshot(request, offer, utcnow())
            cursor.execute(
                "UPDATE demo.Requests SET OfferId=%s,Snapshot=%s,ReviewHash=%s,SourceState='review',"
                "ProjectionState='pending',IntegrationError=NULL WHERE Id=%s",
                (offer_id, canonical(snapshot), review_hash(snapshot), request_id))
            cursor.execute("SELECT * FROM demo.Requests WHERE Id=%s", (request_id,))
            return row(cursor)

    def confirm(self, contact: str, request_id: str, supplied_hash: object, confirmed: object) -> dict:
        with self.transaction() as cursor:
            request = self._request(cursor, contact, request_id)
            offer = self._offer(cursor, request["OfferId"]) if request["OfferId"] else None
            now = utcnow()
            validate_confirmation(request, offer, supplied_hash, confirmed, now)
            if request["SourceState"] == "finalized":
                return request
            # Under the contract lock this check and both writes are indivisible.
            cursor.execute("SELECT Eligible FROM demo.Contracts WHERE Id=%s", (request["ContractId"],))
            if not row(cursor)["Eligible"]:
                raise ApiError("NOT_ELIGIBLE", "The source contract is no longer eligible.", 409)
            protocol = f"DEMO-{request_id}"
            cursor.execute(
                "INSERT INTO demo.Agreements(RequestId,ContractId,Protocol,Snapshot,AcceptedAt)"
                " VALUES(%s,%s,%s,%s,%s)",
                (request_id, request["ContractId"], protocol, request["Snapshot"], now))
            cursor.execute(
                "UPDATE demo.Contracts SET Eligible=0,PositionDate=%s WHERE Id=%s",
                (now, request["ContractId"]))
            cursor.execute(
                "UPDATE demo.Requests SET SourceState='finalized',Protocol=%s,AcceptedAt=%s,"
                "ProjectionState='pending',IntegrationError=NULL WHERE Id=%s",
                (protocol, now, request_id))
            cursor.execute("SELECT * FROM demo.Requests WHERE Id=%s", (request_id,))
            return row(cursor)

    def status(self, contact: str, selector: str, identifier: str) -> dict | None:
        with self.transaction() as cursor:
            if selector == "contractId":
                self._contract(cursor, contact, identifier)
                cursor.execute("SELECT * FROM demo.Requests WHERE ContractId=%s", (identifier,))
                result = row(cursor)
                return authorize_owner(result, contact) if result else None
            cursor.execute("SELECT * FROM demo.Requests WHERE Id=%s", (identifier,))
            return authorize_owner(row(cursor), contact)

    def reconcile(self, contact: str, request_id: str, project) -> dict:
        with self.transaction() as cursor:
            current = self._request(cursor, contact, request_id)
            if current["ProjectionState"] == "synced":
                return current
            error = None
            try:
                bpf = project(current)
            except ProjectionError:
                bpf, error = "pending", "NATIVE_RECONCILIATION_FAILED"
            projection = "synced" if bpf in ("active", "completed") and not error else "pending"
            cursor.execute(
                "UPDATE demo.Requests SET ProjectionState=%s,BpfState=%s,IntegrationError=%s WHERE Id=%s",
                (projection, bpf, error, request_id))
            return {**current, "ProjectionState": projection, "BpfState": bpf, "IntegrationError": error}

    def odata(self, table: str, columns: dict, clauses: list, skip: int, limit: int) -> list[dict]:
        # table/column names come exclusively from module constants, never directly from query input.
        select = ",".join(f"[{source}] AS [{public}]" for public, (source, _) in columns.items())
        predicates = " AND ".join(f"[{columns[name][0]}]=%s" for name, _ in clauses) or "1=1"
        sql = (f"SELECT {select} FROM demo.[{table}] WHERE {predicates} "
               "ORDER BY Id OFFSET %s ROWS FETCH NEXT %s ROWS ONLY")
        with self.transaction() as cursor:
            cursor.execute(sql, tuple(value for _, value in clauses) + (skip, limit))
            return list(cursor.fetchall())
