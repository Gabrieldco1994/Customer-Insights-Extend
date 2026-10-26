"""Explicit, function-key-guarded schema and synthetic source initialization."""

from datetime import timedelta
from pathlib import Path
import math
import re
from uuid import UUID, uuid5

from .core import ApiError, guid, integer, utcnow


def validate(body: object) -> list[dict]:
    if not isinstance(body, dict) or body.keys() - {"fixtures", "managedIdentityName", "managedIdentityClientId"}:
        raise ApiError("INVALID_BOOTSTRAP", "Use fixtures and optional managedIdentityName/managedIdentityClientId.")
    fixtures = body.get("fixtures")
    if not isinstance(fixtures, list) or not 1 <= len(fixtures) <= 1000:
        raise ApiError("INVALID_FIXTURES", "Provide between one and one thousand synthetic fixtures.")
    result, seen = [], set()
    for fixture in fixtures:
        fields = {"contactId", "anchorId", "contractId", "number", "vehicle", "debtCents", "eligible"}
        if not isinstance(fixture, dict) or fixture.keys() != fields:
            raise ApiError("INVALID_FIXTURE", "A fixture does not match the documented schema.")
        cleaned = {**fixture, **{k: guid(fixture[k]) for k in ("contactId", "anchorId", "contractId")}}
        integer(cleaned["debtCents"])
        if type(cleaned["eligible"]) is not bool:
            raise ApiError("INVALID_FIXTURE", "eligible must be boolean.")
        for key, maximum in (("number", 80), ("vehicle", 160)):
            if not isinstance(cleaned[key], str) or not 1 <= len(cleaned[key]) <= maximum:
                raise ApiError("INVALID_FIXTURE", "A fixture string is outside its supported length.")
        if cleaned["contractId"] in seen:
            raise ApiError("DUPLICATE_FIXTURE", "Contract identifiers must be unique.")
        seen.add(cleaned["contractId"])
        result.append(cleaned)
    name, sid = body.get("managedIdentityName"), body.get("managedIdentityClientId")
    if (name is None) != (sid is None):
        raise ApiError("INVALID_MANAGED_IDENTITY", "Supply both the SQL user name and MI client-ID GUID.")
    if name is not None:
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", name):
            raise ApiError("INVALID_MANAGED_IDENTITY", "Use a simple Entra managed-identity display name.")
        guid(sid)
    return result


def initialize(repository, body: dict) -> dict:
    fixtures = validate(body)
    count = max(12, math.ceil(2000 / len(fixtures)))
    now = utcnow().replace(microsecond=0)
    with repository.transaction() as cursor:
        cursor.execute(
            "DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=N'demo-bootstrap',"
            "@LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=30000;"
            "IF @r<0 THROW 51002,'Bootstrap lock unavailable',1;")
        source = Path(__file__).resolve().parent.parent / "schema.sql"
        for batch in re.split(r"(?im)^\s*GO\s*$", source.read_text()):
            if batch.strip():
                cursor.execute(batch)
        name, sid = body.get("managedIdentityName"), body.get("managedIdentityClientId")
        if name is not None:
            # SQL service-principal SIDs use client IDs; Entra administrator
            # assignment instead uses the principal object ID.
            sid_hex = UUID(guid(sid)).bytes_le.hex()
            cursor.execute("SELECT name,sid,type FROM sys.database_principals WHERE name=%s", (name,))
            existing = cursor.fetchone()
            if existing and (bytes(existing["sid"]).hex() != sid_hex or existing["type"] != "E"):
                raise ApiError("IDENTITY_USER_CONFLICT", "An existing SQL principal has a different identity.", 409)
            if not existing:
                cursor.execute(f"CREATE USER [{name}] WITH SID=0x{sid_hex}, TYPE=E")
            cursor.execute(
                f"IF IS_ROLEMEMBER(N'demo_service',N'{name}')<>1 ALTER ROLE demo_service ADD MEMBER [{name}]")
        for fixture in fixtures:
            contact, anchor, contract = (fixture[k] for k in ("contactId", "anchorId", "contractId"))
            cursor.execute("SELECT ContactId,AnchorId,Number,Vehicle,DebtCents FROM demo.Contracts "
                           "WITH (UPDLOCK,HOLDLOCK) WHERE Id=%s", (contract,))
            existing = cursor.fetchone()
            if existing:
                same = (str(existing["ContactId"]).lower() == contact
                        and str(existing["AnchorId"]).lower() == anchor
                        and existing["Number"] == fixture["number"]
                        and existing["Vehicle"] == fixture["vehicle"]
                        and existing["DebtCents"] == fixture["debtCents"])
                if not same:
                    raise ApiError("FIXTURE_CONFLICT", "An existing fixture has different immutable values.", 409)
            else:
                cursor.execute(
                    "INSERT INTO demo.Contracts(Id,ContactId,AnchorId,Number,Vehicle,DebtCents,Eligible,PositionDate)"
                    " VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                    (contract, contact, anchor, fixture["number"], fixture["vehicle"], fixture["debtCents"],
                     fixture["eligible"], now))
            base, remainder = divmod(fixture["debtCents"], count)
            for number in range(1, count + 1):
                identifier = str(uuid5(UUID(contract), f"installment:{number}"))
                cursor.execute(
                    "IF NOT EXISTS(SELECT 1 FROM demo.Installments WHERE Id=%s) "
                    "INSERT INTO demo.Installments(Id,ContractId,ContactId,Number,AmountCents,DueDate,Status)"
                    " VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (identifier, identifier, contract, contact, number, base + (number <= remainder),
                     now - timedelta(days=count - number + 1), "DEMO_EM_ATRASO"))
            # Fixed demo plans, no discount or language-model arithmetic.
            for variant, requested_count in ((0, 6), (1, 12)):
                baseline = fixture["debtCents"] // 10 if variant else 0
                installments = min(requested_count, fixture["debtCents"] - baseline)
                # Move the cent remainder into the entry, so every payment is
                # identical and the public seven-field offer remains exact.
                down = baseline + (fixture["debtCents"] - baseline) % installments
                regular = (fixture["debtCents"] - down) // installments
                final = regular
                identifier = str(uuid5(UUID(contract), f"offer:{variant}"))
                cursor.execute(
                    "IF NOT EXISTS(SELECT 1 FROM demo.Offers WHERE Id=%s) "
                    "INSERT INTO demo.Offers(Id,ContractId,Version,DownPaymentCents,InstallmentCents,"
                    "FinalInstallmentCents,InstallmentCount,TotalCents,ValidUntil)"
                    " VALUES(%s,%s,1,%s,%s,%s,%s,%s,%s)",
                    (identifier, identifier, contract, down, regular, final, installments,
                     fixture["debtCents"], now + timedelta(days=30)))
        cursor.execute("SELECT COUNT_BIG(*) AS Total FROM demo.Installments")
        total = cursor.fetchone()["Total"]
    return {"installmentRows": total, "fixtureContracts": len(fixtures),
            "schema": "demo", "managedIdentityUserConfigured": name is not None}
