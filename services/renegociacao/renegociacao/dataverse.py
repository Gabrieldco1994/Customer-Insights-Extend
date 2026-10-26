"""SDK record projection, with CSDL discovery only for the SDK's metadata gap."""

import json
import logging
import os
import re
import threading
import time
from xml.etree import ElementTree as ET

import requests
from azure.core.exceptions import ClientAuthenticationError, ServiceRequestError, ServiceResponseError
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.config import OperationContext
from PowerPlatform.Dataverse.core.errors import DataverseError, HttpError

from .core import ApiError, ProjectionError, cpf_digits, guid, stamp
from .security import ENTRA_ISSUER, dataverse_url, required


CONTEXT = "app=dataverse-skills/unknown;skill=dv-data;agent=copilot"
NS = {"e": "http://docs.oasis-open.org/odata/ns/edm"}
TRANSIENT = {429, 503, 504}
INTEGRATION_ERRORS = (DataverseError, requests.RequestException, ClientAuthenticationError,
                      ServiceRequestError, ServiceResponseError)


def log_integration_error(operation, error):
    reason = str(error) if isinstance(error, ProjectionError) else type(error).__name__
    if not re.fullmatch(r"[A-Za-z0-9_]+", reason):
        reason = type(error).__name__
    service_code = (getattr(error, "details", None) or {}).get("service_error_code", "")
    if not re.fullmatch(r"0x[0-9a-fA-F]+", service_code):
        service_code = ""
    privileges = ",".join(sorted(set(re.findall(r"\bprv[A-Za-z0-9_]+\b", str(error)))))
    logging.error(
        "REN_DEMO_NATIVE_FAILURE %s reason=%s status=%s code=%s privileges=%s",
        operation, reason, getattr(error, "status_code", 0), service_code, privileges,
    )


def retry(operation):
    for attempt in range(3):
        try:
            return operation()
        except HttpError as error:
            if error.status_code not in TRANSIENT or attempt == 2:
                raise
            delay = error.details.get("retry_after") if error.details else None
            time.sleep(min(10, max(0, delay)) if isinstance(delay, int) else 2 ** attempt)


class Metadata:
    def __init__(self, document: str):
        root = ET.fromstring(document)
        self.types = {node.attrib["Name"]: node for node in root.findall(".//e:EntityType", NS)}
        self.sets = {node.attrib["EntityType"].split(".")[-1]: node.attrib["Name"]
                     for node in root.findall(".//e:EntitySet", NS)}

    def field(self, entity: str, name: str, kind: str | None = None) -> str:
        node = self.types.get(entity)
        if node is not None:
            for prop in node.findall("e:Property", NS):
                if prop.attrib["Name"] == name and (kind is None or prop.attrib["Type"] == kind):
                    return name
        raise ProjectionError("SCHEMA_FIELD_NOT_FOUND")

    def navigation(self, entity: str, attribute: str, target: str) -> str:
        node = self.types.get(entity)
        if node is not None:
            for nav in node.findall("e:NavigationProperty", NS):
                if nav.attrib.get("Type", "").split(".")[-1] != target:
                    continue
                for constraint in nav.findall("e:ReferentialConstraint", NS):
                    if constraint.attrib.get("Property") == f"_{attribute}_value":
                        return nav.attrib["Name"]
        raise ProjectionError("SCHEMA_RELATIONSHIP_NOT_FOUND")

    def bind(self, entity: str, attribute: str, target: str, identifier: str) -> dict:
        nav = self.navigation(entity, attribute, target)
        entity_set = self.sets.get(target)
        if not entity_set:
            raise ProjectionError("SCHEMA_ENTITY_SET_NOT_FOUND")
        return {nav + "@odata.bind": f"/{entity_set}({guid(identifier)})"}

    def contact_attribute(self, entity: str) -> str:
        configured = os.environ.get("ANCHOR_CONTACT_FIELD")
        if configured:
            self.navigation(entity, configured, "contact")
            return configured
        node = self.types.get(entity)
        candidates = set()
        if node is not None:
            for nav in node.findall("e:NavigationProperty", NS):
                if nav.attrib.get("Type", "").split(".")[-1] == "contact":
                    for constraint in nav.findall("e:ReferentialConstraint", NS):
                        prop = constraint.attrib.get("Property", "")
                        if prop.startswith("_") and prop.endswith("_value"):
                            candidates.add(prop[1:-6])
        if len(candidates) != 1:
            raise ProjectionError("ANCHOR_CONTACT_RELATIONSHIP_AMBIGUOUS")
        return candidates.pop()


class NativeProjection:
    def __init__(self, credential):
        self.url, self.credential = dataverse_url(), credential
        self.client = DataverseClient(
            base_url=self.url, credential=credential,
            context=OperationContext(user_agent_context=CONTEXT))
        self._metadata = None
        self._reason = None
        self._workflow = None
        self._lock = threading.RLock()

    @property
    def metadata(self) -> Metadata:
        with self._lock:
            if self._metadata is None:
                # $metadata CSDL is not exposed by SDK's public tables interface.
                response = requests.get(
                    self.url + "/api/data/v9.2/$metadata",
                    headers={
                        "Authorization": "Bearer " + self.credential.get_token(self.url + "/.default").token,
                        "Accept": "application/xml", "User-Agent": f"renegociacao-demo ({CONTEXT})",
                    },
                    timeout=(10, 30), verify=True, allow_redirects=False,
                )
                if response.status_code != 200:
                    raise ProjectionError("METADATA_HTTP_" + str(response.status_code))
                try:
                    self._metadata = Metadata(response.text)
                except ET.ParseError:
                    raise ProjectionError("METADATA_INVALID") from None
            return self._metadata

    def resolve_entra_contact(self, object_id: str, issuer: str) -> str:
        """Read only the server-managed exact oid+issuer binding; never infer one."""
        try:
            if issuer != ENTRA_ISSUER or guid(object_id) != object_id:
                raise ApiError("ACCESS_DENIED", "This identity has no unique active contact binding.", 403)
            for field in ("fsi_authobjectid", "fsi_authissuer"):
                self.metadata.field("contact", field, "Edm.String")
            records = list(retry(lambda: self.client.records.list(
                "contact", select=["contactid", "fsi_authobjectid", "fsi_authissuer", "statecode"],
                filter=f"fsi_authobjectid eq '{object_id}' and fsi_authissuer eq '{issuer}'", top=2)))
            if (len(records) != 1 or records[0].get("fsi_authobjectid") != object_id
                    or records[0].get("fsi_authissuer") != issuer or records[0].get("statecode") != 0):
                raise ApiError("ACCESS_DENIED", "This identity has no unique active contact binding.", 403)
            return guid(str(records[0]["contactid"]))
        except INTEGRATION_ERRORS + (ProjectionError,) as error:
            log_integration_error("identity", error)
            raise ApiError("IDENTITY_LOOKUP_UNAVAILABLE", "Identity validation is temporarily unavailable.", 503) from None

    def profile(self, contact: str, supplied_cpf: str | None) -> dict:
        try:
            field = os.environ.get("CPF_FIELD", "fsi_cpf")
            if field != "fsi_cpf":
                raise ApiError("CONFIGURATION_ERROR", "CPF_FIELD must be the confirmed contact.fsi_cpf field.", 503)
            self.metadata.field("contact", field, "Edm.String")
            record = retry(lambda: self.client.records.retrieve("contact", contact,
                           select=["contactid", "fullname", field, "statecode"]))
            if record is None or record.get("statecode") != 0:
                raise ApiError("ACCESS_DENIED", "This identity is not an active contact.", 403)
            if supplied_cpf is not None:
                supplied = cpf_digits(supplied_cpf)
                stored = record.get(field)
                try:
                    matches = isinstance(stored, str) and supplied == cpf_digits(stored)
                except ApiError:
                    matches = False
                if not matches:
                    raise ApiError("CPF_MISMATCH", "The CPF does not match the authenticated identity.", 403)
            return {"contactId": contact, "name": record.get("fullname", ""), "cpfVerified": supplied_cpf is not None}
        except INTEGRATION_ERRORS + (ProjectionError,) as error:
            log_integration_error("profile", error)
            raise ApiError("IDENTITY_LOOKUP_UNAVAILABLE", "Identity validation is temporarily unavailable.", 503) from None

    def reason(self) -> str:
        with self._lock:
            if self._reason is None:
                field = required("REASON_CODE_FIELD")
                if not re.fullmatch(r"fsi_[a-z0-9_]+", field):
                    raise ProjectionError("REASON_CODE_FIELD_INVALID")
                self.metadata.field("fsi_motivo", field, "Edm.String")
                records = list(retry(lambda: self.client.records.list(
                    "fsi_motivo", select=["fsi_motivoid"], filter=f"{field} eq 'CRE-REN'", top=2)))
                if len(records) != 1:
                    raise ProjectionError("REASON_NOT_UNIQUE")
                self._reason = guid(str(records[0]["fsi_motivoid"]))
            return self._reason

    def workflow_id(self) -> str:
        """Pin the deployed definition before attaching the case reason."""
        with self._lock:
            if self._workflow is None:
                workflow_id = guid(required("BPF_WORKFLOW_ID"))
                workflow = retry(lambda: self.client.records.retrieve(
                    "workflow", workflow_id, select=["workflowid", "uniquename", "statecode"]))
                if (not workflow or workflow.get("uniquename") != "fsi_processorenegociacao"
                        or workflow.get("statecode") != 1):
                    raise ProjectionError("BPF_WORKFLOW_NOT_ACTIVE")
                self._workflow = workflow_id
            return self._workflow

    def _save(self, entity: str, identifier: str, payload: dict, ownership: dict) -> None:
        primary = entity + "id" if entity != "incident" else "incidentid"
        scalar = {key: value for key, value in payload.items() if not key.endswith("@odata.bind")}
        select = list(dict.fromkeys([primary, *ownership.keys(), *scalar.keys()]))
        existing = retry(lambda: self.client.records.retrieve(entity, identifier, select=select))
        if existing is None:
            try:
                retry(lambda: self.client.records.create(entity, {primary: identifier, **payload}))
            except HttpError as error:
                service_code = (error.details or {}).get("service_error_code", "").lower()
                if error.status_code not in (400, 409) or service_code not in {"0x80040237", "0x80060892"}:
                    raise
                existing = retry(lambda: self.client.records.retrieve(entity, identifier, select=select))
                if existing is None:
                    raise
        if existing is not None:
            if any(str(existing.get(key, "")).lower() != str(value).lower() for key, value in ownership.items()):
                raise ProjectionError("NATIVE_IDENTITY_CONFLICT")
            # Rewriting the case reason retriggers BPF selection and can reset its progress.
            if entity == "incident":
                scalar = {}
            else:
                retry(lambda: self.client.records.update(entity, identifier, payload))
        # Readback is required before a projection can be acknowledged.
        saved = retry(lambda: self.client.records.retrieve(entity, identifier, select=select))
        if saved is None or any(str(saved.get(k, "")).lower() != str(v).lower() for k, v in ownership.items()):
            raise ProjectionError("NATIVE_READBACK_FAILED")
        for key, expected in scalar.items():
            actual = saved.get(key)
            if key == "fsi_aceitoem" and actual is not None:
                actual = stamp(actual)
            if actual != expected:
                raise ProjectionError("NATIVE_VALUES_NOT_ACKNOWLEDGED")

    def _bpf(self, request: dict) -> str:
        raw = os.environ.get("BPF_STAGE_MAP")
        if not raw:
            return "pending"
        try:
            mapping = json.loads(raw)
            stages = ["identificacao", "elegibilidade", "simulacao", "confirmacao", "formalizacao"]
            if not isinstance(mapping, dict) or set(mapping) != set(stages):
                raise ValueError()
            mapping = {key: guid(value) for key, value in mapping.items()}
            if len(set(mapping.values())) != 5:
                raise ValueError()
        except (ValueError, TypeError, ApiError):
            raise ProjectionError("BPF_STAGE_MAP_INVALID") from None
        name = "fsi_processorenegociacao"
        self.metadata.navigation(name, "bpf_incidentid", "incident")
        process_id = self.workflow_id()
        for stage_id in mapping.values():
            stage = retry(lambda: self.client.records.retrieve(
                "processstage", stage_id, select=["processstageid", "_processid_value"]))
            if not stage or str(stage.get("_processid_value", "")).lower() != process_id:
                raise ProjectionError("BPF_STAGE_PROCESS_MISMATCH")
        instances = list(retry(lambda: self.client.records.list(
            name, filter=f"_bpf_incidentid_value eq {request['Id']}",
            select=["businessprocessflowinstanceid", "_activestageid_value", "statecode", "traversedpath"], top=2)))
        if not instances:
            return "pending"
        if len(instances) != 1:
            raise ProjectionError("BPF_INSTANCE_NOT_UNIQUE")
        instance = instances[0]
        finalized = request["SourceState"] == "finalized"
        target = "formalizacao" if finalized else "confirmacao" if request["SourceState"] == "review" else "elegibilidade"
        target_index = stages.index(target)
        active = str(instance.get("_activestageid_value", "")).lower()
        if active not in mapping.values():
            raise ProjectionError("BPF_UNKNOWN_ACTIVE_STAGE")
        current_index = next(i for i, slug in enumerate(stages) if mapping[slug] == active)
        if instance.get("statecode") != 0:
            if finalized and active == mapping["formalizacao"] and instance.get("statecode") == 1:
                return "completed"
            raise ProjectionError("BPF_PREMATURELY_INACTIVE")
        identifier = str(instance["businessprocessflowinstanceid"])
        if current_index > target_index:
            # An operator may have advanced the BPF. Never silently regress it.
            raise ProjectionError("BPF_AHEAD_OF_SOURCE")
        # Advance adjacent stages; do not fabricate an instance while the server flow is pending.
        traversed = [item for item in (instance.get("traversedpath") or active).split(",") if item]
        for index in range(current_index + 1, target_index + 1):
            stage_id = mapping[stages[index]]
            traversed.append(stage_id)
            payload = {**self.metadata.bind(name, "activestageid", "processstage", stage_id),
                       "traversedpath": ",".join(dict.fromkeys(traversed))}
            retry(lambda p=payload: self.client.records.update(name, identifier, p))
        if finalized:
            retry(lambda: self.client.records.update(name, identifier, {"statecode": 1, "statuscode": 2}))
        saved = retry(lambda: self.client.records.retrieve(
            name, identifier, select=["statecode", "_activestageid_value"]))
        if not saved or str(saved.get("_activestageid_value", "")).lower() != mapping[target]:
            raise ProjectionError("BPF_READBACK_FAILED")
        if finalized and saved.get("statecode") != 1:
            raise ProjectionError("BPF_COMPLETION_PENDING")
        return "completed" if finalized else "active"

    def project(self, request: dict) -> str:
        try:
            return self._project(request)
        except INTEGRATION_ERRORS as error:
            log_integration_error("projection", error)
            raise ProjectionError("NATIVE_API_UNAVAILABLE") from None
        except ApiError:
            raise ProjectionError("NATIVE_CONFIGURATION_INVALID") from None

    def _project(self, request: dict) -> str:
        metadata = self.metadata
        request_id, contact, anchor_id = request["Id"], request["ContactId"], request["AnchorId"]
        line_nav = metadata.navigation("fsi_produtocliente", "fsi_linhanegocioid", "fsi_linhanegocio")
        # ReferentialConstraint verified above; read the corresponding lookup value, not a guessed line.
        contact_attribute = metadata.contact_attribute("fsi_produtocliente")
        contact_lookup = f"_{contact_attribute}_value"
        anchor = retry(lambda: self.client.records.retrieve(
            "fsi_produtocliente", anchor_id,
            select=["fsi_produtoclienteid", contact_lookup, "_fsi_linhanegocioid_value"]))
        if not line_nav or anchor is None or str(anchor.get(contact_lookup, "")).lower() != contact:
            raise ProjectionError("ANCHOR_IDENTITY_MISMATCH")
        line = anchor.get("_fsi_linhanegocioid_value")
        if not line:
            raise ProjectionError("ANCHOR_LINE_MISSING")
        self.workflow_id()
        case = {"title": f"DEMO Renegociação {request_id}",
                **metadata.bind("incident", "customerid", "contact", contact),
                **metadata.bind("incident", "fsi_motivoid", "fsi_motivo", self.reason()),
                **metadata.bind("incident", "fsi_produtoclienteid", "fsi_produtocliente", anchor_id),
                **metadata.bind("incident", "fsi_linhanegocioid", "fsi_linhanegocio", str(line))}
        self._save("incident", request_id, case,
                   {"_customerid_value": contact, "_fsi_produtoclienteid_value": anchor_id,
                    "_fsi_motivoid_value": self.reason(), "_fsi_linhanegocioid_value": str(line)})
        snapshot = json.loads(request["Snapshot"]) if request.get("Snapshot") else None
        finalized = request["SourceState"] == "finalized"
        slug = "formalizacao" if finalized else "confirmacao" if snapshot else "elegibilidade"
        payload = {
            "fsi_name": f"DEMO {request_id}", "fsi_referenciaexterior": request_id,
            "fsi_status": "Processando" if finalized else "Revisao" if snapshot else "Rascunho",
            "fsi_etapa": "aguardando_bpf", "fsi_canal": request["Channel"],
            "fsi_errointegracao": "BPF_PENDING",
            **metadata.bind("fsi_renegociacao", "fsi_clienteid", "contact", contact),
            **metadata.bind("fsi_renegociacao", "fsi_produtoclienteid", "fsi_produtocliente", anchor_id),
            **metadata.bind("fsi_renegociacao", "fsi_casoid", "incident", request_id),
        }
        if snapshot:
            payload.update({
                "fsi_ofertaref": snapshot["offerId"], "fsi_versaoferta": snapshot["version"],
                "fsi_entradacentavos": snapshot["downPaymentCents"],
                "fsi_parcelacentavos": snapshot["installmentCents"],
                "fsi_totalcentavos": snapshot["totalCents"],
                "fsi_quantidadeparcelas": snapshot["installmentCount"],
                "fsi_snapshot": request["Snapshot"],
            })
        if finalized:
            payload.update({"fsi_aceitoem": stamp(request["AcceptedAt"]),
                            "fsi_protocoloexterno": request["Protocol"]})
        owner = {"_fsi_clienteid_value": contact, "_fsi_produtoclienteid_value": anchor_id,
                 "fsi_referenciaexterior": request_id}
        self._save("fsi_renegociacao", request_id, payload, owner)
        bpf = self._bpf(request)
        if bpf != "pending":
            acknowledged_status = "Concluido" if finalized else payload["fsi_status"]
            retry(lambda: self.client.records.update(
                "fsi_renegociacao", request_id,
                {"fsi_status": acknowledged_status, "fsi_etapa": slug, "fsi_errointegracao": None}))
            saved = retry(lambda: self.client.records.retrieve(
                "fsi_renegociacao", request_id,
                select=["fsi_status", "fsi_etapa", "fsi_snapshot", "fsi_protocoloexterno"]))
            if (not saved or saved.get("fsi_status") != acknowledged_status or saved.get("fsi_etapa") != slug
                    or saved.get("fsi_snapshot") != request.get("Snapshot")
                    or saved.get("fsi_protocoloexterno") != request.get("Protocol")):
                raise ProjectionError("FINAL_READBACK_FAILED")
        return bpf
