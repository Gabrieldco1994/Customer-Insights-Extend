"""Only generated test keys and synthetic claims; no JWKS/Dataverse/SQL calls."""

from datetime import timedelta
from io import BytesIO
import json
import os
import threading
import unittest
from unittest.mock import Mock, patch
from zipfile import ZipFile

import azure.functions as func
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt
import requests

import build_zip
import function_app
from renegociacao.core import ApiError, ProjectionError, utcnow
from renegociacao.dataverse import NativeProjection
from renegociacao import security
from tests.memory import CONTACT, CONTRACT, OTHER, MemoryRepository


AUDIENCE = "77777777-7777-4777-8777-777777777777"
OBJECT_ID = "88888888-8888-4888-8888-888888888888"
WORKFLOW = "99999999-9999-4999-8999-999999999999"


class EntraTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.public = cls.key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
        cls.jwk = {**json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(cls.key.public_key())),
                   "kid": "trusted-test-key", "use": "sig", "alg": "RS256"}

    def setUp(self):
        for context in (
            patch.dict(os.environ, {"AUTH_MODE": "entra", "ENTRA_AUDIENCE": AUDIENCE}, clear=True),
            patch("renegociacao.security._entra_keys", security.TenantJwksClient()),
        ):
            context.start()
            self.addCleanup(context.stop)
        transport = patch("renegociacao.security.requests.get")
        self.get = transport.start()
        self.addCleanup(transport.stop)
        self.get.return_value = Mock(status_code=200, content=b"synthetic-jwks",
                                     json=Mock(return_value={"keys": [self.jwk]}))

    def claims(self, **overrides):
        value = {"oid": OBJECT_ID, "tid": security.ENTRA_TENANT_ID, "iss": security.ENTRA_ISSUER,
                 "aud": AUDIENCE, "exp": utcnow() + timedelta(minutes=5),
                 "sub": "this-subject-is-not-a-contact-guid", "scp": "Renegotiate"}
        value.update(overrides)
        return value

    def token(self, claims=None, headers=None):
        return jwt.encode(claims if claims is not None else self.claims(), self.key, algorithm="RS256",
                          headers={"kid": "trusted-test-key", **(headers or {})})

    def authenticate(self, token):
        return security.authenticate({"Authorization": "Bearer " + token,
                                      "contactid": OTHER, "cpf": "11111111111", "userid": CONTACT})

    def test_verified_oid_issuer_not_sub_bind_the_entra_identity(self):
        identity = self.authenticate(self.token())
        self.assertEqual(identity, security.VerifiedIdentity(
            "entra", security.ENTRA_ISSUER, object_id=OBJECT_ID))
        self.assertIsNone(identity.contact_id)
        self.get.assert_called_once_with(security.ENTRA_JWKS_URL, timeout=(5, 10),
                                         verify=True, allow_redirects=False)
        # PyJWT caches trusted keys; it still verifies each token independently.
        self.authenticate(self.token())
        self.assertEqual(self.get.call_count, 1)

    def test_all_required_claims_and_exact_tid_audience_issuer_oid(self):
        for field in ("exp", "iss", "aud", "oid", "tid"):
            claims = self.claims()
            del claims[field]
            with self.subTest(missing=field), self.assertRaises(ApiError) as error:
                self.authenticate(self.token(claims))
            self.assertEqual(error.exception.http, 401)
        for override in (
            {"tid": OTHER}, {"aud": OTHER}, {"aud": [AUDIENCE]},
            {"iss": security.ENTRA_ISSUER + "/"}, {"oid": "not-guid"},
            {"oid": None}, {"oid": "00000000-0000-0000-0000-000000000000"},
            {"oid": "{" + OBJECT_ID + "}"}, {"exp": 1},
        ):
            with self.subTest(override=override), self.assertRaises(ApiError) as error:
                self.authenticate(self.token(self.claims(**override)))
            self.assertEqual(error.exception.http, 401)

    def test_exact_delegated_scope_and_app_only_rejection(self):
        for override in (
            {"scp": None}, {"scp": ""}, {"scp": "renegotiate"},
            {"scp": "Renegotiate.All"}, {"scp": ["Renegotiate"]},
            {"idtyp": "app"}, {"scp": None, "roles": ["Renegotiate"], "idtyp": "app"},
        ):
            with self.subTest(override=override), self.assertRaises(ApiError) as error:
                self.authenticate(self.token(self.claims(**override)))
            self.assertEqual((error.exception.http, error.exception.code), (403, "DELEGATED_SCOPE_REQUIRED"))
        self.assertEqual(self.authenticate(
            self.token(self.claims(scp="Other Renegotiate More", idtyp="user"))).object_id, OBJECT_ID)

    def test_rs256_and_signature_and_key_id_are_required(self):
        claims = self.claims()
        invalid = [
            jwt.encode(claims, "untrusted-hmac-secret-value-123456", algorithm="HS256",
                       headers={"kid": "trusted-test-key"}),
            self.token(headers={"kid": "../../untrusted"}),
            self.token(headers={"kid": ""}),
            self.token(headers={"crit": ["unsupported-extension"]}),
        ]
        for token in invalid:
            with self.assertRaises(ApiError) as error:
                self.authenticate(token)
            self.assertEqual(error.exception.http, 401)
        self.get.assert_not_called()
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        wrong_signature = jwt.encode(claims, other_key, algorithm="RS256", headers={"kid": "trusted-test-key"})
        with self.assertRaises(ApiError) as error:
            self.authenticate(wrong_signature)
        self.assertEqual(error.exception.http, 401)

    def test_key_url_and_issuer_are_pinned_not_supplied_by_token(self):
        identity = self.authenticate(self.token(headers={"jku": "https://attacker.example/keys"}))
        self.assertEqual(identity.object_id, OBJECT_ID)
        self.assertEqual(self.get.call_args.args[0], security.ENTRA_JWKS_URL)
        with self.assertRaises(ApiError):
            self.authenticate(self.token(self.claims(iss="https://attacker.example/v2.0")))

    def test_jwks_unavailable_redirect_or_bad_data_never_falls_back(self):
        for response in (
            Mock(status_code=302, content=b"", headers={"Location": "https://attacker.example/"}),
            Mock(status_code=200, content=b"bad", json=Mock(return_value=[])),
        ):
            self.get.return_value = response
            with patch("renegociacao.security.authenticate_portal") as portal:
                with self.assertRaises(ApiError) as error:
                    self.authenticate(self.token())
            self.assertEqual((error.exception.http, error.exception.code), (503, "IDENTITY_KEYS_UNAVAILABLE"))
            portal.assert_not_called()
        self.get.side_effect = requests.ConnectionError("synthetic network failure")
        with self.assertRaises(ApiError) as error:
            self.authenticate(self.token())
        self.assertEqual(error.exception.http, 503)

    def test_modes_never_fallback_and_unknown_modes_fail_closed(self):
        portal_environment = {"PORTAL_PUBLIC_KEY_PEM": self.public, "PORTAL_ISSUER": "https://portal.example/",
                              "PORTAL_AUDIENCE": "portal-demo"}
        portal_claims = {"sub": CONTACT, "exp": utcnow() + timedelta(minutes=5),
                         "iss": "https://portal.example/", "aud": "portal-demo"}
        portal_token = self.token(portal_claims)
        with patch.dict(os.environ, portal_environment), patch("renegociacao.security.authenticate_portal") as portal:
            with self.assertRaises(ApiError) as error:
                self.authenticate(portal_token)
            self.assertEqual(error.exception.http, 401)
            portal.assert_not_called()
        self.get.reset_mock()
        with patch.dict(os.environ, {**portal_environment, "AUTH_MODE": "portal"}):
            with patch("renegociacao.security.authenticate_entra") as entra:
                with self.assertRaises(ApiError) as error:
                    self.authenticate(self.token())
                self.assertEqual(error.exception.http, 401)
                entra.assert_not_called()
            identity = self.authenticate(portal_token)
            self.assertEqual(identity.contact_id, CONTACT)
        self.get.assert_not_called()
        for mode in ("", "managed_identity", "azure_cli", "ENTRA", "unknown"):
            with patch.dict(os.environ, {"AUTH_MODE": mode}), self.assertRaises(ApiError) as error:
                self.authenticate(self.token())
            self.assertEqual(error.exception.http, 503)

    def test_entra_audience_configuration_is_required_and_not_the_portal_audience(self):
        for audience in ("", "api://not-the-v2-client-id", "{" + AUDIENCE + "}"):
            with patch.dict(os.environ, {"ENTRA_AUDIENCE": audience,
                                       "PORTAL_AUDIENCE": AUDIENCE}), self.assertRaises(ApiError) as error:
                self.authenticate(self.token())
            self.assertEqual(error.exception.http, 503)
        self.get.assert_not_called()

    def test_every_function_action_passes_the_resolved_contact_not_claim_or_body_ids(self):
        for action in ("profile", "contracts", "offers", "start", "review", "confirm", "status"):
            backend = Mock()
            backend.crm.resolve_entra_contact.return_value = CONTACT
            backend.execute.return_value = ({"status": "ok", "data": {}, "demonstration": True}, 200)
            request = func.HttpRequest(
                "POST", "https://demo/api/renegociacao/" + action,
                headers={"Authorization": "Bearer " + self.token(), "contactid": OTHER,
                         "content-type": "application/json"},
                body=json.dumps({"contactId": OTHER}).encode(), route_params={"action": action})
            with patch("function_app.service", return_value=backend):
                response = function_app.customer(request)
            self.assertEqual(response.status_code, 200)
            backend.crm.resolve_entra_contact.assert_called_once_with(OBJECT_ID, security.ENTRA_ISSUER)
            backend.execute.assert_called_once_with(action, CONTACT, {"contactId": OTHER})

    def test_scope_failure_stops_before_service_and_no_contact_mapping_is_fabricated(self):
        request = func.HttpRequest(
            "POST", "https://demo/api/renegociacao/contracts",
            headers={"Authorization": "Bearer " + self.token(self.claims(scp=None)),
                     "content-type": "application/json"}, body=b"{}", route_params={"action": "contracts"})
        with patch("function_app.service") as factory:
            response = function_app.customer(request)
        self.assertEqual(response.status_code, 403)
        factory.assert_not_called()
        request = func.HttpRequest(
            "POST", "https://demo/api/renegociacao/contracts",
            headers={"Authorization": "Bearer " + self.token(self.claims(oid=OTHER)),
                     "content-type": "application/json"}, body=b"{}", route_params={"action": "contracts"})
        backend = Mock()
        backend.crm.resolve_entra_contact.side_effect = ApiError("ACCESS_DENIED", "No binding.", 403)
        with patch("function_app.service", return_value=backend):
            response = function_app.customer(request)
        self.assertEqual(response.status_code, 403)
        backend.execute.assert_not_called()

    def test_cloud_credentials_are_independent_of_inbound_entra_mode(self):
        with patch.dict(os.environ, {"WEBSITE_HOSTNAME": "demo.azurewebsites.net"}):
            self.assertEqual(type(security.credential()).__name__, "ManagedIdentityCredential")
        with patch.dict(os.environ, {"AZURE_CREDENTIAL_MODE": "azure_cli",
                                    "ALLOW_LOCAL_AZURE_CLI": "1"}):
            self.assertEqual(type(security.credential()).__name__, "AzureCliCredential")


class ContactBindingTests(unittest.TestCase):
    def setUp(self):
        self.projector = NativeProjection.__new__(NativeProjection)
        self.projector._lock = threading.RLock()
        self.projector._metadata = Mock()
        self.projector.client = Mock()
        self.record = {"contactid": CONTACT, "fsi_authobjectid": OBJECT_ID,
                       "fsi_authissuer": security.ENTRA_ISSUER, "statecode": 0}
        self.projector.client.records.list.return_value = [self.record]

    def test_exact_unique_server_managed_binding_and_optimized_query(self):
        self.assertEqual(self.projector.resolve_entra_contact(OBJECT_ID, security.ENTRA_ISSUER), CONTACT)
        self.projector.client.records.list.assert_called_once_with(
            "contact", select=["contactid", "fsi_authobjectid", "fsi_authissuer", "statecode"],
            filter=f"fsi_authobjectid eq '{OBJECT_ID}' and fsi_authissuer eq '{security.ENTRA_ISSUER}'", top=2)
        self.projector.client.records.create.assert_not_called()
        self.projector.client.records.update.assert_not_called()

    def test_missing_duplicate_inactive_or_inexact_binding_is_forbidden(self):
        for records in (
            [], [self.record, self.record], [self.record, {**self.record, "statecode": 1}],
            [{**self.record, "statecode": 1}],
            [{**self.record, "fsi_authobjectid": OTHER}],
            [{**self.record, "fsi_authissuer": security.ENTRA_ISSUER + "/"}],
        ):
            self.projector.client.records.list.return_value = records
            with self.subTest(records=len(records)), self.assertRaises(ApiError) as error:
                self.projector.resolve_entra_contact(OBJECT_ID, security.ENTRA_ISSUER)
            self.assertEqual(error.exception.http, 403)
        self.projector.client.records.retrieve.assert_not_called()

    def test_missing_mapping_metadata_or_dataverse_error_is_explicit_unavailable(self):
        self.projector._metadata.field.side_effect = ProjectionError("SCHEMA_FIELD_NOT_FOUND")
        with self.assertRaises(ApiError) as error:
            self.projector.resolve_entra_contact(OBJECT_ID, security.ENTRA_ISSUER)
        self.assertEqual(error.exception.http, 503)
        self.projector.client.records.list.assert_not_called()

    def test_cpf_reads_only_confirmed_contact_field_after_resolved_identity(self):
        self.projector.client.records.retrieve.return_value = {
            "contactid": CONTACT, "fullname": "Pessoa fictícia", "fsi_cpf": "00000000000", "statecode": 0}
        with patch.dict(os.environ, {}, clear=True):
            result = self.projector.profile(CONTACT, "000.000.000-00")
        self.assertTrue(result["cpfVerified"])
        self.projector.client.records.retrieve.assert_called_once_with(
            "contact", CONTACT, select=["contactid", "fullname", "fsi_cpf", "statecode"])
        with patch.dict(os.environ, {"CPF_FIELD": "emailaddress1"}), self.assertRaises(ApiError):
            self.projector.profile(CONTACT, "00000000000")


class WorkflowAndPackageTests(unittest.TestCase):
    def test_case_uses_reason_without_treating_legacy_processid_as_bpf_authority(self):
        projector = NativeProjection.__new__(NativeProjection)
        projector._metadata, projector._lock = Mock(), threading.RLock()
        projector._metadata.contact_attribute.return_value = "fsi_clienteid"
        projector._metadata.bind.side_effect = lambda entity, attr, target, identifier: {
            attr + "@odata.bind": "/" + target + "(" + identifier + ")"}
        projector.client = Mock()
        projector.client.records.retrieve.return_value = {
            "_fsi_clienteid_value": CONTACT, "_fsi_linhanegocioid_value": OTHER}
        projector.workflow_id = Mock(return_value=WORKFLOW)
        projector.reason = Mock(return_value=OTHER)
        projector._save = Mock()
        projector._bpf = Mock(return_value="pending")
        request = MemoryRepository().start(CONTACT, CONTRACT, "page")
        self.assertEqual(projector._project(request), "pending")
        case_call = projector._save.call_args_list[0]
        self.assertEqual(case_call.args[0], "incident")
        payload = case_call.args[2]
        self.assertNotIn("processid", payload)
        self.assertIn("fsi_motivoid@odata.bind", payload)
        self.assertEqual(case_call.args[3]["_fsi_motivoid_value"], OTHER)

    def test_case_reconciliation_preserves_reason_and_operator_title(self):
        projector = NativeProjection.__new__(NativeProjection)
        projector.client = Mock()
        ownership = {"_customerid_value": CONTACT, "_fsi_motivoid_value": OTHER}
        projector.client.records.retrieve.return_value = {
            "incidentid": CONTRACT, "title": "Operator's description", **ownership,
        }
        projector._save("incident", CONTRACT, {
            "title": "Initial generated description",
            "fsi_motivoid@odata.bind": f"/fsi_motivos({OTHER})",
        }, ownership)
        projector.client.records.update.assert_not_called()
        projector.client.records.create.assert_not_called()

    def test_workflow_pin_is_validated_and_cached_without_guessing_id_or_writing(self):
        projector = NativeProjection.__new__(NativeProjection)
        projector._lock, projector._workflow, projector.client = threading.RLock(), None, Mock()
        projector.client.records.retrieve.return_value = {
            "workflowid": WORKFLOW, "uniquename": "fsi_processorenegociacao", "statecode": 1}
        with patch.dict(os.environ, {"BPF_WORKFLOW_ID": WORKFLOW}, clear=True):
            self.assertEqual(projector.workflow_id(), WORKFLOW)
            self.assertEqual(projector.workflow_id(), WORKFLOW)
        self.assertEqual(projector.client.records.retrieve.call_count, 1)
        projector.client.records.update.assert_not_called()
        projector._workflow = None
        projector.client.records.retrieve.return_value["uniquename"] = "another_process"
        with patch.dict(os.environ, {"BPF_WORKFLOW_ID": WORKFLOW}), self.assertRaises(ProjectionError):
            projector.workflow_id()

    def test_zip_has_exact_runtime_file_manifest_and_no_configuration_or_credentials(self):
        with ZipFile(BytesIO(build_zip.content())) as archive:
            self.assertEqual(archive.namelist(), list(build_zip.FILES))
            self.assertIn("host.json", archive.namelist())
            self.assertIn("schema.sql", archive.namelist())
            for name in archive.namelist():
                self.assertFalse(name.startswith((".venv/", "tests/", ".python_packages/")))
                self.assertNotIn("settings", name)
                info = archive.getinfo(name)
                self.assertEqual(info.create_system, 3)
                self.assertEqual(info.external_attr >> 16, 0o100644)
        settings = json.loads((build_zip.ROOT / "deployment.settings.example.json").read_text())
        self.assertEqual(settings["AUTH_MODE"], "entra")
        self.assertEqual(settings["AZURE_CREDENTIAL_MODE"], "managed_identity")
        self.assertEqual(settings["CPF_FIELD"], "fsi_cpf")
        self.assertEqual(settings["BOOTSTRAP_ENABLED"], "0")


if __name__ == "__main__":
    unittest.main()
