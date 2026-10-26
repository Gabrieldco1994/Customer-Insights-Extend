from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
import json
import os
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4
from xml.etree import ElementTree as ET

import azure.functions as func
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import jwt

import function_app
from renegociacao import bootstrap, odata
from renegociacao.core import (ApiError, ProjectionError, Service, canonical, guid,
                               public_request, review_hash, utcnow)
from renegociacao.dataverse import Metadata, NativeProjection, retry
from renegociacao.security import authenticate, credential
from tests.memory import ANCHOR, CONTACT, CONTRACT, OFFER, OTHER, FakeCrm, MemoryRepository


class IdentityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.public = cls.key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()

    def setUp(self):
        self.environment = patch.dict(os.environ, {
            "PORTAL_PUBLIC_KEY_PEM": self.public, "PORTAL_ISSUER": "https://portal.example/",
            "PORTAL_AUDIENCE": "demo-api"}, clear=True)
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def token(self, **overrides):
        claims = {"sub": CONTACT.upper(), "iss": "https://portal.example/",
                  "aud": "demo-api", "exp": utcnow() + timedelta(minutes=5)}
        claims.update(overrides)
        return jwt.encode(claims, self.key, algorithm="RS256")

    def test_verified_subject_is_only_identity(self):
        token = self.token()
        self.assertEqual(authenticate({"Authorization": "Bearer " + token,
                                      "contactid": OTHER, "userid": OTHER, "cpf": "11111111111"}).contact_id, CONTACT)

    def test_reject_missing_expired_wrong_issuer_audience_subject(self):
        tokens = ["", self.token(exp=1), self.token(iss="evil"), self.token(aud="evil"),
                  self.token(sub="not-a-guid"), self.token(sub=str(uuid4()) + "x")]
        for token in tokens:
            with self.subTest(token_kind=len(token)), self.assertRaises(ApiError) as error:
                authenticate({"Authorization": "Bearer " + token})
            self.assertEqual(error.exception.http, 401)

    def test_all_required_claims_and_rs256_only(self):
        claims = {"sub": CONTACT, "iss": "https://portal.example/", "aud": "demo-api",
                  "exp": int((utcnow() + timedelta(minutes=1)).timestamp())}
        tokens = []
        for missing in claims:
            reduced = dict(claims)
            del reduced[missing]
            tokens.append(jwt.encode(reduced, self.key, algorithm="RS256"))
        tokens.append(jwt.encode(claims, "untrusted-hmac-secret-value-123456", algorithm="HS256"))
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        tokens.append(jwt.encode(claims, other_key, algorithm="RS256"))
        for token in tokens:
            with self.assertRaises(ApiError) as error:
                authenticate({"authorization": "Bearer " + token})
            self.assertEqual(error.exception.http, 401)

    def test_missing_or_invalid_configuration_never_bypasses(self):
        for name in ("PORTAL_PUBLIC_KEY_PEM", "PORTAL_ISSUER", "PORTAL_AUDIENCE"):
            with patch.dict(os.environ, {name: ""}), self.assertRaises(ApiError) as error:
                authenticate({"Authorization": "Bearer " + self.token()})
            self.assertEqual(error.exception.http, 503)
        with patch.dict(os.environ, {"PORTAL_PUBLIC_KEY_PEM": "bad-key"}), self.assertRaises(ApiError) as error:
            authenticate({})
        self.assertEqual(error.exception.http, 503)

    def test_every_action_fails_before_constructing_service_without_identity(self):
        for action in ("profile", "contracts", "offers", "start", "review", "confirm", "status"):
            request = func.HttpRequest("POST", "https://demo/api/renegociacao/" + action,
                                      headers={"contactid": CONTACT}, body=b"{}",
                                      route_params={"action": action})
            with patch("function_app.service") as factory:
                response = function_app.customer(request)
                self.assertEqual(response.status_code, 401)
                factory.assert_not_called()

    def test_host_anonymous_actions_are_separate_from_function_key_source(self):
        definitions = {}
        for function in function_app.app.get_functions():
            trigger = next(b for b in function.get_dict_repr()["bindings"] if b["type"] == "httpTrigger")
            definitions[function.get_function_name()] = trigger["authLevel"]
        self.assertEqual(definitions["CustomerRenegotiation"], func.AuthLevel.ANONYMOUS)
        self.assertEqual(definitions["ExternalSourceOData"], func.AuthLevel.FUNCTION)
        self.assertEqual(definitions["BootstrapDemoSource"], func.AuthLevel.FUNCTION)

    def test_local_cli_requires_explicit_opt_in_and_is_forbidden_in_cloud(self):
        with patch.dict(os.environ, {"AZURE_CREDENTIAL_MODE": "azure_cli"}), self.assertRaises(ApiError):
            credential()
        with patch.dict(os.environ, {"AZURE_CREDENTIAL_MODE": "azure_cli", "ALLOW_LOCAL_AZURE_CLI": "1",
                                    "WEBSITE_HOSTNAME": "demo.azurewebsites.net"}), self.assertRaises(ApiError):
            credential()
        with patch.dict(os.environ, {"AZURE_CREDENTIAL_MODE": "azure_cli", "ALLOW_LOCAL_AZURE_CLI": "1"}):
            self.assertEqual(type(credential()).__name__, "AzureCliCredential")


class JourneyTests(unittest.TestCase):
    def setUp(self):
        self.repo, self.crm = MemoryRepository(), FakeCrm()
        self.service = Service(self.repo, self.crm)

    def execute(self, action, body):
        return self.service.execute(action, CONTACT, body)

    def reviewed(self):
        start, _ = self.execute("start", {"contractId": CONTRACT, "channel": "page"})
        identifier = start["data"]["requestId"]
        review, _ = self.execute("review", {"requestId": identifier, "offerId": OFFER, "version": 1})
        self.assertEqual(set(review["data"]), {"requestId", "reviewHash", "offer"})
        return {**review["data"], "review": json.loads(self.repo.request["Snapshot"])}

    def test_cpf_is_only_a_check_against_signed_contact(self):
        result, _ = self.execute("profile", {"cpf": "000.000.000-00"})
        self.assertTrue(result["data"]["cpfVerified"])
        with self.assertRaises(ApiError) as error:
            self.execute("profile", {"cpf": "11111111111"})
        self.assertEqual(error.exception.http, 403)
        with self.assertRaises(ApiError):
            self.service.execute("profile", OTHER, {"cpf": "00000000000"})
        for body in ({"cpf": None}, {"contactId": OTHER}, {"userid": OTHER}, {"cpf": 0}):
            with self.assertRaises(ApiError):
                self.execute("profile", body)

    def test_money_is_exact_and_review_authoritative(self):
        reviewed = self.reviewed()
        snapshot = reviewed["review"]
        money = snapshot["downPaymentCents"] + snapshot["installmentCents"] * (
            snapshot["installmentCount"] - 1) + snapshot["finalInstallmentCents"]
        self.assertEqual(money, snapshot["totalCents"])
        self.assertEqual(money, 10001)
        self.assertEqual(reviewed["reviewHash"], review_hash(snapshot))
        for field in ("downPaymentCents", "installmentCents", "finalInstallmentCents", "totalCents"):
            self.assertIs(type(snapshot[field]), int)
        public = reviewed["offer"]
        self.assertEqual(set(public), {"id", "version", "entryCents", "installments", "paymentCents",
                                      "totalCents", "validUntil"})
        self.assertEqual(public["entryCents"] + public["installments"] * public["paymentCents"],
                         public["totalCents"])
        with self.assertRaises(ApiError):
            self.execute("review", {"requestId": reviewed["requestId"], "offerId": OFFER,
                                    "version": 1, "totalCents": 1})

    def test_start_and_review_cannot_use_other_contacts_contract_or_request(self):
        for action, body in (("start", {"contractId": OTHER, "channel": "chat"}),
                             ("offers", {"contractId": OTHER}),
                             ("status", {"contractId": OTHER})):
            with self.assertRaises(ApiError) as error:
                self.execute(action, body)
            self.assertEqual(error.exception.http, 403)
        reviewed = self.reviewed()
        for operation in (
            lambda: self.repo.review(OTHER, reviewed["requestId"], OFFER, 1),
            lambda: self.repo.confirm(OTHER, reviewed["requestId"], reviewed["reviewHash"], True),
            lambda: self.repo.status(OTHER, "requestId", reviewed["requestId"]),
        ):
            with self.assertRaises(ApiError) as error:
                operation()
            self.assertEqual(error.exception.http, 403)

    def test_review_version_is_integer_not_bool_and_identifiers_canonicalize(self):
        self.assertEqual(guid("{" + CONTRACT.upper() + "}"), CONTRACT)
        for value in (False, 1.0, "1", 0):
            reviewed = self.reviewed()
            with self.assertRaises(ApiError):
                self.execute("review", {"requestId": reviewed["requestId"], "offerId": OFFER, "version": value})
        for identifier in ("x", "", 12, "00000000-0000-0000-0000-000000000000"):
            with self.assertRaises(ApiError):
                guid(identifier)

    def test_confirm_requires_saved_review_and_literal_true(self):
        start, _ = self.execute("start", {"contractId": CONTRACT, "channel": "chat"})
        with self.assertRaises(ApiError) as error:
            self.execute("confirm", {"requestId": start["data"]["requestId"],
                                    "reviewHash": "a" * 64, "confirmed": True})
        self.assertEqual(error.exception.code, "REVIEW_REQUIRED")
        reviewed = self.reviewed()
        for value in (False, "true", 1, None):
            with self.assertRaises(ApiError) as error:
                self.execute("confirm", {"requestId": reviewed["requestId"], "reviewHash": reviewed["reviewHash"],
                                        "confirmed": value})
            self.assertEqual(error.exception.code, "EXPLICIT_CONFIRMATION_REQUIRED")
        self.assertEqual(len(self.repo.agreements), 0)

    def test_expiry_version_values_and_hash_are_validated_at_confirmation(self):
        for mutation, expected in (
            (lambda r: r.offer.update(ValidUntil=utcnow() - timedelta(seconds=1)), "OFFER_EXPIRED"),
            (lambda r: r.offer.update(Version=2), "OFFER_CHANGED"),
            (lambda r: r.offer.update(TotalCents=10002), "OFFER_CHANGED"),
        ):
            with self.subTest(expected=expected):
                self.setUp()
                reviewed = self.reviewed()
                mutation(self.repo)
                with self.assertRaises(ApiError) as error:
                    self.execute("confirm", {"requestId": reviewed["requestId"],
                                            "reviewHash": reviewed["reviewHash"], "confirmed": True})
                self.assertEqual(error.exception.code, expected)
                self.assertEqual(len(self.repo.agreements), 0)
        for invalid in ("f" * 64, "bad", None):
            with self.assertRaises(ApiError):
                self.execute("confirm", {"requestId": reviewed["requestId"], "reviewHash": invalid, "confirmed": True})

    def test_cross_channel_threaded_start_and_confirm_are_idempotent_model(self):
        def start(index):
            return self.repo.start(CONTACT, CONTRACT, "chat" if index % 2 else "page")["Id"]
        with ThreadPoolExecutor(max_workers=12) as executor:
            identifiers = list(executor.map(start, range(60)))
        self.assertEqual(len(set(identifiers)), 1)
        reviewed = self.reviewed()
        def confirm(_):
            return self.repo.confirm(CONTACT, reviewed["requestId"], reviewed["reviewHash"], True)["Protocol"]
        with ThreadPoolExecutor(max_workers=12) as executor:
            protocols = list(executor.map(confirm, range(60)))
        self.assertEqual(len(set(protocols)), 1)
        self.assertEqual(len(self.repo.agreements), 1)
        self.assertFalse(self.repo.contract["Eligible"])
        self.assertEqual(self.repo.offers(CONTACT, CONTRACT), [])
        self.assertEqual(self.repo.start(CONTACT, CONTRACT, "chat")["Id"], reviewed["requestId"])

    def test_durable_source_success_native_failure_is_processing_until_reconciled(self):
        reviewed = self.reviewed()
        self.crm.fail = True
        body = {"requestId": reviewed["requestId"], "reviewHash": reviewed["reviewHash"], "confirmed": True}
        result, code = self.execute("confirm", body)
        request = result["data"]
        self.assertEqual((code, result["status"]), (202, "processing"))
        self.assertEqual(self.repo.request["SourceState"], "finalized")
        self.assertEqual(self.repo.request["ProjectionState"], "pending")
        self.assertTrue(request["protocol"].startswith("DEMO-"))
        snapshot = deepcopy_json(request["offer"])
        self.crm.fail = False
        self.crm.pending = True
        result, code = self.execute("status", {"contractId": CONTRACT})
        self.assertEqual((code, result["status"]), (202, "processing"))
        self.assertEqual(self.repo.request["BpfState"], "pending")
        self.crm.pending = False
        result, code = self.execute("status", {"requestId": reviewed["requestId"]})
        self.assertEqual((code, result["status"], result["data"]["status"]), (200, "ok", "completed"))
        self.assertEqual(result["data"]["offer"], snapshot)
        # Retry after expiry cannot create another agreement.
        self.repo.offer["ValidUntil"] = utcnow() - timedelta(days=1)
        replay, _ = self.execute("confirm", body)
        self.assertEqual(replay["data"]["protocol"], request["protocol"])
        self.assertEqual(len(self.repo.agreements), 1)
        with self.assertRaises(ApiError):
            self.execute("review", {"requestId": reviewed["requestId"], "offerId": OFFER, "version": 1})

    def test_pending_bpf_is_not_invented_as_active_or_complete(self):
        self.crm.pending = True
        result, code = self.execute("start", {"contractId": CONTRACT, "channel": "page"})
        self.assertEqual((code, result["status"]), (202, "processing"))
        self.assertEqual(result["data"]["status"], "draft")
        self.assertEqual(self.repo.request["BpfState"], "pending")
        self.assertIsNone(self.repo.request["Protocol"])

    def test_status_selector_and_input_schema_are_strict(self):
        for body in ({}, {"requestId": CONTRACT, "contractId": CONTRACT}, {"userid": CONTACT}):
            with self.assertRaises(ApiError):
                self.execute("status", body)
        result, _ = self.execute("status", {"contractId": CONTRACT})
        self.assertIsNone(result["data"]["requestId"])
        self.assertEqual(result["data"]["status"], "not_started")
        with self.assertRaises(ApiError):
            self.execute("contracts", {"contactid": OTHER})
        with self.assertRaises(ApiError):
            self.execute("start", {"contractId": CONTRACT, "channel": "email"})

    def test_exact_standardized_contract_offer_start_and_error_envelopes(self):
        contracts, _ = self.execute("contracts", {})
        self.assertEqual(set(contracts), {"status", "data", "demonstration"})
        self.assertEqual(contracts["status"], "ok")
        self.assertEqual(set(contracts["data"]), {"contracts"})
        self.assertEqual(set(contracts["data"]["contracts"][0]),
                         {"id", "number", "vehicle", "debtCents", "eligible"})
        offers, _ = self.execute("offers", {"contractId": CONTRACT})
        self.assertEqual(set(offers["data"]), {"offers"})
        self.assertEqual(offers["data"]["offers"][0]["id"], OFFER)
        started, _ = self.execute("start", {"contractId": CONTRACT, "channel": "chat"})
        self.assertEqual(set(started["data"]), {"requestId", "status"})
        error = json.loads(function_app.failure(ApiError("DEMO_ERROR", "Safe.", 403)).get_body())
        self.assertEqual(error, {"error": {"code": "DEMO_ERROR", "message": "Safe."}, "demonstration": True})


def deepcopy_json(value):
    return json.loads(json.dumps(value))


class ODataTests(unittest.TestCase):
    def setUp(self):
        self.root = "https://source.example/api/odata"
        self.repo = Mock()
        self.item = {"Id": CONTRACT, "Name": "DEMO-001", "ClientRef": CONTACT, "ProductRef": ANCHOR,
                     "Number": "DEMO-001", "Vehicle": "A and B's DEMO", "DebtCents": 10001,
                     "Eligible": True, "PositionDate": utcnow()}
        self.repo.odata.return_value = [self.item]

    def test_metadata_and_service_document_are_valid_v4(self):
        xml, mime = odata.read(self.repo, "$metadata", {}, self.root)
        root = ET.fromstring(xml)
        ns = {"e": "http://docs.oasis-open.org/odata/ns/edm"}
        self.assertEqual(mime, "application/xml")
        self.assertEqual(root.attrib["Version"], "4.0")
        sets = root.findall(".//e:EntitySet", ns)
        self.assertEqual({node.attrib["Name"] for node in sets}, {"Contracts", "Installments"})
        doc, _ = odata.read(self.repo, "", {}, self.root)
        self.assertEqual(len(doc["value"]), 2)
        self.repo.odata.assert_not_called()
        for name, expected in (("Contract", odata.CONTRACTS), ("Installment", odata.INSTALLMENTS)):
            entity = next(n for n in root.findall(".//e:EntityType", ns) if n.attrib["Name"] == name)
            actual = {n.attrib["Name"]: n.attrib["Type"] for n in entity.findall("e:Property", ns)}
            self.assertEqual(actual, {k: "Edm." + v[1] for k, v in expected.items()})

    def test_whitelisted_typed_conjunction_is_parameterized(self):
        text = f"ClientRef eq '{CONTACT}' and Eligible eq true and DebtCents eq 10001"
        parsed = odata.filters(text, odata.CONTRACTS)
        self.assertEqual(parsed, [("ClientRef", CONTACT), ("Eligible", True), ("DebtCents", 10001)])
        parsed = odata.filters("Vehicle eq 'A and B''s DEMO'", odata.CONTRACTS)
        self.assertEqual(parsed, [("Vehicle", "A and B's DEMO")])
        injection = odata.filters("Vehicle eq '''; DROP TABLE demo.Contracts;--'", odata.CONTRACTS)
        self.assertEqual(injection[0][1], "'; DROP TABLE demo.Contracts;--")
        self.assertEqual(odata.filters(f"Id eq {CONTRACT}", odata.CONTRACTS), [("Id", CONTRACT)])

    def test_unsupported_filters_are_explicit_errors_not_silently_ignored(self):
        for expression in (
            "DebtCents gt 1", "Eligible eq true or Eligible eq false", "contains(Name,'a')",
            "(Eligible eq true)", "Unknown eq 'x'", "DebtCents eq '100'",
            "DebtCents eq 1.5", "DebtCents eq 999999999999", "Eligible eq 1",
            "ClientRef eq 'not-a-guid'", "PositionDate eq yesterday", "Name eq 'abc' and ",
            "Name eq 'abc'; DROP TABLE demo.Contracts", "Id eq '33333333-3333-4333-8333-333333333333'",
        ):
            with self.subTest(expression=expression), self.assertRaises(ApiError) as error:
                odata.filters(expression, odata.CONTRACTS)
            self.assertEqual(error.exception.code, "ODATA_UNSUPPORTED_QUERY")
        for query in ({"$orderby": "Id"}, {"$expand": "*"}, {"$skip": "-1"}, {"$top": "1e4"}):
            with self.assertRaises(ApiError):
                odata.read(self.repo, "Contracts", query, self.root)

    def test_guid_key_read_and_missing_key(self):
        item, _ = odata.read(self.repo, f"Contracts({CONTRACT})", {}, self.root)
        self.assertEqual(item["Id"], CONTRACT)
        self.repo.odata.assert_called_with("Contracts", odata.CONTRACTS, [("Id", CONTRACT)], 0, 1)
        self.repo.odata.return_value = []
        with self.assertRaises(ApiError) as error:
            odata.read(self.repo, f"Contracts({CONTRACT})", {}, self.root)
        self.assertEqual(error.exception.http, 404)

    def test_pagination_retains_filter_top_and_skip_without_secrets(self):
        self.repo.odata.return_value = [self.item] * 101
        query = {"$skip": "10", "$filter": f"ClientRef eq '{CONTACT}'"}
        first, _ = odata.read(self.repo, "Contracts", query, self.root)
        self.assertEqual(len(first["value"]), 100)
        next_query = parse_qs(urlsplit(first["@odata.nextLink"]).query)
        self.assertNotIn("$top", next_query)
        self.assertEqual(next_query["$skip"], ["110"])
        self.assertEqual(next_query["$filter"], [query["$filter"]])
        self.assertNotIn("code=", first["@odata.nextLink"])
        default, _ = odata.read(self.repo, "Contracts", {}, self.root)
        self.assertEqual(len(default["value"]), 100)
        self.assertIn("@odata.nextLink", default)
        limited, _ = odata.read(self.repo, "Contracts", {"$top": "5"}, self.root)
        self.assertEqual(len(limited["value"]), 5)
        self.assertNotIn("@odata.nextLink", limited)
        self.repo.reset_mock()
        zero, _ = odata.read(self.repo, "Contracts", {"$top": "0"}, self.root)
        self.assertEqual(zero["value"], [])
        self.repo.odata.assert_not_called()


class BootstrapCursor:
    def __init__(self):
        self.contracts, self.installments, self.offers = {}, {}, {}
        self.result = None

    def execute(self, sql, params=()):
        if sql.startswith("SELECT ContactId"):
            self.result = self.contracts.get(params[0])
        elif sql.startswith("INSERT INTO demo.Contracts"):
            self.contracts[params[0]] = dict(zip(
                ["Id", "ContactId", "AnchorId", "Number", "Vehicle", "DebtCents", "Eligible", "PositionDate"], params))
        elif sql.startswith("IF NOT EXISTS") and "demo.Installments" in sql:
            self.installments.setdefault(params[0], params)
        elif sql.startswith("IF NOT EXISTS") and "demo.Offers" in sql:
            self.offers.setdefault(params[0], params)
        elif sql.startswith("SELECT COUNT_BIG"):
            self.result = {"Total": len(self.installments)}

    def fetchone(self):
        return self.result


class BootstrapRepository:
    def __init__(self):
        self.cursor = BootstrapCursor()

    @contextmanager
    def transaction(self):
        yield self.cursor


class BootstrapTests(unittest.TestCase):
    def fixture(self):
        return {"contactId": CONTACT, "anchorId": ANCHOR, "contractId": CONTRACT,
                "number": "DEMO-001", "vehicle": "Veículo fictício", "debtCents": 10001, "eligible": True}

    def test_at_least_2000_installments_exact_money_idempotent_no_eligibility_reset(self):
        repo = BootstrapRepository()
        body = {"fixtures": [self.fixture()]}
        result = bootstrap.initialize(repo, body)
        self.assertGreaterEqual(result["installmentRows"], 2000)
        self.assertEqual(sum(p[5] for p in repo.cursor.installments.values()), 10001)
        self.assertEqual(len(repo.cursor.offers), 2)
        for p in repo.cursor.offers.values():
            self.assertEqual(p[3] + p[4] * (p[6] - 1) + p[5], p[7])
        repo.cursor.contracts[CONTRACT]["Eligible"] = False
        repeat = bootstrap.initialize(repo, body)
        self.assertEqual(result, repeat)
        self.assertFalse(repo.cursor.contracts[CONTRACT]["Eligible"])
        changed = {**self.fixture(), "debtCents": 1}
        with self.assertRaises(ApiError) as error:
            bootstrap.initialize(repo, {"fixtures": [changed]})
        self.assertEqual(error.exception.code, "FIXTURE_CONFLICT")

    def test_bootstrap_disabled_even_if_invoked_directly_with_body(self):
        request = func.HttpRequest("POST", "https://demo/api/bootstrap", body=b"{}")
        with patch.dict(os.environ, {"BOOTSTRAP_ENABLED": "0"}), patch("function_app.credential") as factory:
            response = function_app.bootstrap_source(request)
        self.assertEqual(response.status_code, 403)
        factory.assert_not_called()

    def test_guarded_bootstrap_fixture_and_identity_validation(self):
        for body in ({}, {"fixtures": []}, {"fixtures": [{**self.fixture(), "eligible": 1}]},
                     {"fixtures": [self.fixture(), self.fixture()]},
                     {"fixtures": [self.fixture()], "managedIdentityName": "mi"},
                     {"fixtures": [self.fixture()], "managedIdentityName": "x];DROP",
                      "managedIdentityClientId": CONTACT}):
            with self.assertRaises(ApiError):
                bootstrap.validate(body)


class NativeProjectionTests(unittest.TestCase):
    def test_metadata_binding_preserves_schema_case_and_real_entity_sets(self):
        csdl = """<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
        <edmx:DataServices><Schema Namespace="Microsoft.Dynamics.CRM"
          xmlns="http://docs.oasis-open.org/odata/ns/edm">
          <EntityType Name="incident"><NavigationProperty Name="customerid_contact"
          Type="Microsoft.Dynamics.CRM.contact">
          <ReferentialConstraint Property="_customerid_value" ReferencedProperty="contactid"/>
          </NavigationProperty></EntityType>
          <EntityType Name="fsi_produtocliente"><NavigationProperty Name="fsi_ClienteCustom"
          Type="Microsoft.Dynamics.CRM.contact">
          <ReferentialConstraint Property="_fsi_cliente_custom_value" ReferencedProperty="contactid"/>
          </NavigationProperty></EntityType>
          <EntityContainer Name="Default"><EntitySet Name="contacts" EntityType="Microsoft.Dynamics.CRM.contact"/>
          </EntityContainer></Schema></edmx:DataServices></edmx:Edmx>"""
        metadata = Metadata(csdl)
        self.assertEqual(metadata.bind("incident", "customerid", "contact", CONTACT),
                         {"customerid_contact@odata.bind": f"/contacts({CONTACT})"})
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(metadata.contact_attribute("fsi_produtocliente"), "fsi_cliente_custom")
        with self.assertRaises(ProjectionError):
            metadata.bind("incident", "unknown", "contact", CONTACT)

    def test_missing_bpf_stage_map_is_explicit_pending_not_invented(self):
        projector = NativeProjection.__new__(NativeProjection)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(projector._bpf({}), "pending")

    def test_only_known_transient_http_errors_are_retried(self):
        from PowerPlatform.Dataverse.core.errors import HttpError
        for status, attempts in ((400, 1), (401, 1), (403, 1), (409, 1), (429, 3), (503, 3)):
            operation = Mock(side_effect=HttpError("safe", status_code=status))
            with patch("renegociacao.dataverse.time.sleep"), self.assertRaises(HttpError):
                retry(operation)
            self.assertEqual(operation.call_count, attempts)

    def test_unknown_create_conflict_is_not_disguised_as_duplicate(self):
        from PowerPlatform.Dataverse.core.errors import HttpError
        projector = NativeProjection.__new__(NativeProjection)
        projector.client = Mock()
        projector.client.records.retrieve.return_value = None
        projector.client.records.create.side_effect = HttpError("conflict", status_code=409)
        with self.assertRaises(HttpError):
            projector._save("incident", CONTRACT, {"title": "DEMO"}, {})
        projector.client.records.update.assert_not_called()

    def test_confirmed_duplicate_is_recovered_but_ownership_is_checked(self):
        from PowerPlatform.Dataverse.core.errors import HttpError
        projector = NativeProjection.__new__(NativeProjection)
        projector.client = Mock()
        projector.client.records.retrieve.side_effect = [None, {"_customerid_value": OTHER}]
        projector.client.records.create.side_effect = HttpError(
            "duplicate", status_code=409, service_error_code="0x80040237")
        with self.assertRaises(ProjectionError):
            projector._save("incident", CONTRACT, {"title": "DEMO"}, {"_customerid_value": CONTACT})
        projector.client.records.update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
