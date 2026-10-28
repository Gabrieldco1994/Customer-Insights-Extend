from datetime import timedelta
import json
import os
import unittest
from unittest.mock import Mock, patch, MagicMock

import jwt

from renegociacao import assisted
from renegociacao.core import ApiError, canonical, review_hash, utcnow
from renegociacao.security import VerifiedIdentity


class AssistedBoundary(unittest.TestCase):
    def setUp(self):
        self.contract = next(iter(assisted.CONTRACTS))
        self.request = "c460d87e-c2ec-4ec3-9299-3e743f29f9c4"
        self.base = {"id": self.request, "version": 1}
        self.body = {"contractId": self.contract, "entryCents": 0, "discountCents": 100,
                     "installments": 6, "reason": "Cliente solicita parcelas compativeis."}
        self.terms = assisted.proposal(self.base, 10001, self.body)
        self.backend = Mock()
        self.current = {"RequestId": self.request, "OperatorContactId": "operator",
                        "Proposal": canonical(self.terms), "Nonce": "secret-nonce",
                        "ApprovedAt": utcnow().replace(microsecond=0)}
        self.settings = {"ASSISTED_DEMO_ENABLED": "1", "ASSISTED_ACCEPTANCE_SECRET": "x" * 64}

    def test_exact_cents_and_rounding_are_explicit_in_preview(self):
        p = self.terms
        self.assertEqual(p["entryCents"] + p["installments"] * p["paymentCents"], 9901)
        self.assertEqual(p["roundingCents"], 1)
        self.assertEqual(p["requestedEntryCents"], 0)
        for field, value in [("discountCents", 10001), ("discountCents", -1),
                             ("installments", 0), ("installments", True), ("installments", 361),
                             ("entryCents", 9901), ("reason", "curto")]:
            with self.subTest(field=field), self.assertRaises(ApiError):
                assisted.proposal(self.base, 10001, {**self.body, field: value})
        readable = assisted.proposal_text(self.terms)
        self.assertIn("Entrada: R$ 0,01", readable)
        self.assertIn("6 de R$ 16,50", readable)
        self.assertIn("Total: R$ 99,01", readable)
        self.assertNotIn("discountCents", readable)

    def test_operator_requires_verified_entra_allowlist_not_customer_id(self):
        with patch.dict(os.environ, {**self.settings, "ASSISTED_OPERATOR_OIDS": self.request}):
            for identity in [VerifiedIdentity("portal", "issuer", contact_id=self.request),
                             VerifiedIdentity("entra", "issuer", object_id=self.contract)]:
                with self.assertRaises(ApiError):
                    assisted.operator(self.backend, identity)
            self.backend.crm.resolve_entra_contact.assert_not_called()
            assisted.operator(self.backend, VerifiedIdentity("entra", "issuer", object_id=self.request))
            self.backend.crm.resolve_entra_contact.assert_called_once_with(self.request, "issuer")

    def test_arbitrary_customer_and_confirm_are_not_operator_actions(self):
        for action, body in [("contracts", {"contactId": "chosen-by-caller"}), ("confirm", {})]:
            with self.assertRaises(ApiError):
                assisted.execute(self.backend, "operator", action, body)
        self.backend.execute.assert_not_called()

    def test_cpf_identifies_only_the_fixed_active_fictitious_customer(self):
        stored = {
            "firstname": "Cliente", "lastname": "Ficticio Portal Assistido",
            "emailaddress1": "assistido-demo@renegociacao.invalid", "statecode": 0,
            "fsi_authobjectid": None, "fsi_authissuer": None, "fsi_cpf": "80000000400",
        }
        self.backend.crm.client.records.retrieve.return_value = stored
        number, anchor = assisted.CONTRACTS[self.contract]
        rows = [{"Id": self.contract, "AnchorId": anchor, "Number": number, "Vehicle": "Ficticio",
                 "DebtCents": 10001, "Eligible": True, "AssistedRequestId": None},
                {"Id": "11111111-1111-4111-8111-111111111111", "Number": "REAL", "AnchorId": anchor}]
        with patch.object(assisted.Store, "portfolio", return_value=rows) as portfolio:
            for cpf in ["80000000400", "800.000.004-00"]:
                result = assisted.execute(self.backend, "operator", "contracts", {"cpf": cpf})
                self.assertEqual(len(result["contracts"]), 1)
                self.assertEqual(result["contracts"][0]["id"], self.contract)
                self.assertEqual(result["customer"]["name"], "Cliente Ficticio Portal Assistido")
            portfolio.assert_called_with("operator")
        for cpf in ["80000000230", "123", None]:
            with self.subTest(cpf=cpf), patch.object(assisted.Store, "portfolio") as portfolio:
                with self.assertRaises(ApiError):
                    assisted.execute(self.backend, "operator", "contracts", {"cpf": cpf})
                portfolio.assert_not_called()
        self.assertTrue(all(call.args[:2] == ("contact", assisted.CUSTOMER)
                            for call in self.backend.crm.client.records.retrieve.call_args_list))

    def test_missing_cpf_does_not_consult_the_portfolio(self):
        with self.assertRaises(ApiError):
            assisted.execute(self.backend, "operator", "contracts", {})
        self.backend.crm.client.records.retrieve.assert_not_called()
        self.backend.repository.transaction.assert_not_called()

    def test_portfolio_uses_one_sql_connection_and_never_reads_proposal_payloads(self):
        repository = Mock()
        transaction = MagicMock()
        repository.transaction.return_value = transaction
        cursor = transaction.__enter__.return_value
        cursor.fetchall.return_value = []
        self.assertEqual(assisted.Store(repository).portfolio("operator"), [])
        repository.transaction.assert_called_once()
        cursor.execute.assert_called_once()
        query, parameters = cursor.execute.call_args.args
        self.assertIn("LEFT JOIN demo.AssistedRequests", query)
        self.assertNotIn("Proposal", query)
        self.assertIn("a.OperatorContactId=%s", query)
        self.assertEqual(parameters, ("operator", assisted.CUSTOMER))

    def token(self, **overrides):
        claims = {"sub": self.request, "nonce": "secret-nonce",
                  "proposal": review_hash(self.terms), "aud": assisted.TOKEN_AUDIENCE,
                  "iss": assisted.TOKEN_AUDIENCE, "iat": utcnow(),
                  "exp": utcnow() + timedelta(hours=1)}
        return jwt.encode({**claims, **overrides}, "x" * 64, algorithm="HS256")

    def test_email_link_rejects_expiry_wrong_audience_and_modified_terms(self):
        with patch.dict(os.environ, self.settings):
            for token in [self.token(exp=utcnow() - timedelta(seconds=1)),
                          self.token(aud="customer-login"), self.token(nonce="other"),
                          self.token(proposal="changed")]:
                with patch.object(assisted.Store, "get", return_value=self.current):
                    with self.assertRaises(ApiError):
                        assisted.acceptance(self.backend, "review", {"token": token})
        self.backend.execute.assert_not_called()

    def test_rejected_or_pending_crm_decision_never_issues_customer_link(self):
        with patch.dict(os.environ, self.settings), \
             patch.object(assisted.Store, "get", return_value=self.current):
            for decision in [assisted.PENDING, assisted.REJECTED, None]:
                with patch.object(assisted, "native_request", return_value={"fsi_decisaoalcada": decision}):
                    with self.assertRaises(ApiError):
                        assisted.prepare_mail(self.backend, {"requestId": self.request})
        self.backend.execute.assert_not_called()

    def test_reading_email_link_does_not_confirm_or_review_new_terms(self):
        snapshot = {"offerId": self.request, "version": 1, "downPaymentCents": 1,
                    "installmentCount": 6, "installmentCents": 1650, "totalCents": 9901,
                    "validUntil": "2026-11-01T00:00:00Z"}
        self.backend.repository.status.return_value = {
            "Snapshot": json.dumps(snapshot), "ReviewHash": "hash", "SourceState": "review",
        }
        with patch.dict(os.environ, self.settings), \
             patch.object(assisted.Store, "get", return_value=self.current), \
             patch.object(assisted, "native_request", return_value={"fsi_decisaoalcada": assisted.APPROVED}):
            result = assisted.acceptance(self.backend, "review", {"token": self.token()})
        self.assertEqual(result["status"], "awaiting_acceptance")
        self.assertEqual(result["reviewHash"], "hash")
        self.backend.execute.assert_not_called()

    def test_revocation_prevents_customer_confirmation(self):
        with patch.dict(os.environ, self.settings), \
             patch.object(assisted.Store, "get", return_value=self.current), \
             patch.object(assisted, "native_request", return_value={"fsi_decisaoalcada": assisted.REJECTED}):
            with self.assertRaises(ApiError):
                assisted.acceptance(self.backend, "confirm", {
                    "token": self.token(), "reviewHash": "x" * 64, "confirmed": True,
                })
        self.backend.execute.assert_not_called()
