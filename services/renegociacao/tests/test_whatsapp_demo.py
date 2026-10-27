import json
import os
import unittest
from unittest.mock import Mock, patch

import azure.functions as func
import function_app
from renegociacao.core import ApiError
from renegociacao import whatsapp_demo as demo


class IsolatedWhatsAppSimulation(unittest.TestCase):
    def setUp(self):
        self.backend = Mock()
        self.backend.crm.client.records.retrieve.return_value = {
            "firstname": "Cliente", "lastname": "Ficticio WhatsApp - Renegociacao",
            "emailaddress1": demo.EMAIL, "statecode": 0,
            "fsi_authobjectid": None, "fsi_authissuer": None,
        }
        self.contract = next(iter(demo.CONTRACTS))
        name, anchor = demo.CONTRACTS[self.contract]
        self.backend.repository.contracts.return_value = [
            {"Id": self.contract, "Number": name, "AnchorId": anchor},
        ]
        self.backend.execute.return_value = (
            {"status": "ok", "data": {"contracts": [
                {"id": self.contract},
                {"id": "11111111-1111-4111-8111-111111111111"},
            ]}, "demonstration": True}, 200,
        )

    def test_fixed_customer_only_without_fabricated_user_authentication(self):
        result, status = demo.execute(self.backend, "contracts", {"payload": "{}"})
        self.assertEqual(status, 200)
        self.assertTrue(result["simulationOnly"])
        self.assertEqual(result["data"]["contracts"], [{"id": self.contract}])
        self.backend.execute.assert_called_once_with("contracts", demo.CONTACT, {})
        self.backend.crm.resolve_entra_contact.assert_not_called()

    def test_caller_cannot_choose_contact_phone_or_token(self):
        for field in ["contactId", "customerId", "userId", "phone", "code", "accessToken"]:
            with self.subTest(field=field), self.assertRaises(ApiError):
                demo.execute(self.backend, "profile", {"payload": json.dumps({field: "anything"})})
        self.backend.execute.assert_not_called()

    def test_non_demo_or_identity_bound_contact_fails_closed(self):
        for field, value in [
            ("emailaddress1", "real@example.com"),
            ("firstname", "Real customer"),
            ("statecode", 1),
            ("fsi_authobjectid", "11111111-1111-4111-8111-111111111111"),
            ("fsi_authissuer", "https://login.microsoftonline.com/tenant/v2.0"),
        ]:
            with self.subTest(field=field):
                self.setUp()
                self.backend.crm.client.records.retrieve.return_value[field] = value
                with self.assertRaises(ApiError) as error:
                    demo.execute(self.backend, "profile", {"payload": "{}"})
                self.assertEqual(error.exception.code, "DEMO_CUSTOMER_NOT_ISOLATED")
                self.backend.execute.assert_not_called()

    def test_existing_portal_contract_is_not_accessible(self):
        with self.assertRaises(ApiError) as error:
            demo.execute(self.backend, "start", {"payload": json.dumps({
                "contractId": "9fbf8f2a-2ebe-5186-b634-41f735b76e02", "channel": "chat",
            })})
        self.assertEqual(error.exception.http, 403)
        self.backend.execute.assert_not_called()

    def test_request_from_another_contract_is_rejected(self):
        self.backend.repository.status.return_value = {
            "ContractId": "11111111-1111-4111-8111-111111111111",
        }
        with self.assertRaises(ApiError) as error:
            demo.execute(self.backend, "confirm", {"payload": json.dumps({
                "requestId": "22222222-2222-4222-8222-222222222222",
                "reviewHash": "x", "confirmed": True,
            })})
        self.assertEqual(error.exception.http, 403)
        self.backend.execute.assert_not_called()

    def test_malformed_bridge_payload_is_not_silently_normalized(self):
        for body in [None, {}, {"payload": None}, {"payload": "null"},
                     {"payload": "[]"}, {"payload": "invalid"}, {"payload": "{}", "customerId": "x"}]:
            with self.subTest(body=body), self.assertRaises(ApiError):
                demo.execute(self.backend, "profile", body)
        self.backend.execute.assert_not_called()

    def test_disabled_demo_does_not_initialize_service(self):
        request = func.HttpRequest(
            "POST", "https://demo/api/whatsapp-demo/profile",
            headers={"Content-Type": "application/json"},
            body=b'{"payload":"{}"}', route_params={"action": "profile"},
        )
        with patch.dict(os.environ, {"WHATSAPP_DEMO_ENABLED": "0"}), \
             patch.object(function_app, "service") as service:
            response = function_app.whatsapp_simulation(request)
        self.assertEqual(response.status_code, 403)
        service.assert_not_called()


if __name__ == "__main__":
    unittest.main()
