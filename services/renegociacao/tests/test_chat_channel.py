import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import azure.functions as func

import function_app
from renegociacao.core import ApiError
from renegociacao.directline_token import ConversationToken, PORTAL_ORIGIN


class AuthenticatedChatChannel(unittest.TestCase):
    def request(self, body=b"{}"):
        return func.HttpRequest(
            "POST", "https://demo/api/renegociacao/chat-token",
            headers={"Content-Type": "application/json", "Origin": PORTAL_ORIGIN},
            body=body, route_params={"action": "chat-token"},
        )

    def test_channel_token_requires_resolved_customer_and_delegated_identity(self):
        identity = SimpleNamespace(
            mode="entra", object_id="11111111-1111-4111-8111-111111111111",
            issuer="https://login.microsoftonline.com/tenant/v2.0",
        )
        backend = Mock()
        backend.crm.resolve_entra_contact.return_value = "22222222-2222-4222-8222-222222222222"
        channel = ConversationToken("ephemeral-test", "test-conversation", "dl_test", 1800, PORTAL_ORIGIN)
        with patch.object(function_app, "authenticate", return_value=identity), \
             patch.object(function_app, "service", return_value=backend), \
             patch.object(function_app, "required", return_value="server-test-secret"), \
             patch.object(function_app, "issue_token", return_value=channel) as issue:
            response = function_app.customer(self.request())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.get_body())["conversationId"], "test-conversation")
        self.assertIn("no-store", response.headers["Cache-Control"])
        self.assertEqual(response.headers["Access-Control-Allow-Origin"], PORTAL_ORIGIN)
        backend.crm.resolve_entra_contact.assert_called_once_with(identity.object_id, identity.issuer)
        backend.execute.assert_not_called()
        self.assertEqual(issue.call_args.kwargs["verified_oid"], identity.object_id)

    def test_missing_identity_cannot_mint_a_channel_token(self):
        with patch.object(function_app, "authenticate", side_effect=ApiError("IDENTITY_REQUIRED", "Sign in.", 401)), \
             patch.object(function_app, "issue_token") as issue:
            response = function_app.customer(self.request())
        self.assertEqual(response.status_code, 401)
        issue.assert_not_called()

    def test_unmapped_identity_cannot_mint_a_channel_token(self):
        identity = SimpleNamespace(mode="entra", object_id="test", issuer="test")
        backend = Mock()
        backend.crm.resolve_entra_contact.side_effect = ApiError("ACCESS_DENIED", "Not mapped.", 403)
        with patch.object(function_app, "authenticate", return_value=identity), \
             patch.object(function_app, "service", return_value=backend), \
             patch.object(function_app, "issue_token") as issue:
            response = function_app.customer(self.request())
        self.assertEqual(response.status_code, 403)
        issue.assert_not_called()


if __name__ == "__main__":
    unittest.main()
