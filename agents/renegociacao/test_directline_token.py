import io
import json
import unittest
import urllib.error

from services.renegociacao.renegociacao.directline_token import (
    DirectLineTokenError, OFFICIAL_DEMO_ORIGIN, _NoRedirect, PORTAL_ORIGIN, TENANT,
    issue_token,
)


class TokenBrokerTests(unittest.TestCase):
    def setUp(self):
        self.requests = []
        self.options = {
            "secret": "unit-test-not-a-live-secret",
            "origin": PORTAL_ORIGIN,
            "verified_oid": "4e9974ab-2251-4cfa-a98a-4be41612f6e8",
            "verified_issuer": f"https://login.microsoftonline.com/{TENANT}/v2.0",
            "opener": self.open,
        }

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        return io.BytesIO(json.dumps({
            "token": "unit-test-conversation-token",
            "conversationId": "unit-test-conversation",
            "expires_in": 1800,
        }).encode())

    def test_portal_token_restricts_exact_origin_and_binds_user(self):
        result = issue_token(**self.options)
        request, timeout = self.requests[0]
        payload = json.loads(request.data)
        self.assertEqual(payload["trustedOrigins"], [PORTAL_ORIGIN])
        self.assertEqual(payload["user"], {"id": result.user_id})
        self.assertTrue(result.user_id.startswith("dl_"))
        self.assertLessEqual(len(result.user_id), 64)
        self.assertNotIn(self.options["verified_oid"], result.user_id)
        self.assertEqual(request.headers["Authorization"], "Bearer " + self.options["secret"])
        self.assertEqual(timeout, 15)

    def test_disallowed_origins_never_reach_directline(self):
        for origin in ("*", "null", "", "https://evil.example", PORTAL_ORIGIN + "/",
                       PORTAL_ORIGIN + ".evil.example", "http://portalatendimento01.powerappsportals.com",
                       "https://web.powerva.microsoft.com", OFFICIAL_DEMO_ORIGIN):
            with self.subTest(origin=origin), self.assertRaises(DirectLineTokenError) as caught:
                issue_token(**{**self.options, "origin": origin})
            self.assertEqual(caught.exception.code, "CHAT_ORIGIN_DENIED")
        self.assertEqual(self.requests, [])

    def test_official_demo_is_explicitly_opt_in_and_still_single_origin(self):
        issue_token(**{**self.options, "origin": OFFICIAL_DEMO_ORIGIN, "allow_official_demo": True})
        self.assertEqual(json.loads(self.requests[0][0].data)["trustedOrigins"], [OFFICIAL_DEMO_ORIGIN])

    def test_issuer_must_match_the_approved_tenant(self):
        with self.assertRaises(DirectLineTokenError) as caught:
            issue_token(**{**self.options, "verified_issuer": "https://evil.example/"})
        self.assertEqual(caught.exception.code, "CHAT_IDENTITY_DENIED")
        self.assertEqual(self.requests, [])

    def test_subject_is_not_a_cpf_or_contact_id_supplied_by_a_customer(self):
        for subject in ("12345678901", "", None):
            with self.subTest(subject=subject), self.assertRaises(DirectLineTokenError):
                issue_token(**{**self.options, "verified_oid": subject})
        self.assertEqual(self.requests, [])

    def test_secret_is_required_and_never_in_client_payload_or_repr(self):
        with self.assertRaises(DirectLineTokenError):
            issue_token(**{**self.options, "secret": ""})
        result = issue_token(**self.options)
        self.assertNotIn(self.options["secret"], json.dumps(result.client_payload()))
        self.assertNotIn(result.token, repr(result))
        self.assertEqual(result.response_headers()["Access-Control-Allow-Origin"], PORTAL_ORIGIN)
        self.assertEqual(result.response_headers()["Cache-Control"], "no-store")

    def test_upstream_error_body_is_not_exposed(self):
        def fail(request, timeout):
            raise urllib.error.HTTPError(request.full_url, 403, "Forbidden", {},
                                         io.BytesIO(b'{"token":"do-not-expose"}'))
        with self.assertRaises(DirectLineTokenError) as caught:
            issue_token(**{**self.options, "opener": fail})
        self.assertEqual(str(caught.exception), "CHAT_CHANNEL_UNAVAILABLE")
        self.assertNotIn("do-not-expose", str(caught.exception))

    def test_transport_does_not_forward_a_secret_to_a_redirect_target(self):
        import urllib.request
        request = urllib.request.Request("https://directline.botframework.com/v3/directline/tokens/generate")
        with self.assertRaises(urllib.error.HTTPError):
            _NoRedirect().redirect_request(request, None, 307, "Redirect", {},
                                           "https://evil.example")

    def test_invalid_or_non_json_response_fails_closed(self):
        for payload in (b"not-json", b"[]", b"{}", b'{"token":"x","conversationId":"c","expires_in":true}'):
            def invalid(request, timeout, data=payload):
                return io.BytesIO(data)
            with self.subTest(payload=payload), self.assertRaises(DirectLineTokenError):
                issue_token(**{**self.options, "opener": invalid})


if __name__ == "__main__":
    unittest.main()
