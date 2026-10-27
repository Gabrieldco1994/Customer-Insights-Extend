import json
import os
import unittest
from unittest.mock import Mock, patch

import function_app
from renegociacao.whatsapp_demo import offer_summary


class NativeMcpTools(unittest.TestCase):
    def test_disabled_tools_do_not_initialize_customer_services(self):
        with patch.dict(os.environ, {"WHATSAPP_DEMO_ENABLED": "0"}):
            with patch.object(function_app, "service") as service:
                result = json.loads(function_app.list_demo_contracts_mcp('{"arguments":{}}'))
        self.assertEqual(result["error"]["code"], "DEMO_DISABLED")
        service.assert_not_called()

    def test_calls_the_fixed_customer_guard_and_formats_only_source_amounts(self):
        offer = {"id": "demo-offer", "version": 1, "entryCents": 5,
                 "installments": 6, "paymentCents": 300020, "totalCents": 1800125,
                 "validUntil": "2026-11-01T00:00:00Z"}
        backend = Mock()
        with patch.dict(os.environ, {"WHATSAPP_DEMO_ENABLED": "1"}), \
             patch.object(function_app, "service", return_value=backend), \
             patch.object(function_app.whatsapp_demo, "execute",
                          return_value=({"data": {"offers": [offer]}}, 200)) as execute:
            result = json.loads(function_app.list_demo_offers_mcp(
                '{"arguments":{"contractId":"demo-contract"}}'))
        execute.assert_called_once_with(backend, "offers", {"payload": '{"contractId": "demo-contract"}'})
        self.assertIn("Entrada: R$ 0,05; 6 parcelas de R$ 3.000,20; total: R$ 18.001,25",
                      result["data"]["offers"][0]["resumo"])

    def test_malformed_arguments_produce_explicit_error_not_empty_success(self):
        with patch.dict(os.environ, {"WHATSAPP_DEMO_ENABLED": "1"}):
            with patch.object(function_app, "service") as service:
                for context in ["null", "[]", "not-json", '{"arguments":[]}']:
                    result = json.loads(function_app.list_demo_contracts_mcp(context))
                    self.assertIn("error", result)
                    self.assertNotIn("data", result)
                service.assert_not_called()

    def test_mcp_cannot_execute_writes_even_with_valid_service_credentials(self):
        with patch.dict(os.environ, {"WHATSAPP_DEMO_ENABLED": "1"}):
            with patch.object(function_app, "service") as service:
                result = json.loads(function_app.mcp_read("confirm", '{"arguments":{}}'))
        self.assertEqual(result["error"]["code"], "MCP_READ_ONLY")
        service.assert_not_called()

    def test_summary_does_not_round_or_accept_noninteger_money(self):
        offer = {"id": "o", "version": 1, "entryCents": 0.1, "installments": 1,
                 "paymentCents": 10, "totalCents": 10, "validUntil": "later"}
        with self.assertRaises(function_app.ApiError):
            offer_summary(offer)
