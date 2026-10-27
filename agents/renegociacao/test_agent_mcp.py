import json
from pathlib import Path
import unittest
import xml.etree.ElementTree as ET

import yaml


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def load(name):
    return yaml.safe_load((HERE / (name + ".yaml")).read_text(encoding="utf-8"))


def nodes(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from nodes(child)
    elif isinstance(value, list):
        for child in value:
            yield from nodes(child)


class ExistingAgentMcpBoundary(unittest.TestCase):
    def test_numeric_access_is_not_customer_authentication(self):
        access = load("AcessoDemonstrativo")
        serialized = json.dumps(access)
        self.assertNotIn("System.User.", serialized)
        self.assertIn("[0-9]+", serialized)
        top = access["beginDialog"]["actions"]
        ids = [node["id"] for node in top]
        self.assertLess(ids.index("mock_code"), ids.index("invalid_code"))
        self.assertLess(ids.index("invalid_code"), ids.index("grant_mock_only"))
        start = load("ConversationStart")
        access_init = next(n for n in nodes(start) if n.get("id") == "initialize_mock_access")
        self.assertIs(access_init["value"], False)
        self.assertEqual(access_init["variable"], "init:Global.AcessoSimulado")
        grant = next(n for n in nodes(access) if n.get("id") == "grant_mock_only")
        self.assertEqual(grant["variable"], "Global.AcessoSimulado")

    def test_mcp_uses_maker_only_for_fixed_fictitious_dataset(self):
        mcp = load("McpRenegociacao")
        self.assertEqual(mcp["triggerCondition"], "=Global.AcessoSimulado = true")
        self.assertEqual(mcp["action"]["connectionProperties"], {"mode": "Maker"})
        self.assertEqual(mcp["action"]["operationDetails"]["kind"], "ModelContextProtocolMetadata")
        self.assertTrue(mcp["action"]["connectionReference"].startswith("fsi_renegociacao."))
        self.assertIs(load("HiddenDemoProfile")["triggerCondition"], False)

    def test_only_one_write_after_explicit_user_confirmation(self):
        doc = load("ConfirmarDemonstrativo")
        top = doc["beginDialog"]["actions"]
        ids = [node["id"] for node in top]
        self.assertLess(ids.index("explicit_demo_acceptance"), ids.index("demo_not_accepted"))
        self.assertLess(ids.index("demo_not_accepted"), ids.index("demo_confirm"))
        calls = [n for n in nodes(doc) if n.get("kind") == "InvokeConnectorAction"]
        self.assertEqual(sum(n["operationId"] == "confirm" for n in calls), 1)
        self.assertTrue(all(n["connectionProperties"] == {"mode": "Maker"} for n in calls))
        self.assertFalse(any(n.get("kind") in ("HttpRequestAction", "AdaptiveCardPrompt", "OAuthInput")
                             for n in nodes(doc)))
        self.assertIn("confirmed: true", next(n for n in calls if n["operationId"] == "confirm")["input"]["binding"]["payload"])
        error = next(n for n in nodes(doc) if n.get("id") == "demo_start_error_case")
        self.assertIn("Error < 200", error["condition"])
        self.assertIn("Error >= 300", error["condition"])

    def test_original_real_authentication_remains_separate(self):
        signin = json.dumps(load("Signin"))
        self.assertIn("OAuthInput", signin)
        self.assertIn("System.User.AccessToken", signin)
        original = load("Renegociar")
        queries = original["beginDialog"]["intent"]["triggerQueries"]
        self.assertIn("renegociar autenticado", queries)
        requests = [n for n in nodes(original) if n.get("kind") == "HttpRequestAction"]
        self.assertTrue(requests)
        self.assertTrue(all(n["headers"]["Authorization"] == '="Bearer " & System.User.AccessToken'
                            for n in requests))

    def test_connectors_use_the_platform_reserved_api_key_parameter(self):
        for folder in ["renegociacao-mcp", "renegociacao-whatsapp-demo"]:
            directory = ROOT / "connectors" / folder
            api = json.loads((directory / "apiDefinition.json").read_text())
            properties = json.loads((directory / "apiProperties.json").read_text())["properties"]
            self.assertEqual(set(properties["connectionParameters"]), {"api_key"})
            self.assertEqual(api["security"], [{"api_key": []}])
            self.assertEqual(api["securityDefinitions"]["api_key"]["name"], "x-functions-key")

    def test_native_solution_contains_the_complete_existing_agent(self):
        solution = ROOT / "solutions" / "FSIInsights"
        component_root = solution / "botcomponents"
        for name in ["Signin", "Renegociar", "Andamento", "ConversationStart",
                     "AcessoDemonstrativo", "ConfirmarDemonstrativo"]:
            exported = yaml.safe_load(
                (component_root / ("fsi_renegociacao.topic." + name) / "data").read_text(encoding="utf-8-sig"))
            authored_ids = {n["id"] for n in nodes(load(name)) if isinstance(n.get("id"), str)}
            exported_ids = {n["id"] for n in nodes(exported) if isinstance(n.get("id"), str)}
            self.assertEqual(authored_ids, exported_ids, name)
        bot = ET.parse(solution / "bots" / "fsi_renegociacao" / "bot.xml").getroot()
        self.assertEqual(bot.findtext(".//authenticationmode"), "3")
        self.assertEqual(bot.findtext(".//authenticationtrigger"), "0")
