import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location(
    "assisted_pages", ROOT / "scripts" / "renegociacao_assisted_pages.py",
)
pages = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pages)


class OmnichannelPolicyTests(unittest.TestCase):
    def test_only_exact_script_origins_are_added_without_new_unsafe_or_wildcards(self):
        baseline = ("script-src 'self' content.powerapps.com 'nonce' 'unsafe-eval'; "
                    "style-src 'unsafe-inline' https:; frame-ancestors 'self';")
        result = pages.allow_chat_scripts(baseline)
        self.assertIn("style-src 'unsafe-inline' https:;", result)
        self.assertIn("frame-ancestors 'self';", result)
        before = set(baseline.split(";")[0].split())
        after = set(result.split(";")[0].split())
        self.assertEqual(after - before, set(pages.CHAT_SCRIPT_ORIGINS))
        self.assertEqual(before - after, set())
        self.assertNotIn("*", result)
        self.assertEqual(result, pages.allow_chat_scripts(result))

    def test_explicit_element_policy_is_extended_but_not_attributes_or_other_origins(self):
        baseline = ("script-src 'self' 'nonce'; script-src-elem 'self'; "
                    "script-src-attr 'none'; connect-src 'self' https://example.com;")
        result = pages.allow_chat_scripts(baseline)
        self.assertIn("script-src-attr 'none';", result)
        self.assertIn("connect-src 'self' https://example.com;", result)
        self.assertEqual(result.count("https://oc-cdn-ocprod.azureedge.net"), 2)

    def test_missing_duplicate_or_explicitly_disabled_script_policy_is_not_silently_replaced(self):
        for policy in ["", "default-src 'self';", "script-src 'none';",
                       "script-src 'self'; script-src 'none';"]:
            with self.subTest(policy=policy), self.assertRaises(RuntimeError):
                pages.allow_chat_scripts(policy)
