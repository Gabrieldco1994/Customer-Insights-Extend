"""Offline checks for the dedicated-page deployment safeguards."""
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location(
    "renegociacao_pages", ROOT / "scripts" / "renegociacao_pages.py"
)
pages = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pages)


class DeploymentTests(unittest.TestCase):
    def test_sdk_parts_preserve_javascript_without_liquid_evaluation(self):
        source = "const v='{% raw %}{{literal}}{% endraw %}';" * 20
        fragments = pages.sdk_sources(source, size=100)
        self.assertGreater(len(fragments), 1)
        rebuilt = "".join(part.removeprefix(pages.SDK_MARKER) for part in fragments)
        self.assertEqual(json.loads(rebuilt), source)
        self.assertTrue(all(len(part) < 1_048_576 for part in fragments))
        self.assertTrue(all("{%" not in part.removeprefix(pages.SDK_MARKER)
                            and "{{" not in part.removeprefix(pages.SDK_MARKER)
                            for part in fragments))
        escaped = pages.sdk_code("const v='\x15\x18\x0f\x12';")
        self.assertEqual(escaped, "const v='\\u0015\\u0018\\u000f\\u0012';")
        self.assertFalse(any(ord(c) < 32 for c in escaped))
        self.assertEqual(pages.sdk_code("const v='</script>';"), "const v='<\\/script>';")

    def deployment(self):
        deployment = object.__new__(pages.PageDeployment)
        deployment.client = Mock()
        deployment.client.tables.get.return_value = {"primary_id_attribute": "mspp_webpageid"}
        deployment.save_before = Mock()
        return deployment

    def test_read_back_verifies_simple_fields_and_bindings(self):
        deployment = self.deployment()
        deployment.client.records.retrieve.return_value = {
            "mspp_title": "Dedicated page", "_mspp_publishingstateid_value": "draft-id",
            "_mspp_rootwebpageid_value": None,
        }
        deployment.verify("mspp_webpage", "page-id", {
            "mspp_title": "Dedicated page",
            "mspp_publishingstateid@odata.bind": "/mspp_publishingstates(draft-id)",
            "mspp_rootwebpageid@odata.bind": None,
        })
        with self.assertRaisesRegex(RuntimeError, "Read-back mismatch"):
            deployment.verify("mspp_webpage", "page-id", {"mspp_title": "Wrong"})

    def test_root_lookup_disambiguates_automatic_localized_copy(self):
        deployment = self.deployment()
        deployment.rows = Mock(return_value=[{
            "mspp_webpageid": "root-id", "mspp_name": "FSI - Renegociacao (root)",
        }])
        deployment.verify = Mock()
        self.assertEqual(deployment.upsert(
            "mspp_webpage", "FSI - Renegociacao (root)", {"mspp_isroot": True}
        ), "root-id")
        self.assertIn("mspp_isroot eq true", deployment.rows.call_args.args[1])

    def test_localized_creation_reuses_the_automatic_copy(self):
        deployment = self.deployment()
        deployment.rows = Mock(side_effect=[[], [{
            "mspp_webpageid": "generated-id", "_mspp_webpagelanguageid_value": "language-id",
        }]])
        deployment.verify = Mock()
        result = deployment.upsert("mspp_webpage", "FSI - Renegociacao (pt-BR)", {
            "mspp_isroot": False,
            "mspp_rootwebpageid@odata.bind": "/mspp_webpages(root-id)",
            "mspp_webpagelanguageid@odata.bind": "/mspp_websitelanguages(language-id)",
        })
        self.assertEqual(result, "generated-id")
        deployment.client.records.create.assert_not_called()
        self.assertEqual(deployment.client.records.update.call_args.args[2]["mspp_name"],
                         "FSI - Renegociacao (pt-BR)")
        # The virtual table returns this lookup but cannot filter it reliably.
        self.assertNotIn("_mspp_webpagelanguageid_value", deployment.rows.call_args.args[1])

    def test_removal_only_applies_to_matching_generated_drafts(self):
        deployment = self.deployment()
        copy = {
            "mspp_webpageid": "generated-id", "_mspp_webpagelanguageid_value": "language-id",
            "_mspp_publishingstateid_value": "draft-id", "mspp_partialurl": pages.SLUG,
        }
        deployment.client.records.list.side_effect = [[copy], []]
        deployment.remove_generated_localized("root-id", "language-id", "draft-id")
        deployment.client.records.delete.assert_called_once_with(
            "mspp_webpage", "generated-id"
        )
        deployment.save_before.assert_called_once_with("mspp_webpage", copy)
        deployment.client.records.delete.reset_mock()
        deployment.client.records.list.side_effect = None
        deployment.client.records.list.return_value = [
            {**copy, "_mspp_publishingstateid_value": "published-id"}
        ]
        with self.assertRaisesRegex(RuntimeError, "refusing to remove"):
            deployment.remove_generated_localized("root-id", "language-id", "draft-id")
        deployment.client.records.delete.assert_not_called()

    def test_publish_requires_source_validation_before_connecting(self):
        with patch.object(sys, "argv", [
            "renegociacao_pages.py", "publish", "--environment", pages.ORG,
            "--backup-dir", "pages/renegociacao/.local/backup", "--apply",
        ]), patch("sys.stderr", new_callable=io.StringIO), \
                patch.object(pages, "DataverseClient") as client:
            with self.assertRaises(SystemExit) as caught:
                pages.main()
            self.assertEqual(caught.exception.code, 2)
            client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
