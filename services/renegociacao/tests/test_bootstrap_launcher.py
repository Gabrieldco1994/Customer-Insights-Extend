"""Only retry the explicitly disabled, pre-execution bootstrap response."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location(
    "bootstrap_launcher",
    Path(__file__).resolve().parents[3] / "scripts" / "renegociacao_bootstrap.py",
)
launcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(launcher)


class BootstrapPropagationTests(unittest.TestCase):
    def response(self, status, code=None):
        return Mock(status_code=status, json=Mock(return_value={"error": {"code": code}}))

    def test_waits_only_for_disabled_flag(self):
        success = self.response(200)
        with patch.object(launcher.requests, "post", side_effect=[
                self.response(403, "BOOTSTRAP_DISABLED"), success]) as post:
            with patch.object(launcher.time, "sleep") as sleep:
                self.assertIs(launcher.initialize_source({}, "test-key"), success)
        self.assertEqual(post.call_count, 2)
        sleep.assert_called_once_with(5)

    def test_other_failures_and_processing_are_not_retried(self):
        for status, code in [(403, "FORBIDDEN"), (401, None), (503, "BOOTSTRAP_FAILED"), (202, None)]:
            with self.subTest(status=status):
                response = self.response(status, code)
                with patch.object(launcher.requests, "post", return_value=response) as post:
                    with patch.object(launcher.time, "sleep") as sleep:
                        self.assertIs(launcher.initialize_source({}, "test-key"), response)
                post.assert_called_once()
                sleep.assert_not_called()

    def test_disabled_setting_has_a_bounded_wait(self):
        response = self.response(403, "BOOTSTRAP_DISABLED")
        with patch.object(launcher.requests, "post", return_value=response) as post:
            with patch.object(launcher.time, "sleep") as sleep:
                with patch("builtins.print"):
                    self.assertIs(launcher.initialize_source({}, "test-key"), response)
        self.assertEqual(post.call_count, 25)
        self.assertEqual(sleep.call_count, 24)
