"""Fresh-process real imports and v2 indexing, without mocks or network access."""

import json
import os
from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parent.parent
PROBE = r"""
import __future__
import inspect
import json
import sys
from pathlib import Path

network_events = []
def deny_network(event, args):
    if event in {
        "socket.connect", "socket.getaddrinfo", "socket.sendto",
        "urllib.Request", "http.client.connect",
    }:
        network_events.append(event)
        raise AssertionError("Module import/indexing attempted network I/O: " + event)

sys.addaudithook(deny_network)
sys.path.insert(0, str(Path.cwd()))
import function_app
import azure.functions as func

bindings = {}
for function in function_app.app.get_functions():
    handler = function.get_user_function()
    signature = inspect.signature(handler, eval_str=False)
    assert not handler.__code__.co_flags & __future__.annotations.compiler_flag
    assert handler.__annotations__ == {
        "request": func.HttpRequest, "return": func.HttpResponse,
    }, "HTTP annotations must be real SDK classes, not strings"
    assert signature.parameters["request"].annotation is func.HttpRequest
    assert signature.return_annotation is func.HttpResponse
    metadata = json.loads(function.get_function_json())
    assert metadata["scriptFile"] == "function_app.py"
    argument_names = {binding["name"] for binding in metadata["bindings"]
                      if binding["name"] != "$return"}
    parameter_names = set(signature.parameters)
    assert parameter_names == argument_names, (
        function.get_function_name() + " has a Python/trigger argument mismatch")
    trigger = next(binding for binding in metadata["bindings"]
                   if binding["type"] == "httpTrigger")
    bindings[function.get_function_name()] = {
        "route": trigger["route"], "authLevel": trigger["authLevel"].lower(),
        "methods": trigger["methods"], "argument": trigger["name"],
    }

assert function_app._service is None, "Import eagerly constructed SQL/Dataverse services"
assert not network_events, "Import/indexing attempted network access"
print(json.dumps({"bindings": bindings, "networkEvents": network_events,
                  "serviceInitialized": function_app._service is not None}))
"""


class RealStartupTests(unittest.TestCase):
    def probe(self, settings: dict) -> dict:
        # -I prevents PYTHONPATH/user-site influence; dependencies are real imports
        # from the executing interpreter's isolated venv, never sys.modules mocks.
        completed = subprocess.run(
            [sys.executable, "-I", "-c", PROBE], cwd=ROOT,
            env={"PATH": os.defpath, **settings}, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45, check=False)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["bindings"], {
            "CustomerRenegotiation": {
                "route": "renegociacao/{action}", "authLevel": "anonymous", "methods": ["POST"],
                "argument": "request"},
            "ExternalSourceOData": {
                "route": "odata/{*path}", "authLevel": "function", "methods": ["GET"],
                "argument": "request"},
            "BootstrapDemoSource": {
                "route": "bootstrap", "authLevel": "function", "methods": ["POST"],
                "argument": "request"},
        })
        self.assertEqual(result["networkEvents"], [])
        self.assertFalse(result["serviceInitialized"])
        return result

    def test_no_settings_or_portal_config_are_needed_for_real_import_and_indexing(self):
        self.probe({})

    def test_reported_entra_cloud_settings_index_without_portal_or_backend_io(self):
        self.probe({
            "AUTH_MODE": "entra",
            "AZURE_CREDENTIAL_MODE": "managed_identity",
            "ENTRA_AUDIENCE": "22630d43-3353-4fb7-8496-3979bd7617d9",
            "DATAVERSE_URL": "https://org88957f9a.crm.dynamics.com",
            "SQL_SERVER": "fsi-reneg-88957f9a.database.windows.net",
            "SQL_DATABASE": "renegociacao",
            "CPF_FIELD": "fsi_cpf",
            "REASON_CODE_FIELD": "<not-required-during-import>",
            "BPF_WORKFLOW_ID": "<not-required-during-import>",
            "BPF_STAGE_MAP": "<not-required-during-import>",
            "ODATA_PUBLIC_ROOT": "https://fsi-reneg-88957f9a.azurewebsites.net/api/odata",
            "BOOTSTRAP_ENABLED": "0",
            "WEBSITE_HOSTNAME": "fsi-reneg-88957f9a.azurewebsites.net",
        })


if __name__ == "__main__":
    unittest.main()
