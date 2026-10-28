import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("assisted_flow", ROOT / "scripts/renegociacao_assisted_flow.py")
flow = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(flow)


class AssistedFlowContract(unittest.TestCase):
    def test_native_export_preserves_the_verified_flow_and_connector_binding(self):
        source = ROOT / "solutions/FSIInsights/Workflows/FSI-Alcadaaprovadaenviaaceiteporemail-8BD6CA29-5CF8-5EAF-9465-6A9BE0108F51.json"
        native = json.loads(source.read_text(encoding="utf-8-sig"))
        connector = "shared_new-5ffsi-20portal-20assistido-5fa3c7d3a820ae6f10"
        host = native["properties"]["definition"]["actions"]["Preparar_aceite"]["inputs"]["host"]
        self.assertEqual(host["connectionName"], connector)
        if host["apiId"] == "":
            host["apiId"] = "/providers/Microsoft.PowerApps/apis/" + connector
        self.assertEqual(native["properties"]["definition"], flow.definition(connector))
        references = native["properties"]["connectionReferences"]
        self.assertEqual(references[connector]["connection"]["connectionReferenceLogicalName"],
                         flow.MAIL_REFERENCE)
        self.assertEqual(references["shared_commondataserviceforapps"]["connection"]["connectionReferenceLogicalName"],
                         flow.DATAVERSE_REFERENCE)

    def test_only_approved_crm_decisions_trigger_email(self):
        definition = flow.definition("test_connector")
        trigger = definition["triggers"]["Decisao_aprovada_no_CRM"]
        parameters = trigger["inputs"]["parameters"]
        self.assertEqual(parameters["subscriptionRequest/filteringattributes"], "fsi_decisaoalcada")
        self.assertEqual(parameters["subscriptionRequest/filterexpression"],
                         "fsi_decisaoalcada eq 712400001 and fsi_pedidoalcada ne null")
        self.assertEqual(trigger["runtimeConfiguration"]["concurrency"]["runs"], 1)

    def test_power_automate_sends_email_but_never_accepts_the_offer(self):
        definition = flow.definition("test_connector")
        self.assertEqual(set(definition["actions"]), {
            "Preparar_aceite", "Consultar_email", "Enviar_somente_rascunho", "Registrar_envio",
        })
        send = definition["actions"]["Enviar_somente_rascunho"]
        self.assertEqual(send["expression"], {"equals": ["@body('Consultar_email')?['statuscode']", 1]})
        parameters = send["actions"]["Enviar_email"]["inputs"]["parameters"]
        self.assertEqual(parameters["actionName"], "Microsoft.Dynamics.CRM.SendEmail")
        self.assertIs(parameters["item/IssueSend"], True)
        prepare = definition["actions"]["Preparar_aceite"]
        self.assertEqual(prepare["runtimeConfiguration"]["secureData"]["properties"], ["inputs", "outputs"])
        self.assertEqual(set(prepare["inputs"]["parameters"]), {"body/requestId"})
