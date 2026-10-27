"""Generate the dedicated, fail-closed Copilot Studio renegotiation draft.

Uses native topics, authenticated HTTP nodes and Adaptive Cards only.
Deployment uses the existing cps_agent.py workflow; this script never publishes.
"""
import argparse
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "agents" / "renegociacao"
CPS = ROOT / ".github/skills/copilot-studio-agent-builder/scripts/cps_agent.py"
ORG = "https://org88957f9a.crm.dynamics.com"
BOT = "fsi_renegociacao"
API = "https://fsi-reneg-88957f9a.azurewebsites.net/api/renegociacao/"
OFFER = {
    "id": {"type": "String"}, "version": "Number", "entryCents": "Number",
    "installments": "Number", "paymentCents": "Number", "totalCents": "Number",
    "validUntil": "String",
}
RESULT = {
    "requestId": "String", "status": "String", "protocol": "String",
    "offer": {"kind": "Record", "properties": OFFER},
}


class CopilotDumper(yaml.SafeDumper):
    def increase_indent(self, flow=False, indentless=False):
        return super().increase_indent(flow, False)

    def ignore_aliases(self, data):
        return True


def save(name, doc):
    path = OUT / name
    path.write_text(yaml.dump(doc, Dumper=CopilotDumper, allow_unicode=True,
                              sort_keys=False, width=1_000_000), encoding="utf-8")
    return path


def node(kind, id, **kwargs):
    return {"kind": kind, "id": id, **kwargs}


def say(id, text):
    if text.startswith("="):
        text = "{" + text[1:] + "}"
    return node("SendActivity", id, activity={"text": [text]})


def setvar(id, variable, value, initialize=True):
    return node("SetVariable", id, variable=("init:" if initialize else "") + variable, value=value)


def branch(id, condition, actions, else_actions=None):
    result = node("ConditionGroup", id, conditions=[
        {"id": id + "_case", "condition": condition, "actions": actions}])
    if else_actions:
        result["elseActions"] = else_actions
    return result


def stop(id, text):
    return [say(id + "_message", text), node("EndDialog", id + "_end")]


def question(id, variable, prompt, entity="BooleanPrebuiltEntity"):
    return node("Question", id, interruptionPolicy={"allowInterruption": False},
                repeatCount=2, alwaysPrompt=True, variable="init:" + variable,
                prompt={"text": [prompt]}, defaultValue="=Blank()", entity=entity)


def response_schema(properties):
    def typed(fields):
        return {name: {"type": {"kind": value["kind"], "properties": typed(value["properties"])}}
                if isinstance(value, dict) and value.get("kind") in ("Record", "Table")
                else value for name, value in fields.items()}

    return {"kind": "Record", "properties": {
        "status": "String", "demonstration": "Boolean",
        "data": {"type": {"kind": "Record", "properties": typed(properties)}},
    }}


def http(id, action, content, properties, recover=False, allow_processing=False):
    variable = "Topic." + id
    actions = [
        setvar(id + "_reset_error", variable + "Error", "=Blank()"),
        node("HttpRequestAction", id, method="Post", url=API + action,
             headers={"Authorization": '="Bearer " & System.User.AccessToken',
                      "Content-Type": "application/json"},
             body={"kind": "JsonRequestContent", "content": content},
             requestTimeoutInMilliseconds=30000, response="init:" + variable,
             responseSchema=response_schema(properties),
             errorHandling={"kind": "ContinueOnErrorBehavior",
                            "statusCode": variable + "Error",
                            "errorResponseBody": "init:" + variable + "ErrorBody"}),
    ]
    if not recover:
        actions.append(branch(
            id + "_error", "=!IsBlank(" + variable + "Error)",
            [branch(id + "_unauthorized",
                    "=" + variable + "Error = 401 || " + variable + "Error = 403",
                    stop(id + "_identity", "Demonstração fictícia. A identidade não foi autorizada. Entre com a conta vinculada ao seu cadastro. CPF não autentica e nenhum contrato será consultado por CPF."),
                    stop(id + "_unavailable", "Demonstração fictícia. O serviço não confirmou esta operação. Não vou inventar condições nem informar que foi concluída. Tente consultar o andamento mais tarde."))]))
        invalid_status = variable + '.status <> "ok"'
        if allow_processing:
            invalid_status = "(" + invalid_status + " && " + variable + '.status <> "processing")'
        actions.append(branch(
            id + "_wrapper", "=" + variable + ".demonstration <> true || " + invalid_status,
            stop(id + "_invalid", "Demonstração fictícia. O serviço ainda não disponibilizou uma resposta válida. Nenhuma conclusão foi confirmada.")))
    return actions


def money(expression):
    return '"R$ " & Text((' + expression + ') / 100, "#,##0.00", "pt-BR")'


def connector_call(reference, id, action, business, properties, recover=False, processing=False):
    variable = "Topic." + id
    actions = [
        setvar(id + "_reset_error", variable + "Error", "=Blank()"),
        node("InvokeConnectorAction", id,
             input={"binding": {"payload": "=" + business}},
             output={"kind": "SingleVariableOutputBinding", "variable": "init:" + variable},
             connectionReference=reference, connectionProperties={"mode": "Maker"},
             operationId=action, dynamicOutputSchema=response_schema(properties),
             errorHandling={"kind": "ContinueOnErrorBehavior",
                            "statusCode": variable + "Error",
                            "errorResponseBody": "init:" + variable + "ErrorBody"},
             requestTimeoutInMilliseconds=30000),
    ]
    if not recover:
        actions.append(branch(id + "_error", "=!IsBlank(" + variable + "Error) && (" +
                              variable + "Error < 200 || " + variable + "Error >= 300)",
                              stop(id + "_unavailable", "SIMULAÇÃO. O serviço não confirmou a operação. Não vou repetir escritas nem afirmar conclusão. Diga consultar andamento.")))
        status = variable + '.status <> "ok"'
        if processing:
            status = "(" + status + " && " + variable + '.status <> "processing")'
        actions.append(branch(id + "_wrapper",
                              "=" + variable + ".demonstration <> true || " + status,
                              stop(id + "_invalid", "SIMULAÇÃO. Resposta inválida ou pendente; nenhuma conclusão foi confirmada.")))
    return actions


def summary(offer):
    return ('"Oferta " & ' + offer + '.id & ", versão " & Text(' + offer +
            '.version) & ". Entrada: " & ' + money(offer + ".entryCents") +
            ' & ". Parcelas: " & Text(' + offer + '.installments) & " de " & ' +
            money(offer + ".paymentCents") + ' & ". Total: " & ' +
            money(offer + ".totalCents") + ' & ". Validade (informada pelo serviço): " & ' +
            offer + '.validUntil')


def selection(id, variable, title, choices):
    card = ('={type: "AdaptiveCard", version: "1.3", body: ['
            '{type: "TextBlock", text: "' + title + '", wrap: true}, '
            '{type: "Input.ChoiceSet", id: "selectedId", label: "' + title + '", style: "expanded", '
            'isRequired: true, errorMessage: "Selecione uma opção.", choices: ' +
            choices + '}], actions: [{type: "Action.Submit", title: "Selecionar"}]}')
    return node("AdaptiveCardPrompt", id,
                interruptionPolicy={"allowInterruption": False}, repeatCount=2,
                card=card, output={"binding": {"selectedId": "init:" + variable}},
                outputType={"properties": {"selectedId": "String"}})


def topic(kind, actions, **trigger):
    return {"kind": "AdaptiveDialog",
            "beginDialog": {"kind": kind, "id": "main", **trigger, "actions": actions},
            "inputType": {}, "outputType": {}}


def final_result(id, variable):
    return branch(
        id + "_complete",
        "=" + variable + '.demonstration = true && ' + variable +
        '.status = "ok" && ' + variable + '.data.requestId = Global.RenegRequestId && ' +
        variable + '.data.status = "completed" && !IsBlank(Trim(' +
        variable + ".data.protocol))",
        [say(id + "_success", '="DEMONSTRAÇÃO FICTÍCIA — renegociação concluída somente no ambiente demonstrativo. Protocolo: " & ' +
             variable + '.data.protocol & ". Solicitação: " & ' + variable +
             '.data.requestId & ". " & ' + summary(variable + ".data.offer"))],
        [branch(id + "_pending",
                "=" + variable + '.demonstration = true && ' + variable +
                '.data.requestId = Global.RenegRequestId && (' + variable +
                '.status = "processing" || ' + variable + '.data.status = "pending" || ' +
                variable + '.data.status = "processing" || ' + variable +
                '.data.status = "draft" || ' + variable + '.data.status = "reviewed")',
                [say(id + "_pending_message", '="Demonstração fictícia. Solicitação " & Global.RenegRequestId & " PENDENTE/EM PROCESSAMENTO. Ainda não há conclusão confirmada. Diga consultar andamento para verificar; não refaça a confirmação."')],
                [say(id + "_failure", '="Demonstração fictícia. Não há conclusão confirmada para a solicitação " & Global.RenegRequestId & ". O serviço retornou falha ou resposta não reconhecida. Diga consultar andamento. Não vou repetir uma escrita de resultado incerto."')])])


def build():
    OUT.mkdir(parents=True, exist_ok=True)
    actions = [
        say("demo", "DEMONSTRAÇÃO FICTÍCIA — nenhum acordo real será criado. Podemos concluir a simulação neste chat, sem abrir outra página. Sua conta autenticada determina quais contratos você pode consultar; CPF é apenas confirmação opcional."),
        node("OAuthInput", "login", title="Entrar com segurança",
             text="Entre com a conta vinculada ao seu cadastro para acessar somente seus contratos."),
        branch("identity_required", "=System.User.IsLoggedIn <> true || IsBlank(System.User.AccessToken)",
               stop("no_identity", "Demonstração fictícia. É necessário entrar com a conta vinculada. CPF ou identificador informado não substitui autenticação.")),
        question("ask_cpf", "Topic.CheckCpf", "Deseja confirmar também seu CPF? É opcional e não substitui entrar na conta."),
        branch("cpf_answer", "=IsBlank(Topic.CheckCpf)", stop("no_cpf_answer", "Demonstração fictícia. Operação interrompida sem consultar contratos.")),
        setvar("empty_cpf", "Topic.Cpf", '=""'),
        branch("optional_cpf", "=Topic.CheckCpf = true", [
            question("cpf", "Topic.CpfInput", "Informe somente o seu CPF, com 11 dígitos.", "StringPrebuiltEntity"),
            setvar("normalize_cpf", "Topic.Cpf", '=Substitute(Substitute(Substitute(Trim(Topic.CpfInput), ".", ""), "-", ""), " ", "")', initialize=False),
            branch("validate_cpf", '=!IsMatch(Topic.Cpf, "[0-9]{11}")',
                   stop("bad_cpf", "Demonstração fictícia. CPF em formato inválido. Nenhum contrato foi consultado.")),
        ]),
    ]
    actions += http("profile", "profile", '=If(Topic.CheckCpf, {cpf: Topic.Cpf}, {})',
                    {"contactId": "String", "name": "String", "cpfVerified": "Boolean"})
    next(n for n in actions if n.get("id") == "profile")["body"] = {
        "kind": "RawRequestContent", "contentType": "application/json",
        "content": '=If(Topic.CheckCpf, JSON({cpf: Topic.Cpf}), "{}")',
    }
    actions += [
        branch("mapped_profile", "=IsBlank(Topic.profile.data.contactId) || (Topic.CheckCpf && Topic.profile.data.cpfVerified <> true)",
               stop("unmapped", "Demonstração fictícia. Não foi possível validar seu cadastro autenticado ou confirmar o CPF. Não consultarei contratos de outra pessoa.")),
        say("profile_name", '="Demonstração fictícia. Cadastro autenticado: " & Topic.profile.data.name & "."'),
    ]
    contract = {"id": {"type": "String"}, "number": "String", "vehicle": "String",
                "debtCents": "Number", "eligible": "Boolean"}
    actions += http("contracts", "contracts", "={}",
                    {"contracts": {"kind": "Table", "properties": contract}})
    actions += [
        branch("no_contracts", "=CountRows(Topic.contracts.data.contracts) = 0",
               stop("no_contract", "Demonstração fictícia. Não há contratos vinculados à sua identidade.")),
        say("own_contracts", '="Demonstração fictícia. Seus contratos: " & Concat(Topic.contracts.data.contracts, number & " — " & vehicle & " — saldo " & ' +
            money("debtCents") + ' & If(eligible, " — elegível", " — não elegível"), Char(10))'),
        branch("no_eligible_contracts", "=CountRows(Filter(Topic.contracts.data.contracts, eligible = true)) = 0",
               stop("not_eligible", "Demonstração fictícia. Nenhum de seus contratos está elegível para renegociação.")),
        selection("select_contract", "Topic.ContractId", "Demonstração fictícia: selecione seu contrato",
                  'ForAll(Filter(Topic.contracts.data.contracts, eligible = true), {title: number & " — " & vehicle & " — " & ' +
                  money("debtCents") + ', value: id})'),
        branch("valid_contract_selection", "=IsBlank(Topic.ContractId) || CountRows(Filter(Topic.contracts.data.contracts, id = Topic.ContractId && eligible = true)) <> 1",
               stop("invalid_contract", "Demonstração fictícia. Selecione somente um contrato elegível retornado para sua conta.")),
    ]
    actions += http("offers", "offers", "={contractId: Topic.ContractId}",
                    {"offers": {"kind": "Table", "properties": OFFER}})
    actions += [
        branch("no_offers", "=CountRows(Topic.offers.data.offers) = 0",
               stop("unavailable_offers", "Demonstração fictícia. Não há ofertas disponíveis para esse contrato.")),
        selection("select_offer", "Topic.OfferId", "Demonstração fictícia: selecione uma oferta",
                  "ForAll(Topic.offers.data.offers, {title: " + summary("ThisRecord") + ", value: id})"),
        branch("valid_offer_selection", "=IsBlank(Topic.OfferId) || CountRows(Filter(Topic.offers.data.offers, id = Topic.OfferId)) <> 1",
               stop("invalid_offer", "Demonstração fictícia. Selecione somente uma oferta retornada pelo serviço.")),
        setvar("selected_offer", "Topic.SelectedOffer", "=First(Filter(Topic.offers.data.offers, id = Topic.OfferId))"),
    ]
    actions += http("start", "start", '={contractId: Topic.ContractId, channel: "chat"}',
                    {"requestId": "String", "status": "String"}, allow_processing=True)
    actions += [
        branch("start_request_required", "=IsBlank(Topic.start.data.requestId)",
               stop("no_request", "Demonstração fictícia. O serviço não identificou a solicitação. Nenhuma confirmação foi enviada.")),
        setvar("remember_request", "Global.RenegRequestId", "=Topic.start.data.requestId"),
        branch("start_pending", '=Topic.start.status = "processing"',
               stop("start_wait", '="Demonstração fictícia. Solicitação " & Global.RenegRequestId & " PENDENTE: o atendimento está sendo preparado. Ainda não enviei confirmação nem concluí acordo. Diga consultar andamento para verificar. Quando o serviço estiver disponível, diga renegociar para retomar a seleção e revisão."')),
    ]
    actions += http("review", "review",
                    "={requestId: Global.RenegRequestId, offerId: Topic.SelectedOffer.id, version: Topic.SelectedOffer.version}",
                    {"requestId": "String", "reviewHash": "String",
                     "offer": {"kind": "Record", "properties": OFFER}})
    reviewed = "Topic.review.data.offer"
    invalid_terms = " || ".join(
        "IsBlank(" + reviewed + "." + field + ") || " + reviewed + "." + field +
        " < " + str(minimum) + " || Mod(" + reviewed + "." + field + ", 1) <> 0"
        for field, minimum in [("version", 1), ("entryCents", 0), ("installments", 1),
                               ("paymentCents", 0), ("totalCents", 0)])
    actions += [
        branch("valid_review", '=IsBlank(Topic.review.data.reviewHash) || Topic.review.data.requestId <> Global.RenegRequestId || ' +
               reviewed + ".id <> Topic.SelectedOffer.id || " + reviewed + ".version <> Topic.SelectedOffer.version || " +
               invalid_terms + " || IsBlank(" + reviewed + ".validUntil)",
               stop("invalid_review", "Demonstração fictícia. A revisão não corresponde à oferta selecionada ou contém condições inválidas. Não vou confirmar.")),
        say("read_exact_review", '="DEMONSTRAÇÃO FICTÍCIA. Contrato selecionado: " & First(Filter(Topic.contracts.data.contracts, id = Topic.ContractId)).number & ". " & ' + summary(reviewed)),
        question("explicit_confirmation", "Topic.Confirmed", "Confirma expressamente a simulação com exatamente a entrada, parcelas, total, versão e validade lidos acima? Sim envia a confirmação; Não encerra sem finalizar."),
        branch("not_confirmed", "=Topic.Confirmed <> true",
               stop("cancelled", "Demonstração fictícia. Não enviei a confirmação. Nenhum acordo foi concluído por este chat.")),
    ]
    actions += http("confirm", "confirm",
                    "={requestId: Global.RenegRequestId, reviewHash: Topic.review.data.reviewHash, confirmed: true}",
                    RESULT, recover=True)
    actions += [
        branch("confirm_transport_error", "=!IsBlank(Topic.confirmError)", [
            say("uncertain_write", "Demonstração fictícia. A confirmação teve resposta incerta. Vou consultar o andamento uma vez, sem repetir a confirmação."),
            *http("recover", "status", "={requestId: Global.RenegRequestId}", RESULT, recover=True),
            final_result("recovery", "Topic.recover"),
            node("EndDialog", "recovery_end"),
        ]),
        final_result("confirmed", "Topic.confirm"),
    ]
    docs = {"Renegociar.yaml": topic("OnRecognizedIntent", actions, intent={
        "displayName": "Renegociar financiamento",
        "triggerQueries": ["quero renegociar meu financiamento", "renegociar", "negociar dívida",
                           "consultar meus contratos", "ofertas de renegociação", "simular acordo"]})}
    status_actions = [
        node("OAuthInput", "status_login", title="Entrar com segurança",
             text="Entre com sua conta para consultar apenas suas solicitações."),
        branch("status_identity", "=System.User.IsLoggedIn <> true || IsBlank(System.User.AccessToken)",
               stop("status_no_identity", "Demonstração fictícia. Entre com sua conta para consultar o andamento.")),
        branch("status_request", "=IsBlank(Global.RenegRequestId)",
               stop("status_no_request", "Demonstração fictícia. Não há solicitação neste chat. Diga renegociar para consultar seus contratos.")),
        *http("status", "status", "={requestId: Global.RenegRequestId}", RESULT, recover=True),
        final_result("current_status", "Topic.status"),
    ]
    docs["Andamento.yaml"] = topic("OnRecognizedIntent", status_actions, intent={
        "displayName": "Consultar andamento",
        "triggerQueries": ["consultar andamento", "status do acordo", "minha solicitação", "já concluiu"]})
    docs["ConversationStart.yaml"] = topic("OnConversationStart", [say("welcome",
        "DEMONSTRAÇÃO FICTÍCIA — assistente de renegociação. Diga renegociar para entrar com segurança, consultar seus contratos e concluir a simulação inteiramente neste chat. CPF não autentica.")])
    docs["Signin.yaml"] = topic("OnSignIn", [
        node("OAuthInput", "required_login", title="Entrar com segurança",
             text="Demonstração fictícia. Entre com sua conta para continuar; CPF não substitui autenticação."),
        branch("required_login_result",
               "=System.User.IsLoggedIn <> true || IsBlank(System.User.AccessToken)",
               stop("required_login_failed", "Demonstração fictícia. Não foi possível autenticar sua conta. Nenhum contrato foi consultado.")),
    ])
    docs["Fallback.yaml"] = topic("OnUnknownIntent", [say("fallback",
        "Demonstração fictícia. Posso renegociar seu financiamento ou consultar andamento. Não invento valores nem acesso contratos por CPF. Diga renegociar ou consultar andamento.")])
    docs["OnError.yaml"] = topic("OnError", [say("safe_error",
        "Demonstração fictícia. Não consegui verificar esta operação. Não afirmo que foi concluída e não repetirei confirmações automaticamente. Se já confirmou, diga consultar andamento."), node("CancelAllDialogs", "cancel_error")])
    docs["instructions.yaml"] = {"kind": "GptComponentMetadata", "instructions": (
        "Atenda exclusivamente à demonstração fictícia de renegociação. Use apenas os tópicos "
        "determinísticos Renegociar e Andamento. Nunca invente valores, ofertas, protocolos ou "
        "conclusões. CPF não autentica. Somente a identidade delegada e validada pelo serviço "
        "autoriza contratos. Não peça tokens, senhas nem contactId. Não use credenciais do maker. "
        "A confirmação e a conclusão acontecem neste chat, sem redirecionar para página. "
        "Somente status ok com data.status completed e protocolo não vazio significa conclusão. "
        "HTTP 202/processing, pendente, timeout e erro nunca são sucesso. Uma escrita ambígua "
        "só permite consultar status; nunca repetir confirm. Sempre identifique demonstração fictícia."),
        "gptCapabilities": {"webBrowsing": False},
        "aISettings": {"useModelKnowledge": False}}
    for name, doc in docs.items():
        save(name, doc)
    template = {
        "kind": "BotDefinition",
        "entity": {"accessControlPolicy": "Any",
                   "authenticationMode": "CustomAzureActiveDirectory",
                   "authenticationTrigger": "Always", "language": 1046,
                   "configuration": {"settings": {"GenerativeActionsEnabled": False},
                                     "publishOnCreate": False, "publishOnImport": False},
                   "template": "kickStartTemplate-1.0.0"},
        "components": [{
            "kind": "DialogComponent", "displayName": "ConversationStart",
            "schemaName": "template-content.topic.ConversationStart", "dialog": docs["ConversationStart.yaml"],
            "state": "Active", "status": "Active",
        }],
    }
    save("template.yaml", template)
    return docs


def check(docs, bot=None):
    for name in docs:
        args = [sys.executable, str(CPS), "--org", ORG, "check", str(OUT / name),
                "--schema", str(OUT / "bot.schema.yaml-authoring.json")]
        if bot:
            args += ["--bot", bot]
        subprocess.run(args, check=True)


def self_test(docs):
    dialogs = [doc for doc in docs.values() if doc["kind"] == "AdaptiveDialog"]
    sys.path.insert(0, str(CPS.parent))
    from cps_agent import nodes_of, struct_issues, style_issues
    for doc in dialogs:
        assert not struct_issues(doc)[0]
        text = yaml.dump(doc, Dumper=CopilotDumper, width=1_000_000)
        assert not style_issues(text)
        assert not any(token in text for token in ("&id0", "*id0"))
    all_nodes = [n for doc in dialogs for n in nodes_of(doc)]
    requests = [n for n in all_nodes if n.get("kind") == "HttpRequestAction"]
    assert {n["url"].removeprefix(API) for n in requests} == {
        "profile", "contracts", "offers", "start", "review", "confirm", "status"}
    assert all(n["method"] == "Post" and n["headers"]["Authorization"] ==
               '="Bearer " & System.User.AccessToken' for n in requests)
    assert sum(n["url"] == API + "confirm" for n in requests) == 1
    assert all("contactId" not in n["body"]["content"] for n in requests)
    assert sum(n.get("kind") == "AdaptiveCardPrompt" for n in all_nodes) == 2
    assert not any(n.get("kind") in ("InvokeConnectorAction", "InvokeFlowAction", "SearchAndSummarizeContent")
                   for n in all_nodes)
    assert all(n["responseSchema"]["properties"]["demonstration"] == "Boolean" for n in requests)
    assert all(n["responseSchema"]["properties"]["data"]["type"]["kind"] == "Record" for n in requests)
    assert all(n["response"].startswith("init:Topic.") for n in requests)
    assert all(not text.startswith("=") for n in all_nodes if n.get("kind") == "SendActivity"
               for text in n["activity"]["text"])
    reneg_actions = docs["Renegociar.yaml"]["beginDialog"]["actions"]
    action_ids = [n["id"] for n in reneg_actions]
    assert action_ids.index("remember_request") < action_ids.index("start_pending") < action_ids.index("review")
    start_wrapper = next(n for n in all_nodes if n.get("id") == "start_wrapper_case")
    assert 'Topic.start.status <> "processing"' in start_wrapper["condition"]
    profile = next(n for n in requests if n["id"] == "profile")
    assert profile["body"]["kind"] == "RawRequestContent"
    assert profile["body"]["content"].endswith(', "{}")')
    assert all('id: "selectedId", label:' in n["card"] for n in all_nodes
               if n.get("kind") == "AdaptiveCardPrompt")
    complete_conditions = [n["condition"] for n in all_nodes if
                           isinstance(n.get("id"), str) and n["id"].endswith("_complete_case")]
    assert len(complete_conditions) == 3
    for condition in complete_conditions:
        assert all(required in condition for required in
                   ('.demonstration = true', '.status = "ok"', '.data.status = "completed"',
                    '.data.requestId = Global.RenegRequestId', 'IsBlank(Trim(', '.data.protocol'))
    print(f"PASS: {len(dialogs)} deterministic topics, {len(requests)} delegated HTTP nodes; one explicit confirm, two selections, no maker tools.")


def check_native(docs):
    sys.path.insert(0, str(CPS.parent))
    from cps_agent import nodes_of
    roundtrip = yaml.safe_load((OUT / "native-roundtrip.yaml").read_text(encoding="utf-8"))
    native_topics = {c["displayName"]: c["dialog"] for c in roundtrip["components"]
                     if c.get("kind") == "DialogComponent"}
    for name, doc in docs.items():
        if doc["kind"] == "AdaptiveDialog":
            expected = {n["id"] for n in nodes_of(doc) if n.get("kind") and isinstance(n.get("id"), str)}
            actual = {n["id"] for n in nodes_of(native_topics[Path(name).stem])
                      if n.get("kind") and isinstance(n.get("id"), str)}
            assert expected == actual, f"Native reader dropped nodes in {name}: {expected - actual}"
    print("PASS: native PAC reader preserved every topic action ID.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["generate", "check", "self-test", "native-check", "apply"])
    parser.add_argument("--bot", default=BOT)
    args = parser.parse_args()
    docs = build()
    self_test(docs)
    if args.command == "native-check":
        check_native(docs)
    if args.command in ("check", "apply"):
        check(docs, args.bot if args.command == "apply" else None)
    if args.command == "apply":
        for name in docs:
            command = [sys.executable, str(CPS), "--org", ORG, "apply", str(OUT / name),
                       "--bot", args.bot, "--yes"]
            if name != "instructions.yaml":
                command += ["--suffix", "topic." + Path(name).stem]
            subprocess.run(command, check=True)
        check(docs, args.bot)
    print(json.dumps({"bot": args.bot, "output": str(OUT), "publishPerformed": False}))


if __name__ == "__main__":
    main()
