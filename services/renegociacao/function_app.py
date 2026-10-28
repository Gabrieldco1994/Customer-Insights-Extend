"""Azure Functions v2 entry points. Only customer actions are host-anonymous."""

import json
import logging
import os
import threading
import time
from urllib.parse import parse_qsl, urlsplit

import azure.functions as func
from azure.core.exceptions import AzureError
import pytds
from PowerPlatform.Dataverse.core.errors import HttpError

from renegociacao import assisted, bootstrap, odata, whatsapp_demo
from renegociacao.core import ApiError, Service
from renegociacao.dataverse import NativeProjection, log_integration_error
from renegociacao.directline_token import DirectLineTokenError, issue_token
from renegociacao.security import authenticate, credential, required
from renegociacao.sql import SqlRepository


app = func.FunctionApp()
_lock = threading.Lock()
_service = None


def service() -> Service:
    global _service
    with _lock:
        if _service is None:
            identity = credential()
            _service = Service(SqlRepository(identity), NativeProjection(identity))
        return _service


def reply(payload: object, status: int = 200, content_type: str = "application/json") -> func.HttpResponse:
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return func.HttpResponse(
        text, status_code=status, mimetype=content_type, charset="utf-8",
        headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
                 "OData-Version": "4.0"} if content_type != "application/json" else
                {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})


def failure(error: ApiError, is_odata: bool = False) -> func.HttpResponse:
    problem = {"code": error.code, "message": error.message}
    payload = {"error": problem} if is_odata else {"error": problem, "demonstration": True}
    response = reply(payload, error.http)
    if error.http == 401:
        response.headers["WWW-Authenticate"] = "Bearer"
    if is_odata:
        response.headers["OData-Version"] = "4.0"
    return response


def body_json(request: func.HttpRequest) -> object:
    raw = request.get_body()
    if len(raw) > 1_048_576:
        raise ApiError("BODY_TOO_LARGE", "The request body exceeds the allowed size.", 413)
    if request.headers.get("content-type", "").split(";")[0].lower() != "application/json":
        raise ApiError("CONTENT_TYPE_REQUIRED", "Use application/json.", 415)
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise ApiError("INVALID_JSON", "A valid JSON body is required.") from None


def source_diagnostic(error: Exception) -> str:
    if isinstance(error, HttpError):
        log_integration_error("source", error)
    numbers = [
        str(value) if isinstance(value, int) else "0"
        for value in (getattr(error, "number", 0), getattr(error, "state", 0),
                      getattr(error, "line", 0))
    ]
    code = type(error).__name__ + ":" + ":".join(numbers)
    logging.error("REN_DEMO_SOURCE_FAILURE %s", code)
    return code


@app.function_name(name="CustomerRenegotiation")
@app.route(route="renegociacao/{action}", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS,
           trigger_arg_name="request")
def customer(request: func.HttpRequest) -> func.HttpResponse:
    try:
        identity = authenticate(request.headers)
        backend = service()
        contact = (backend.crm.resolve_entra_contact(identity.object_id, identity.issuer)
                   if identity.mode == "entra" else identity.contact_id)
        body = body_json(request)
        if request.route_params["action"] == "chat-token":
            if identity.mode != "entra":
                raise ApiError("CHAT_IDENTITY_DENIED", "Delegated Entra authentication is required.", 403)
            if body != {}:
                raise ApiError("INVALID_BODY", "The chat-token action accepts an empty JSON object.")
            conversation = issue_token(
                secret=required("DIRECTLINE_SECRET"), origin=request.headers.get("origin"),
                verified_oid=identity.object_id, verified_issuer=identity.issuer,
            )
            response = reply(conversation.client_payload())
            response.headers.update(conversation.response_headers())
            return response
        payload, code = backend.execute(request.route_params["action"], contact, body)
        return reply(payload, code)
    except ApiError as error:
        return failure(error)
    except DirectLineTokenError as error:
        return failure(ApiError(error.code, "The authenticated chat channel is unavailable.", error.status))
    except (pytds.Error, AzureError) as error:
        source_diagnostic(error)
        return failure(ApiError("SOURCE_UNAVAILABLE", "Retry status to check durable source state.", 503))
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        # Boundary only: do not log exception objects, stack locals, tokens, CPF or request bodies.
        logging.error("REN_DEMO_UNEXPECTED_CUSTOMER_FAILURE")
        return failure(ApiError("INTERNAL_ERROR", "The request could not be processed. Check status before retrying.", 500))


@app.function_name(name="WhatsAppDemoRenegotiation")
@app.route(route="whatsapp-demo/{action}", methods=["POST"], auth_level=func.AuthLevel.FUNCTION,
           trigger_arg_name="request")
def whatsapp_simulation(request: func.HttpRequest) -> func.HttpResponse:
    if os.environ.get("WHATSAPP_DEMO_ENABLED") != "1":
        return failure(ApiError("DEMO_DISABLED", "The isolated WhatsApp simulation is disabled.", 403))
    try:
        payload, status = whatsapp_demo.execute(
            service(), request.route_params["action"], body_json(request),
        )
        return reply(payload, status)
    except ApiError as error:
        return failure(error)
    except (pytds.Error, AzureError) as error:
        source_diagnostic(error)
        return failure(ApiError("DEMO_SOURCE_UNAVAILABLE", "The synthetic source is unavailable.", 503))
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_WHATSAPP_DEMO_UNEXPECTED_FAILURE")
        return failure(ApiError("DEMO_INTERNAL_ERROR", "The simulation could not be processed.", 500))


@app.function_name(name="AssistedPortal")
@app.route(route="assisted/{action}", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS,
           trigger_arg_name="request")
def assisted_portal(request: func.HttpRequest) -> func.HttpResponse:
    started = time.perf_counter()
    phases = {}
    action = request.route_params.get("action", "")
    try:
        identity = authenticate(request.headers)
        phases["jwt"] = round((time.perf_counter() - started) * 1000)
        backend = service()
        phases["service"] = round((time.perf_counter() - started) * 1000)
        contact = assisted.operator(backend, identity)
        phases["operator"] = round((time.perf_counter() - started) * 1000)
        data = assisted.execute(backend, contact, action, body_json(request))
        response = reply({"status": "ok", "data": data, "demonstration": True})
        response.headers["Server-Timing"] = f"assisted;dur={(time.perf_counter() - started) * 1000:.0f}"
        return response
    except ApiError as error:
        return failure(error)
    except (pytds.Error, AzureError, HttpError) as error:
        source_diagnostic(error)
        return failure(ApiError("ASSISTED_UNAVAILABLE", "O servico assistido nao confirmou a operacao.", 503))
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_ASSISTED_UNEXPECTED_FAILURE")
        return failure(ApiError("ASSISTED_ERROR", "Consulte a ocorrencia antes de repetir a solicitacao.", 500))
    finally:
        if action in {"contracts", "offers", "preview", "request", "status"}:
            logging.warning(
                "REN_ASSISTED_TIMING action=%s elapsed_ms=%d phases=%s",
                action, round((time.perf_counter() - started) * 1000), json.dumps(phases),
            )


@app.function_name(name="AssistedAutomation")
@app.route(route="assisted-automation/prepare-mail", methods=["POST"], auth_level=func.AuthLevel.FUNCTION,
           trigger_arg_name="request")
def assisted_automation(request: func.HttpRequest) -> func.HttpResponse:
    try:
        data = assisted.prepare_mail(service(), body_json(request))
        return reply(data)
    except ApiError as error:
        return failure(error)
    except (pytds.Error, AzureError, HttpError) as error:
        source_diagnostic(error)
        return failure(ApiError("ASSISTED_UNAVAILABLE", "O email de aceite nao foi preparado.", 503))
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_ASSISTED_MAIL_UNEXPECTED_FAILURE")
        return failure(ApiError("ASSISTED_ERROR", "O email de aceite nao foi preparado.", 500))


@app.function_name(name="AssistedCustomerAcceptance")
@app.route(route="assisted-acceptance/{action}", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS,
           trigger_arg_name="request")
def assisted_customer_acceptance(request: func.HttpRequest) -> func.HttpResponse:
    try:
        data = assisted.acceptance(service(), request.route_params["action"], body_json(request))
        return reply({"status": "processing" if data.get("integrationPending") else "ok",
                      "data": data, "demonstration": True},
                     202 if data.get("integrationPending") else 200)
    except ApiError as error:
        return failure(error)
    except (pytds.Error, AzureError, HttpError) as error:
        source_diagnostic(error)
        return failure(ApiError("ASSISTED_UNAVAILABLE", "O aceite nao foi confirmado. Consulte novamente.", 503))
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_ASSISTED_ACCEPTANCE_UNEXPECTED_FAILURE")
        return failure(ApiError("ASSISTED_ERROR", "O aceite nao foi confirmado.", 500))


def mcp_read(action: str, context: str) -> str:
    try:
        if os.environ.get("WHATSAPP_DEMO_ENABLED") != "1":
            raise ApiError("DEMO_DISABLED", "The isolated simulation is disabled.", 403)
        if action not in {"contracts", "offers", "status"}:
            raise ApiError("MCP_READ_ONLY", "MCP tools cannot create or confirm agreements.", 403)
        try:
            invocation = json.loads(context)
        except ValueError:
            raise ApiError("INVALID_JSON", "MCP arguments must contain valid JSON.") from None
        if not isinstance(invocation, dict) or not isinstance(invocation.get("arguments", {}), dict):
            raise ApiError("INVALID_BODY", "MCP arguments must be a JSON object.")
        payload, _ = whatsapp_demo.execute(service(), action, {
            "payload": json.dumps(invocation.get("arguments", {})),
        })
        if action == "offers":
            for offer in payload["data"]["offers"]:
                offer["resumo"] = whatsapp_demo.offer_summary(offer)
        return json.dumps(payload, ensure_ascii=False)
    except ApiError as error:
        logging.warning("REN_MCP_FAILURE %s", error.code)
        return json.dumps({"error": {"code": error.code, "message": error.message},
                           "demonstration": True, "simulationOnly": True})
    except (pytds.Error, AzureError) as error:
        source_diagnostic(error)
        return json.dumps({"error": {"code": "DEMO_SOURCE_UNAVAILABLE",
                                    "message": "The synthetic source is unavailable."}})
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_MCP_UNEXPECTED_FAILURE")
        return json.dumps({"error": {"code": "DEMO_INTERNAL_ERROR",
                                    "message": "The simulation could not be queried."}})


@app.function_name(name="ListDemoContractsMcp")
@app.mcp_tool_trigger(arg_name="context", tool_name="listar_contratos_demo",
                      description="Lista somente os contratos ficticios WHATS-DEMO e sua elegibilidade. Nao consulta clientes reais, nao autentica e nao grava acordos.",
                      tool_properties="[]")
def list_demo_contracts_mcp(context: str) -> str:
    return mcp_read("contracts", context)


@app.function_name(name="ListDemoOffersMcp")
@app.mcp_tool_trigger(arg_name="context", tool_name="consultar_ofertas_demo",
                      description="Consulta ofertas reais da fonte ficticia para um contractId retornado por listar_contratos_demo. Apresente resumo, valores, parcelas, total, validade, id e versao sem calcular ou inventar descontos. Nao confirma acordos.",
                      tool_properties=json.dumps([{
                          "propertyName": "contractId", "propertyType": "string",
                          "description": "ID de contrato WHATS-DEMO retornado pela ferramenta de contratos.",
                          "isRequired": True,
                      }]))
def list_demo_offers_mcp(context: str) -> str:
    return mcp_read("offers", context)


@app.function_name(name="GetDemoStatusMcp")
@app.mcp_tool_trigger(arg_name="context", tool_name="consultar_andamento_demo",
                      description="Consulta andamento de um contrato ficticio WHATS-DEMO. Somente completed com protocolo confirma conclusao; processing nao e sucesso.",
                      tool_properties=json.dumps([{
                          "propertyName": "contractId", "propertyType": "string",
                          "description": "ID de contrato WHATS-DEMO retornado pela ferramenta de contratos.",
                          "isRequired": True,
                      }]))
def get_demo_status_mcp(context: str) -> str:
    return mcp_read("status", context)


@app.function_name(name="ExternalSourceOData")
@app.route(route="odata/{*path}", methods=["GET"], auth_level=func.AuthLevel.FUNCTION,
           trigger_arg_name="request")
def external_odata(request: func.HttpRequest) -> func.HttpResponse:
    try:
        query_pairs = parse_qsl(urlsplit(request.url).query, keep_blank_values=True)
        # Functions host consumes ?code; never echo it into a nextLink.
        query_pairs = [(key, value) for key, value in query_pairs if key != "code"]
        if len(dict(query_pairs)) != len(query_pairs):
            raise odata.unsupported()
        origin = required("ODATA_PUBLIC_ROOT").rstrip("/")
        parsed = urlsplit(origin)
        if parsed.scheme != "https" or not parsed.hostname or parsed.query or parsed.fragment or parsed.username:
            raise ApiError("CONFIGURATION_ERROR", "Configure the HTTPS OData public root.", 503)
        # OData source reads do not depend on Dataverse or portal JWT configuration.
        repository = SqlRepository(credential())
        payload, mime = odata.read(repository, request.route_params.get("path", ""), dict(query_pairs), origin)
        response = reply(payload, content_type=mime)
        response.headers["OData-Version"] = "4.0"
        return response
    except ApiError as error:
        logging.error(
            "REN_DEMO_ODATA_FAILURE %s parameter_names=%s",
            error.code, sorted(key for key in request.params if key != "code"),
        )
        return failure(error, True)
    except (pytds.Error, AzureError) as error:
        source_diagnostic(error)
        return failure(ApiError("SOURCE_UNAVAILABLE", "The external source is unavailable.", 503), True)
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_DEMO_UNEXPECTED_ODATA_FAILURE")
        return failure(ApiError("INTERNAL_ERROR", "The external source request failed.", 500), True)


@app.function_name(name="BootstrapDemoSource")
@app.route(route="bootstrap", methods=["POST"], auth_level=func.AuthLevel.FUNCTION,
           trigger_arg_name="request")
def bootstrap_source(request: func.HttpRequest) -> func.HttpResponse:
    if os.environ.get("BOOTSTRAP_ENABLED") != "1":
        return failure(ApiError("BOOTSTRAP_DISABLED", "Source bootstrap is disabled.", 403))
    try:
        result = bootstrap.initialize(SqlRepository(credential()), body_json(request))
        return reply({"status": "ok", "data": result, "demonstration": True})
    except ApiError as error:
        return failure(error)
    except (pytds.Error, AzureError) as error:
        diagnostic = source_diagnostic(error)
        return failure(ApiError(
            "BOOTSTRAP_FAILED",
            f"Source initialization failed ({diagnostic}); check the dedicated SQL source.",
            503,
        ))
    except (ValueError, TypeError, KeyError, RuntimeError, OSError):
        logging.error("REN_DEMO_UNEXPECTED_BOOTSTRAP_FAILURE")
        return failure(ApiError("INTERNAL_ERROR", "Source initialization failed.", 500))
