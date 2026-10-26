"""Azure Functions v2 entry points. Only customer actions are host-anonymous."""

import json
import logging
import os
import threading
from urllib.parse import parse_qsl, urlsplit

import azure.functions as func
from azure.core.exceptions import AzureError
import pytds

from renegociacao import bootstrap, odata
from renegociacao.core import ApiError, Service
from renegociacao.dataverse import NativeProjection
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
