"""Fail-closed identity verification. Caller-supplied identifiers never authorize."""

from dataclasses import dataclass
import os
import re
from urllib.parse import urlsplit

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
import requests

from .core import ApiError, guid


ENTRA_TENANT_ID = "3a3c8be3-6a50-455f-a303-f9df0c768430"
ENTRA_ISSUER = f"https://login.microsoftonline.com/{ENTRA_TENANT_ID}/v2.0"
ENTRA_JWKS_URL = f"https://login.microsoftonline.com/{ENTRA_TENANT_ID}/discovery/v2.0/keys"
ENTRA_SCOPE = "Renegotiate"


@dataclass(frozen=True)
class VerifiedIdentity:
    mode: str
    issuer: str
    contact_id: str | None = None
    object_id: str | None = None


class TenantJwksClient(jwt.PyJWKClient):
    """Use PyJWT caching/rotation, but never follow redirects or token key URLs."""

    def __init__(self):
        super().__init__(ENTRA_JWKS_URL, cache_jwk_set=True, lifespan=300, timeout=10)

    def fetch_data(self) -> dict:
        try:
            response = requests.get(ENTRA_JWKS_URL, timeout=(5, 10), verify=True, allow_redirects=False)
            if response.status_code != 200 or len(response.content) > 1_048_576:
                raise jwt.PyJWKClientConnectionError("Trusted signing keys unavailable")
            document = response.json()
            if not isinstance(document, dict) or not isinstance(document.get("keys"), list):
                raise jwt.PyJWKClientConnectionError("Trusted signing keys unavailable")
        except (requests.RequestException, ValueError):
            raise jwt.PyJWKClientConnectionError("Trusted signing keys unavailable") from None
        if self.jwk_set_cache is not None:
            self.jwk_set_cache.put(document)
        return document


_entra_keys = TenantJwksClient()


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ApiError("CONFIGURATION_ERROR", "The service configuration is incomplete.", 503)
    return value


def bearer_token(headers: dict) -> str:
    authorization = headers.get("authorization", headers.get("Authorization", ""))
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or len(token) > 16384:
        raise ApiError("IDENTITY_REQUIRED", "A signed customer identity is required.", 401)
    return token


def authenticate(headers: dict) -> VerifiedIdentity:
    """Choose exactly one trust boundary; a failed mode never invokes the other."""
    mode = os.environ.get("AUTH_MODE", "portal")
    if mode == "portal":
        return authenticate_portal(headers)
    if mode == "entra":
        return authenticate_entra(headers)
    raise ApiError("CONFIGURATION_ERROR", "AUTH_MODE must be portal or entra.", 503)


def authenticate_portal(headers: dict) -> VerifiedIdentity:
    key = required("PORTAL_PUBLIC_KEY_PEM").replace("\\n", "\n")
    issuer, audience = required("PORTAL_ISSUER"), required("PORTAL_AUDIENCE")
    try:
        parsed_key = serialization.load_pem_public_key(key.encode("ascii"))
        # Accept a public RSA key, never an HMAC secret or a private-key configuration.
        if not isinstance(parsed_key, RSAPublicKey) or parsed_key.key_size < 2048:
            raise ValueError("Public RSA key required")
    except (ValueError, TypeError, jwt.InvalidKeyError):
        raise ApiError("CONFIGURATION_ERROR", "A trusted RSA public key must be configured.", 503) from None
    token = bearer_token(headers)
    try:
        claims = jwt.decode(token, parsed_key, algorithms=["RS256"], audience=audience, issuer=issuer,
                            options={"require": ["exp", "aud", "iss", "sub"]})
        return VerifiedIdentity("portal", issuer, contact_id=guid(claims["sub"]))
    except (jwt.InvalidTokenError, ApiError, TypeError, ValueError):
        raise ApiError("INVALID_IDENTITY", "The portal identity is invalid or expired.", 401) from None


def authenticate_entra(headers: dict) -> VerifiedIdentity:
    audience = required("ENTRA_AUDIENCE")
    try:
        if guid(audience) != audience:
            raise ValueError()
    except (ApiError, ValueError):
        raise ApiError("CONFIGURATION_ERROR", "Configure the dedicated v2 API application client-ID audience.", 503) from None
    token = bearer_token(headers)
    try:
        header = jwt.get_unverified_header(token)
        if (header.get("alg") != "RS256" or header.get("crit")
                or not isinstance(header.get("kid"), str)
                or not re.fullmatch(r"[A-Za-z0-9_.-]{1,256}", header["kid"])):
            raise jwt.InvalidTokenError()
        signing_key = _entra_keys.get_signing_key_from_jwt(token)
        if (signing_key.algorithm_name != "RS256" or not isinstance(signing_key.key, RSAPublicKey)
                or signing_key.key.key_size < 2048):
            raise jwt.InvalidTokenError()
        claims = jwt.decode(
            token, signing_key.key, algorithms=["RS256"], issuer=ENTRA_ISSUER, audience=audience,
            options={"require": ["exp", "iss", "aud", "oid", "tid"], "strict_aud": True})
        if claims["tid"] != ENTRA_TENANT_ID:
            raise jwt.InvalidTokenError()
        object_id = guid(claims["oid"])
        if object_id != claims["oid"]:
            raise jwt.InvalidTokenError()
    except jwt.PyJWKClientConnectionError:
        raise ApiError("IDENTITY_KEYS_UNAVAILABLE", "Trusted identity keys are temporarily unavailable.", 503) from None
    except (jwt.PyJWTError, ApiError, TypeError, ValueError, KeyError):
        raise ApiError("INVALID_IDENTITY", "The Entra identity is invalid or expired.", 401) from None
    scopes = claims.get("scp")
    if claims.get("idtyp") == "app" or not isinstance(scopes, str) or ENTRA_SCOPE not in scopes.split():
        raise ApiError("DELEGATED_SCOPE_REQUIRED", "A delegated Renegotiate access token is required.", 403)
    return VerifiedIdentity("entra", ENTRA_ISSUER, object_id=object_id)


def credential():
    from azure.identity import AzureCliCredential, ManagedIdentityCredential
    mode = os.environ.get("AZURE_CREDENTIAL_MODE", "managed_identity")
    cloud = bool(os.environ.get("WEBSITE_HOSTNAME") or os.environ.get("WEBSITE_INSTANCE_ID"))
    if mode == "azure_cli" and not cloud and os.environ.get("ALLOW_LOCAL_AZURE_CLI") == "1":
        return AzureCliCredential()
    if mode != "managed_identity":
        raise ApiError("CONFIGURATION_ERROR", "Only managed identity is allowed in the hosted service.", 503)
    client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
    return ManagedIdentityCredential(client_id=client_id) if client_id else ManagedIdentityCredential()


def dataverse_url() -> str:
    url = required("DATAVERSE_URL").rstrip("/")
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or not parsed.hostname.endswith(".dynamics.com"):
        raise ApiError("CONFIGURATION_ERROR", "A trusted HTTPS Dataverse URL is required.", 503)
    if parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.port:
        raise ApiError("CONFIGURATION_ERROR", "Use the Dataverse organization origin only.", 503)
    return url
