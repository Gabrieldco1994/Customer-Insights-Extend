"""Server-side Direct Line token issuance for the dedicated renegotiation bot.

The caller must first validate the delegated JWT, required scope and contact
mapping. Neither an Origin header nor the Direct Line user ID authenticates a
customer. Never log the secret, upstream response or returned token.
"""
from dataclasses import dataclass, field
import hashlib
import hmac
import json
import urllib.error
import urllib.request
import uuid

PORTAL_ORIGIN = "https://portalatendimento01.powerappsportals.com"
OFFICIAL_DEMO_ORIGIN = "https://copilotstudio.microsoft.com"
TENANT = "3a3c8be3-6a50-455f-a303-f9df0c768430"
ISSUERS = frozenset({
    f"https://login.microsoftonline.com/{TENANT}/v2.0",
    f"https://sts.windows.net/{TENANT}/",
})
TOKEN_ENDPOINT = "https://directline.botframework.com/v3/directline/tokens/generate"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        raise urllib.error.HTTPError(request.full_url, code, message, headers, fp)


class DirectLineTokenError(RuntimeError):
    """Intentionally excludes upstream bodies and credential-bearing details."""

    def __init__(self, code, status=503):
        self.code = code
        self.status = status
        super().__init__(code)


@dataclass(frozen=True)
class ConversationToken:
    token: str = field(repr=False)
    conversation_id: str
    user_id: str
    expires_in: int
    origin: str

    def client_payload(self):
        return {
            "token": self.token,
            "conversationId": self.conversation_id,
            "userId": self.user_id,
            "expires_in": self.expires_in,
        }

    def response_headers(self):
        return {
            "Cache-Control": "no-store",
            "Pragma": "no-cache",
            "Vary": "Origin",
            "Access-Control-Allow-Origin": self.origin,
            "X-Content-Type-Options": "nosniff",
        }


def issue_token(*, secret, origin, verified_oid, verified_issuer,
                allow_official_demo=False, opener=None):
    allowed = {PORTAL_ORIGIN}
    if allow_official_demo:
        allowed.add(OFFICIAL_DEMO_ORIGIN)
    if origin not in allowed:
        raise DirectLineTokenError("CHAT_ORIGIN_DENIED", 403)
    if verified_issuer not in ISSUERS:
        raise DirectLineTokenError("CHAT_IDENTITY_DENIED", 403)
    try:
        oid = str(uuid.UUID(verified_oid))
    except (ValueError, TypeError, AttributeError):
        raise DirectLineTokenError("CHAT_IDENTITY_DENIED", 403) from None
    if not isinstance(secret, str) or not secret.strip():
        raise DirectLineTokenError("CHAT_NOT_CONFIGURED")
    subject = (verified_issuer + "\0" + oid).encode()
    user_id = "dl_" + hmac.new(secret.encode(), subject, hashlib.sha256).hexdigest()[:48]
    request = urllib.request.Request(
        TOKEN_ENDPOINT,
        data=json.dumps({"user": {"id": user_id}, "trustedOrigins": [origin]}).encode(),
        headers={"Authorization": "Bearer " + secret, "Content-Type": "application/json"},
        method="POST",
    )
    if opener is None:
        opener = urllib.request.build_opener(_NoRedirect()).open
    try:
        with opener(request, timeout=15) as response:
            payload = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        raise DirectLineTokenError("CHAT_CHANNEL_UNAVAILABLE") from None
    token = payload.get("token") if isinstance(payload, dict) else None
    conversation_id = payload.get("conversationId") if isinstance(payload, dict) else None
    expires_in = payload.get("expires_in") if isinstance(payload, dict) else None
    if (not isinstance(token, str) or not token.strip()
            or not isinstance(conversation_id, str) or not conversation_id.strip()
            or type(expires_in) is not int or expires_in <= 0):
        raise DirectLineTokenError("CHAT_CHANNEL_INVALID_RESPONSE")
    return ConversationToken(token, conversation_id, user_id, expires_in, origin)
