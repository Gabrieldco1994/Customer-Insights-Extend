import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";
import { chatSession, requestChat, mountChat } from "../src/chat.js";
import { SignInRedirect } from "../src/identity.js";

const session = {
  token: "unit-test-conversation-token",
  conversationId: "conversation-1", userId: "dl_verified_customer", expires_in: 1800,
};

test("a fresh server token starts a conversation instead of reconnecting to an unstarted ID", () => {
  let options;
  let rendered;
  let closed = 0;
  const directLine = {
    connectionStatus$: { subscribe() { return { unsubscribe() {} }; } },
    end() { closed++; },
  };
  const context = {
    WebChat: {
      createDirectLine(value) { options = value; return directLine; },
      renderWebChat(value) { rendered = value; },
    },
    document: { getElementById() { return {}; } },
  };
  const source = readFileSync(new URL("../src/chat-frame.js", import.meta.url), "utf8")
    .replace('import "botframework-webchat";', "");
  runInNewContext(source, context);
  const controller = context.FSIRenegChat.start(session, () => {});
  assert.deepEqual(Object.keys(options), ["token"]);
  assert.equal(options.token, session.token);
  assert.equal(rendered.userID, session.userId);
  assert.equal(rendered.directLine, directLine);
  controller.end();
  assert.equal(closed, 1);
});

test("chat accepts only the exact server-issued short-lived session contract", () => {
  assert.equal(chatSession(session), session);
  for (const change of [
    { token: "" }, { conversationId: null }, { userId: "browser-contact-id" },
    { expires_in: 0 }, { expires_in: "1800" }, { expires_in: 1.5 },
    { secret: "never-send-a-secret-to-the-browser" },
  ]) assert.throws(() => chatSession({ ...session, ...change }), /inválida/);
  assert.throws(() => chatSession({ status: "ok", data: session, demonstration: true }));
});

test("chat-token uses the existing delegated identity, no customer selector or URL credential", async () => {
  let observed;
  const result = await requestChat("https://api.example.test", "unit-test-identity", async (url, options) => {
    observed = { url, options };
    return { ok: true, status: 200, async json() { return session; } };
  });
  assert.equal(result, session);
  assert.equal(observed.url, "https://api.example.test/api/renegociacao/chat-token");
  assert.equal(observed.options.headers.Authorization, "Bearer unit-test-identity");
  assert.equal(observed.options.body, "{}");
  assert.equal(observed.options.credentials, "omit");
  assert.equal(observed.options.cache, "no-store");
});

test("chat errors preserve safe exact codes without displaying server bodies or tokens", async () => {
  await assert.rejects(() => requestChat("https://api.example.test", "unit-test", async () => ({
    ok: false, status: 403,
    async json() { return { error: { code: "ORIGIN_DENIED", message: "private server content" } }; },
  })), (failure) => failure.message.includes("ORIGIN_DENIED") && !failure.message.includes("private"));
  await assert.rejects(() => requestChat("https://api.example.test", "unit-test", async () => ({
    ok: true, status: 202, async json() { return session; },
  })), /HTTP_202/);
});

function ui(overrides = {}) {
  let handler;
  let ended = false;
  let rendered;
  let removed = false;
  const button = {
    textContent: "Iniciar conversa", disabled: false,
    addEventListener(event, callback) { assert.equal(event, "click"); handler = callback; },
  };
  const status = { textContent: "", setAttribute() {}, focus() {} };
  const host = { replaceChildren() { removed = true; } };
  mountChat({
    button, host, status,
    identity: { async token() { return "unit-test-identity"; } },
    configuration: () => ({ api: "https://api.example.test" }),
    async frameLoader() {
      return { contentWindow: { FSIRenegChat: {
        start(value, onStatus) {
          rendered = value;
          onStatus(2);
          return { end() { ended = true; } };
        },
      } } };
    },
    async chatRequester() { return session; },
    ...overrides,
  });
  return {
    click: () => handler(), button, status,
    rendered: () => rendered, ended: () => ended, removed: () => removed,
  };
}

test("optional chat opens and closes its isolated host without touching the financial journey", async () => {
  const fixture = ui();
  await fixture.click();
  assert.equal(fixture.rendered(), session);
  assert.match(fixture.status.textContent, /conectado/);
  assert.equal(fixture.button.textContent, "Encerrar conversa");
  await fixture.click();
  assert.ok(fixture.ended());
  assert.ok(fixture.removed());
  assert.equal(fixture.button.textContent, "Iniciar conversa");
});

test("chat failures remain in the optional section and re-enable a retry", async () => {
  const fixture = ui({ async chatRequester() { throw new Error("ORIGIN_DENIED"); } });
  await fixture.click();
  assert.equal(fixture.status.textContent, "ORIGIN_DENIED");
  assert.equal(fixture.button.disabled, false);
  assert.equal(fixture.rendered(), undefined);
  assert.ok(fixture.removed());
});

test("MSAL redirect does not create an anonymous conversation or fake chat success", async () => {
  let requested = false;
  const fixture = ui({
    identity: { async token() { throw new SignInRedirect(); } },
    async chatRequester() { requested = true; return session; },
  });
  await fixture.click();
  assert.equal(requested, false);
  assert.equal(fixture.rendered(), undefined);
  assert.match(fixture.status.textContent, /login seguro/);
});
