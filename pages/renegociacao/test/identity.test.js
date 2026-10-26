import test from "node:test";
import assert from "node:assert/strict";
import { InteractionRequiredAuthError } from "@azure/msal-browser";
import { createIdentity, SignInRedirect } from "../src/identity.js";

globalThis.location = { origin: "https://portal.example.test" };
const config = { client: "client-id", tenant: "tenant-id", audience: "api-id" };
const scopes = ["api://api-id/Renegotiate"];

function setup(overrides = {}) {
  const calls = [];
  let active;
  const client = {
    async initialize() { calls.push("initialize"); },
    async handleRedirectPromise() { calls.push("handleRedirect"); return null; },
    setActiveAccount(account) { active = account; },
    getActiveAccount() { return active; },
    async loginRedirect(request) { calls.push(["loginRedirect", request]); },
    async acquireTokenSilent(request) { calls.push(["silent", request]); return { accessToken: "unit-test-token" }; },
    async acquireTokenRedirect(request) { calls.push(["tokenRedirect", request]); },
    ...overrides,
  };
  let options;
  const identity = createIdentity(() => config, (value) => { options = value; return client; });
  return { identity, calls, client, options: () => options };
}

test("initializes MSAL once and persists only in the browser session for redirect callbacks", async () => {
  const fixture = setup();
  await Promise.all([fixture.identity.initialize(), fixture.identity.initialize()]);
  assert.deepEqual(fixture.calls, ["initialize", "handleRedirect"]);
  assert.equal(fixture.options().cache.cacheLocation, "sessionStorage");
  assert.equal(fixture.options().auth.redirectUri, "https://portal.example.test/renegociacao-financiamento/");
  assert.equal(fixture.options().auth.authority, "https://login.microsoftonline.com/tenant-id");
});

test("an unsigned-in user uses supported redirect, not a blocked popup or fabricated token", async () => {
  const { identity, calls } = setup();
  await assert.rejects(() => identity.token(), SignInRedirect);
  assert.deepEqual(calls.at(-1), ["loginRedirect", { scopes }]);
});

test("redirect completion restores the signed account before requesting the exact API scope", async () => {
  const account = { homeAccountId: "unit-test-account" };
  const { identity, calls } = setup({ async handleRedirectPromise() { return { account }; } });
  assert.equal((await identity.initialize()).account, account);
  assert.equal(await identity.token(), "unit-test-token");
  assert.deepEqual(calls.at(-1), ["silent", { scopes, account }]);
  assert.equal(calls.some((call) => call[0] === "loginRedirect"), false);
});

test("interaction-required uses the same signed account and scope through redirect", async () => {
  const account = { homeAccountId: "unit-test-account" };
  const { identity, calls } = setup({
    getActiveAccount() { return account; },
    async acquireTokenSilent() { throw new InteractionRequiredAuthError("interaction_required"); },
  });
  await assert.rejects(() => identity.token(), SignInRedirect);
  assert.deepEqual(calls.at(-1), ["tokenRedirect", { scopes, account }]);
});

test("unexpected token and callback failures propagate without an alternate trust mode", async () => {
  const original = new Error("strict-auth-failure");
  const fixture = setup({
    getActiveAccount() { return {}; },
    async acquireTokenSilent() { throw original; },
  });
  await assert.rejects(() => fixture.identity.token(), (failure) => failure === original);
  assert.equal(fixture.calls.some((call) => Array.isArray(call) && call[0].includes("Redirect")), false);
  const callback = setup({ async handleRedirectPromise() { throw original; } });
  await assert.rejects(() => callback.identity.initialize(), (failure) => failure === original);
});
