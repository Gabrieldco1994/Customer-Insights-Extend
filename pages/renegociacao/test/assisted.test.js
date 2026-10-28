import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { cents, customerCpf, displayName, initializeOmnichannel, WIDGET } from "../src/assisted-utils.js";

test("assisted input preserves cents and rejects ambiguous or overprecise money", () => {
  for (const [text, expected] of [["0", 0], ["0,05", 5], ["1234,56", 123456], ["12.3", 1230]]) {
    assert.equal(cents(text), expected);
  }
  for (const text of ["", "-1", "1.234,56", "1,234", "NaN", "1e3", "21474836,48"]) {
    assert.throws(() => cents(text));
  }
});

test("Omnichannel uses exactly the widget provided by the user, not Direct Line", () => {
  assert.equal(WIDGET.app, "313fd382-e266-42b1-b030-59947f59de40");
  assert.equal(WIDGET.org, "3fbb46bf-29c7-f111-a04e-6045bd00313d");
  assert.match(WIDGET.primary, /^https:\/\/oc-cdn-ocprod\.azureedge\.net\//);
  assert.match(WIDGET.fallback, /^https:\/\/ocprodocprodnamgs\.blob\.core\.windows\.net\//);
});

test("the sidebar cards and duplicate chat controls are absent while the native widget stays enabled", () => {
  const html = readFileSync(new URL("../assisted.html", import.meta.url), "utf8");
  assert.doesNotMatch(html, /<aside|<h2>Operação \+ Alçada|<h2>Atendimento Omnichannel|assisted-chat-open|assisted-chat-status|Abrir atendimento/);
  assert.match(html, /class="assisted-workspace"/);
  assert.match(html, /id="assisted-chat-error" role="alert"><\/div>/);
  const js = readFileSync(new URL("../src/assisted.js", import.meta.url), "utf8");
  assert.match(js, /initializeOmnichannel\(document.getElementById\("assisted-chat-error"\)\)/);
});

function widgetEnvironment(t, existing) {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const previous = { window: globalThis.window, document: globalThis.document };
  const events = new EventTarget();
  const scripts = [];
  globalThis.window = events;
  globalThis.document = {
    getElementById() { return existing; },
    createElement() {
      const attributes = new Map();
      return { setAttribute(key, value) { attributes.set(key, value); },
        getAttribute(key) { return attributes.get(key); }, remove() {} };
    },
    body: { append(script) { scripts.push(script); } },
  };
  t.after(() => {
    globalThis.window = previous.window;
    globalThis.document = previous.document;
    t.mock.timers.reset();
  });
  return { events, scripts };
}

test("native widget loads without a custom open button or an always-visible availability message", (t) => {
  const { events, scripts } = widgetEnvironment(t);
  const error = { textContent: "" };
  initializeOmnichannel(error);
  assert.equal(scripts.length, 1);
  assert.equal(scripts[0].src, WIDGET.primary);
  assert.equal(scripts[0].getAttribute("data-app-id"), WIDGET.app);
  assert.equal(error.textContent, "");
  events.dispatchEvent(new Event("lcw:ready"));
  t.mock.timers.tick(45000);
  assert.equal(error.textContent, "");
});

test("native widget failures remain explicit and primary-load failures use the provided fallback", (t) => {
  const { scripts } = widgetEnvironment(t);
  const error = { textContent: "" };
  initializeOmnichannel(error);
  scripts[0].onerror();
  assert.equal(scripts[1].src, WIDGET.fallback);
  scripts[1].onerror();
  assert.match(error.textContent, /Não foi possível carregar/);
  t.mock.timers.tick(45000);
  assert.match(error.textContent, /Não foi possível carregar/);
});

test("an existing unrelated native widget is preserved rather than replaced or hidden", (t) => {
  const { scripts } = widgetEnvironment(t, { getAttribute() { return "other-app"; } });
  const error = { textContent: "" };
  initializeOmnichannel(error);
  assert.equal(scripts.length, 0);
  assert.match(error.textContent, /outro widget/);
});

test("visible names omit demo qualifiers while source identifiers and values stay intact", () => {
  const contract = { number: "ASSIST-DEMO-002", vehicle: "Veiculo ficticio assistido 2", debtCents: 2000120 };
  const before = { ...contract };
  assert.equal(displayName(contract.number), "Financiamento 002");
  assert.equal(displayName(contract.vehicle), "Veiculo assistido 2");
  assert.equal(displayName("Cliente Ficticio Portal Assistido"), "Cliente Portal Assistido");
  assert.equal(displayName("Carteira demonstrativa"), "Carteira");
  assert.equal(displayName("Cliente Fictício"), "Cliente");
  assert.equal(displayName("Operação e Alçada"), "Operação e Alçada");
  assert.deepEqual(contract, before);
  assert.throws(() => displayName(null));
});

test("CPF is the initial customer lookup, with exact digits and checksum validation", () => {
  for (const value of ["800.000.004-00", "80000000400", " 80000000400 "]) {
    assert.equal(customerCpf(value), "80000000400");
  }
  for (const value of ["", "123", "80000000401", "00000000000", null, "80000000400extra", "٨٠٠٠٠٠٠٠٤٠٠"]) {
    assert.throws(() => customerCpf(value));
  }
  const html = readFileSync(new URL("../assisted.html", import.meta.url), "utf8");
  assert.match(html, /id="assisted-cpf".*autocomplete="off"/);
  assert.match(html, /Buscar cliente e financiamentos/);
  const js = readFileSync(new URL("../src/assisted.js", import.meta.url), "utf8");
  assert.match(js, /api\("contracts", \{ cpf \}\)/);
  assert.match(js, /cpfInput\.addEventListener\("input"/);
  assert.match(js, /AbortSignal\.timeout\(20000\)/);
  assert.match(js, /action === "request" \? 60000 : 30000/);
});

test("customer page does not accept through GET, URL opening or a prechecked input", () => {
  const html = readFileSync(new URL("../acceptance.html", import.meta.url), "utf8");
  const js = readFileSync(new URL("../src/acceptance.js", import.meta.url), "utf8");
  assert.match(html, /type="checkbox" required/);
  assert.doesNotMatch(html, /\schecked(?:\s|>)/);
  assert.match(js, /new URLSearchParams\(location.hash/);
  assert.match(js, /history.replaceState/);
  assert.match(js, /form.addEventListener\("submit"/);
  assert.match(js, /confirmed: true/);
  assert.doesNotMatch(js, /localStorage|sessionStorage/);
});
