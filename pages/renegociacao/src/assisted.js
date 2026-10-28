import { createIdentity, SignInRedirect } from "./identity.js";
import { money } from "./format.js";
import { cents, customerCpf, displayName, initializeOmnichannel } from "./assisted-utils.js";

const root = document.getElementById("fsi-assisted");
const content = document.getElementById("assisted-content");
const heading = document.getElementById("assisted-heading");
const status = document.getElementById("assisted-status");
const error = document.getElementById("assisted-error");
const cpfInput = document.getElementById("assisted-cpf");
const customer = document.getElementById("assisted-customer");
const identity = createIdentity(() => ({ ...root.dataset, redirectPath: "/portal-assistido/" }));
let busy = false;
let requestId = sessionStorage.getItem("fsi-assisted-request") || "";
let selectedCpf = "";

function node(tag, text) {
  const result = document.createElement(tag);
  if (text !== undefined) result.textContent = text;
  return result;
}

function button(text, action) {
  const result = node("button", text);
  result.type = "button";
  result.addEventListener("click", () => run(action));
  return result;
}

async function run(action) {
  if (busy) return;
  busy = true;
  cpfInput.disabled = true;
  error.textContent = "";
  root.setAttribute("aria-busy", "true");
  root.querySelectorAll(".assisted-workspace button").forEach((b) => { b.disabled = true; });
  try {
    await action();
  } catch (failure) {
    if (failure instanceof SignInRedirect) {
      status.textContent = "Conclua o login seguro para entrar como operador.";
    } else {
      error.textContent = failure?.name === "TimeoutError"
        ? "A etapa demorou mais que o limite de espera. A operação não foi confirmada; consulte o acompanhamento antes de repetir qualquer solicitação."
        : failure instanceof Error ? failure.message : "A operação não foi confirmada.";
      error.focus();
      status.textContent = "Consulte o acompanhamento antes de repetir uma solicitação.";
    }
  } finally {
    busy = false;
    cpfInput.disabled = false;
    root.removeAttribute("aria-busy");
    root.querySelectorAll(".assisted-workspace button").forEach((b) => { b.disabled = false; });
  }
}

async function api(action, body = {}) {
  status.textContent = "Validando a sessão segura do operador.";
  const token = await identity.token({ signal: AbortSignal.timeout(20000) });
  status.textContent = action === "contracts"
    ? "Operador validado. Localizando o CPF e consultando os financiamentos."
    : "Operador validado. Consultando o serviço de renegociação.";
  const response = await fetch(`${root.dataset.api}/api/assisted/${action}`, {
    method: "POST", cache: "no-store", credentials: "omit",
    signal: AbortSignal.timeout(action === "request" ? 60000 : 30000),
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
    body: JSON.stringify(body),
  });
  const result = await response.json();
  if (!response.ok || result.demonstration !== true || !result.data) {
    throw new Error(result.error?.message || `O serviço não confirmou a operação (${response.status}).`);
  }
  return result.data;
}

function screen(title) {
  heading.textContent = title;
  content.replaceChildren();
  heading.focus();
}

function conditions(value) {
  const dl = node("dl");
  for (const [label, text] of [
    ["Entrada", money(value.entryCents)], ["Parcelas", `${value.installments} de ${money(value.paymentCents)}`],
    ["Total", money(value.totalCents)],
  ]) dl.append(node("dt", label), node("dd", text));
  return dl;
}

async function load() {
  content.replaceChildren();
  customer.hidden = true;
  const cpf = customerCpf(cpfInput.value);
  const result = await api("contracts", { cpf });
  selectedCpf = cpf;
  screen("Financiamentos do cliente");
  customer.textContent = `Cliente localizado: ${displayName(result.customer.name)}`;
  customer.hidden = false;
  const cards = node("div");
  cards.className = "cards";
  for (const contract of result.contracts) {
    const card = node("article");
    card.className = "card";
    card.append(node("h3", displayName(contract.number)), node("p", displayName(contract.vehicle)),
      node("p", `Saldo: ${money(contract.debtCents)}`));
    if (contract.assistedRequestId) {
      card.append(button("Acompanhar solicitação existente", async () => {
        requestId = contract.assistedRequestId;
        sessionStorage.setItem("fsi-assisted-request", requestId);
        await follow();
      }));
    } else if (contract.eligible) {
      card.append(button("Ver condições disponíveis", () => offers(contract)));
    } else card.append(node("p", "Contrato sem novas condições elegíveis."));
    cards.append(card);
  }
  content.append(cards);
  if (requestId) content.append(button("Acompanhar última solicitação", follow));
  status.textContent = result.contracts.length
    ? "Cliente localizado. Escolha um contrato para apresentar as condições disponíveis na fonte."
    : "Cliente localizado, mas não há financiamentos disponíveis nesta carteira.";
}

async function offers(contract) {
  status.textContent = "Consultando condições vigentes na fonte.";
  const result = await api("offers", { contractId: contract.id });
  screen(`Condições — ${displayName(contract.number)}`);
  for (const offer of result.offers) {
    const card = node("article");
    card.className = "card";
    card.append(conditions(offer), node("p", `Validade: ${offer.validUntil}`),
      button("Preparar solicitação com estas condições", () => edit(contract, offer)));
    content.append(card);
  }
  if (!result.offers.length) content.append(node("p", "Não há ofertas vigentes. Não é possível criar condições sem base."));
  content.append(button("Voltar à carteira", load));
  status.textContent = "Desconto ou alteração de parcelas exigem decisão no CRM. Os valores da fonte não serão alterados nesta etapa.";
}

function input(form, id, label, value, type = "text") {
  const caption = node("label", label);
  caption.htmlFor = id;
  const field = node(type === "textarea" ? "textarea" : "input");
  field.id = id;
  field.value = value;
  field.required = true;
  if (type !== "textarea") field.type = type;
  form.append(caption, field);
  return field;
}

function edit(contract, offer) {
  screen(`Solicitação de alçada — ${displayName(contract.number)}`);
  const form = node("form");
  form.append(node("p", "Informe os termos desejados. A solicitação pode manter a oferta ou pedir desconto e novas parcelas; nenhuma opção é aprovada automaticamente."));
  const discount = input(form, "assisted-discount", "Desconto solicitado (R$)", "0,00");
  discount.inputMode = "decimal";
  const entry = input(form, "assisted-entry", "Entrada desejada (R$)", (offer.entryCents / 100).toFixed(2).replace(".", ","));
  entry.inputMode = "decimal";
  const installments = input(form, "assisted-installments", "Quantidade de parcelas solicitada", offer.installments, "number");
  installments.min = "1";
  installments.max = "360";
  installments.step = "1";
  const reason = input(form, "assisted-reason", "Justificativa para a operação", "", "textarea");
  reason.minLength = 10;
  reason.maxLength = 1000;
  const preview = node("div");
  const submit = node("button", "Revisar solicitação");
  submit.type = "submit";
  form.append(submit);
  form.addEventListener("input", () => preview.replaceChildren());
  form.addEventListener("submit", (event) => {
    event.preventDefault();
    if (!form.reportValidity()) return;
    run(async () => {
      status.textContent = "Validando os valores solicitados antes de enviar para alçada.";
      const body = { contractId: contract.id, offerId: offer.id, version: offer.version,
        discountCents: cents(discount.value), entryCents: cents(entry.value),
        installments: Number(installments.value), reason: reason.value };
      const model = await api("preview", body);
      preview.replaceChildren(node("h3", "Confira o pedido antes de gerar a ocorrência"), conditions(model.proposal));
      preview.append(node("p", `Desconto solicitado: ${money(model.proposal.discountCents)}.`));
      if (model.proposal.roundingCents) preview.append(node("p",
        `A entrada inclui ${money(model.proposal.roundingCents)} de ajuste de centavos para manter todas as parcelas iguais. Confira esse valor antes de enviar.`));
      preview.append(node("p", "Próximo passo: decisão humana no CRM. Aprovação não é aceite do cliente."),
        button("Enviar para alçada e gerar ocorrência", async () => {
          const created = await api("request", { ...body, previewHash: model.previewHash });
          requestId = created.requestId;
          sessionStorage.setItem("fsi-assisted-request", requestId);
          await follow();
        }));
      status.textContent = "Solicitação revisada; ainda não foi enviada nem aprovada.";
    });
  });
  content.append(form, preview, button("Voltar às condições", () => offers(contract)));
}

async function follow() {
  status.textContent = "Consultando decisão da operação, email e aceite do cliente.";
  const model = await api("status", { requestId });
  screen("Acompanhamento do atendimento assistido");
  const states = { 712400000: "Aguardando decisão no CRM", 712400001: "Aprovada pela operação", 712400002: "Recusada pela operação" };
  content.append(node("h3", states[model.decision] || "Decisão não reconhecida"), conditions(model.proposal));
  if (model.decisionNote) content.append(node("p", `Justificativa da operação: ${model.decisionNote}`));
  content.append(node("p", `Email: ${model.emailStatus}`));
  const link = node("a", "Abrir ocorrência e decisão de alçada no CRM");
  const url = new URL(model.caseUrl);
  if (url.origin !== "https://org88957f9a.crm.dynamics.com" || url.pathname !== "/main.aspx") throw new Error("O endereço da ocorrência não é válido.");
  link.href = url.href;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  content.append(link);
  if (model.status === "completed" && model.protocol) {
    content.append(node("h3", "Cliente aceitou — formalização concluída"), node("p", `Protocolo: ${model.protocol}`));
  } else {
    content.append(node("p", "O atendimento só será concluído após o aceite explícito do cliente pelo link do email e a confirmação do serviço."));
  }
  content.append(button("Atualizar acompanhamento", follow), button("Voltar à carteira", load));
  status.textContent = model.status === "completed" ? "Formalização confirmada." : "Atendimento ainda não concluído.";
}

document.getElementById("assisted-identify").addEventListener("submit", (event) => {
  event.preventDefault();
  if (busy) return;
  run(load);
});
cpfInput.addEventListener("input", () => {
  if (selectedCpf) {
    selectedCpf = "";
    requestId = "";
    sessionStorage.removeItem("fsi-assisted-request");
    content.replaceChildren();
    customer.textContent = "";
    customer.hidden = true;
    heading.textContent = "Identifique o cliente";
    status.textContent = "Informe o CPF e busque o cliente novamente.";
  }
});
initializeOmnichannel(document.getElementById("assisted-chat-error"));
