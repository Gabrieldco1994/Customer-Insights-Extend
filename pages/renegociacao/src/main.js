import { money, offerTerms } from "./format.js";
import { createJourney } from "./journey.js";
import { createIdentity, SignInRedirect } from "./identity.js";
import { mountChat } from "./chat.js";

const root = document.getElementById("fsi-reneg");
const connect = document.getElementById("reneg-connect");
const content = document.getElementById("reneg-content");
const status = document.getElementById("reneg-status");
const error = document.getElementById("reneg-error");
const heading = document.getElementById("reneg-heading");
const result = document.getElementById("reneg-result");
let busy = false;
const journey = createJourney(api);
const identity = createIdentity(configuration);

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined) node.textContent = text;
  if (className) node.className = className;
  return node;
}

function button(label, action, secondary = false) {
  const node = element("button", label, secondary ? "secondary" : undefined);
  node.type = "button";
  node.addEventListener("click", () => run(action));
  return node;
}

function message(text) {
  status.textContent = text;
}

async function run(action) {
  if (busy) return;
  busy = true;
  error.textContent = "";
  root.setAttribute("aria-busy", "true");
  root.querySelectorAll("button").forEach((node) => { node.disabled = true; });
  try {
    await action();
  } catch (failure) {
    journey.invalidate();
    if (failure instanceof SignInRedirect) {
      message("Conclua o login seguro para consultar seus financiamentos.");
      return;
    }
    screen("Não foi possível concluir esta etapa");
    content.append(element("p", "Nenhuma conclusão foi presumida. Consulte o atendimento para verificar o resultado antes de tentar novamente."));
    if (journey.requestId) content.append(button("Consultar atendimento", poll));
    content.append(button("Voltar aos financiamentos", loadContracts, true));
    error.textContent = failure instanceof Error ? failure.message : "Não foi possível concluir a operação.";
    error.focus();
    message("A operação não foi confirmada como concluída.");
  } finally {
    busy = false;
    root.removeAttribute("aria-busy");
    root.querySelectorAll("button").forEach((node) => { node.disabled = false; });
  }
}

function configuration() {
  const { api, client, audience, tenant } = root.dataset;
  if (!api || !client || !audience || !tenant) {
    throw new Error("O serviço de demonstração ainda não foi configurado no portal.");
  }
  const endpoint = new URL(api);
  if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password) {
    throw new Error("A configuração do serviço é inválida.");
  }
  return { api: endpoint.href.replace(/\/$/, ""), client, audience, tenant };
}

async function api(action, data = {}) {
  const accessToken = await identity.token();
  const response = await fetch(`${configuration().api}/api/renegociacao/${action}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${accessToken}` },
    body: JSON.stringify(data),
    cache: "no-store",
    credentials: "omit",
    signal: AbortSignal.timeout(45000),
  });
  let body;
  try {
    body = await response.json();
  } catch {
    throw new Error("O serviço está indisponível ou retornou uma resposta inválida. Consulte o atendimento novamente.");
  }
  if (!response.ok) {
    const known = {
      OFFER_EXPIRED: "A oferta expirou. Consulte as opções e revise novamente.",
      OFFER_CHANGED: "As condições da oferta mudaram. Consulte as opções e revise novamente.",
      NOT_ELIGIBLE: "Este financiamento não está elegível para renegociação.",
      ACCESS_DENIED: "O financiamento não está disponível para esta conta.",
      INVALID_IDENTITY: "Sua sessão não pôde ser validada. Entre novamente para consultar seus financiamentos.",
      ALREADY_FORMALIZED: "Já existe um acordo para esta solicitação. Consulte o atendimento.",
    };
    throw new Error(known[body?.error?.code] || `O serviço não confirmou a operação (erro ${response.status}). Consulte o atendimento.`);
  }
  if (!body || body.demonstration !== true || !body.data || typeof body.data !== "object" ||
      !["ok", "processing"].includes(body.status) ||
      (response.status === 202 && body.status !== "processing")) {
    throw new Error("A resposta do serviço está incompleta.");
  }
  return body;
}

function screen(title) {
  heading.textContent = title;
  content.replaceChildren();
  result.hidden = true;
  result.replaceChildren();
  heading.focus();
}

function terms(offer) {
  const list = element("dl");
  for (const [label, value] of offerTerms(offer)) {
    list.append(element("dt", label), element("dd", value));
  }
  return list;
}

async function loadContracts() {
  message("Consultando seus financiamentos.");
  render(await journey.contracts());
}

function render(model) {
  if (model.kind === "contracts") return showContracts(model.contracts);
  if (model.kind === "offers") return showOffers(model.offers);
  if (model.kind === "review") return showReview(model.offer);
  if (model.kind === "completed") return showCompleted(model.data);
  if (model.kind === "processing") {
    screen("Renegociação em processamento");
    content.append(element("p", "A confirmação externa ou a atualização do atendimento ainda está pendente. Não envie uma nova contratação."));
    content.append(button("Consultar atualização", poll));
    message("Em processamento, ainda não concluído.");
  } else if (model.kind === "not_started") {
    screen("Nenhuma solicitação iniciada");
    content.append(element("p", "Não há atendimento de renegociação para este financiamento."));
    content.append(button("Solicitar renegociação", () => begin()));
    message("Você ainda não confirmou nenhuma renegociação.");
  } else if (model.kind === "ineligible") {
    screen("Financiamento não elegível");
    content.append(element("p", "Este financiamento não possui condições elegíveis para renegociação."));
    message("Nenhum acordo foi formalizado nesta etapa.");
  } else if (model.kind === "expired") {
    screen("Oferta expirada");
    content.append(element("p", "Consulte as condições atuais e faça uma nova revisão antes de confirmar."));
    content.append(button("Consultar ofertas atuais", loadOffers));
    message("A oferta anterior não pode ser confirmada.");
  } else {
    screen("Atendimento não concluído");
    content.append(element("p", "A solicitação apresenta uma falha. Consulte a atualização antes de tentar novamente."));
    content.append(button("Consultar atualização", poll));
    message("Não há confirmação de conclusão.");
  }
  content.append(button("Voltar aos financiamentos", loadContracts, true));
}

function showContracts(contracts) {
  screen("Seus financiamentos");
  const cards = element("div", undefined, "cards");
  for (const contract of contracts) {
    const card = element("article", undefined, "card");
    card.append(element("h3", `Contrato ${contract.number}`));
    card.append(element("p", contract.vehicle));
    card.append(element("p", `Saldo demonstrativo: ${money(contract.debtCents)}`));
    if (contract.eligible === true) {
      card.append(button("Solicitar renegociação", () => begin(contract.id)));
    } else {
      card.append(element("p", "Sem opções de renegociação disponíveis."));
    }
    card.append(button("Consultar atendimento", () => checkContract(contract.id), true));
    cards.append(card);
  }
  content.append(cards);
  message(contracts.length ? "Escolha o financiamento que deseja tratar. Consulte um atendimento existente para retomar a mesma solicitação, inclusive de outro canal." : "Nenhum financiamento disponível para sua conta.");
  connect.textContent = "Atualizar financiamentos";
}

async function begin(id) {
  message("Consultando as opções elegíveis.");
  render(await journey.begin(id));
}

async function loadOffers() {
  message("Consultando as condições atuais.");
  render(await journey.offers());
}

function showOffers(offers) {
  screen("Escolha uma opção de renegociação");
  const cards = element("div", undefined, "cards");
  offers.forEach((offer, index) => {
    const card = element("article", undefined, "card");
    card.append(element("h3", `Opção ${index + 1}`), terms(offer));
    card.append(button("Escolher esta opção", () => review(offer)));
    cards.append(card);
  });
  content.append(cards, button("Voltar aos financiamentos", loadContracts, true));
  if (!offers.length) content.prepend(element("p", "Não há ofertas vigentes disponíveis para este financiamento. Nenhum acordo foi formalizado nesta etapa."));
  message(offers.length ? "A escolha ainda não formaliza o acordo." : "Não há ofertas elegíveis disponíveis.");
}

async function review(offer) {
  message("Validando as condições selecionadas.");
  render(await journey.review(offer));
}

function showReview(offer) {
  screen("Revise antes de confirmar");
  content.append(terms(offer));
  content.append(element("p", "Ao confirmar, você aceita exatamente os valores acima para esta demonstração. Nenhuma contratação bancária real será realizada."));
  content.append(button("Confirmar renegociação", confirm));
  content.append(button("Escolher outra opção", loadOffers, true));
  message("Confira entrada, parcelas, total e validade antes de confirmar.");
}

async function confirm() {
  message("Processando sua renegociação. Não é necessário confirmar novamente.");
  render(await journey.confirm());
}

function showCompleted(data) {
  screen("Renegociação de demonstração concluída");
  result.replaceChildren(
    element("h3", `Protocolo ${data.protocol}`),
    element("p", `Solicitação ${data.requestId}`),
    terms(data.offer),
    element("p", "Resultado confirmado na fonte externa fictícia. Este resumo não é um contrato bancário."),
  );
  result.hidden = false;
  content.append(button("Voltar aos financiamentos", loadContracts, true));
  message("Concluído. Você pode consultar este resultado novamente.");
}

async function poll() {
  message("Consultando o resultado da solicitação.");
  render(await journey.poll());
}

async function checkContract(id) {
  message("Consultando atendimento existente.");
  render(await journey.check(id));
}

if (connect) {
  connect.addEventListener("click", () => run(loadContracts));
  identity.initialize().then((response) => {
    if (response?.account) run(loadContracts);
  }).catch((failure) => run(async () => { throw failure; }));
  const chatButton = document.getElementById("reneg-chat-open");
  if (chatButton) mountChat({
    button: chatButton,
    host: document.getElementById("reneg-chat-host"),
    status: document.getElementById("reneg-chat-status"),
    identity,
    configuration,
  });
}
