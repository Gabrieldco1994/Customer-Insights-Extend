import { offerTerms } from "./format.js";

const root = document.getElementById("fsi-acceptance");
const status = document.getElementById("acceptance-status");
const error = document.getElementById("acceptance-error");
const terms = document.getElementById("acceptance-terms");
const form = document.getElementById("acceptance-form");
const check = document.getElementById("acceptance-check");
const confirm = document.getElementById("acceptance-confirm");
const token = new URLSearchParams(location.hash.slice(1)).get("token");
history.replaceState(null, "", location.pathname);
const policy = document.createElement("meta");
policy.name = "referrer";
policy.content = "no-referrer";
document.head.append(policy);
let model;
let submitting = false;

async function api(action, data = {}) {
  const response = await fetch(`${root.dataset.api}/api/assisted-acceptance/${action}`, {
    method: "POST", credentials: "omit", cache: "no-store", referrerPolicy: "no-referrer",
    headers: { "Content-Type": "application/json" }, signal: AbortSignal.timeout(60000),
    body: JSON.stringify({ token, ...data }),
  });
  const result = await response.json();
  if (!response.ok || result.demonstration !== true || !result.data) throw new Error(
    result.error?.message || "O serviço não confirmou esta etapa. Reabra o email para consultar novamente.");
  return result;
}

function showFailure(failure) {
  error.textContent = failure instanceof Error ? failure.message : "O aceite não foi confirmado.";
  error.focus();
}

async function initialize() {
  if (!token) throw new Error("Abra o link de revisão recebido por email. Nenhum aceite foi registrado.");
  const result = await api("review");
  model = result.data;
  const list = document.createElement("dl");
  for (const [label, value] of offerTerms(model.offer)) {
    const dt = document.createElement("dt");
    const dd = document.createElement("dd");
    dt.textContent = label;
    dd.textContent = value;
    list.append(dt, dd);
  }
  terms.replaceChildren(list);
  form.hidden = model.status !== "awaiting_acceptance";
  status.textContent = model.status === "completed"
    ? `Esta proposta já foi aceita. Protocolo: ${model.protocol}. Nenhuma nova contratação foi realizada.`
    : model.status === "processing"
      ? "O aceite foi recebido, mas a formalização ainda está em processamento. Não confirme novamente."
      : "Revise todos os termos antes de aceitar. Nenhum acordo foi confirmado.";
}

check.addEventListener("change", () => { confirm.disabled = !check.checked || submitting; });
form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (submitting || !check.checked || !model) return;
  submitting = true;
  confirm.disabled = true;
  error.textContent = "";
  try {
    const result = await api("confirm", { reviewHash: model.reviewHash, confirmed: true });
    form.hidden = true;
    if (result.status === "ok" && result.data.status === "completed" && result.data.protocol) {
      status.textContent = `Aceite e formalização confirmados. Protocolo: ${result.data.protocol}`;
    } else {
      status.textContent = "O serviço ainda não confirmou a formalização. Reabra o link do email para consultar; não envie outro aceite.";
    }
  } catch (failure) {
    showFailure(failure);
    status.textContent = "O resultado é incerto. Reabra o email para consultar antes de tentar novamente.";
  }
});
initialize().catch(showFailure);
