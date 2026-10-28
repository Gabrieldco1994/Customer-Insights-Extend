export function cents(value) {
  if (typeof value !== "string" || !/^[0-9]+(?:[,.][0-9]{1,2})?$/.test(value.trim())) {
    throw new Error("Informe o valor em reais, sem separador de milhar e com no máximo duas casas decimais.");
  }
  const [whole, fraction = ""] = value.trim().replace(",", ".").split(".");
  const result = Number(whole) * 100 + Number(fraction.padEnd(2, "0"));
  if (!Number.isSafeInteger(result) || result > 2147483647) throw new Error("Valor fora do intervalo permitido.");
  return result;
}

export function customerCpf(value) {
  const text = typeof value === "string" ? value.trim() : "";
  if (!/^(?:[0-9]{11}|[0-9]{3}\.[0-9]{3}\.[0-9]{3}-[0-9]{2})$/.test(text)) {
    throw new Error("Informe o CPF com 11 números, com ou sem pontuação.");
  }
  const digits = text.replace(/[.-]/g, "");
  if (/^([0-9])\1{10}$/.test(digits)) throw new Error("O CPF informado não é válido.");
  for (const size of [9, 10]) {
    const sum = Array.from(digits.slice(0, size), Number)
      .reduce((total, digit, index) => total + digit * (size + 1 - index), 0);
    const remainder = sum % 11;
    if (Number(digits[size]) !== (remainder < 2 ? 0 : 11 - remainder)) {
      throw new Error("O CPF informado não é válido.");
    }
  }
  return digits;
}

export const WIDGET = Object.freeze({
  id: "Microsoft_Omnichannel_LCWidget",
  primary: "https://oc-cdn-ocprod.azureedge.net/livechatwidget/scripts/LiveChatBootstrapper.js",
  fallback: "https://ocprodocprodnamgs.blob.core.windows.net/livechatwidget/scripts/LiveChatBootstrapper.js",
  app: "313fd382-e266-42b1-b030-59947f59de40",
  org: "3fbb46bf-29c7-f111-a04e-6045bd00313d",
  url: "https://m-3fbb46bf-29c7-f111-a04e-6045bd00313d.us.omnichannelengagementhub.com",
});

export function initializeOmnichannel(open, status) {
  let ready = false;
  const timeout = setTimeout(() => {
    if (!ready) {
      status.textContent = "O chat Omnichannel não ficou disponível. Você pode continuar o atendimento pelo portal e consultar a operação no CRM.";
    }
  }, 45000);
  window.addEventListener("lcw:ready", () => {
    ready = true;
    clearTimeout(timeout);
    open.disabled = false;
    status.textContent = "Chat Omnichannel disponível.";
  });
  window.addEventListener("lcw:error", () => {
    status.textContent = "O Omnichannel informou uma falha. O restante do Portal Assistido continua disponível.";
  });
  open.addEventListener("click", async () => {
    try {
      if (!window.Microsoft?.Omnichannel?.LiveChatWidget?.SDK) throw new Error("O chat ainda não está disponível.");
      await window.Microsoft.Omnichannel.LiveChatWidget.SDK.startChat();
    } catch (failure) {
      status.textContent = failure instanceof Error ? failure.message : "Não foi possível iniciar o atendimento Omnichannel.";
    }
  });
  const existing = document.getElementById(WIDGET.id);
  if (existing) {
    if (existing.getAttribute("data-app-id") !== WIDGET.app) status.textContent = "Já existe outro widget no portal. Ele foi preservado; confira a configuração do canal.";
    return;
  }
  function load(url, fallback) {
    const script = document.createElement("script");
    script.id = WIDGET.id;
    script.src = url;
    script.async = true;
    for (const [name, value] of Object.entries({
      "data-app-id": WIDGET.app, "data-lcw-version": "prod", "data-org-id": WIDGET.org,
      "data-org-url": WIDGET.url, "data-widget-ui-version": "modern",
    })) script.setAttribute(name, value);
    script.onerror = () => {
      script.remove();
      if (!fallback) load(WIDGET.fallback, true);
      else {
        clearTimeout(timeout);
        status.textContent = "Não foi possível carregar o chat Omnichannel. O atendimento pelo portal não foi interrompido.";
      }
    };
    document.body.append(script);
  }
  load(WIDGET.primary, false);
}
