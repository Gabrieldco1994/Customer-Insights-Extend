import { SignInRedirect } from "./identity.js";

export function chatSession(value) {
  const keys = ["token", "conversationId", "userId", "expires_in"];
  if (!value || typeof value !== "object" || Object.keys(value).length !== keys.length ||
      !keys.every((key) => Object.hasOwn(value, key)) ||
      typeof value.token !== "string" || value.token.length < 20 || value.token.length > 16384 ||
      typeof value.conversationId !== "string" || !value.conversationId.trim() ||
      value.conversationId.length > 256 ||
      typeof value.userId !== "string" || !/^dl_[A-Za-z0-9_-]{1,128}$/.test(value.userId) ||
      !Number.isSafeInteger(value.expires_in) || value.expires_in < 1 || value.expires_in > 86400) {
    throw new Error("A sessão do assistente está incompleta ou inválida.");
  }
  return value;
}

export async function requestChat(api, accessToken, fetcher = fetch) {
  const response = await fetcher(`${api}/api/renegociacao/chat-token`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${accessToken}` },
    body: "{}",
    cache: "no-store",
    credentials: "omit",
    signal: AbortSignal.timeout(45000),
  });
  let body;
  try {
    body = await response.json();
  } catch {
    throw new Error("O assistente retornou uma resposta inválida.");
  }
  if (!response.ok || response.status !== 200) {
    const code = typeof body?.error?.code === "string" && /^[A-Z][A-Z0-9_]{0,63}$/.test(body.error.code)
      ? body.error.code : `HTTP_${response.status}`;
    throw new Error(`Não foi possível iniciar o assistente (${code}). A página continua disponível.`);
  }
  return chatSession(body);
}

function loadFrame(host) {
  const frame = document.createElement("iframe");
  frame.title = "Assistente de renegociação — demonstração";
  frame.className = "reneg-chat-frame";
  frame.src = "/renegociacao-financiamento/chat-app/";
  frame.setAttribute("sandbox", "allow-scripts allow-same-origin allow-forms allow-popups allow-popups-to-escape-sandbox");
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      frame.remove();
      reject(new Error("O assistente não carregou. Continue pelo autosserviço da página."));
    }, 45000);
    frame.addEventListener("load", () => {
      clearTimeout(timeout);
      try {
        if (frame.contentWindow.location.origin !== location.origin || !frame.contentWindow.FSIRenegChat?.start) {
          throw new Error("O componente do assistente não está disponível nesta página.");
        }
        resolve(frame);
      } catch (failure) {
        frame.remove();
        reject(failure);
      }
    }, { once: true });
    host.replaceChildren(frame);
  });
}

export function mountChat({
  button, host, status, identity, configuration,
  frameLoader = loadFrame, chatRequester = requestChat,
}) {
  let controller;
  let loading = false;
  const initialLabel = button.textContent;
  button.addEventListener("click", async () => {
    if (loading) return;
    status.setAttribute("role", "status");
    if (controller) {
      controller.end();
      controller = undefined;
      host.replaceChildren();
      button.textContent = initialLabel;
      status.textContent = "Conversa encerrada. O autosserviço da página continua disponível.";
      return;
    }
    loading = true;
    button.disabled = true;
    status.textContent = "Carregando o assistente. Você pode continuar pelo autosserviço da página.";
    try {
      const frame = await frameLoader(host);
      const session = await chatRequester(configuration().api, await identity.token());
      status.textContent = "Conectando o assistente.";
      controller = frame.contentWindow.FSIRenegChat.start(session, (state) => {
        if (state === 2) status.textContent = "Assistente conectado. Faça login no assistente quando solicitado.";
        if ([3, 4, 5].includes(state)) {
          status.setAttribute("role", "alert");
          status.textContent = "A conversa perdeu a conexão. Reinicie o assistente ou continue pela página.";
        }
      });
      button.textContent = "Encerrar conversa";
    } catch (failure) {
      host.replaceChildren();
      if (failure instanceof SignInRedirect) {
        status.textContent = "Conclua o login seguro para abrir o assistente.";
      } else {
        status.setAttribute("role", "alert");
        status.textContent = failure instanceof Error ? failure.message : "Não foi possível abrir o assistente.";
        status.focus();
      }
    } finally {
      loading = false;
      button.disabled = false;
    }
  });
}
