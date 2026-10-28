import { PublicClientApplication, InteractionRequiredAuthError } from "@azure/msal-browser";

export class SignInRedirect extends Error {}

function waitForIdentity(promise, signal) {
  if (!signal) return promise;
  return new Promise((resolve, reject) => {
    const abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener("abort", abort));
    if (signal.aborted) abort();
  });
}

export function createIdentity(configuration, createClient = (config) => new PublicClientApplication(config)) {
  let client;
  let initialization;

  async function initialize() {
    if (!initialization) {
      const config = configuration();
      client = createClient({
        auth: {
          clientId: config.client,
          authority: `https://login.microsoftonline.com/${config.tenant}`,
          redirectUri: `${location.origin}${config.redirectPath || "/renegociacao-financiamento/"}`,
        },
        cache: { cacheLocation: "sessionStorage" },
      });
      initialization = (async () => {
        await client.initialize();
        const response = await client.handleRedirectPromise();
        if (response?.account) client.setActiveAccount(response.account);
        return response;
      })();
    }
    return initialization;
  }

  return {
    initialize,
    async token({ signal } = {}) {
      await waitForIdentity(initialize(), signal);
      signal?.throwIfAborted();
      const scopes = [`api://${configuration().audience}/Renegotiate`];
      const account = client.getActiveAccount();
      if (!account) {
        await client.loginRedirect({ scopes });
        throw new SignInRedirect();
      }
      try {
        return (await waitForIdentity(client.acquireTokenSilent({ scopes, account }), signal)).accessToken;
      } catch (failure) {
        if (!(failure instanceof InteractionRequiredAuthError)) throw failure;
        signal?.throwIfAborted();
        await client.acquireTokenRedirect({ scopes, account });
        throw new SignInRedirect();
      }
    },
  };
}
