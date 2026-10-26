import { PublicClientApplication, InteractionRequiredAuthError } from "@azure/msal-browser";

export class SignInRedirect extends Error {}

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
          redirectUri: `${location.origin}/renegociacao-financiamento/`,
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
    async token() {
      await initialize();
      const scopes = [`api://${configuration().audience}/Renegotiate`];
      const account = client.getActiveAccount();
      if (!account) {
        await client.loginRedirect({ scopes });
        throw new SignInRedirect();
      }
      try {
        return (await client.acquireTokenSilent({ scopes, account })).accessToken;
      } catch (failure) {
        if (!(failure instanceof InteractionRequiredAuthError)) throw failure;
        await client.acquireTokenRedirect({ scopes, account });
        throw new SignInRedirect();
      }
    },
  };
}
