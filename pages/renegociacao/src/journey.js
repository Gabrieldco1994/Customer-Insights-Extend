import { completedResult, currentOffer } from "./format.js";

export function createJourney(api, now = Date.now) {
  let contractId;
  let requestId;
  let acceptedReview;

  function clearReview() {
    acceptedReview = undefined;
  }

  function remember(data) {
    if (typeof data.requestId !== "string" || !data.requestId.trim()) {
      throw new Error("O serviço não retornou a identificação da solicitação.");
    }
    if (requestId && data.requestId !== requestId) {
      throw new Error("A resposta não corresponde à solicitação em atendimento.");
    }
    requestId = data.requestId;
  }

  async function offers() {
    clearReview();
    if (!requestId || !contractId) throw new Error("Consulte o atendimento antes de escolher uma oferta.");
    // Re-read the request: another channel may already have formalized the agreement.
    const response = await api("status", { requestId });
    if (response.status === "ok" && response.data.status === "expired") {
      remember(response.data);
      return availableOffers();
    }
    return resolve(response);
  }

  async function availableOffers() {
    const result = await api("offers", { contractId });
    if (result.status === "processing") return { kind: "processing" };
    if (!Array.isArray(result.data.offers)) throw new Error("A lista de ofertas está incompleta.");
    return {
      kind: "offers",
      offers: result.data.offers.filter((offer) => {
        currentOffer(offer, -Infinity);
        return new Date(offer.validUntil).getTime() > now();
      }),
    };
  }

  async function resolve(response) {
    clearReview();
    const data = response.data;
    if (data.status === "not_started" && !data.requestId && response.status === "ok") {
      requestId = undefined;
      return { kind: "not_started" };
    }
    remember(data);
    if (response.status === "processing" || data.status === "processing") {
      return { kind: "processing" };
    }
    if (data.status === "completed") return { kind: "completed", data: completedResult(data) };
    if (["draft", "review"].includes(data.status)) {
      return availableOffers();
    }
    if (["ineligible", "not_eligible", "expired", "failed"].includes(data.status)) {
      return { kind: data.status === "not_eligible" ? "ineligible" : data.status };
    }
    throw new Error("O serviço retornou um estado de atendimento desconhecido.");
  }

  return {
    get requestId() { return requestId; },
    async contracts() {
      clearReview();
      requestId = undefined;
      contractId = undefined;
      const response = await api("contracts");
      if (response.status !== "ok" || !Array.isArray(response.data.contracts)) {
        throw new Error("A lista de contratos ainda não está disponível.");
      }
      return { kind: "contracts", contracts: response.data.contracts };
    },
    async begin(id = contractId) {
      clearReview();
      requestId = undefined;
      contractId = id;
      const response = await api("start", { contractId, channel: "page" });
      remember(response.data);
      // Start intentionally omits the saved protocol/offer. Read the authoritative status.
      return resolve(await api("status", { requestId }));
    },
    async check(id) {
      clearReview();
      requestId = undefined;
      contractId = id;
      return resolve(await api("status", { contractId }));
    },
    async poll() {
      clearReview();
      if (!requestId) throw new Error("Selecione um financiamento para consultar o atendimento.");
      return resolve(await api("status", { requestId }));
    },
    offers,
    async review(offer) {
      clearReview();
      if (!requestId) throw new Error("Consulte o atendimento antes de revisar uma oferta.");
      currentOffer(offer, now());
      const response = await api("review", { requestId, offerId: offer.id, version: offer.version });
      remember(response.data);
      if (response.status === "processing") return { kind: "processing" };
      const data = response.data;
      currentOffer(data.offer, now());
      if (data.offer.id !== offer.id || data.offer.version !== offer.version ||
          typeof data.reviewHash !== "string" || !/^[0-9a-f]{64}$/.test(data.reviewHash)) {
        throw new Error("Não foi possível validar os termos da confirmação. Consulte as ofertas novamente.");
      }
      acceptedReview = { requestId, hash: data.reviewHash, offer: { ...data.offer } };
      return { kind: "review", offer: acceptedReview.offer };
    },
    async confirm() {
      const review = acceptedReview;
      clearReview();
      if (!review || review.requestId !== requestId) {
        throw new Error("Revise a oferta atual antes de confirmar.");
      }
      currentOffer(review.offer, now());
      // Consume the review even on a timeout: status, not another acceptance, resolves ambiguity.
      return resolve(await api("confirm", { requestId, reviewHash: review.hash, confirmed: true }));
    },
    invalidate: clearReview,
  };
}
