export function money(cents) {
  if (!Number.isSafeInteger(cents) || cents < 0) {
    throw new Error("O serviço retornou um valor financeiro inválido.");
  }
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(cents / 100);
}

export function offerTerms(offer) {
  if (!offer || !Number.isSafeInteger(offer.installments) || offer.installments < 1) {
    throw new Error("As condições da oferta estão incompletas.");
  }
  for (const amount of [offer.entryCents, offer.paymentCents, offer.totalCents]) money(amount);
  const total = offer.entryCents + offer.installments * offer.paymentCents;
  if (!Number.isSafeInteger(total) || total !== offer.totalCents) {
    throw new Error("Os valores da oferta não conferem.");
  }
  if (typeof offer.validUntil !== "string" ||
      !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(offer.validUntil)) {
    throw new Error("A validade da oferta é inválida.");
  }
  const validity = new Date(offer.validUntil);
  if (!Number.isFinite(validity.getTime())) {
    throw new Error("A validade da oferta é inválida.");
  }
  return [
    ["Entrada", money(offer.entryCents)],
    ["Quantidade de parcelas", String(offer.installments)],
    ["Valor de cada parcela", money(offer.paymentCents)],
    ["Total do acordo", money(offer.totalCents)],
    ["Validade da oferta", validity.toLocaleString("pt-BR")],
  ];
}

export function completedResult(data) {
  if (!data || data.status !== "completed" ||
      typeof data.protocol !== "string" || !data.protocol.trim() ||
      typeof data.requestId !== "string" || !data.requestId.trim()) {
    throw new Error("A formalização ainda não foi confirmada pelo serviço.");
  }
  offerTerms(data.offer);
  return data;
}

export function currentOffer(offer, now = Date.now()) {
  offerTerms(offer);
  if (typeof offer.id !== "string" || !offer.id.trim() ||
      !Number.isSafeInteger(offer.version) || offer.version < 1) {
    throw new Error("A identificação da oferta está incompleta.");
  }
  if (new Date(offer.validUntil).getTime() <= now) {
    throw new Error("A oferta expirou. Consulte as opções atualizadas e revise novamente.");
  }
  return offer;
}
