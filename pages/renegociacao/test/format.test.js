import test from "node:test";
import assert from "node:assert/strict";
import { money, offerTerms, completedResult, currentOffer } from "../src/format.js";

const offer = {
  entryCents: 12345,
  installments: 12,
  paymentCents: 10001,
  totalCents: 132357,
  validUntil: "2030-01-01T00:00:00Z",
};

test("renders exact cent values", () => {
  assert.match(money(12345), /123,45/);
  assert.equal(offerTerms(offer).length, 5);
});
test("rejects inconsistent totals and invalid amounts", () => {
  assert.throws(() => offerTerms({ ...offer, totalCents: 132356 }));
  assert.throws(() => money(1.1));
  assert.throws(() => money(-1));
});
test("does not turn pending or incomplete formalization into success", () => {
  assert.throws(() => completedResult({ status: "processing", requestId: "x" }));
  assert.throws(() => completedResult({ status: "completed", requestId: "x", offer }));
  assert.equal(completedResult({ status: "completed", requestId: "x", protocol: "DEMO-1", offer }).protocol, "DEMO-1");
});

test("validates cents before arithmetic and rejects overflow or invalid dates", () => {
  for (const field of ["entryCents", "paymentCents", "totalCents"]) {
    for (const value of [null, "100", NaN, Infinity, -1, 1.25]) {
      assert.throws(() => offerTerms({ ...offer, [field]: value }));
    }
  }
  assert.throws(() => offerTerms({ ...offer, installments: Number.MAX_SAFE_INTEGER }));
  for (const validUntil of [null, 123, "1", "invalid", "2030-01-01T00:00:00"]) {
    assert.throws(() => offerTerms({ ...offer, validUntil }));
  }
});

test("expired terms remain visible in completed history but cannot be accepted", () => {
  const historical = { ...offer, id: "offer-1", version: 1, validUntil: "2020-01-01T00:00:00Z" };
  assert.throws(() => currentOffer(historical));
  assert.equal(completedResult({
    status: "completed", requestId: "r-1", protocol: "DEMO-1", offer: historical,
  }).offer, historical);
  assert.throws(() => currentOffer({ ...historical, validUntil: "2030-01-01T00:00:00Z", version: 0 }));
  assert.throws(() => completedResult({ status: "completed", requestId: "r-1", protocol: {}, offer }));
});
