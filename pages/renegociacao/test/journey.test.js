import test from "node:test";
import assert from "node:assert/strict";
import { createJourney } from "../src/journey.js";

const NOW = Date.parse("2026-10-26T12:00:00Z");
const offer = {
  id: "offer-1", version: 1, entryCents: 1001, installments: 6,
  paymentCents: 1500, totalCents: 10001, validUntil: "2026-11-01T00:00:00Z",
};
const hash = "a".repeat(64);
const response = (data, status = "ok") => ({ demonstration: true, status, data });
const state = (status, extra = {}, envelope = "ok") =>
  response({ requestId: "request-1", status, protocol: null, offer: null, ...extra }, envelope);
const listing = (offers = [offer]) => response({ offers });
const reviewed = (value = offer, reviewHash = hash) =>
  response({ requestId: "request-1", reviewHash, offer: value });
const completed = () => state("completed", { protocol: "DEMO-1", offer });

function setup(steps, clock = () => NOW) {
  const remaining = [...steps];
  const calls = [];
  const journey = createJourney(async (action, body) => {
    calls.push({ action, body });
    const [expected, result] = remaining.shift() || [];
    assert.equal(action, expected, `Unexpected API call: ${action}`);
    if (result instanceof Error) throw result;
    return result;
  }, clock);
  return { journey, calls, done: () => assert.equal(remaining.length, 0) };
}

test("new page flow reviews server terms, explicitly confirms and displays the real protocol", async () => {
  const exact = { ...offer, entryCents: 1007, totalCents: 10007 };
  const { journey, calls, done } = setup([
    ["start", state("draft")], ["status", state("draft")], ["offers", listing()],
    ["review", reviewed(exact)], ["confirm", state("completed", { protocol: "DEMO-1", offer: exact })],
  ]);
  assert.equal((await journey.begin("contract-1")).kind, "offers");
  assert.deepEqual((await journey.review(offer)).offer, exact);
  const result = await journey.confirm();
  assert.equal(result.kind, "completed");
  assert.equal(result.data.protocol, "DEMO-1");
  assert.deepEqual(calls[0].body, { contractId: "contract-1", channel: "page" });
  assert.deepEqual(calls.at(-1).body, { requestId: "request-1", reviewHash: hash, confirmed: true });
  done();
});

test("start resumes an already completed request without loading offers or confirming again", async () => {
  const { journey, calls, done } = setup([
    ["start", state("completed")], ["status", completed()],
  ]);
  assert.equal((await journey.begin("contract-1")).kind, "completed");
  assert.deepEqual(calls.map((call) => call.action), ["start", "status"]);
  done();
});

test("fresh page resumes the same request from another channel, requiring review again", async () => {
  const { journey, calls, done } = setup([["status", state("review")], ["offers", listing()]]);
  assert.equal((await journey.check("contract-1")).kind, "offers");
  assert.equal(journey.requestId, "request-1");
  await assert.rejects(() => journey.confirm(), /Revise/);
  assert.deepEqual(calls[0].body, { contractId: "contract-1" });
  done();
});

test("not_started is a normal empty state and starts only on explicit request", async () => {
  const { journey, calls, done } = setup([["status", state("not_started", { requestId: null })]]);
  assert.deepEqual(await journey.check("contract-1"), { kind: "not_started" });
  assert.equal(journey.requestId, undefined);
  assert.equal(calls.length, 1);
  done();
});

test("processing overrides completed-looking data until the full service is synchronized", async () => {
  const { journey, done } = setup([
    ["status", state("completed", { protocol: "DEMO-1", offer }, "processing")],
    ["status", completed()],
  ]);
  assert.equal((await journey.check("contract-1")).kind, "processing");
  assert.equal((await journey.poll()).kind, "completed");
  done();
});

test("pending draft and finalized-source processing do not load offers or show success", async () => {
  for (const pending of [state("draft", {}, "processing"), state("processing")]) {
    const { journey, done } = setup([["start", state("draft")], ["status", pending]]);
    assert.equal((await journey.begin("contract-1")).kind, "processing");
    done();
  }
});

test("empty and expired offer lists are normal no-offer states", async () => {
  for (const offers of [[], [{ ...offer, validUntil: "2026-10-26T12:00:00Z" }]]) {
    const { journey, done } = setup([["status", state("draft")], ["offers", listing(offers)]]);
    assert.deepEqual(await journey.check("contract-1"), { kind: "offers", offers: [] });
    done();
  }
});

test("malformed offers fail closed rather than exposing confirmation", async () => {
  const { journey, done } = setup([
    ["status", state("draft")], ["offers", listing([{ ...offer, totalCents: 1 }])],
  ]);
  await assert.rejects(() => journey.check("contract-1"), /valores/);
  await assert.rejects(() => journey.confirm(), /Revise/);
  done();
});

test("changing selection invalidates the old acceptance and uses the new hash", async () => {
  const second = { ...offer, id: "offer-2" };
  const secondHash = "b".repeat(64);
  const { journey, calls, done } = setup([
    ["status", state("draft")], ["offers", listing([offer, second])],
    ["review", reviewed()], ["status", state("review")], ["offers", listing([offer, second])],
    ["review", reviewed(second, secondHash)], ["confirm", completed()],
  ]);
  await journey.check("contract-1");
  await journey.review(offer);
  await journey.offers();
  await assert.rejects(() => journey.confirm(), /Revise/);
  await journey.review(second);
  await journey.confirm();
  assert.equal(calls.at(-1).body.reviewHash, secondHash);
  done();
});

test("changing selection checks whether the other channel already formalized", async () => {
  const { journey, done } = setup([
    ["status", state("draft")], ["offers", listing()], ["review", reviewed()],
    ["status", completed()],
  ]);
  await journey.check("contract-1");
  await journey.review(offer);
  assert.equal((await journey.offers()).kind, "completed");
  await assert.rejects(() => journey.confirm(), /Revise/);
  done();
});

test("expiry between review and confirmation consumes the review without sending confirmation", async () => {
  let now = NOW;
  const { journey, calls, done } = setup([
    ["status", state("draft")], ["offers", listing()], ["review", reviewed()],
  ], () => now);
  await journey.check("contract-1");
  await journey.review(offer);
  now = Date.parse(offer.validUntil);
  await assert.rejects(() => journey.confirm(), /expirou/);
  assert.equal(calls.filter((call) => call.action === "confirm").length, 0);
  await assert.rejects(() => journey.confirm(), /Revise/);
  done();
});

test("ambiguous confirmation failure requires status, not an accidental second acceptance", async () => {
  const { journey, calls, done } = setup([
    ["status", state("draft")], ["offers", listing()], ["review", reviewed()],
    ["confirm", new Error("network timeout")], ["status", completed()],
  ]);
  await journey.check("contract-1");
  await journey.review(offer);
  await assert.rejects(() => journey.confirm(), /timeout/);
  await assert.rejects(() => journey.confirm(), /Revise/);
  assert.equal((await journey.poll()).kind, "completed");
  assert.equal(calls.filter((call) => call.action === "confirm").length, 1);
  done();
});

test("pending review and changed offer/version cannot enable acceptance", async () => {
  for (const result of [
    { ...reviewed(), status: "processing" },
    reviewed({ ...offer, id: "different" }),
    reviewed({ ...offer, version: 2 }),
    reviewed(offer, "invalid-hash"),
  ]) {
    const { journey, done } = setup([
      ["status", state("draft")], ["offers", listing()], ["review", result],
    ]);
    await journey.check("contract-1");
    if (result.status === "processing") assert.equal((await journey.review(offer)).kind, "processing");
    else await assert.rejects(() => journey.review(offer));
    await assert.rejects(() => journey.confirm(), /Revise/);
    done();
  }
});

test("request correlation, unknown states and missing completed protocol fail closed", async () => {
  for (const result of [
    state("completed", { protocol: null, offer }),
    state("unknown"),
    state("completed", { requestId: null, protocol: "DEMO-1", offer }),
  ]) {
    const { journey, done } = setup([["status", result]]);
    await assert.rejects(() => journey.check("contract-1"));
    done();
  }
  const { journey, done } = setup([
    ["start", state("draft")], ["status", state("draft", { requestId: "different" })],
  ]);
  await assert.rejects(() => journey.begin("contract-1"), /não corresponde/);
  done();
});

test("ineligible, expired and failed requests have distinct non-success states", async () => {
  for (const status of ["ineligible", "not_eligible", "expired", "failed"]) {
    const { journey, done } = setup([["status", state(status)]]);
    assert.equal((await journey.check("contract-1")).kind, status === "not_eligible" ? "ineligible" : status);
    done();
  }
});
