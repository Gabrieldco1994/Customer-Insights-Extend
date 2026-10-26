// node test_servicotransacional.js  -> falha (exit 1) se a logica de selecao/exibicao quebrar
const vm = require("vm"), fs = require("fs"), assert = require("assert"), path = require("path");
const linhas = [
  { fsi_name: "Endosso", fsi_processo: "fsi_processoatendimento", fsi_etapa: "execucao", fsi_tipotela: 712400000, fsi_identificador: "sec_endosso", _fsi_motivoid_value: "M-END" },
  { fsi_name: "Aviso", fsi_processo: "fsi_processoatendimento", fsi_etapa: "execucao", fsi_tipotela: 712400000, fsi_identificador: "sec_aviso", _fsi_motivoid_value: "M-AVI" },
  { fsi_name: "Canvas", fsi_processo: "fsi_processoatendimento", fsi_etapa: "execucao", fsi_tipotela: 712400002, fsi_identificador: "sec_canvas", fsi_parametros: '{"app":"fsi_app_x"}', _fsi_motivoid_value: "M-CNV" },
  { fsi_name: "Pagina", fsi_processo: "fsi_processoatendimento", fsi_etapa: "execucao", fsi_tipotela: 712400001, fsi_identificador: "fsi_pagina", _fsi_motivoid_value: null },
];
function run(stageSlug, motivo, semSetSrc, extras = []) {
  const notif = [], actions = [], opened = [], warnings = [], vis = {}, handlers = [], iframe = { src: null, getSrc() { return this.src; }, setSrc(u) { this.src = u; } };
  const sec = n => ({ setVisible: v => (vis[n] = v) });
  const tab = { setVisible: v => (vis.aba = v), sections: { get: n => sec(n) } };
  const stages = { "S-EXEC": "execucao", "S-IDE": "identificacao" };
  const dom = { src: null, getAttribute() { return this.src; }, setAttribute(k, v) { this.src = v; } };
  const ctx = {
    window: { top: { document: { getElementById: id => (id === "IFRAME_sec_canvas" ? dom : null) } } },
    setTimeout, URL,
    console: { warn: (...items) => warnings.push(items.join(" ")) },
    Xrm: {
      WebApi: {
        retrieveMultipleRecords: async (t) => (t === "canvasapp" ? { entities: [{ appopenuri: "https://apps.powerapps.com/play/e/E/a/A?tenantId=T" }] } : { entities: linhas.concat(extras) }),
        retrieveRecord: async (t, id, q) => (t === "fsi_produtocliente" ? { fsi_numero: "5162 54** **** 7741" } : t === "workflow" ? { uniquename: "fsi_processoatendimento" } : { stagename: stages[id.toUpperCase()] }),
      },
      App: { addGlobalNotification: n => { notif.push(n.message); actions.push(n.action.eventHandler); } },
      Navigation: { navigateTo: () => {}, openUrl: url => opened.push(url) },
    },
  };
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname, "servicotransacional.js"), "utf8"), ctx);
  const form = {
    data: { entity: { getEntityName: () => "incident", getId: () => "{X}" }, process: {
      getActiveProcess: () => ({ getId: () => "{P}" }), getActiveStage: () => ({ getId: () => "{" + stageSlug + "}" }),
      addOnStageChange: f => handlers.push(f), addOnProcessStatusChange: f => handlers.push(f) } },
    ui: { tabs: { get: () => tab } },
    getControl: n => (n === "IFRAME_sec_canvas" && !semSetSrc ? iframe : { getControlType: () => "standard" }),
    getAttribute: n => (n === "title" ? { getValue: () => "Caso X" } : n === "fsi_produtoclienteid" ? { getValue: () => [{ id: "{P1}" }] } : { getValue: () => (motivo ? [{ id: motivo }] : null), addOnChange: f => handlers.push(f) }),
  };
  return ctx.FSI.Servico.onLoad({ getFormContext: () => form }).then(() => ({ vis, notif, actions, opened, warnings, iframe: iframe.src || dom.src, handlers: handlers.length }));
}
(async () => {
  let r = await run("S-EXEC", "m-end");
  assert.deepStrictEqual([r.vis.sec_endosso, r.vis.sec_aviso, r.vis.aba, r.notif.length, r.handlers], [true, false, true, 1, 3], "endosso na execucao");
  r = await run("S-EXEC", "m-avi");
  assert.deepStrictEqual([r.vis.sec_endosso, r.vis.sec_aviso, r.vis.aba], [false, true, true], "aviso na execucao");
  r = await run("S-EXEC", null);
  assert.deepStrictEqual([r.vis.sec_endosso, r.vis.sec_aviso, r.notif], [false, false, ["Pagina"]], "sem motivo: so o generico (pagina)");
  r = await run("S-IDE", "m-end");
  assert.deepStrictEqual([r.vis.sec_endosso, r.vis.aba, r.notif.length], [false, false, 0], "outra etapa: tudo oculto");
  r = await run("S-EXEC", "m-cnv");
  assert.strictEqual(r.vis.sec_canvas, true, "canvas visivel na etapa certa");
  const u = new URL(r.iframe);
  assert.deepStrictEqual([u.searchParams.get("source"), u.searchParams.get("casoid"), u.searchParams.get("etapa"), u.searchParams.get("cartao"), u.searchParams.get("titulo"), u.searchParams.get("tenantId")],
    ["iframe", "x", "execucao", "5162 54** **** 7741", "Caso X", "T"], "url do iframe do canvas");
  r = await run("S-IDE", "m-cnv");
  assert.strictEqual(r.iframe, null, "canvas nao carrega em outra etapa");
  r = await run("S-EXEC", "m-cnv", true);
  assert.strictEqual(new URL(r.iframe).searchParams.get("etapa"), "execucao", "fallback: src definido direto no elemento do DOM");
  const portal = {
    fsi_name: "Renegociacao", fsi_processo: "fsi_processoatendimento", fsi_etapa: "execucao",
    fsi_tipotela: 712400003, fsi_identificador: "renegociacao-financiamento",
    fsi_parametros: '{"url":"https://portalatendimento01.powerappsportals.com/renegociacao-financiamento/"}'
  };
  r = await run("S-EXEC", null, false, [portal]);
  r.actions.at(-1)();
  assert.deepStrictEqual(r.opened, ["https://portalatendimento01.powerappsportals.com/renegociacao-financiamento/"]);
  assert.equal(r.iframe, null, "Pages nao deve ser tratado como canvas em iframe");
  r = await run("S-EXEC", null, false, [{ ...portal, fsi_parametros: '{"url":"https://example.org/"}' }]);
  assert.equal(r.notif.includes("Renegociacao"), false);
  assert.match(r.warnings.join(" "), /nao autorizado/);
  r = await run("S-IDE", null, false, [portal]);
  assert.equal(r.notif.includes("Renegociacao"), false);
  console.log("ok: 10 cenarios");
})().catch(e => { console.error(e.message); process.exit(1); });
