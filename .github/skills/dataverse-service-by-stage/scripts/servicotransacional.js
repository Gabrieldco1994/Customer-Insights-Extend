var FSI = FSI || {};
// Mostra a tela do servico transacional da etapa ativa do BPF (catalogo fsi_servicotransacional).
// Registrar no OnLoad do formulario (passar contexto de execucao) de Caso e Oportunidade.
FSI.Servico = (function () {
    var ABA = "tab_servico";
    var ENTIDADE = { incident: 712400000, opportunity: 712400001 };
    var TELA_SECAO = 712400000, TELA_PAGINA = 712400001, TELA_CANVAS = 712400002, TELA_PORTAL = 712400003;
    var ids = {};

    function limpa(id) { return (id || "").replace(/[{}]/g, "").toLowerCase(); }

    // Resolve o identificador estavel (uniquename do BPF e slug do estagio) pela mesma tabela que o BPF usa.
    function resolve(tabela, id, campo) {
        id = limpa(id);
        var k = tabela + id;
        if (ids[k]) return ids[k];
        ids[k] = Xrm.WebApi.retrieveRecord(tabela, id, "?$select=" + campo).then(function (r) { return r[campo]; });
        return ids[k];
    }

    // Servicos que valem para (processo, etapa, motivo). Linha com motivo so vale para esse motivo.
    function escolher(linhas, processo, etapa, motivoId) {
        var m = limpa(motivoId);
        return linhas.filter(function (l) {
            return l.fsi_processo === processo && l.fsi_etapa === etapa &&
                (!l._fsi_motivoid_value || limpa(l._fsi_motivoid_value) === m);
        });
    }

    function daSecao(l) { return l.fsi_tipotela === TELA_SECAO || l.fsi_tipotela === TELA_CANVAS; }

    // Canvas app em iframe: o app recebe o registro por parametro de URL (Param("casoid"), "etapa", "cartao", "titulo").
    function carregarCanvas(form, l, etapa) {
        var app = (function () { try { return JSON.parse(l.fsi_parametros || "{}").app; } catch (e) { return null; } })();
        if (!app) return Promise.resolve();
        var prod = form.getAttribute("fsi_produtoclienteid");
        var pid = prod && prod.getValue() ? limpa(prod.getValue()[0].id) : null;
        var cartao = pid ? Xrm.WebApi.retrieveRecord("fsi_produtocliente", pid, "?$select=fsi_numero").then(function (r) { return r.fsi_numero; }) : Promise.resolve("");
        var uri = Xrm.WebApi.retrieveMultipleRecords("canvasapp", "?$select=appopenuri&$filter=name eq '" + app + "'");
        return Promise.all([uri, cartao]).then(function (r) {
            if (!r[0].entities.length) return;
            var t = form.getAttribute("title");
            var url = r[0].entities[0].appopenuri + "&source=iframe&casoid=" + limpa(form.data.entity.getId()) + "&etapa=" + etapa +
                "&cartao=" + encodeURIComponent(r[1] || "") + "&titulo=" + encodeURIComponent(t ? t.getValue() || "" : "");
            return definirSrc(form, l.fsi_identificador, url);
        });
    }

    // No UCI getControl() devolve um controle 'standard' (sem setSrc) para o iframe, e o elemento so existe depois que a aba e aberta pela primeira vez:
    // define o src direto no DOM, esperando o elemento aparecer (cancela a espera anterior se a etapa mudar).
    var espera = 0, pendente = {};
    function aplicarSrc(id) {
        var el = window.top.document.getElementById("IFRAME_" + id);
        if (el && pendente[id] && el.getAttribute("src") !== pendente[id]) el.setAttribute("src", pendente[id]);
        return !!el;
    }
    function definirSrc(form, id, url) {
        pendente[id] = url;
        var tab = form.ui.tabs.get(ABA);
        if (tab && tab.addTabStateChange && !tab.__fsi) { tab.__fsi = true; tab.addTabStateChange(function () { setTimeout(function () { aplicarSrc(id); }, 300); }); }
        var ctl = form.getControl("IFRAME_" + id);
        if (ctl && ctl.setSrc) { if (ctl.getSrc() !== url) ctl.setSrc(url); return Promise.resolve(); }
        var minha = ++espera;
        return new Promise(function (ok) {
            (function tenta() {
                if (minha !== espera) return ok();
                if (aplicarSrc(id)) return ok();
                setTimeout(tenta, 1000);
            })();
        });
    }

    function mostrar(form, linhas, ativas, etapa) {
        var aba = form.ui.tabs.get(ABA);
        var secoes = {};
        linhas.forEach(function (l) { if (daSecao(l)) secoes[l.fsi_identificador] = false; });
        ativas.forEach(function (l) { if (daSecao(l)) secoes[l.fsi_identificador] = true; });
        if (aba) {
            var alguma = false;
            Object.keys(secoes).forEach(function (s) {
                var sec = aba.sections.get(s);
                if (sec) sec.setVisible(secoes[s]);
                alguma = alguma || secoes[s];
            });
            aba.setVisible(alguma);
        }
        ativas.filter(function (l) { return l.fsi_tipotela === TELA_PAGINA; }).forEach(function (l) { avisarPagina(form, l); });
        ativas.filter(function (l) { return l.fsi_tipotela === TELA_PORTAL; }).forEach(function (l) { avisarPortal(form, l); });
        return Promise.all(ativas.filter(function (l) { return l.fsi_tipotela === TELA_CANVAS; }).map(function (l) { return carregarCanvas(form, l, etapa); }));
    }

    function avisarPagina(form, l) {
        var ent = form.data.entity;
        var params = {};
        try { params = l.fsi_parametros ? JSON.parse(l.fsi_parametros) : {}; } catch (e) { /* parametros invalidos: abre sem eles */ }
        Xrm.App.addGlobalNotification({
            type: 2, level: 3, showCloseButton: true, message: l.fsi_name,
            action: {
                actionLabel: "Abrir",
                eventHandler: function () {
                    Xrm.Navigation.navigateTo(
                        { pageType: "custom", name: l.fsi_identificador, entityName: ent.getEntityName(), recordId: limpa(ent.getId()) },
                        { target: 2, position: 2, width: { value: 40, unit: "%" }, title: l.fsi_name });
                }
            }
        });
    }

    function avisarPortal(form, l) {
        var destino = new URL(JSON.parse(l.fsi_parametros || "{}").url);
        if (destino.origin !== "https://portalatendimento01.powerappsportals.com" ||
            destino.pathname !== "/renegociacao-financiamento/" || destino.search || destino.hash) {
            throw new Error("Destino do autosservico nao autorizado.");
        }
        Xrm.App.addGlobalNotification({
            type: 2, level: 3, showCloseButton: true, message: l.fsi_name,
            action: {
                actionLabel: "Abrir autosservico",
                eventHandler: function () {
                    return Xrm.Navigation.openUrl(destino.href);
                }
            }
        });
    }

    function atualizar(form, linhas) {
        var p = form.data.process;
        var proc = p.getActiveProcess(), est = p.getActiveStage();
        if (!proc || !est) { return mostrar(form, linhas, [], ""); }
        var mot = form.getAttribute("fsi_motivoid");
        var motivoId = mot && mot.getValue() ? mot.getValue()[0].id : null;
        return Promise.all([resolve("workflow", proc.getId(), "uniquename"), resolve("processstage", est.getId(), "stagename")])
            .then(function (r) { return mostrar(form, linhas, escolher(linhas, r[0], r[1], motivoId), r[1]); });
    }

    function onLoad(ctx) {
        var form = ctx.getFormContext();
        var ent = ENTIDADE[form.data.entity.getEntityName()];
        if (!ent || !form.data.process) return Promise.resolve();
        return Xrm.WebApi.retrieveMultipleRecords("fsi_servicotransacional",
            "?$select=fsi_name,fsi_processo,fsi_etapa,fsi_tipotela,fsi_identificador,fsi_parametros,_fsi_motivoid_value&$filter=statecode eq 0 and fsi_entidade eq " + ent)
            .then(function (res) {
                var linhas = res.entities, refresh = seguro(function () { return atualizar(form, linhas); });
                form.data.process.addOnStageChange(refresh);
                form.data.process.addOnProcessStatusChange(refresh);
                var mot = form.getAttribute("fsi_motivoid");
                if (mot) mot.addOnChange(refresh);
                return refresh();
            });
    }

    // Falha do servico transacional nunca pode derrubar o formulario: registra no console e segue.
    function seguro(fn) {
        return function () {
            try { return Promise.resolve(fn.apply(null, arguments)).catch(function (e) { console.warn("FSI.Servico:", e && e.message || e); }); }
            catch (e) { console.warn("FSI.Servico:", e && e.message || e); return Promise.resolve(); }
        };
    }

    return { onLoad: seguro(onLoad), escolher: escolher };
})();
