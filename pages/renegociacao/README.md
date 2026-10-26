# Autosserviço de renegociação — demonstração

Página específica do site existente `PortalAtendimento01`:
`https://portalatendimento01.powerappsportals.com/renegociacao-financiamento/`.
O fluxo não depende de chat e não modifica autenticação, navegação nem bots globais.

Após o login no portal, o botão de consulta obtém um token delegado Entra para
`api://22630d43-3353-4fb7-8496-3979bd7617d9/Renegotiate` via MSAL. O backend
valida a identidade assinada e resolve o contato; nenhum CPF/contactId informado
pelo navegador autoriza acesso.
O login da aplicação usa redirecionamento MSAL na própria página, não popups
(que podem ser bloqueados no navegador integrado). O cache MSAL fica apenas em
`sessionStorage`; a resposta de redirecionamento é validada antes das consultas.
Isso não muda o login global do portal, o aplicativo Entra nem o escopo autorizado.

## Verificação local

```sh
npm --prefix pages/renegociacao test
npm --prefix pages/renegociacao run build
services/renegociacao/.venv/bin/python -m unittest discover \
  -s pages/renegociacao/test -p '*_test.py'
```

Os testes cobrem retomada, solicitação inexistente, ausência/expiração de ofertas,
valores em centavos, troca de oferta, revisão obrigatória, falha/timeout e
processamento pendente. Uma nova visita usa **Consultar atendimento** no contrato
para recuperar a mesma solicitação mantida pelo servidor, inclusive de outro canal.
O aceite não é armazenado no navegador: após recarregar ou trocar a oferta, revise
novamente. Timeout na confirmação exige consultar o status, não repetir o aceite.

Somente `status: "ok"` com `data.status: "completed"`, protocolo e termos válidos
exibe sucesso. HTTP 202/`processing` nunca significa conclusão.

## Preparação e publicação

Com a conexão aprovada em CCaaS e o ambiente Python existente:

```sh
services/renegociacao/.venv/bin/python scripts/renegociacao_pages.py prepare \
  --environment https://org88957f9a.crm.dynamics.com \
  --backup-dir pages/renegociacao/.local/backup --apply
```

O script empacota o bundle no template dedicado, mantém a página em **Draft**,
faz backup dos componentes anteriores e lê cada escrita de volta. Não publica
nem muda o menu. Configura apenas
`FSI/Renegociacao/{ApiUrl,ClientId,Audience,TenantId,ChatEnabled}`.
Reutiliza a localização que o Power Pages gera automaticamente; se já houver
uma localização explícita, remove somente a cópia automática duplicada desta
página, ainda em Draft, após preservar seu backup. Apenas inativar a duplicata
não basta: o resolvedor do portal ainda pode selecioná-la e responder 403.

Publicar requer validação prévia da fonte SQL e da projeção nativa pelo responsável
pela integração; o teste de rejeição HTTP 401 sozinho não prova essa validação:

```sh
services/renegociacao/.venv/bin/python scripts/renegociacao_pages.py publish \
  --environment https://org88957f9a.crm.dynamics.com \
  --backup-dir pages/renegociacao/.local/backup --apply --source-validated
```

O aceite ao vivo exige navegador logado pelo usuário, transação fictícia real,
protocolo lido da fonte e projeção/BPF sincronizados. Testes locais ou mocks não
substituem esse aceite. Não informar credenciais por automação.

## Assistente dedicado opcional

Somente após o endpoint autenticado estar disponível, adicione `--enable-chat`
ao comando `prepare` e publique com a mesma validação da fonte. Sem essa opção,
o assistente fica oculto e o autosserviço permanece independente.

O botão usa `POST /api/renegociacao/chat-token`, corpo `{}` e o mesmo token
delegado Entra da página. A resposta é **raw**, não o envelope das sete ações:
`{token, conversationId, userId, expires_in}`. Apenas o token efêmero Direct Line
vai ao chat; nenhum segredo de canal ou token Entra é enviado ao iframe.
O `userId` vem do servidor, nunca de um contato escolhido pelo navegador.
O login OAuth do agente continua obrigatório quando solicitado; não há
promessa de SSO nem troca automática de token.

O Web Chat Microsoft 4.19.2 é isolado em
`/renegociacao-financiamento/chat-app/`, na mesma origem do portal. O SDK é
publicado como código estático em web templates nativos, codificado em um
literal JSON dividido abaixo do limite de 1 MB. Isso preserva também a gramática
Liquid que existe dentro do próprio SDK e exclui os caracteres C0 rejeitados
pelo XML do Dataverse. O HTML nonce nativo carrega apenas esse build Microsoft
fixado; nenhum dado de usuário entra no código executado. A política existente
de scripts, inclusive `unsafe-eval`, não é alterada.
Não altera o bot global, CSP, CORS nem a política que bloqueia anexos `.js`.
Erro no assistente não remove a revisão ou o resultado financeiro da página.

## Aceite real — 26/10/2026

Evidência sem tokens, hash de aceite ou CPF: [e2e-acceptance.json](e2e-acceptance.json).

- REN-DEMO002 concluído pela página: `DEMO-a7d207f2-a57f-43b9-b0dc-d423c16740ec`.
- Entrada de 240024 + 12 parcelas de 180018 = 2400240 centavos, sem arredondamento.
- Retomada e uma repetição idempotente devolveram o mesmo protocolo e termos.
- Dataverse `Concluido/formalizacao`, erro null e BPF finalizado nas cinco etapas.
- Contrato de outro cliente e CPF divergente rejeitados com 403; seleção de
  contato no corpo rejeitada com 400; contrato inelegível rejeitado com 409.
- 32 testes JavaScript, 6 testes de implantação e build aprovados.
- Página e widget `Published`; endpoint de sessão 200, Web Chat conectado e
  entrada OAuth preservada. Aceite financeiro REN-DEMO001 pelo chat pertence ao
  agente Studio e foi confirmado separadamente pelo responsável.

O fechamento deste relatório não publica novamente nem desliga recursos Azure.
