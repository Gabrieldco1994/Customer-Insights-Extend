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

## Portal Assistido e Operação + Alçada

Nova página no mesmo portal, independente do autosserviço:
`https://portalatendimento01.powerappsportals.com/portal-assistido/`.
O desenho retoma os trechos de renegociação do transcript compartilhado:
assessoria/central como canal de entrada, condições válidas antes de oferecer,
aceite no centavo e assistência humana como exceção, não como requisito de
toda renegociação.

Fluxo:

1. Operador informa primeiro o **CPF do cliente**. Na demonstração, use
   **800.000.004-00**. O serviço valida o login delegado do operador e confere
   o CPF somente no contato fictício fixo, sem busca livre em clientes reais.
   Só então apresenta os contratos `ASSIST-DEMO`. Trocar o CPF limpa a tela
   do cliente e exige nova consulta; o CPF não é armazenado no navegador.
   Na apresentação, os nomes não usam os qualificadores "fictício" ou
   "demonstrativo": por exemplo, `ASSIST-DEMO-002` aparece como
   **Financiamento 002**. Os nomes, marcadores de isolamento e IDs da fonte
   permanecem intactos; somente os rótulos do portal são ajustados.
2. Seleciona uma oferta vigente da fonte SQL e solicita desconto ou parcelas.
3. O serviço calcula em centavos e apresenta qualquer ajuste na entrada antes
   de enviar. Não há política de alçadas ativa no Dataverse; nenhuma condição
   é aprovada automaticamente ou inventada pelo agente.
4. Envio cria/reutiliza uma solicitação na fonte e a ocorrência/BPF existentes,
   com os termos imutáveis da exceção. O operador não dispõe de ação de aceite.
5. A operação abre **FSI - Alçada do Portal Assistido** no CRM e decide
   Pendente/Aprovado/Recusado. O formulário permite editar somente a decisão e
   sua justificativa; não substitui nem modifica o formulário anterior.
6. **Power Automate** reage à aprovação humana, solicita a preparação segura
   da revisão e do rascunho de email, e chama o envio nativo pelo Dataverse.
   O email de demonstração destina-se exclusivamente à caixa admin autorizada.
7. O cliente abre o link de revisão em
   `https://portalatendimento01.powerappsportals.com/aceite-renegociacao/`,
   confere os valores e marca o aceite antes de confirmar. Abrir o email/link
   não formaliza o acordo.
8. A conclusão exige o protocolo da fonte e a projeção/BPF sincronizados.
   O acompanhamento distingue email em rascunho, pendente, enviado e falha.

O chat dessa página é o **widget Omnichannel enviado pelo usuário**,
app `313fd382-e266-42b1-b030-59947f59de40`, na organização CCaaS. Ele não usa o
broker Direct Line do autosserviço. O vínculo do workstream **Chat** é
preservado **sem agente**, conforme solicitado. A disponibilidade/transferência
humana depende da operação e das filas existentes, não é simulada pelo portal.
Os cartões laterais explicativos de **Operação + Alçada** e **Atendimento
Omnichannel** foram removidos da página por solicitação do usuário. O espaço
principal ocupa a largura disponível; a operação e o aceite não mudam. O chat
continua pelo lançador do próprio widget, sem botão duplicado "Abrir atendimento"
nem mensagem permanente de disponibilidade. Falhas de carregamento continuam
sendo exibidas explicitamente, sem bloquear a jornada financeira.
**CSP:** o bloqueio inicial foi confirmado no navegador. Após esclarecimento
e autorização explícita, foram acrescentadas somente as origens
`https://oc-cdn-ocprod.azureedge.net` e
`https://ocprodocprodnamgs.blob.core.windows.net` ao `script-src`. O cabeçalho
servido pelo portal foi lido de volta com ambas as origens. As diretivas,
nonce e permissões preexistentes foram preservadas, sem curingas, desativação
da CSP, novas permissões `unsafe-*`, proxy ou espelhamento de scripts.
O carregamento e atendimento do chat após essa mudança ainda precisam de
verificação em sessão de navegador logada.

### Consulta inicial e tempo de espera

A tela separa **validação do operador** de **consulta do CPF e contratos**.
A espera de aquisição silenciosa de identidade é limitada a 20 segundos;
as leituras da API, a 30 segundos. A solicitação de alçada mantém 60 segundos
e nunca é repetida automaticamente. Um timeout exibe erro explícito e libera
a tela, sem iniciar um login tardio depois de a espera ter sido cancelada.

A carteira e o vínculo com solicitações existentes agora vêm de **uma única
consulta/conexão SQL**, com JOIN filtrado pelo operador, em vez de duas conexões
e leitura dos JSONs de proposta. A identidade do operador e a integridade do
cliente fictício continuam sendo verificadas ao vivo; não há cache de
autorização nem de contratos. A API registra apenas duração e etapa, sem CPF,
token ou corpo, e retorna `Server-Timing` para diagnosticar latência.
Essa redução de round trips foi testada; não equivale a uma promessa de
latência do serviço em cold start ou de login no navegador.

### Limites de segurança

- `/api/assisted/*` exige JWT delegado Entra, operador explicitamente permitido
  e vínculo ativo; não aceita seleção livre de cliente.
- Os contratos assistidos e o contato fictício são distintos dos contratos do
  autosserviço e WhatsApp. Nenhum acordo concluído é reaberto.
- `/api/assisted-automation/prepare-mail` exige chave exclusiva, verifica a
  decisão humana e os termos originais, e cria apenas rascunho de email. O envio
  é feito pelo Power Automate com a conexão admin existente. A chave fica na
  conexão criptografada e não é salva no fluxo, página ou repositório.
- O link de aceite tem assinatura dedicada, finalidade distinta de login,
  expiração de 48 horas e vínculo com a proposta. O token vai no **fragmento**,
  não na query string, e é removido do endereço após carregar. Não é salvo em
  armazenamento do navegador. Revisar não aceita; a confirmação requer o hash
  dos termos e booleano `true`. Revogação no CRM bloqueia o aceite.
- O email tem ID determinístico: reprocessar o fluxo não cria outra atividade
  e o envio só é chamado para email ainda em rascunho.
- Não se altera autenticação global, bot, workstream WhatsApp ou dados reais.
  A única extensão de CSP é a lista explícita de origens do widget autorizada
  acima. Não há NBA/NBO, estratégia de cobrança inventada ou envio por Journeys.

### Componentes da alçada

Os campos novos ficam na entidade existente `fsi_renegociacao`, na solution
`FSIInsights`:

| Campo | Nome lógico | Tipo |
|---|---|---|
| Decisão de alçada | `fsi_decisaoalcada` | Choice: Pendente/Aprovado/Recusado |
| Condições solicitadas | `fsi_pedidoalcada` | Memo, termos originais |
| Justificativa da decisão | `fsi_justificativaalcada` | Memo |
| Email de aceite | `fsi_emailaceitestatus` | Texto |

O histórico financeiro permanece na fonte externa; somente pedido, decisão e
evidência do processo persistem no CRM. A tabela técnica SQL
`demo.AssistedRequests` vincula operador, termos e aprovação à solicitação
existente e não replica o histórico de parcelas.

Implantação: [`renegociacao_assisted.py`](../../scripts/renegociacao_assisted.py)
(metadados e fixtures), [`renegociacao_assisted_flow.py`](../../scripts/renegociacao_assisted_flow.py)
(Power Automate e conector) e
[`renegociacao_assisted_pages.py`](../../scripts/renegociacao_assisted_pages.py)
(somente as duas páginas). As escritas são lidas de volta e preservam backup.
O preparo nativo da caixa admin é uma opção separada
`--approve-admin-mailbox`, utilizável somente após autorização explícita.
O ajuste de CSP é outra ação separada, `--enable-omnichannel-csp`; essa opção
altera apenas a política e não reaplica nem publica as páginas.

### Aceite real do Portal Assistido — 28/10/2026

Evidência sanitizada: [e2e-assisted-acceptance.json](e2e-assisted-acceptance.json).

- `ASSIST-DEMO-001` foi solicitado pela tela, com desconto de **R$ 100,00**,
  entrada de **R$ 1.000,00**, **10 × R$ 1.790,12**, total **R$ 18.901,20**.
- Ocorrência/renegociação:
  `f84bda89-8f98-4d40-a1c2-b41dc616dd9b`. A aprovação foi salva no CRM e,
  antes do aceite, protocolo nulo e situação Revisão foram lidos de volta.
- O fluxo **FSI - Alçada aprovada envia aceite por email**
  (`8bd6ca29-5cf8-5eaf-9465-6a9be0108f51`) terminou `Succeeded`.
  A atividade `b7cd424d-eab3-53cf-8461-402c36f08e42` foi enviada pelo servidor
  às **2026-10-28T13:32:58Z**, destinada somente à caixa admin autorizada.
  Isso comprova o envio nativo, não uma confirmação de leitura da caixa.
- A caixa admin foi aprovada/testada por autorização explícita: saída Êxito.
  O runtime recebeu somente `prvReadUser` com profundidade Local, também
  autorizado, para validar o aprovador/remetente. O envio permanece no fluxo.
- O link efetivamente gravado nesse email abriu a revisão sem aceitar.
  O checkbox começou desmarcado, o botão desabilitado e o token foi removido
  do endereço. Só o checkbox + confirmação explícita finalizaram o acordo.
- Protocolo exibido: `DEMO-f84bda89-8f98-4d40-a1c2-b41dc616dd9b`.
  Backend/CRM: Concluído, formalização e erro de integração nulo.
- BPF único `3c7af69c-cfd2-f111-a05b-000d3a343828`: cinco etapas, finalizado.
  O Caso não foi resolvido automaticamente.
- Acompanhamento do operador mostrou email Enviado e o mesmo protocolo.
  Após recarregar, a carteira recuperou a solicitação pelo servidor.
- `ASSIST-DEMO-002` e `WHATS-DEMO-002` continuaram elegíveis; os acordos
  anteriores do autosserviço e chat foram preservados.
- O novo CPF inicial e os limites de espera têm testes próprios. Após a
  publicação, o usuário confirmou que **cliente e contratos carregaram**
  com o novo fluxo. A latência comparativa e o chat após a CSP autorizada
  continuam não verificados; não são provas implícitas pelo aceite anterior.

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
