# Agente dedicado de renegociação

**Aceite concluído em 2026-10-26: conversa autenticada no canal publicado, protocolo entregue no chat, status consultado e registros nativos lidos de volta.**

- Ambiente autorizado: `https://org88957f9a.crm.dynamics.com`.
- Solution: `FSIInsights`.
- Nome: **FSI Renegociação Demonstrativa**.
- Schema: `fsi_renegociacao`.
- Bot ID: `b23c421e-e534-4acf-8d21-4e4c41d0ed62`.
- [Abrir o agente](https://copilotstudio.microsoft.com/environments/b3638821-b73d-e1f4-93d0-798d3720e915/bots/b23c421e-e534-4acf-8d21-4e4c41d0ed62/overview).

## Aceite real publicado — contrato 001

Evidência sanitizada e transcrição real: [`acceptance-001.json`](acceptance-001.json).

- Protocolo entregue ao usuário: **DEMO-4c939339-1fef-4dd3-8e3f-62879d54115a**.
- Usuário concluiu OAuth nativo; CPF foi dispensado, não coletado e não utilizado
  como autenticação. O chat mostrou somente os contratos próprios e selecionou 001.
- Oferta `e2a246b3-8c22-5a2f-b83b-106f948aac71`, versão **1**:
  entrada **R$ 0,05**, **6 × R$ 3.000,20**, total **R$ 18.001,25**,
  validade **2026-11-25T13:09:39Z**. Revisão integral lida antes do **Sim** explícito.
- Confirmação aceita em **2026-10-26T16:22:35Z**. Outra mensagem,
  **consultar andamento**, leu o status do serviço e repetiu o mesmo protocolo.
- Leitura direta do Dataverse: `fsi_status: Concluido`, `fsi_etapa: formalizacao`,
  `fsi_canal: chat`, erro de integração nulo e os mesmos valores/versão/protocolo.
- BPF `7cf07b33-59d1-f111-a059-000d3a3437e2`: cinco etapas percorridas,
  formalização ativa, **statecode 1 / statuscode 2** (finalizado).
  O Caso existe com estado ativo; não houve fechamento manual do Caso.
- Última publicação: **2026-10-26T16:16:16Z**, `Succeeded/Synchronized`,
  verificador nativo **0 erros/avisos**.
- Contrato 002, bot global, login e navegação do portal não foram alterados por
  este agente. A página não foi usada para confirmar o acordo 001.

Correções exercitadas no canal real: CPF opcional sem campo vazio/nulo usa corpo
JSON literal `{}`; os seletores `Input.ChoiceSet` têm `label` obrigatório para o
SDK 4.19.2. A conversa anterior foi reiniciada para carregar os cartões publicados
novos; o cache OAuth foi reutilizado, sem nova entrada de credenciais.
As seções de diagnóstico abaixo preservam o histórico anterior a esse aceite.

## Jornada

### Extensão IA/MCP no mesmo agente (2026-10-27)

O agente e o vínculo do workstream **Renegociação** são preservados. Não é
necessário criar ou selecionar outro agente para este cenário.
**Publicação verificada: `Succeeded/Synchronized` em 2026-10-27T17:03:19Z.**
Evidência sanitizada: [`acceptance-mcp.json`](acceptance-mcp.json).

- `renegociar`: `AcessoDemonstrativo` solicita um código **simulado**, aceita
  somente dígitos e não envia SMS nem autentica uma pessoa.
- A IA consulta o servidor **FSI Renegociacao MCP**, com conexão maker
  criptografada e limitada pelo servidor ao cliente fictício WHATS-DEMO.
  Seus três tools são `listar_contratos_demo`, `consultar_ofertas_demo` e
  `consultar_andamento_demo`. Eles são somente leitura e não formalizam acordos.
- A IA explica as propostas usando os valores e o resumo retornados pela
  fonte, em texto simples, sem depender de cartões ou botões no WhatsApp.
- `ConfirmarDemonstrativo` recebe os IDs/versão da fonte, obtém a revisão,
  lê os valores e pergunta ao usuário antes de executar a única confirmação.
  O serviço mantém as validações, o hash, a idempotência e a projeção Caso/BPF.
- `renegociar autenticado`: mantém o fluxo anterior, com token delegado e
  contratos da conta real vinculada. O código fictício nunca autoriza esse fluxo.

O modo de autenticação continua manual (`authenticationmode: 3`), mas seu gatilho
passa a **conforme necessário** (`authenticationtrigger: 0`), para que o login
real não bloqueie a simulação de WhatsApp antes de qualquer mensagem.
O acesso autenticado continua verificando o token no tópico e no backend.
Portal, broker Direct Line e vínculos de identidade não são modificados.

Geração: [`renegociacao_agent_mcp.py`](../../scripts/renegociacao_agent_mcp.py).
Conector: [`renegociacao-mcp`](../../connectors/renegociacao-mcp/).
Probe real do protocolo: `python scripts/renegociacao_mcp.py probe`.

Aceite: seleção em linguagem natural da oferta de 6 parcelas do WHATS-DEMO-001,
revisão lida, “sim” explícito, protocolo
`DEMO-b12301f8-1547-48d5-b634-c16fd3c2b622` entregue no chat e Caso/BPF
lidos de volta. Entrada **R$ 0,00**, **6 × R$ 2.500,20**, total **R$ 15.001,20**.
O BPF único percorreu as cinco etapas e terminou. O Caso não foi resolvido.

No **publicado**, um novo chat Direct Line aceitou código `1`, consultou as
propostas atuais do WHATS-DEMO-002 pelo MCP e retornou o protocolo concluído
do WHATS-DEMO-001. **WHATS-DEMO-002 continua elegível para a demonstração do
usuário**. Não houve envio de mensagem por um aparelho WhatsApp nesta validação.
Para testar, abra uma conversa nova, diga `renegociar`, informe qualquer código
numérico e peça as opções do contrato 002. Não reutilize o contrato 001 para
formalizar outro acordo.

O nome reservado do parâmetro de autenticação dos conectores é `api_key`;
`apiKey` pode resultar em conexão marcada `Connected` que não autentica a
chamada real. Status de conexão não substitui o teste de uma operação.
Respostas 2xx do conector não devem ser tratadas como erro apenas porque
o campo de status HTTP está preenchido; a conclusão ainda exige o envelope,
o estado e o protocolo validados.

Antes de exportar, inclua todos os componentes do agente na solution com
`--ensure-solution-backup-dir` no gerador. Ele descobre o tipo nativo pela
associação existente; o tipo desta instalação é `10270`, não um código fixo
presumido. Confirme no pacote que os tópicos novos e autenticados estão presentes.
O empacotador PAC 2.5.1 emite um aviso de root `ECConnector` para os dois
conectores, mesmo incluindo suas definições e arquivos no pacote; essa presença
foi verificada no ZIP. Importação em outro ambiente permanece não verificada.

### Fluxo autenticado anterior

Os tópicos determinísticos `Renegociar` e `Andamento` usam exclusivamente `POST`
para a API compartilhada `https://fsi-reneg-88957f9a.azurewebsites.net/api/renegociacao/`.

1. Login pelo provedor configurado do agente. CPF opcional **não** autentica.
2. `profile`: vínculo da identidade verificada ao contato; CPF opcional apenas confere o vínculo.
3. `contracts`: lista contratos próprios e seleciona um elegível em Adaptive Card 1.3.
4. `offers`: seleciona uma oferta retornada pelo serviço em Adaptive Card 1.3.
5. `start`, com `channel: chat`: registra a solicitação demonstrativa.
6. `review`: obtém `reviewHash` e condições da versão escolhida. O tópico valida
   campos monetários inteiros em centavos e lê entrada, parcelas, total, versão e
   validade exatas da resposta. Não calcula uma oferta nova.
7. Pergunta explícita de confirmação. Somente `true` chama `confirm` com o hash.
8. Conclusão exige `status: ok`, `demonstration: true`, mesmo `requestId`,
   `data.status: completed` e protocolo não vazio. `processing`/pendente não é sucesso.
   Falha de transporte na confirmação permite **uma consulta de status**, nunca
   repetição automática de confirmação.

A conclusão acontece no chat, sem exigir confirmação em uma página. Todos os
resultados são identificados como demonstração fictícia. Nesse fluxo autenticado
não há conectores com credenciais do maker, pesquisa generativa, fluxo externo, bot público por CPF,
tokens em eventos fornecidos pelo usuário nem envio de `contactId` pelo cliente.

## Autenticação federada configurada

O aceite original usou autenticação Entra personalizada (`authenticationmode: 3`),
login obrigatório (`authenticationtrigger: 1`) e público autenticado sem exigir
associação ao grupo de autores (`accesscontrolpolicy: 0`).
Em 2026-10-26, após a aprovação MFA pelo usuário, foi configurado no portal
**Settings > Security > Authentication > Authenticate manually** o provedor
**Microsoft Entra ID V2 with federated credentials**, mantendo login obrigatório:

- Tenant: `3a3c8be3-6a50-455f-a303-f9df0c768430`.
- Cliente aprovado: `945076d4-6ec8-4ec7-8cdd-1fe34c686262`.
- Callback web já registrado: `https://token.botframework.com/.auth/web/redirect`.
- Escopos: `openid profile offline_access api://22630d43-3353-4fb7-8496-3979bd7617d9/Renegotiate`.
- Audience da API: `22630d43-3353-4fb7-8496-3979bd7617d9`.
- Conexão OAuth lida do bot: `2b56bd71-4ef1-42c4-9468-11c3106dd54c`.
- Credencial federada dedicada: `fsi-renegociacao-copilot`,
  ID `34fecb3d-f1c4-4912-a50a-511a2f345b8c`, audience
  `api://AzureADTokenExchange`. Issuer e subject vieram da interface do próprio bot.

A única mudança no aplicativo compartilhado foi adicionar essa credencial federada.
Os redirects web/SPA foram lidos de volta e preservados; `passwordCredentials`
continua vazio. Não foi criado segredo nem copiada a configuração incompleta do bot
existente. Uma conexão configurada ainda **não prova** aquisição do token correto
ou funcionamento do canal.

Todos os HTTP nodes usam unicamente:
`Authorization: ="Bearer " & System.User.AccessToken`.
Não substituir por token do maker, Graph, token inventado, CPF ou contactId.
Um token de audience incorreta deve continuar sendo rejeitado pelo backend.

**Histórico de acesso:** a primeira tentativa parou em senha/MFA. A solicitação foi
reenviada pelo controle nativo e aprovada pelo usuário; a sessão do Studio foi
confirmada às 10:36 BRT. Não houve bypass nem entrada automatizada de credenciais.
O verificador do portal revelou erros de declaração de variáveis, ausentes no
JSON Schema; os inicializadores foram corrigidos e reaplicados. Também foram
corrigidos os descritores `type` de records/tabelas aninhados nas respostas HTTP,
o formato numérico de `Text` e a interpolação de mensagens. O portal confirmou
**Renegociar e Andamento com 0 erros e nenhum aviso**.

## Publicação e bloqueadores reais do canal

- `PvaPublish` retornou 200; leitura de volta:
  `publishedon: 2026-10-26T13:54:01Z`, `Succeeded`, `Provisioned/Synchronized`.
  A interface também confirmou a publicação. Nenhum outro bot foi republicado.
- Revisão de pendência publicada em `2026-10-26T14:36:19Z`, novamente
  `Succeeded/Synchronized` e verificador nativo 0 erros/avisos em ambos os tópicos:
  `start` com wrapper `processing` preserva requestId e responde pendente, sem
  executar revisão/confirmação; `draft/reviewed` também não são conclusão.
- O chat de teste do Studio apresentou o cartão de login, mas o clique resultou
  em **OAuth is not supported on this Direct Line adapter**.
- O site de demonstração publicado iniciou uma conversa real e apresentou o
  cartão obrigatório de login. Após o consentimento nativo **Você confia neste
  editor?**, o Direct Line respondeu:
  `403 ServiceError` em `/v3/directline/session/getsessionid`,
  **Request origin https://web.powerva.microsoft.com/ is not in the list of trusted origins.**
- O demo no domínio atual `copilotstudio.microsoft.com` e o embed `/webchat`
  fornecido pela interface também chegaram ao cartão e falharam no login com 403.
  Não foi desabilitada a validação de origens nem retirada a autenticação.
- A tela de segurança Web não oferece edição de origens, apenas segredos e
  exigência de acesso seguro. Os dois segredos Direct Line do **novo bot** foram
  rotacionados após essa inspeção; valores não são usados nos arquivos.
- A indisponibilidade inicial `503 IDENTITY_LOOKUP_UNAVAILABLE` foi corrigida pelo
  coordenador, e Pages informou `profile/contracts` 200. A projeção nativa do
  contrato 002 permaneceu pendente na frente Pages, tratada pelo coordenador.
  Nenhuma chamada `start`, `review` ou `confirm` foi enviada pelo agente e
  **REN-DEMO001 não foi consumido**.

Isso prova o início do canal publicado e o bloqueio de autenticação, **não** uma
renegociação concluída. A transcrição positiva, protocolo e leitura final do
serviço permanecem não verificados. Não afirmar que a autenticação manual é
incompatível com todo canal; os erros acima são dos adaptadores efetivamente testados.

## Correção oficial: token com origem vinculada no servidor

Referências Microsoft:
[segurança Web do Studio](https://learn.microsoft.com/en-us/microsoft-copilot-studio/configure-web-security)
e [autenticação Direct Line](https://learn.microsoft.com/en-us/azure/bot-service/rest-api/bot-framework-rest-direct-line-3-0-authentication).
O controle de acesso seguro do Studio não é uma lista de websites. A API oficial
`POST https://directline.botframework.com/v3/directline/tokens/generate` aceita
`trustedOrigins` no corpo e um segredo no cabeçalho **do servidor**.

O helper canônico `services/renegociacao/renegociacao/directline_token.py` aplica:

- Origem padrão única: `https://portalatendimento01.powerappsportals.com`,
  comparação exata do cabeçalho `Origin`, sem wildcard, subdomínios ou trailing slash.
- Origem oficial `https://copilotstudio.microsoft.com` somente por opt-in explícito
  para teste; desabilitada por padrão. Host legado não permitido.
- `user.id` opaco, prefixado `dl_`, vinculado por HMAC ao oid/issuer **já verificados
  pelo servidor**, com menos de 64 caracteres. Não identifica/autentica o cliente
  perante a API transacional.
- Segredo somente no servidor; tokens efêmeros em resposta `no-store`. Não segue
  redirects e não devolve corpos de erro upstream.
- O agente continua exigindo seu OAuth delegado próprio. O token Direct Line não
  substitui `System.User.AccessToken`.

**Validação real executada:** segredo copiado pelo controle nativo para o
**Keychain macOS**, serviço `fsi-renegociacao-directline`, conta
`fsi_renegociacao`; clipboard limpo imediatamente, nenhum valor em arquivos/saída.
A emissão com origem do portal retornou **200**. O mesmo token obteve **200** em
`/v3/directline/session/getsessionid` global e regional com a origem permitida,
e **403** ao apresentar origem não permitida. Isso resolve a causa do 403 sem
desabilitar a política. Nove testes unitários cobrem emissão, negações, respostas
inválidas, sanitização e redirects.

### Contrato para a ligação pelo coordenador

Endpoint adicional publicado pelo coordenador (deployment `1568be52`):
`POST /api/renegociacao/chat-token`, corpo `{}`, bearer delegado MSAL e `Origin`
do navegador. Antes de chamar `issue_token`, reutilizar validação de JWT,
audience, escopo `Renegotiate` e mapeamento servidor → contato. Obter oid/issuer
do resultado validado, nunca do corpo do cliente. O coordenador confirmou
configuração do segredo somente no servidor Azure, sem cópia para repo/Pages.

Resposta **sem o wrapper das sete ações transacionais**:
`{token, conversationId, userId, expires_in}`, com os cabeçalhos retornados por
`response_headers()`. As sete ações existentes permanecem inalteradas.

Pages pode montar Web Chat isolado na origem do próprio portal usando apenas
o token efêmero e `userID: userId` retornado. Direct Line global padrão funciona.
Para token recém-emitido, iniciar com `createDirectLine({token})`; não passar
`conversationId` como reconexão de uma conversa que ainda não foi iniciada.
Não reutilizar o iframe externo legado que apresenta a origem errada, não trocar
o bot global e não alterar login/menu/fluxo independente da página.
O coordenador autorizou diretamente essa integração à frente Pages.

Uma tentativa de cliente de laboratório não concluiu a conexão, pois o contexto
de requisições da ferramenta retornou `Storage.getCookies: Method not found`.
O servidor local de uso único foi encerrado. Não foi contornada essa limitação;
naquele momento, o teste completo aguardava a integração Pages. Uma nova aba independente do portal,
em consulta somente leitura, retornou 403 na rota após o callback MSAL; essa
tentativa foi encerrada e Pages avisado. A aba original de Pages foi preservada.

**Integração publicada e lida em 12:31 BRT:** `#reneg-chat-host iframe`, URL
`https://portalatendimento01.powerappsportals.com/renegociacao-financiamento/chat-app/`,
origem efetiva do portal, Web Chat conectado. O SDK foi servido pela autoria
nativa aprovada, sem alterar CSP/política de anexos ou o bot global.
A mensagem **quero renegociar meu financiamento** foi enviada ao agente publicado,
que respondeu com o cartão obrigatório **Entrar com segurança**.
O clique acionou `/directline/session/getsessionid` com **200**, confirmando a
correção de origem no canal real, não apenas no teste de servidor.

A aba permanece `visibilityState: hidden` para a ferramenta. Clique nativo
automatizado não ativou o botão; clique DOM acionou o login e `window.open`
retornou WindowProxy, mas nenhum popup de autenticação ficou acessível à automação.
Foi solicitado exibir a aba Portal e clicar o botão nativo uma vez. Não há MFA
pendente comprovado e nenhuma senha/código foi digitado. O iframe e o atendimento
002 foram preservados; 001 ainda não iniciou transação.

## Validação executada

- Sete componentes aplicados com `cps_agent.py apply`; leitura do servidor igual
  aos arquivos.
- `cps_agent.py check` com o schema oficial Microsoft: **0 erros, 0 avisos** nos
  sete YAML, inclusive com `--bot`. O tópico obrigatório `Signin` usa o mesmo
  padrão `OnSignIn` + `OAuthInput` lido do bot existente, sem modificá-lo.
- `pac copilot extract-template` do agente novo: parser nativo preservou os seis
  diálogos, os oito HTTP nodes e as duas seleções.
- O dumper desabilita aliases YAML: um roundtrip inicial descartou silenciosamente
  os nós após `&id001`, embora o YAML e o JSON Schema fossem válidos. Isso foi
  corrigido, reaplicado e conferido novamente pelo PAC. O self-test agora compara
  também todos os IDs de nós com o roundtrip nativo pelo comando `native-check`.
- `python3 scripts/renegociacao_agent.py self-test`: invariantes estruturais de
  autenticação delegada, seleção e confirmação única.
- Os sete endpoints, chamados sem Authorization e com corpo vazio, responderam
  **401**, `demonstration: true`, `error.code: IDENTITY_REQUIRED`.
- Comparação dos 53 bots preexistentes: nenhuma mudança.
- Verificador nativo: `Renegociar` e `Andamento` com **0 erros/avisos** após as
  correções de tipagem, inicialização, formato monetário e interpolação.
- **Executados no canal publicado:** OAuth, CPF dispensado, contratos próprios,
  seleção de contrato/oferta, revisão exata, confirmação explícita, protocolo,
  consulta de status e leitura nativa final. Não executados no chat: ramo CPF
  informado/inválido e sessão de usuário não administrador.

## Regerar e aplicar somente ao agente dedicado

```sh
mkdir -p agents/renegociacao
curl -fLsS https://raw.githubusercontent.com/microsoft/skills-for-copilot-studio/main/reference/bot.schema.yaml-authoring.json \
  -o agents/renegociacao/bot.schema.yaml-authoring.json
python3 scripts/renegociacao_agent.py check
python3 scripts/renegociacao_agent.py apply --bot fsi_renegociacao
pac copilot extract-template --bot fsi_renegociacao \
  --templateFileName agents/renegociacao/native-roundtrip.yaml --overwrite
python3 scripts/renegociacao_agent.py native-check
python3 -m unittest discover -s agents/renegociacao -p 'test_directline_token.py' -v
```

O gerador nunca publica e não altera autenticação. `template.yaml` serve somente
para recriar um agente novo por PAC, não para importar por cima de outro bot.
O PAC aplicou inicialmente `GroupMembership` apesar de `Any` no template; o bot
dedicado foi corrigido por PATCH para `accesscontrolpolicy: 0`, preservando o
login obrigatório. Conferir esses três campos em qualquer recriação.

O aceite positivo de 001 está concluído e deve permanecer como evidência; não
refazer a transação 001 ou 002. Em mudanças futuras, exigir novo verificador,
publicação `Succeeded/Synchronized` e leitura do serviço. Casos negativos de CPF,
identidade incorreta, oferta expirada/alterada, confirmação não e pendência são
cenários de reexecução futura, não novos testes de escrita nesta sessão.
