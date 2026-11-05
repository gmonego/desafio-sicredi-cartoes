# Execução no Databricks

Passo a passo para rodar o pipeline de ponta a ponta em um workspace Databricks. O código é o mesmo da execução local, sem nenhuma alteração. O que muda é a raiz dos dados, apontada pela variável de ambiente `PIPELINE_DATA_ROOT`.

A execução validada foi feita em Azure Databricks com Unity Catalog, runtime com Spark 4.2.0 e Python 3.11.

## 1. O que precisa antes

- Um workspace com Unity Catalog habilitado.
- Um cluster ligado, ou serverless.
- Permissão para criar catálogo, schema e Volume. Se não tiver permissão de criar catálogo, dá para usar um catálogo existente e só criar o schema e o Volume dentro dele.

Os nomes usados aqui são os da execução validada. Pode trocar, desde que troque também nos notebooks.

## 2. Criar catálogo, schema e Volume

Em um notebook SQL, ou na aba SQL Editor:

```sql
create catalog if not exists avaliacao_sicredi;
create schema if not exists avaliacao_sicredi.novarota;
create schema if not exists avaliacao_sicredi.novarota_gold;

create volume if not exists avaliacao_sicredi.novarota.dados;
```

O Volume é onde ficam os arquivos da landing e as tabelas Delta das camadas bronze, prata e quarentena. O schema `novarota_gold` é onde as quatro tabelas da ouro são registradas como managed tables.

O caminho do Volume fica assim:

```
/Volumes/avaliacao_sicredi/novarota/dados
```

## 3. Clonar o repositório

No menu lateral, em Workspace, usar a opção de adicionar repositório Git e informar a URL do projeto.

Depois de clonar, confirmar que a pasta tem `src/`, `tests/`, `conf/`, `sql/` e `notebooks/`.

## 4. Rodar os testes

Abrir `notebooks/run_tests` e executar as células na ordem. O notebook faz quatro coisas:

1. Instala as dependências com `%pip install -r ../requirements.txt`.
2. Reinicia o Python para as dependências valerem.
3. Aponta `PIPELINE_DATA_ROOT` para o Volume e ajusta o `sys.path` para a raiz do repositório.
4. Executa a suíte com `pytest.main`.

O resultado esperado é **109 passed, 1 skipped**.

O teste ignorado é o `test_delta_habilitado_na_sessao_local`. Ele confere se a sessão local carregou a extensão do Delta, coisa que no Databricks não existe porque o Delta é nativo do runtime. O teste está marcado com `skipif` para o ambiente. O teste que valida o comportamento do Delta, gravar e ler uma tabela, roda aqui normalmente.

A suíte leva por volta de 7 minutos.

## 5. Rodar o pipeline

Abrir `notebooks/run_pipeline` e executar as células na ordem. Ele gera a massa fictícia, executa as três camadas e registra as tabelas da ouro como managed tables em `avaliacao_sicredi.novarota_gold`.

O log mostra início, fim e duração de cada etapa, e a contagem por entidade.

## 6. Conferir o resultado

Os números esperados, idênticos aos da execução local:

| Camada | clientes | contas | cartões | transações | estornos |
|---|---|---|---|---|---|
| bronze | 318 | 340 | 400 | 5728 | 174 |
| silver | 285 | 323 | 388 | 5659 | 160 |
| quarentena | 18 | 17 | 12 | 41 | 14 |

Na ouro: `gold_transacoes_cartao` com 5659 linhas, `gold_resumo_cliente_mes` com 463, `gold_ranking_estabelecimento` com 80 e `gold_alertas_qualidade` com 11.

Para conferir pelo catálogo:

```sql
select 'transacoes_cartao' as tabela, count(*) from avaliacao_sicredi.novarota_gold.gold_transacoes_cartao
union all
select 'resumo_cliente_mes', count(*) from avaliacao_sicredi.novarota_gold.gold_resumo_cliente_mes
union all
select 'ranking_estabelecimento', count(*) from avaliacao_sicredi.novarota_gold.gold_ranking_estabelecimento
union all
select 'alertas_qualidade', count(*) from avaliacao_sicredi.novarota_gold.gold_alertas_qualidade;
```

## 7. Executar as consultas analíticas

As quatro consultas de `sql/analises/` não são materializadas. Para rodar dentro do Databricks, basta registrar as views da ouro e executar o script:

```python
from scripts.executar_analises import executar_todas

executar_todas()
```

Ou abrir cada arquivo e colar o SQL no editor, trocando o nome da tabela pelo caminho completo no catálogo.

## 8. Problemas conhecidos

**`Public DBFS root is disabled`.** Workspaces novos no Azure vêm com a raiz pública do DBFS desabilitada. Por isso os dados vão para um Volume do Unity Catalog, e não para `/dbfs/tmp`. Se o erro aparecer, confira se o `PIPELINE_DATA_ROOT` está apontando para o Volume.

O mesmo vale para os arquivos temporários do pytest. O notebook de testes define `PYTEST_DEBUG_TEMPROOT` para uma pasta dentro do Volume, senão o pytest tenta escrever na raiz do DBFS e falha.

**`INVALID_PARAMETER_VALUE: Missing cloud file system scheme`.** Acontece ao tentar criar tabela externa com `LOCATION` apontando para dentro de um Volume. O Unity Catalog não permite isso. Por esse motivo as tabelas da ouro são registradas como managed tables, com `saveAsTable`, e não como externas.

**Avisos de `DeprecationWarning` sobre `distutils`.** Vêm do cliente Spark Connect do próprio Databricks, que usa `LooseVersion` para checar a versão do PyArrow. Não é código do projeto e não afeta o resultado.

**Cluster desligado no meio da execução.** O cluster encerra sozinho por inatividade. Se isso acontecer, religue e rode o notebook desde a primeira célula, porque a instalação das dependências se perde junto com o cluster.