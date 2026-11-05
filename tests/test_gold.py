"""Testes da camada ouro."""

import os

import pytest
from pyspark.sql import functions as F

from scripts.gerar_massa import DEFEITOS, DEFEITOS_TRANSACAO, gerar_massa
from src.config import resolve_target
from src.ingestion.bronze import ingerir_tudo
from src.transformations.gold import construir_tudo, registrar_views
from src.transformations.silver import processar_tudo


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory, spark):
    """Pipeline completo, bronze a ouro, executado uma vez para o módulo."""
    raiz = tmp_path_factory.mktemp("gold")
    landing = gerar_massa(str(raiz / "landing"))

    os.environ["PIPELINE_DATA_ROOT"] = str(raiz)

    ingerir_tudo(landing)
    processar_tudo()
    construir_tudo()

    yield raiz

    os.environ.pop("PIPELINE_DATA_ROOT", None)


def ler(spark, camada, tabela):
    return spark.read.format("delta").load(resolve_target(camada, tabela))


# ---------------------------------------------------------------------------
# Motor SQL
# ---------------------------------------------------------------------------

def test_views_sao_registradas(spark, pipeline):
    """As views são a ponte entre o caminho físico e o nome que o SQL usa."""
    registrar_views()

    for esperada in ("silver_transacoes", "quarantine_clientes", "silver_cartoes"):
        assert spark.catalog.tableExists(esperada), f"view {esperada} nao registrada"


def test_as_quatro_tabelas_foram_materializadas(spark, pipeline):
    for tabela in (
        "gold_transacoes_cartao",
        "gold_resumo_cliente_mes",
        "gold_ranking_estabelecimento",
        "gold_alertas_qualidade",
    ):
        assert ler(spark, "gold", tabela).count() > 0, f"{tabela} vazia"


# ---------------------------------------------------------------------------
# gold_transacoes_cartao
# ---------------------------------------------------------------------------

def test_enriquecimento_nao_perde_nem_cria_transacao(spark, pipeline):
    """A ouro tem exatamente as transações da silver."""
    silver = ler(spark, "silver", "transacoes").count()
    gold = ler(spark, "gold", "gold_transacoes_cartao").count()

    assert gold == silver


def test_toda_transacao_encontra_o_cartao(spark, pipeline):
    """Transação sem cartão e rejeitada na quarentena, então não chega na ouro."""
    df = ler(spark, "gold", "gold_transacoes_cartao")
    assert df.filter(F.col("id_cartao").isNull()).count() == 0


def test_cartao_orfao_deixa_conta_e_cliente_nulos(spark, pipeline):
    """Cartão rejeitado por FK aponta para uma conta que nao existe."""
    df = ler(spark, "gold", "gold_transacoes_cartao")
    sem_conta = df.filter(F.col("id_conta").isNull())

    assert sem_conta.count() > 0, "a massa deveria ter transacoes de cartao orfao"
    assert sem_conta.filter(~F.col("cadastro_com_restricao")).count() == 0, (
        "transacao sem conta precisa estar marcada com restricao"
    )


def test_cadastro_com_restricao_e_marcado(spark, pipeline):
    """Transação enriquecida a partir de registro em quarentena fica
    sinalizada para que a análise saiba da procedência."""
    df = ler(spark, "gold", "gold_transacoes_cartao")
    com_restricao = df.filter(F.col("cadastro_com_restricao"))

    assert com_restricao.count() > 0, "nenhuma transacao marcada com restricao"
    assert com_restricao.count() < df.count(), "todas marcadas: filtro invertido"


def test_colunas_de_enriquecimento_presentes(spark, pipeline):
    df = ler(spark, "gold", "gold_transacoes_cartao")
    for coluna in (
        "bandeira", "tipo_conta", "nome_cliente", "ano_mes",
        "estornada", "cadastro_com_restricao",
    ):
        assert coluna in df.columns


def test_ano_mes_derivado_da_data_do_evento(spark, pipeline):
    """Análise se organiza por data_transacao, não por data_referencia."""
    df = ler(spark, "gold", "gold_transacoes_cartao")
    divergentes = df.filter(
        F.date_format("data_transacao", "yyyy-MM") != F.col("ano_mes")
    )
    assert divergentes.count() == 0


# ---------------------------------------------------------------------------
# Estornos nos indicadores
# ---------------------------------------------------------------------------

def test_valor_liquido_exclui_estornos(spark, pipeline):
    """A soma do liquido é menor que a do bruto exatamente pelo montante
    estornado."""
    df = ler(spark, "gold", "gold_transacoes_cartao")

    bruto = df.agg(F.sum("valor")).collect()[0][0]
    liquido = df.agg(F.sum("valor_liquido")).collect()[0][0]
    estornado = df.filter(F.col("estornada")).agg(F.sum("valor")).collect()[0][0]

    assert liquido == bruto - estornado


def test_resumo_separa_bruto_e_liquido(spark, pipeline):
    """valor_estornado é a diferenca entre os dois."""
    df = ler(spark, "gold", "gold_resumo_cliente_mes")

    inconsistentes = df.filter(
        F.col("valor_estornado") != F.col("valor_bruto") - F.col("valor_liquido")
    )
    assert inconsistentes.count() == 0


def test_clientes_sem_estorno_tem_bruto_igual_ao_liquido(spark, pipeline):
    df = ler(spark, "gold", "gold_resumo_cliente_mes")
    sem_estorno = df.filter(F.col("qtd_estornadas") == 0)

    assert sem_estorno.count() > 0
    assert sem_estorno.filter(
        F.col("valor_bruto") != F.col("valor_liquido")
    ).count() == 0


# ---------------------------------------------------------------------------
# gold_resumo_cliente_mes
# ---------------------------------------------------------------------------

def test_resumo_tem_uma_linha_por_cliente_e_mes(spark, pipeline):
    """Granularidade declarada da tabela."""
    df = ler(spark, "gold", "gold_resumo_cliente_mes")
    chaves = df.select("id_cliente", "ano_mes").distinct().count()

    assert df.count() == chaves


def test_contagem_do_resumo_bate_com_as_transacoes_que_tem_cliente(spark, pipeline):

    transacoes = ler(spark, "gold", "gold_transacoes_cartao")
    resumo = ler(spark, "gold", "gold_resumo_cliente_mes")

    com_cliente = transacoes.filter(F.col("id_cliente").isNotNull()).count()

    assert resumo.agg(F.sum("qtd_transacoes")).collect()[0][0] == com_cliente


def test_soma_do_resumo_bate_com_as_transacoes_que_tem_cliente(spark, pipeline):
    """Agregação não pode perder nem inventar valor."""
    transacoes = ler(spark, "gold", "gold_transacoes_cartao")
    resumo = ler(spark, "gold", "gold_resumo_cliente_mes")

    total_base = (
        transacoes.filter(F.col("id_cliente").isNotNull())
        .agg(F.sum("valor_liquido")).collect()[0][0]
    )
    total_resumo = resumo.agg(F.sum("valor_liquido")).collect()[0][0]

    assert total_base == total_resumo


def test_ticket_medio_usa_apenas_transacoes_efetivas(spark, pipeline):
    """Compra estornada nao representa consumo, entao não entra no divisor."""
    df = ler(spark, "gold", "gold_resumo_cliente_mes")
    com_movimento = df.filter(F.col("qtd_transacoes") > F.col("qtd_estornadas"))

    assert com_movimento.count() > 0

    esperado = F.round(
        F.col("valor_liquido") / (F.col("qtd_transacoes") - F.col("qtd_estornadas")), 2
    )
    assert com_movimento.filter(F.col("ticket_medio") != esperado).count() == 0


def test_resumo_nao_tem_cliente_nulo(spark, pipeline):
    df = ler(spark, "gold", "gold_resumo_cliente_mes")
    assert df.filter(F.col("id_cliente").isNull()).count() == 0


# ---------------------------------------------------------------------------
# gold_ranking_estabelecimento
# ---------------------------------------------------------------------------

def test_ranking_por_valor_esta_ordenado(spark, pipeline):
    df = ler(spark, "gold", "gold_ranking_estabelecimento")
    linhas = df.orderBy("rank_valor").select("rank_valor", "valor_liquido").collect()

    for anterior, atual in zip(linhas, linhas[1:]):
        assert anterior["valor_liquido"] >= atual["valor_liquido"], (
            "rank_valor nao acompanha o valor_liquido"
        )


def test_ranking_por_quantidade_esta_ordenado(spark, pipeline):
    df = ler(spark, "gold", "gold_ranking_estabelecimento")
    linhas = (
        df.orderBy("rank_quantidade")
        .select("rank_quantidade", "qtd_transacoes")
        .collect()
    )

    for anterior, atual in zip(linhas, linhas[1:]):
        assert anterior["qtd_transacoes"] >= atual["qtd_transacoes"]


def test_dense_rank_nao_pula_posicao(spark, pipeline):
    """DENSE_RANK mantem a sequencia contínua mesmo com empate."""
    df = ler(spark, "gold", "gold_ranking_estabelecimento")
    posicoes = sorted(
        linha["rank_valor"] for linha in df.select("rank_valor").distinct().collect()
    )

    assert posicoes == list(range(1, len(posicoes) + 1))


def test_ranking_por_cidade_reinicia_em_cada_cidade(spark, pipeline):
    """A janela é particionada por cidade, então cada cidade tem seu primeiro
    colocado."""
    df = ler(spark, "gold", "gold_ranking_estabelecimento")

    cidades_com_lider = (
        df.filter(F.col("rank_valor_na_cidade") == 1)
        .select("cidade_estabelecimento")
        .distinct()
        .count()
    )
    total_cidades = df.select("cidade_estabelecimento").distinct().count()

    assert cidades_com_lider == total_cidades


def test_ranking_soma_a_mesma_quantidade_da_base(spark, pipeline):
    ranking = ler(spark, "gold", "gold_ranking_estabelecimento")
    transacoes = ler(spark, "gold", "gold_transacoes_cartao")

    assert (
        ranking.agg(F.sum("qtd_transacoes")).collect()[0][0] == transacoes.count()
    )


def test_ranking_agrupa_por_estabelecimento(spark, pipeline):
    ranking = ler(spark, "gold", "gold_ranking_estabelecimento")
    transacoes = ler(spark, "gold", "gold_transacoes_cartao")

    distintos = transacoes.select(
        "nome_estabelecimento", "mcc", "cidade_estabelecimento"
    ).distinct().count()

    assert ranking.count() == distintos


# ---------------------------------------------------------------------------
# gold_alertas_qualidade
# ---------------------------------------------------------------------------

def test_alertas_registra_rejeicoes_e_alertas(spark, pipeline):
    """As duas dimensões so existem porque as regras tem severidade."""
    df = ler(spark, "gold", "gold_alertas_qualidade")
    tipos = {linha["tipo"] for linha in df.select("tipo").distinct().collect()}

    assert tipos == {"rejeicao", "alerta"}


def test_rejeicoes_batem_com_a_quarentena(spark, pipeline):
    """Contagem exata vinda dos defeitos declarados no gerador."""
    df = ler(spark, "gold", "gold_alertas_qualidade").filter(
        F.col("tipo") == "rejeicao"
    )

    def quantidade(entidade, motivo):
        linha = df.filter(
            (F.col("entidade") == entidade) & (F.col("motivo") == motivo)
        ).collect()
        return linha[0]["qtd_registros"] if linha else 0

    assert quantidade("contas", "fk_invalida_id_cliente") == (
        DEFEITOS["contas_sem_cliente"]
    )
    assert quantidade("contas", "status_conta_fora_do_dominio") == (
        DEFEITOS["contas_status_invalido"]
    )
    assert quantidade("cartoes", "fk_invalida_id_conta") == (
        DEFEITOS["cartoes_sem_conta"]
    )
    assert quantidade("transacoes", "valor_nao_positivo") == sum(
        DEFEITOS_TRANSACAO["valor_negativo"]
    )
    assert quantidade("transacoes", "data_transacao_futura") == sum(
        DEFEITOS_TRANSACAO["data_invalida"]
    )
    assert quantidade("estornos", "fk_invalida_id_transacao") == (
        DEFEITOS["estornos_orfaos"]
    )


def test_alertas_registram_bandeira_invalida(spark, pipeline):
    df = ler(spark, "gold", "gold_alertas_qualidade")
    linha = df.filter(
        (F.col("entidade") == "cartoes")
        & (F.col("tipo") == "alerta")
        & (F.col("motivo") == "bandeira_fora_do_dominio")
    ).collect()

    assert linha, "alerta de bandeira invalida nao registrado"
    assert linha[0]["qtd_registros"] == DEFEITOS["cartoes_bandeira_invalida"]


def test_registro_com_alerta_nao_foi_rejeitado(spark, pipeline):
    cartoes = ler(spark, "silver", "cartoes")
    com_alerta = cartoes.filter(F.size("alertas") > 0)

    assert com_alerta.count() >= DEFEITOS["cartoes_bandeira_invalida"]


# ---------------------------------------------------------------------------
# Reprocessamento
# ---------------------------------------------------------------------------

def test_reconstruir_a_ouro_e_idempotente(spark, pipeline):
    """Overwrite completo."""
    tabelas = (
        "gold_transacoes_cartao",
        "gold_resumo_cliente_mes",
        "gold_ranking_estabelecimento",
        "gold_alertas_qualidade",
    )
    antes = {tabela: ler(spark, "gold", tabela).count() for tabela in tabelas}

    construir_tudo()

    depois = {tabela: ler(spark, "gold", tabela).count() for tabela in tabelas}

    assert antes == depois

# ---------------------------------------------------------------------------
# Consultas analiticas
# ---------------------------------------------------------------------------

def test_resumo_cobre_mais_de_um_mes(spark, pipeline):
    df = ler(spark, "gold", "gold_resumo_cliente_mes")
    meses = {linha["ano_mes"] for linha in df.select("ano_mes").distinct().collect()}

    assert len(meses) >= 2, f"apenas {len(meses)} mes na ouro"


def test_consultas_analiticas_executam(spark, pipeline):
    """Toda consulta de sql/analises/ precisa rodar sobre a ouro materializada."""
    from src.transformations.gold import registrar_views_gold
    from scripts.executar_analises import listar_consultas

    registrar_views_gold()
    consultas = listar_consultas()

    assert consultas, "nenhuma consulta encontrada em sql/analises/"

    for arquivo in consultas:
        sql = arquivo.read_text(encoding="utf-8")
        spark.sql(sql).limit(1).collect()


def test_quartis_classificam_todos_os_clientes(spark, pipeline):
    """A soma dos quartis tem que fechar com o número de clientes com movimentação."""
    from src.transformations.gold import registrar_views_gold
    from pathlib import Path

    registrar_views_gold()
    caminho = Path("sql/analises/quartis_clientes.sql")
    resultado = spark.sql(caminho.read_text(encoding="utf-8"))

    quartis = {linha["quartil"] for linha in resultado.select("quartil").collect()}
    assert quartis == {1, 2, 3, 4}

    clientes_na_ouro = (
        ler(spark, "gold", "gold_resumo_cliente_mes")
        .select("id_cliente").distinct().count()
    )
    assert resultado.count() == clientes_na_ouro