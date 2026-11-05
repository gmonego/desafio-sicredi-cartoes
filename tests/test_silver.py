"""Testes da camada silver.

As contagens afirmadas aqui vem de DEFEITOS e DEFEITOS_TRANSACAO, do gerador
de massa. Como a massa é deterministica e os defeitos são declarados, cada
teste afirma um numero exato em vez de apenas conferir que alguma coisa foi
rejeitada.
"""

import csv
import os

import pytest
from pyspark.sql import functions as F

from scripts.gerar_massa import DEFEITOS, DEFEITOS_TRANSACAO, gerar_massa
from src.config import resolve_target
from src.ingestion.bronze import ingerir_tudo
from src.quality.rules import cpf_valido, expandir
from src.transformations.silver import (
    deduplicar,
    processar_entidade,
    processar_tudo,
)


@pytest.fixture(scope="module")
def pipeline(tmp_path_factory, spark):
    """Executa o pipeline completo uma vez para todos os testes do módulo."""
    raiz = tmp_path_factory.mktemp("pipeline")
    landing = gerar_massa(str(raiz / "landing"))

    os.environ["PIPELINE_DATA_ROOT"] = str(raiz)

    ingerir_tudo(landing)
    processar_tudo()

    yield raiz

    os.environ.pop("PIPELINE_DATA_ROOT", None)


def ler(spark, camada, entidade):
    return spark.read.format("delta").load(resolve_target(camada, entidade))


def motivos(spark, entidade):
    """Contagem de linhas por motivo de rejeição."""
    linhas = (
        ler(spark, "quarantine", entidade)
        .select(F.explode("motivos_erro").alias("motivo"))
        .groupBy("motivo").count().collect()
    )
    return {linha["motivo"]: linha["count"] for linha in linhas}


def total(chave):
    """Soma, entre os dias, a quantidade declarada de um defeito de transação."""
    return sum(DEFEITOS_TRANSACAO[chave])


# ---------------------------------------------------------------------------
# Validacao de CPF
# ---------------------------------------------------------------------------

def test_expressao_de_cpf_aceita_valido(spark):
    df = spark.createDataFrame([("52998224725",)], ["cpf"])
    assert df.select(F.expr(cpf_valido("cpf")).alias("ok")).collect()[0]["ok"] is True


def test_expressao_de_cpf_rejeita_digito_errado(spark):
    df = spark.createDataFrame([("52998224726",)], ["cpf"])
    assert df.select(F.expr(cpf_valido("cpf")).alias("ok")).collect()[0]["ok"] is False


def test_expressao_de_cpf_rejeita_digitos_repetidos(spark):
    """11111111111 satisfaz o cálculo do verificador mas não é CPF válido."""
    df = spark.createDataFrame([("11111111111",)], ["cpf"])
    assert df.select(F.expr(cpf_valido("cpf")).alias("ok")).collect()[0]["ok"] is False


def test_expressao_de_cpf_rejeita_formato_invalido(spark):
    for invalido in ("123", "abcdefghijk", "111.222.333-44"):
        df = spark.createDataFrame([(invalido,)], ["cpf"])
        resultado = df.select(F.expr(cpf_valido("cpf")).alias("ok")).collect()[0]
        assert resultado["ok"] is False, f"{invalido} deveria ser rejeitado"


def test_expandir_substitui_apenas_funcoes_conhecidas():
    """Funções nativas do Spark ficam intactas."""
    assert "substring" in expandir("cpf_valido(cpf)")
    assert expandir("trim(nome) <> ''") == "trim(nome) <> ''"


# ---------------------------------------------------------------------------
# Deduplicacao
# ---------------------------------------------------------------------------

def test_dedup_mantem_a_versao_mais_recente(spark):
    df = spark.createDataFrame(
        [
            ("C1", "antigo", "2026-01-01 00:00:00", "hash_a"),
            ("C1", "novo", "2026-06-01 00:00:00", "hash_b"),
        ],
        ["id", "nome", "versao", "hash_linha"],
    )
    resultado = deduplicar(df, ["id"], "versao").collect()

    assert len(resultado) == 1
    assert resultado[0]["nome"] == "novo"


def test_dedup_desempata_por_hash_quando_a_versao_empata(spark):

    df = spark.createDataFrame(
        [
            ("C1", "linha_b", "2026-01-01 00:00:00", "hash_bbb"),
            ("C1", "linha_a", "2026-01-01 00:00:00", "hash_aaa"),
        ],
        ["id", "nome", "versao", "hash_linha"],
    )

    primeira = deduplicar(df, ["id"], "versao").collect()
    segunda = deduplicar(df, ["id"], "versao").collect()

    assert len(primeira) == 1
    assert primeira == segunda, "dedup nao foi deterministica"
    # Ordenacao ascendente do hash, o menor vence.
    assert primeira[0]["hash_linha"] == "hash_aaa"


def test_dedup_preserva_chaves_distintas(spark):
    df = spark.createDataFrame(
        [
            ("C1", "a", "2026-01-01 00:00:00", "h1"),
            ("C2", "b", "2026-01-01 00:00:00", "h2"),
            ("C3", "c", "2026-01-01 00:00:00", "h3"),
        ],
        ["id", "nome", "versao", "hash_linha"],
    )
    assert deduplicar(df, ["id"], "versao").count() == 3


# ---------------------------------------------------------------------------
# Quarentena — contagens exatas
# ---------------------------------------------------------------------------

def test_clientes_rejeitados_por_cpf(spark, pipeline):

    contagem = motivos(spark, "clientes")

    esperado_digito = (
        DEFEITOS["clientes_cpf_invalido"] + DEFEITOS["clientes_cpf_malformado"]
    )
    assert contagem["cpf_digito_verificador_valido"] == esperado_digito
    assert contagem["cpf_formato_valido"] == DEFEITOS["clientes_cpf_malformado"]


def test_uma_linha_pode_acumular_varios_motivos(spark, pipeline):

    df = ler(spark, "quarantine", "clientes")
    assert df.filter(F.size("motivos_erro") > 1).count() == (
        DEFEITOS["clientes_cpf_malformado"]
    )


def test_contas_rejeitadas(spark, pipeline):
    contagem = motivos(spark, "contas")
    assert contagem["fk_invalida_id_cliente"] == DEFEITOS["contas_sem_cliente"]
    assert contagem["status_conta_valido"] == DEFEITOS["contas_status_invalido"]


def test_cartoes_rejeitados(spark, pipeline):
    contagem = motivos(spark, "cartoes")
    assert contagem["fk_invalida_id_conta"] == DEFEITOS["cartoes_sem_conta"]


def test_transacoes_rejeitadas(spark, pipeline):
    contagem = motivos(spark, "transacoes")
    assert contagem["valor_positivo"] == total("valor_negativo")
    assert contagem["data_transacao_valida"] == total("data_invalida")
    assert contagem["fk_invalida_id_cartao"] == total("sem_cartao")


def test_estornos_orfaos_rejeitados(spark, pipeline):
    contagem = motivos(spark, "estornos")
    assert contagem["fk_invalida_id_transacao"] == DEFEITOS["estornos_orfaos"]


def test_rejeicao_nao_cascateia(spark, pipeline):

    contagem = motivos(spark, "transacoes")
    assert contagem["fk_invalida_id_cartao"] == total("sem_cartao")


# ---------------------------------------------------------------------------
# Deduplicacao sobre a massa real
# ---------------------------------------------------------------------------

def test_silver_nao_tem_chave_duplicada(spark, pipeline):
    for entidade, chave in [
        ("clientes", "id_cliente"),
        ("contas", "id_conta"),
        ("cartoes", "id_cartao"),
        ("transacoes", "id_transacao"),
        ("estornos", "id_estorno"),
    ]:
        df = ler(spark, "silver", entidade)
        assert df.count() == df.select(chave).distinct().count(), (
            f"{entidade} com chave duplicada"
        )


def test_cliente_duplicado_mantem_a_versao_valida_mais_recente(spark, pipeline):

    landing = pipeline / "landing"
    with open(landing / "clientes.csv", encoding="utf-8", newline="") as arquivo:
        brutos = list(csv.DictReader(arquivo))

    por_id = {}
    for linha in brutos:
        por_id.setdefault(linha["id_cliente"], []).append(linha)
    duplicados = {k: v for k, v in por_id.items() if len(v) > 1}
    assert duplicados, "a massa nao tem clientes duplicados"

    silver = ler(spark, "silver", "clientes")
    quarentena = ler(spark, "quarantine", "clientes")
    hashes_rejeitados = {
        linha["hash_linha"] for linha in quarentena.select("hash_linha").collect()
    }

    bronze = ler(spark, "bronze", "clientes")
    hash_por_cpf = {
        linha["cpf"]: linha["hash_linha"]
        for linha in bronze.select("cpf", "hash_linha").collect()
    }

    conferidos = 0
    for id_cliente, versoes in duplicados.items():
        aprovadas = [
            versao for versao in versoes
            if hash_por_cpf.get(versao["cpf"]) not in hashes_rejeitados
        ]
        if not aprovadas:
            continue  # todas as versoes foram rejeitadas

        esperada = max(aprovadas, key=lambda linha: linha["data_atualizacao"])
        na_silver = silver.filter(F.col("id_cliente") == id_cliente).collect()

        assert na_silver, f"{id_cliente} tinha versao aprovada e sumiu da silver"
        assert na_silver[0]["cidade"] == esperada["cidade"], (
            f"{id_cliente}: a silver nao ficou com a versao valida mais recente"
        )
        conferidos += 1

    assert conferidos > 0, "nenhum cliente duplicado chegou a silver"


def test_transacoes_duplicadas_foram_removidas(spark, pipeline):
    bronze = ler(spark, "bronze", "transacoes").count()
    silver = ler(spark, "silver", "transacoes").count()
    quarentena = ler(spark, "quarantine", "transacoes").count()

    assert bronze - quarentena - silver == total("duplicadas")


# ---------------------------------------------------------------------------
# Estornos
# ---------------------------------------------------------------------------

def test_transacoes_estornadas_sao_marcadas(spark, pipeline):
    transacoes = ler(spark, "silver", "transacoes")
    estornos = ler(spark, "silver", "estornos")

    ids_estornados = {linha["id_transacao"] for linha in estornos.collect()}
    marcadas = transacoes.filter(F.col("estornada")).collect()

    assert marcadas, "nenhuma transacao marcada como estornada"
    for linha in marcadas:
        assert linha["id_transacao"] in ids_estornados


def test_valor_liquido_zera_nas_estornadas(spark, pipeline):

    transacoes = ler(spark, "silver", "transacoes")

    estornadas = transacoes.filter(F.col("estornada"))
    assert estornadas.filter(F.col("valor_liquido") != 0).count() == 0

    normais = transacoes.filter(~F.col("estornada"))
    assert normais.filter(F.col("valor_liquido") != F.col("valor")).count() == 0


def test_estorno_orfao_nao_marca_nenhuma_transacao(spark, pipeline):

    quarentena = ler(spark, "quarantine", "estornos")
    transacoes = ler(spark, "silver", "transacoes")

    ids_orfaos = {linha["id_transacao"] for linha in quarentena.collect()}
    marcadas = {
        linha["id_transacao"]
        for linha in transacoes.filter(F.col("estornada")).collect()
    }

    assert not (ids_orfaos & marcadas)


# ---------------------------------------------------------------------------
# Conservacao e idempotencia
# ---------------------------------------------------------------------------

def test_nenhuma_linha_e_criada_do_nada(spark, pipeline):
    """Silver mais quarentena nunca excede a entrada."""
    for entidade in ("clientes", "contas", "cartoes", "transacoes", "estornos"):
        bronze = ler(spark, "bronze", entidade).count()
        silver = ler(spark, "silver", entidade).count()
        quarentena = ler(spark, "quarantine", entidade).count()

        assert silver + quarentena <= bronze, (
            f"{entidade}: {silver} + {quarentena} > {bronze}"
        )


def test_merge_e_idempotente(spark, pipeline):
    """Processar duas vezes o mesmo dado produz o mesmo resultado."""
    antes = ler(spark, "silver", "clientes").count()
    processar_entidade("clientes")
    depois = ler(spark, "silver", "clientes").count()

    assert antes == depois


def test_silver_preserva_os_metadados_da_bronze(spark, pipeline):
    df = ler(spark, "silver", "transacoes")
    for coluna in ("arquivo_origem", "data_referencia", "hash_linha", "config_version"):
        assert coluna in df.columns
        assert df.filter(F.col(coluna).isNull()).count() == 0


def test_tipagem_foi_aplicada(spark, pipeline):
    """A bronze guarda tudo como string; a silver entrega os tipos do YAML."""
    df = ler(spark, "silver", "transacoes")
    tipos = {campo.name: campo.dataType.simpleString() for campo in df.schema.fields}

    assert tipos["valor"] == "decimal(18,2)"
    assert tipos["data_transacao"] == "timestamp"
    assert tipos["id_transacao"] == "string"


def test_quarentena_preserva_o_valor_original(spark, pipeline):

    df = ler(spark, "quarantine", "clientes")
    assert "cpf_original" in df.columns

    com_digito_errado = df.filter(
        F.array_contains("motivos_erro", "cpf_digito_verificador_valido")
        & ~F.array_contains("motivos_erro", "cpf_formato_valido")
    )
    assert com_digito_errado.count() == DEFEITOS["clientes_cpf_invalido"]
    assert com_digito_errado.filter(F.col("cpf_original").isNull()).count() == 0