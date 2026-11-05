"""Camada silver: tipagem, qualidade, deduplicacao e integridade referencial.

A silver transforma a copia bruta da bronze em dado confiavel. Cinco etapas,
nesta ordem:

  1. tipagem, preservando o valor original quando a conversão falha
  2. validação das regras declaradas no YAML, por severidade
  3. integridade referencial contra as entidades já processadas
  4. deduplicacao por chave de negocio, com desempate estável
  5. marcação de estornos (apenas transações)
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from src.config import get_entidade, get_spark, resolve_target
from src.quality.rules import (
    COLUNA_ALERTAS,
    MOTIVO_CAST,
    MOTIVO_FK,
    acrescentar_motivo,
    aplicar_regras,
    iniciar_motivos,
    separar_validos_e_rejeitados,
)
from src.utils.logging import etapa, get_logger

logger = get_logger(__name__)

# Sufixo das colunas que guardam o valor como veio da origem.
SUFIXO_ORIGINAL = "_original"

# Ordem de processamento: uma entidade só pode ter a integridade referencial
# validada depois que a entidade referenciada ja existe na silver.
ORDEM_PROCESSAMENTO = ["clientes", "contas", "cartoes", "transacoes", "estornos"]

METADADOS_BRONZE = [
    "arquivo_origem",
    "data_referencia",
    "timestamp_ingestao",
    "hash_linha",
    "config_version",
]


# ---------------------------------------------------------------------------
# Tipagem
# ---------------------------------------------------------------------------

def tipar(df: DataFrame, tipos: dict) -> DataFrame:
    """Converte as colunas para os tipos declarados, preservando o original."""
    resultado = df
    for coluna, tipo in tipos.items():
        resultado = (
            resultado
            .withColumn(f"{coluna}{SUFIXO_ORIGINAL}", F.col(coluna))
            .withColumn(coluna, F.col(coluna).cast(tipo))
        )
    return resultado


def marcar_falhas_de_cast(df: DataFrame, tipos: dict) -> DataFrame:
    """Marca as linhas em que a conversão falhou."""
    resultado = df
    for coluna in tipos:
        original = F.col(f"{coluna}{SUFIXO_ORIGINAL}")
        falhou = (
            original.isNotNull()
            & (F.trim(original) != "")
            & F.col(coluna).isNull()
        )
        resultado = acrescentar_motivo(resultado, falhou, f"{MOTIVO_CAST}_{coluna}")
    return resultado


# ---------------------------------------------------------------------------
# Deduplicacao
# ---------------------------------------------------------------------------

def deduplicar(df: DataFrame, chave: list, coluna_versao: str) -> DataFrame:
    """Mantem uma linha por chave de negocio, a versão mais recente."""
    janela = Window.partitionBy(*chave).orderBy(
        F.col(coluna_versao).desc_nulls_last(),
        F.col("hash_linha").asc(),
    )
    return (
        df
        .withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .drop("_ordem")
    )


# ---------------------------------------------------------------------------
# Integridade referencial
# ---------------------------------------------------------------------------

def marcar_fk_invalida(df: DataFrame, fks: list) -> DataFrame:
    """Marca as linhas cuja chave estrangeira não existe na entidade referenciada."""
    spark = get_spark()
    resultado = df

    for fk in fks:
        chaves_validas = (
            spark.read.format("delta")
            .load(resolve_target("silver", fk["referencia"]))
            .select(F.col(fk["coluna_referencia"]).alias("_chave_valida"))
            .unionByName(
                spark.read.format("delta")
                .load(resolve_target("quarantine", fk["referencia"]))
                .select(F.col(fk["coluna_referencia"]).alias("_chave_valida"))
            )
            .distinct()
        )

        resultado = resultado.join(
            chaves_validas,
            resultado[fk["coluna"]] == chaves_validas["_chave_valida"],
            "left",
        )
        orfa = F.col(fk["coluna"]).isNotNull() & F.col("_chave_valida").isNull()
        resultado = acrescentar_motivo(
            resultado, orfa, f"{MOTIVO_FK}_{fk['coluna']}"
        ).drop("_chave_valida")

        logger.debug(
            "integridade referencial: %s -> %s",
            fk["coluna"], fk["referencia"],
        )

    return resultado


# ---------------------------------------------------------------------------
# Estornos
# ---------------------------------------------------------------------------

def marcar_estornos(df: DataFrame) -> DataFrame:
    """Acrescenta as colunas de estorno às transações."""
    spark = get_spark()
    estornos = spark.read.format("delta").load(resolve_target("bronze", "estornos"))

    estornadas = estornos.select(
        F.col("id_transacao").alias("_id_estornada")
    ).distinct()

    return (
        df
        .join(estornadas, df["id_transacao"] == estornadas["_id_estornada"], "left")
        .withColumn("estornada", F.col("_id_estornada").isNotNull())
        .withColumn(
            "valor_liquido",
            F.when(F.col("_id_estornada").isNotNull(), F.lit(0).cast("decimal(18,2)"))
            .otherwise(F.col("valor")),
        )
        .drop("_id_estornada")
    )


# ---------------------------------------------------------------------------
# Gravacao
# ---------------------------------------------------------------------------

def gravar_merge(df: DataFrame, entidade: str, chave: list) -> None:
    """Grava na silver com MERGE por chave de negocio."""
    destino = resolve_target("silver", entidade)
    spark = get_spark()

    if not DeltaTable.isDeltaTable(spark, destino):
        logger.debug("%s: primeira carga, gravacao direta", entidade)
        df.write.format("delta").save(destino)
        return

    condicao = " AND ".join(f"destino.{coluna} = origem.{coluna}" for coluna in chave)
    logger.debug("%s: MERGE ON %s", entidade, condicao)

    (
        DeltaTable.forPath(spark, destino).alias("destino")
        .merge(df.alias("origem"), condicao)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def gravar_quarentena(df: DataFrame, entidade: str) -> None:
    """Grava os registros rejeitados, com motivo e valores originais."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(resolve_target("quarantine", entidade))
    )


# ---------------------------------------------------------------------------
# Orquestracao
# ---------------------------------------------------------------------------

def colunas_finais(tipos: dict, extras=None) -> list:
    """Colunas que ficam na silver: campos tipados, metadados e alertas."""
    return list(tipos) + METADADOS_BRONZE + [COLUNA_ALERTAS] + list(extras or [])


def processar_entidade(entidade: str) -> dict:
    """Processa uma entidade da bronze para a silver."""
    spark = get_spark()
    conf_entidade = get_entidade(entidade)
    tipos = conf_entidade["tipos"]
    chave = conf_entidade["chave_negocio"]

    with etapa(logger, f"silver.{entidade}"):
        bronze = spark.read.format("delta").load(resolve_target("bronze", entidade))
        total_entrada = bronze.count()

        # tipagem e falhas de conversao
        df = iniciar_motivos(bronze)
        df = tipar(df, tipos)
        df = marcar_falhas_de_cast(df, tipos)

        # regras do YAML
        regras = conf_entidade.get("regras_qualidade", [])
        df = aplicar_regras(df, regras)
        logger.debug("%s: %d regra(s) aplicada(s)", entidade, len(regras))

        # integridade referencial
        fks = conf_entidade.get("integridade_referencial", [])
        if fks:
            df = marcar_fk_invalida(df, fks)

        validos, rejeitados = separar_validos_e_rejeitados(df)

        # deduplicacao
        antes_dedup = validos.count()
        validos = deduplicar(validos, chave, conf_entidade["coluna_versao"])
        depois_dedup = validos.count()

        # estornos
        extras = []
        if entidade == "transacoes":
            validos = marcar_estornos(validos)
            extras = ["estornada", "valor_liquido"]

        validos = validos.select(*colunas_finais(tipos, extras))
        gravar_merge(validos, entidade, chave)
        gravar_quarentena(rejeitados, entidade)

        total_rejeitados = rejeitados.count()

    logger.info(
        "silver.%s | entrada=%d | aprovados=%d | quarentena=%d | duplicatas_removidas=%d",
        entidade, total_entrada, depois_dedup, total_rejeitados,
        antes_dedup - depois_dedup,
    )

    return {
        "entrada": total_entrada,
        "aprovados": depois_dedup,
        "rejeitados": total_rejeitados,
        "duplicatas_removidas": antes_dedup - depois_dedup,
    }


def processar_tudo() -> dict:
    """Processa todas as entidades na ordem que a integridade referencial exige."""
    resultado = {}

    with etapa(logger, "camada silver"):
        for entidade in ORDEM_PROCESSAMENTO:
            resultado[entidade] = processar_entidade(entidade)

    total_rejeitados = sum(c["rejeitados"] for c in resultado.values())
    logger.info("silver concluida | %d registro(s) em quarentena", total_rejeitados)

    return resultado