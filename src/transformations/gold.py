"""Camada ouro: motor que executa as consultas SQL de sql/."""

from pathlib import Path

from src.config import get_spark, load_conf, resolve_target
from src.utils.logging import etapa, get_logger

logger = get_logger(__name__)

# Ordem de execucao.
TABELAS = [
    "gold_transacoes_cartao",
    "gold_resumo_cliente_mes",
    "gold_ranking_estabelecimento",
    "gold_alertas_qualidade",
]


def _raiz_sql() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "sql"


def registrar_views() -> None:
    """Registra as tabelas das camadas anteriores como views temporárias."""
    spark = get_spark()
    registradas = 0

    for entidade in load_conf()["entidades"]:
        for camada in ("silver", "quarantine"):
            (
                spark.read.format("delta")
                .load(resolve_target(camada, entidade))
                .createOrReplaceTempView(f"{camada}_{entidade}")
            )
            registradas += 1

    logger.debug("%d view(s) de silver/quarentena registrada(s)", registradas)


def registrar_views_gold() -> None:
    """Registra as tabelas da ouro já materializadas como views."""
    spark = get_spark()

    for tabela in TABELAS:
        (
            spark.read.format("delta")
            .load(resolve_target("gold", tabela))
            .createOrReplaceTempView(tabela)
        )

    logger.debug("%d view(s) da ouro registrada(s)", len(TABELAS))


def executar(tabela: str) -> int:
    """Executa a consulta de uma tabela e materializa o resultado."""
    spark = get_spark()
    arquivo = _raiz_sql() / f"{tabela}.sql"

    with etapa(logger, f"gold.{tabela}"):
        consulta = arquivo.read_text(encoding="utf-8")
        resultado = spark.sql(consulta)
        destino = resolve_target("gold", tabela)

        (
            resultado.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(destino)
        )

        gravada = spark.read.format("delta").load(destino)
        gravada.createOrReplaceTempView(tabela)
        linhas = gravada.count()

    logger.info("gold.%s | %d linhas | fonte=%s", tabela, linhas, arquivo.name)
    return linhas


def construir_tudo() -> dict:
    """Constrói as quatro tabelas da ouro, na ordem de dependência."""
    resultado = {}

    with etapa(logger, "camada ouro"):
        registrar_views()
        for tabela in TABELAS:
            resultado[tabela] = executar(tabela)

    logger.info("ouro concluida | %d tabela(s) materializada(s)", len(resultado))
    return resultado