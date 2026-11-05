"""Executa as consultas analíticas e grava as saídas como evidência."""

import argparse
from pathlib import Path

from src.config import get_spark
from src.transformations.gold import registrar_views, registrar_views_gold
from src.utils.logging import etapa, get_logger

logger = get_logger(__name__)

LINHAS_PADRAO = 20


def _raiz_projeto() -> Path:
    return Path(__file__).resolve().parent.parent


def listar_consultas() -> list:
    """Arquivos .sql da pasta de analises, em ordem alfabetica."""
    pasta = _raiz_projeto() / "sql" / "analises"
    return sorted(pasta.glob("*.sql"))


def executar_consulta(arquivo: Path, linhas: int) -> str:
    """Executa uma consulta e devolve o resultado formatado como texto."""
    spark = get_spark()
    consulta = arquivo.read_text(encoding="utf-8")

    resultado = spark.sql(consulta)
    total = resultado.count()
    tabela = resultado._jdf.showString(linhas, 60, False)

    logger.info("%s | %d linha(s)", arquivo.name, total)

    return (
        f"{'=' * 78}\n"
        f"CONSULTA: {arquivo.name}\n"
        f"LINHAS RETORNADAS: {total}\n"
        f"{'=' * 78}\n\n"
        f"{consulta.strip()}\n\n"
        f"{'-' * 78}\n"
        f"RESULTADO (primeiras {min(linhas, total)} linhas)\n"
        f"{'-' * 78}\n"
        f"{tabela}\n"
    )


def executar_todas(linhas: int = LINHAS_PADRAO) -> Path:
    """Executa todas as consultas e grava a evidencia consolidada."""
    destino = _raiz_projeto() / "docs" / "evidencias"
    destino.mkdir(parents=True, exist_ok=True)

    with etapa(logger, "consultas analiticas"):
        registrar_views()
        registrar_views_gold()

        partes = []
        for arquivo in listar_consultas():
            partes.append(executar_consulta(arquivo, linhas))

    saida = destino / "consultas_analiticas.txt"
    saida.write_text("\n\n".join(partes), encoding="utf-8")

    logger.info("evidencia gravada em %s", saida)
    return saida


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa as consultas analiticas")
    parser.add_argument("--linhas", type=int, default=LINHAS_PADRAO)
    executar_todas(parser.parse_args().linhas)