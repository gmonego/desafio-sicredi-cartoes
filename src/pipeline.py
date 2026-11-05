"""Orquestração ponta a ponta do pipeline.

Executa landing -> bronze -> silver -> ouro.
"""

import argparse

from src.ingestion.bronze import ingerir_tudo
from src.transformations.gold import construir_tudo
from src.transformations.silver import processar_tudo
from src.utils.logging import etapa, get_logger

logger = get_logger(__name__)

CAMADAS_PADRAO = ["bronze", "silver", "gold"]


def executar(gerar_massa: bool = False, escala: int = 1,
             camadas: list = None) -> dict:
    """Executa o pipeline e devolve o resumo de cada camada.

    Args:
        gerar_massa: regenera os arquivos da landing antes de ingerir.
        escala: multiplicador de volume da massa, para teste de carga.
        camadas: subconjunto de camadas a executar.
    """
    camadas = camadas or CAMADAS_PADRAO
    resumo = {}

    with etapa(logger, "pipeline completo"):
        if gerar_massa:
            from scripts.gerar_massa import gerar_massa as gerar

            logger.info("gerando massa ficticia (escala=%d)", escala)
            gerar(escala=escala)

        if "bronze" in camadas:
            resumo["bronze"] = ingerir_tudo()

        if "silver" in camadas:
            resumo["silver"] = processar_tudo()

        if "gold" in camadas:
            resumo["gold"] = construir_tudo()

    _resumir(resumo)
    return resumo


def _resumir(resumo: dict) -> None:
    """Imprime o resumo final da execucao."""
    logger.info("=" * 72)
    logger.info("RESUMO DA EXECUCAO")
    logger.info("=" * 72)

    if "bronze" in resumo:
        for entidade, linhas in resumo["bronze"].items():
            logger.info("bronze.%-14s %8d linhas", entidade, linhas)

    if "silver" in resumo:
        for entidade, contagem in resumo["silver"].items():
            logger.info(
                "silver.%-14s %8d aprovados | %5d quarentena | %5d duplicatas",
                entidade,
                contagem["aprovados"],
                contagem["rejeitados"],
                contagem["duplicatas_removidas"],
            )

    if "gold" in resumo:
        for tabela, linhas in resumo["gold"].items():
            logger.info("%-28s %8d linhas", tabela, linhas)

    logger.info("=" * 72)


def _argumentos():
    parser = argparse.ArgumentParser(
        description="Pipeline de movimentacoes de cartao — NovaRota"
    )
    parser.add_argument(
        "--gerar-massa",
        action="store_true",
        help="regenera os arquivos da landing antes de ingerir",
    )
    parser.add_argument(
        "--escala",
        type=int,
        default=1,
        help="multiplicador de volume da massa (teste de carga)",
    )
    parser.add_argument(
        "--camadas",
        nargs="+",
        choices=CAMADAS_PADRAO,
        default=CAMADAS_PADRAO,
        help="camadas a executar (padrao: todas)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _argumentos()
    executar(
        gerar_massa=args.gerar_massa,
        escala=args.escala,
        camadas=args.camadas,
    )