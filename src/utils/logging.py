"""Configuração de log do pipeline."""

import logging
import os
import sys
import time
from contextlib import contextmanager

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)-34s | %(message)s"
FORMATO_DATA = "%Y-%m-%d %H:%M:%S"
NIVEL_PADRAO = "INFO"

_configurado = False


def configurar(nivel: str = None) -> None:
    """Configura o log raiz do pipeline."""
    global _configurado
    if _configurado:
        return

    nivel_efetivo = (nivel or os.environ.get("LOG_LEVEL", NIVEL_PADRAO)).upper()

    manipulador = logging.StreamHandler(sys.stdout)
    manipulador.setFormatter(logging.Formatter(FORMATO, datefmt=FORMATO_DATA))

    raiz = logging.getLogger()
    raiz.handlers.clear()
    raiz.addHandler(manipulador)
    raiz.setLevel(nivel_efetivo)

    logging.getLogger("py4j").setLevel(logging.WARNING)

    _configurado = True


def get_logger(nome: str) -> logging.Logger:
    """Devolve o logger de um módulo, configurando o log raiz na primeira vez."""
    configurar()
    return logging.getLogger(nome)


@contextmanager
def etapa(logger: logging.Logger, descricao: str):
    """Marca início, fim e duração de uma etapa."""
    logger.info("inicio: %s", descricao)
    comeco = time.perf_counter()
    try:
        yield
    except Exception:
        duracao = time.perf_counter() - comeco
        logger.exception("FALHA: %s apos %.1fs", descricao, duracao)
        raise
    else:
        duracao = time.perf_counter() - comeco
        logger.info("fim: %s em %.1fs", descricao, duracao)