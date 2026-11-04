"""Configuração central do pipeline."""

import os
from functools import lru_cache
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

CONF_PATH = "conf/pipeline.yml"
SEVERIDADES = ("critica", "alerta")

PYTHON_MIN = (3, 8)
PYTHON_MAX = (3, 11)


def _raiz_repo() -> Path:
    """Raiz do repositório a partir da posição deste arquivo."""
    return Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Configuração declarativa
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def load_conf(path=None) -> dict:
    """Lê conf/pipeline.yml."""
    conf_path = Path(path) if path else _raiz_repo() / CONF_PATH
    if not conf_path.exists():
        raise FileNotFoundError(f"configuracao nao encontrada em {conf_path}")

    with open(conf_path, encoding="utf-8") as arquivo:
        conf = yaml.safe_load(arquivo)

    _validar(conf)
    return conf


def _validar(conf: dict) -> None:
    """Falha na carga se a configuração estiver incompleta."""
    for chave in ("config_version", "projeto", "camadas", "entidades", "ingestao"):
        if chave not in conf:
            raise ValueError(f"configuracao sem a chave obrigatoria {chave!r}")

    for nome, entidade in conf["entidades"].items():
        if not entidade.get("chave_negocio"):
            raise ValueError(f"entidade {nome!r} sem chave_negocio")
        if not entidade.get("tipos"):
            raise ValueError(f"entidade {nome!r} sem tipos")

        for regra in entidade.get("regras_qualidade", []):
            if regra.get("severidade") not in SEVERIDADES:
                raise ValueError(
                    f"entidade {nome!r}, regra {regra.get('nome')!r}: "
                    f"severidade deve ser uma de {SEVERIDADES}"
                )


def get_entidade(nome: str, conf=None) -> dict:
    """Bloco de configuração de uma entidade."""
    conf = conf or load_conf()
    if nome not in conf["entidades"]:
        raise ValueError(
            f"entidade {nome!r} nao configurada. "
            f"Disponiveis: {sorted(conf['entidades'])}"
        )
    return conf["entidades"][nome]


def get_config_version(conf=None) -> str:
    """Versão da configuração, gravada como metadado nas tabelas."""
    return (conf or load_conf())["config_version"]


def get_camadas(conf=None) -> tuple:
    """Camadas declaradas na configuração."""
    return tuple((conf or load_conf())["camadas"])


# ---------------------------------------------------------------------------
# Ambiente e caminhos
# ---------------------------------------------------------------------------

def get_env() -> str:
    """Retorna 'databricks' ou 'local'."""
    override = os.environ.get("PIPELINE_ENV")
    if override:
        return override.strip().lower()
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return "databricks"
    return "local"


def get_data_root() -> str:
    """Raiz dos dados no ambiente atual."""
    override = os.environ.get("PIPELINE_DATA_ROOT")
    if override:
        return override
    if get_env() == "databricks":
        return "/tmp/novarota"
    return str(_raiz_repo() / "data")


def resolve_target(camada: str, tabela: str) -> str:
    """Traduz um destino lógico (camada, tabela) no caminho físico do ambiente."""
    if camada not in get_camadas():
        raise ValueError(f"camada invalida: {camada!r}. Esperado: {get_camadas()}")
    return f"{get_data_root()}/{camada}/{tabela}"


def resolve_source(arquivo: str) -> str:
    """Caminho de um arquivo de entrada na landing."""
    return f"{get_data_root()}/landing/{arquivo}"


# ---------------------------------------------------------------------------
# Sessao Spark
# ---------------------------------------------------------------------------

def _checar_python() -> None:
    """Interrompe cedo se a versão do Python não servir para o PySpark 3.5."""
    import sys

    atual = sys.version_info[:2]
    if not (PYTHON_MIN <= atual <= PYTHON_MAX):
        raise RuntimeError(
            f"Python {atual[0]}.{atual[1]} nao e compativel com PySpark 3.5. "
            f"Use entre {PYTHON_MIN[0]}.{PYTHON_MIN[1]} e "
            f"{PYTHON_MAX[0]}.{PYTHON_MAX[1]}."
        )


def get_spark() -> SparkSession:
    """Devolve a SparkSession do ambiente."""
    sessao = SparkSession.getActiveSession()
    if sessao is not None:
        return sessao

    _checar_python()

    from delta import configure_spark_with_delta_pip

    conf = load_conf()
    builder = (
        SparkSession.builder.appName(conf["projeto"]["nome"])
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", conf["projeto"]["timezone"])
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark