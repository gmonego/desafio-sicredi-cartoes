"""Testes da camada bronze.

Cobrem o que a bronze promete, sendo copia fiel da origem, metadados preenchidos,
data de referência vinda do nome do arquivo e reprocessamento que substitui
apenas a partição reprocessada.
"""

import pytest

from scripts.gerar_massa import DEFEITOS, DIAS, gerar_massa
from src.config import get_entidade
from src.ingestion.bronze import (
    adicionar_metadados,
    extrair_data_referencia,
    ingerir_entidade,
    ler_csv_como_texto,
    listar_arquivos,
)

METADADOS = [
    "arquivo_origem",
    "data_referencia",
    "timestamp_ingestao",
    "hash_linha",
    "config_version",
]


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    """Massa gerada uma vez para todos os testes deste arquivo."""
    destino = tmp_path_factory.mktemp("landing")
    return gerar_massa(str(destino))


@pytest.fixture
def bronze(monkeypatch, tmp_path, landing):
    """Aponta a raíz de dados para um diretório temporário."""
    monkeypatch.setenv("PIPELINE_DATA_ROOT", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# Data de referencia
# ---------------------------------------------------------------------------

def test_extrai_data_do_nome_do_arquivo():
    assert extrair_data_referencia("transacoes_cartao_20261001.csv") == "2026-10-01"


def test_nome_sem_data_levanta_erro():
    with pytest.raises(ValueError, match="data de referencia"):
        extrair_data_referencia("clientes.csv")


# ---------------------------------------------------------------------------
# Listagem de arquivos
# ---------------------------------------------------------------------------

def test_entidade_de_cadastro_tem_um_arquivo(landing):
    assert len(listar_arquivos("clientes", landing)) == 1


def test_entidade_particionada_tem_um_arquivo_por_dia(landing):
    assert len(listar_arquivos("transacoes", landing)) == DIAS


def test_arquivo_inexistente_levanta_erro(tmp_path):
    with pytest.raises(FileNotFoundError):
        listar_arquivos("clientes", tmp_path)


# ---------------------------------------------------------------------------
# Leitura
# ---------------------------------------------------------------------------

def test_le_todas_as_colunas_como_string(spark, landing):

    colunas = list(get_entidade("transacoes")["tipos"])
    df = ler_csv_como_texto(listar_arquivos("transacoes", landing)[0], colunas)

    tipos = {campo.name: campo.dataType.simpleString() for campo in df.schema.fields}
    assert set(tipos.values()) == {"string"}


def test_leitura_preserva_a_contagem_do_arquivo(spark, landing):
    import csv

    arquivo = listar_arquivos("clientes", landing)[0]
    with open(arquivo, encoding="utf-8", newline="") as f:
        esperado = len(list(csv.DictReader(f)))

    colunas = list(get_entidade("clientes")["tipos"])
    assert ler_csv_como_texto(arquivo, colunas).count() == esperado


# ---------------------------------------------------------------------------
# Metadados
# ---------------------------------------------------------------------------

def test_metadados_sao_adicionados(spark, landing):
    colunas = list(get_entidade("clientes")["tipos"])
    arquivo = listar_arquivos("clientes", landing)[0]

    df = ler_csv_como_texto(arquivo, colunas)
    com_metadados = adicionar_metadados(df, arquivo.name, "2026-10-01", colunas)

    for coluna in METADADOS:
        assert coluna in com_metadados.columns, f"{coluna} ausente"


def test_nenhum_metadado_vem_nulo(spark, landing):
    colunas = list(get_entidade("clientes")["tipos"])
    arquivo = listar_arquivos("clientes", landing)[0]

    df = adicionar_metadados(
        ler_csv_como_texto(arquivo, colunas), arquivo.name, "2026-10-01", colunas
    )

    for coluna in METADADOS:
        assert df.filter(f"{coluna} IS NULL").count() == 0, f"{coluna} com nulo"


def test_hash_e_estavel_entre_execucoes(spark, landing):

    colunas = list(get_entidade("clientes")["tipos"])
    arquivo = listar_arquivos("clientes", landing)[0]

    primeira = adicionar_metadados(
        ler_csv_como_texto(arquivo, colunas), arquivo.name, "2026-10-01", colunas
    ).select("id_cliente", "hash_linha").collect()

    segunda = adicionar_metadados(
        ler_csv_como_texto(arquivo, colunas), arquivo.name, "2026-10-01", colunas
    ).select("id_cliente", "hash_linha").collect()

    assert sorted(primeira) == sorted(segunda)


def test_linhas_diferentes_tem_hash_diferente(spark, landing):
    colunas = list(get_entidade("clientes")["tipos"])
    arquivo = listar_arquivos("clientes", landing)[0]

    df = adicionar_metadados(
        ler_csv_como_texto(arquivo, colunas), arquivo.name, "2026-10-01", colunas
    )

    assert df.select("hash_linha").distinct().count() == df.count()


# ---------------------------------------------------------------------------
# Ingestao completa
# ---------------------------------------------------------------------------

def test_ingere_cadastro_e_le_de_volta(spark, bronze, landing):
    total = ingerir_entidade("clientes", landing)

    df = spark.read.format("delta").load(str(bronze / "bronze" / "clientes"))
    assert df.count() == total
    assert df.count() > DEFEITOS["clientes_duplicados"]


def test_ingere_transacoes_de_todas_as_cargas(spark, bronze, landing):
    from scripts.gerar_massa import DATAS_CARGA

    ingerir_entidade("transacoes", landing)

    df = spark.read.format("delta").load(str(bronze / "bronze" / "transacoes"))
    datas = {
        linha["data_referencia"]
        for linha in df.select("data_referencia").collect()
    }

    esperadas = {data.strftime("%Y-%m-%d") for data in DATAS_CARGA}
    assert datas == esperadas


def test_transacao_atrasada_fica_na_particao_do_arquivo(spark, bronze, landing):
    """Uma transação ocorrida no dia anterior fica na partição do arquivo em
    que chegou, e não na do dia em que ocorreu.
    """
    ingerir_entidade("transacoes", landing)
    df = spark.read.format("delta").load(str(bronze / "bronze" / "transacoes"))

    atrasadas = df.filter("substring(data_transacao, 1, 10) < data_referencia")
    assert atrasadas.count() > 0

    for linha in atrasadas.limit(5).collect():
        assert linha["data_transacao"][:10] < linha["data_referencia"]


# ---------------------------------------------------------------------------
# Reprocessamento
# ---------------------------------------------------------------------------

def test_reprocessar_nao_duplica(spark, bronze, landing):
    """Ingerir duas vezes produz o mesmo resultado."""
    ingerir_entidade("transacoes", landing)
    caminho = str(bronze / "bronze" / "transacoes")
    primeira = spark.read.format("delta").load(caminho).count()

    ingerir_entidade("transacoes", landing)
    segunda = spark.read.format("delta").load(caminho).count()

    assert primeira == segunda


def test_reprocessar_uma_carga_nao_afeta_as_outras(spark, bronze, landing):
    """O predicado filtra por data_referencia, então as demais partições não são tocadas."""
    from src.ingestion.bronze import (
        adicionar_metadados as add_meta,
        extrair_data_referencia,
        gravar,
        ler_csv_como_texto as ler,
    )

    ingerir_entidade("transacoes", landing)
    caminho = str(bronze / "bronze" / "transacoes")

    def contagens():
        df = spark.read.format("delta").load(caminho)
        return {
            linha["data_referencia"]: linha["qtd"]
            for linha in df.groupBy("data_referencia").count()
            .withColumnRenamed("count", "qtd").collect()
        }

    antes = contagens()

    # Reprocessa apenas a carga do meio.
    colunas = list(get_entidade("transacoes")["tipos"])
    arquivo = listar_arquivos("transacoes", landing)[1]
    data_referencia = extrair_data_referencia(arquivo.name)
    df = add_meta(ler(arquivo, colunas), arquivo.name, data_referencia, colunas)
    gravar(df, "transacoes", data_referencia)

    assert antes == contagens(), "o reprocessamento alterou particoes alheias"


def test_ingestao_grava_a_versao_da_configuracao(spark, bronze, landing):
    """Permite saber sob qual conjunto de regras cada linha foi processada."""
    from src.config import get_config_version

    ingerir_entidade("estornos", landing)
    df = spark.read.format("delta").load(str(bronze / "bronze" / "estornos"))

    versoes = {linha["config_version"] for linha in df.select("config_version").collect()}
    assert versoes == {get_config_version()}