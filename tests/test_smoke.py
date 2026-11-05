"""Teste de fumaca do ambiente."""

import pytest

from src.config import get_env, resolve_target


def test_spark_session_disponivel(spark):
    """Existe sessão e ela responde."""
    assert spark is not None
    assert spark.conf.get("spark.app.name")


def test_timezone_fixada_em_utc(spark):
    """Timezone decide o resultado de current_date e dos casts de timestamp."""
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


@pytest.mark.skipif(
    get_env() == "databricks",
    reason="no Databricks o Delta e nativo do runtime, nao carregado por extensao",
)
def test_delta_habilitado_na_sessao_local(spark):
    """No ambiente local, o Delta vem de uma extensão que precisa estar ativa."""
    assert "DeltaSparkSessionExtension" in spark.conf.get("spark.sql.extensions", "")


def test_escreve_e_le_tabela_delta(spark, tmp_path):
    """Ciclo completo de escrita e leitura em Delta."""
    destino = str(tmp_path / "tabela_fumaca")

    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "letra"])
    df.write.format("delta").mode("overwrite").save(destino)

    lido = spark.read.format("delta").load(destino)

    assert lido.count() == 3
    assert set(lido.columns) == {"id", "letra"}


def test_resolve_target_monta_caminho_da_camada():
    assert resolve_target("bronze", "transacoes").endswith("/bronze/transacoes")


def test_resolve_target_rejeita_camada_invalida():
    with pytest.raises(ValueError):
        resolve_target("camada_inexistente", "qualquer")