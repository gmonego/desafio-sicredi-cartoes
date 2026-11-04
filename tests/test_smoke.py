"""
Teste de fumaca do ambiente.
Não valida regra de negocio, valida que a fundação esta de pé: PySpark
operante, Delta habilitado, pytest importando o pacote src e configuração
funcional.
"""

import os

import pytest

from src.config import resolve_target

def test_spark_session_disponivel(spark):
    assert spark is not None
    assert spark.sparkContext.appName == "pipeline_cartoes_novarota"

def test_delta_habilitado_na_sessao(spark):
    """Verifica a configuração da sessão, e não por tentativa de escrita."""
    assert "DeltaSparkSessionExtension" in spark.conf.get("spark.sql.extensions", "")

def test_escreve_e_le_tabela_delta(spark, tmp_path):
    """Ciclo completo de escrita e leitura em Delta."""
    destino = str(tmp_path / "tabela_fumaca")

    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "letra"])
    df.write.format("delta").mode("overwrite").save(destino)

    lido = spark.read.format("delta").load(destino)

    assert lido.count() == 3
    assert set(lido.columns) == {"id", "letra"}
    assert os.path.isdir(os.path.join(destino, "_delta_log"))

def test_resolve_target_monta_caminho_da_camada():
    assert resolve_target("bronze", "transacoes").endswith("/bronze/transacoes")

def test_resolve_target_rejeita_camada_invalida():
    with pytest.raises(ValueError):
        resolve_target("camada_inexistente", "qualquer")