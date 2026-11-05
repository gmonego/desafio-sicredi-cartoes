"""Regras de qualidade: aplicação das expressões declaradas em conf/pipeline.yml."""

import re

from pyspark.sql import DataFrame, functions as F

COLUNA_MOTIVOS = "motivos_erro"
COLUNA_ALERTAS = "alertas"

# Prefixo dos motivos gerados fora das regras do YAML.
MOTIVO_CAST = "cast_invalido"
MOTIVO_FK = "fk_invalida"


def array_vazio():
    """Array de strings vazio, com o tipo declarado."""
    return F.array().cast("array<string>")

PADRAO_CHAMADA = re.compile(r"(\w+)\(\s*([A-Za-z_]\w*)\s*\)")


def _digito_cpf(coluna: str, qtd_digitos: int) -> str:
    """Expressão SQL de um dígito verificador de CPF."""
    peso_inicial = qtd_digitos + 1
    soma = " + ".join(
        f"CAST(substring({coluna}, {posicao}, 1) AS INT) * {peso_inicial - posicao + 1}"
        for posicao in range(1, qtd_digitos + 1)
    )
    return f"CASE WHEN ({soma}) % 11 < 2 THEN 0 ELSE 11 - (({soma}) % 11) END"


def cpf_valido(coluna: str) -> str:
    """Expressão SQL que valida um CPF completo."""
    return (
        f"({coluna} IS NOT NULL"
        f" AND {coluna} RLIKE '^[0-9]{{11}}$'"
        f" AND {coluna} NOT RLIKE '^([0-9])\\\\1{{10}}$'"
        f" AND CAST(substring({coluna}, 10, 1) AS INT) = ({_digito_cpf(coluna, 9)})"
        f" AND CAST(substring({coluna}, 11, 1) AS INT) = ({_digito_cpf(coluna, 10)}))"
    )

EXPRESSOES_NOMEADAS = {
    "cpf_valido": cpf_valido,
}


def expandir(expressao: str) -> str:
    """Substitui as chamadas nomeadas pela expressão SQL correspondente."""
    def substituir(encontrado):
        nome, argumento = encontrado.group(1), encontrado.group(2)
        if nome in EXPRESSOES_NOMEADAS:
            return EXPRESSOES_NOMEADAS[nome](argumento)
        return encontrado.group(0)

    return PADRAO_CHAMADA.sub(substituir, expressao)


# ---------------------------------------------------------------------------
# Aplicacao das regras
# ---------------------------------------------------------------------------

def _coluna_motivos(regras: list, severidade: str):
    """Monta um array com os nomes das regras violadas de uma severidade."""
    elementos = [
        F.when(
            ~F.coalesce(F.expr(expandir(regra["expressao"])), F.lit(False)),
            F.lit(regra["nome"]),
        )
        for regra in regras
        if regra["severidade"] == severidade
    ]
    if not elementos:
        return array_vazio()
    return F.array_compact(F.array(*elementos))


def aplicar_regras(df: DataFrame, regras: list) -> DataFrame:
    """Acrescenta as colunas de motivos críticos e de alertas."""
    criticos = _coluna_motivos(regras, "critica")
    alertas = _coluna_motivos(regras, "alerta")

    return (
        df
        .withColumn(
            COLUNA_MOTIVOS,
            F.array_union(F.coalesce(F.col(COLUNA_MOTIVOS), array_vazio()), criticos),
        )
        .withColumn(COLUNA_ALERTAS, alertas)
    )


def acrescentar_motivo(df: DataFrame, condicao, motivo: str) -> DataFrame:
    """Acrescenta um motivo de rejeição às linhas que satisfazem a condição."""
    return df.withColumn(
        COLUNA_MOTIVOS,
        F.when(
            condicao,
            F.array_union(
                F.coalesce(F.col(COLUNA_MOTIVOS), array_vazio()),
                F.array(F.lit(motivo)),
            ),
        ).otherwise(F.coalesce(F.col(COLUNA_MOTIVOS), array_vazio())),
    )


def iniciar_motivos(df: DataFrame) -> DataFrame:
    """Cria a coluna de motivos vazia, antes de qualquer validação."""
    return df.withColumn(COLUNA_MOTIVOS, array_vazio())


def separar_validos_e_rejeitados(df: DataFrame):
    """Divide o DataFrame entre linhas aprovadas e rejeitadas."""
    tem_erro = F.size(F.col(COLUNA_MOTIVOS)) > 0
    return df.filter(~tem_erro), df.filter(tem_erro)