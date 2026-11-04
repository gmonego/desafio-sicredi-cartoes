"""Testes do gerador de massa.

Validam o instrumento de medicao, nao o pipeline. Os testes das camadas vao
afirmar quantos registros foram rejeitados ou deduplicados, e esses numeros
so valem se a massa for sempre a mesma e tiver exatamente os defeitos
declarados em DEFEITOS.
"""

import csv
import random
from pathlib import Path

from scripts.gerar_massa import (
    DEFEITOS,
    DEFEITOS_TRANSACAO,
    DIAS,
    cpf_digito_errado,
    cpf_valido,
    digito_cpf,
    gerar_massa,
)


def ler_csv(caminho: Path) -> list:
    with open(caminho, encoding="utf-8", newline="") as arquivo:
        return list(csv.DictReader(arquivo))


def cpf_ok(cpf: str) -> bool:
    """Confere os dois digitos verificadores."""
    if len(cpf) != 11 or not cpf.isdigit():
        return False
    primeiro = digito_cpf(cpf[:9], 10)
    segundo = digito_cpf(cpf[:9] + primeiro, 11)
    return cpf[9:] == primeiro + segundo


def total(chave: str) -> int:
    """Soma, entre os dias, a quantidade declarada de um defeito de transacao."""
    return sum(DEFEITOS_TRANSACAO[chave])


# ---------------------------------------------------------------------------
# Determinismo
# ---------------------------------------------------------------------------

def test_massa_e_sempre_igual(tmp_path):
    """Duas execucoes geram arquivos identicos.

    E o teste que sustenta todos os outros: sem ele, qualquer contagem exata
    afirmada nos testes das camadas passaria hoje e falharia amanha.
    """
    primeira = gerar_massa(str(tmp_path / "a"))
    segunda = gerar_massa(str(tmp_path / "b"))

    nomes = sorted(p.name for p in primeira.glob("*.csv"))
    assert nomes == sorted(p.name for p in segunda.glob("*.csv"))

    for nome in nomes:
        assert (primeira / nome).read_bytes() == (segunda / nome).read_bytes(), (
            f"{nome} mudou entre execucoes"
        )


# ---------------------------------------------------------------------------
# Arquivos gerados
# ---------------------------------------------------------------------------

def test_gera_todos_os_arquivos(tmp_path):
    raiz = gerar_massa(str(tmp_path))

    for nome in ("clientes.csv", "contas.csv", "cartoes.csv", "estornos.csv"):
        assert (raiz / nome).exists(), f"{nome} nao foi gerado"

    assert len(sorted(raiz.glob("transacoes_cartao_*.csv"))) == DIAS


def test_dias_tem_volumes_diferentes(tmp_path):
    """Volume uniforme esconderia erro de filtro por data na carga incremental."""
    raiz = gerar_massa(str(tmp_path))
    contagens = [len(ler_csv(a)) for a in sorted(raiz.glob("transacoes_cartao_*.csv"))]
    assert len(set(contagens)) > 1, f"todos os dias com o mesmo volume: {contagens}"


# ---------------------------------------------------------------------------
# Defeitos nas quantidades declaradas
# ---------------------------------------------------------------------------

def test_clientes_duplicados(tmp_path):
    clientes = ler_csv(gerar_massa(str(tmp_path)) / "clientes.csv")
    ids = [c["id_cliente"] for c in clientes]
    assert len(ids) - len(set(ids)) == DEFEITOS["clientes_duplicados"]


def test_duplicatas_tem_datas_diferentes(tmp_path):
    """Sem data_atualizacao distinta a deduplicacao nao teria o que desempatar."""
    clientes = ler_csv(gerar_massa(str(tmp_path)) / "clientes.csv")

    por_id = {}
    for cliente in clientes:
        por_id.setdefault(cliente["id_cliente"], []).append(cliente)

    duplicados = {k: v for k, v in por_id.items() if len(v) > 1}
    assert duplicados

    for id_cliente, versoes in duplicados.items():
        datas = {v["data_atualizacao"] for v in versoes}
        assert len(datas) == len(versoes), f"{id_cliente}: versoes com a mesma data"


def test_cpfs_invalidos(tmp_path):
    clientes = ler_csv(gerar_massa(str(tmp_path)) / "clientes.csv")

    malformados = [c for c in clientes if len(c["cpf"]) != 11 or not c["cpf"].isdigit()]
    digito_errado = [
        c for c in clientes
        if len(c["cpf"]) == 11 and c["cpf"].isdigit() and not cpf_ok(c["cpf"])
    ]

    assert len(malformados) == DEFEITOS["clientes_cpf_malformado"]
    assert len(digito_errado) == DEFEITOS["clientes_cpf_invalido"]


def test_nenhum_cpf_invalido_por_acidente(tmp_path):
    """Se houvesse, a contagem de rejeicoes da silver nao bateria."""
    clientes = ler_csv(gerar_massa(str(tmp_path)) / "clientes.csv")
    invalidos = [c for c in clientes if not cpf_ok(c["cpf"])]
    esperado = DEFEITOS["clientes_cpf_invalido"] + DEFEITOS["clientes_cpf_malformado"]
    assert len(invalidos) == esperado


def test_contas_orfas(tmp_path):
    raiz = gerar_massa(str(tmp_path))
    clientes = {c["id_cliente"] for c in ler_csv(raiz / "clientes.csv")}
    orfas = [c for c in ler_csv(raiz / "contas.csv") if c["id_cliente"] not in clientes]
    assert len(orfas) == DEFEITOS["contas_sem_cliente"]


def test_cartoes_orfaos(tmp_path):
    raiz = gerar_massa(str(tmp_path))
    contas = {c["id_conta"] for c in ler_csv(raiz / "contas.csv")}
    orfaos = [c for c in ler_csv(raiz / "cartoes.csv") if c["id_conta"] not in contas]
    assert len(orfaos) == DEFEITOS["cartoes_sem_conta"]


def test_transacoes_duplicadas(tmp_path):
    raiz = gerar_massa(str(tmp_path))
    ids = []
    for arquivo in sorted(raiz.glob("transacoes_cartao_*.csv")):
        ids.extend(t["id_transacao"] for t in ler_csv(arquivo))
    assert len(ids) - len(set(ids)) == total("duplicadas")


def test_valores_negativos(tmp_path):
    raiz = gerar_massa(str(tmp_path))
    negativas = 0
    for arquivo in sorted(raiz.glob("transacoes_cartao_*.csv")):
        negativas += sum(1 for t in ler_csv(arquivo) if float(t["valor"]) < 0)
    assert negativas == total("valor_negativo")


def test_transacoes_atrasadas_em_todos_os_dias(tmp_path):
    """Atrasada = data_transacao anterior ao dia do arquivo."""
    raiz = gerar_massa(str(tmp_path))

    encontradas = 0
    for arquivo in sorted(raiz.glob("transacoes_cartao_*.csv")):
        aaaammdd = arquivo.stem.split("_")[-1]
        dia = f"{aaaammdd[:4]}-{aaaammdd[4:6]}-{aaaammdd[6:]}"
        atrasadas = [t for t in ler_csv(arquivo) if t["data_transacao"][:10] < dia]
        assert atrasadas, f"{arquivo.name} sem transacao atrasada"
        encontradas += len(atrasadas)

    assert encontradas == total("atrasadas")


def test_estornos_orfaos(tmp_path):
    raiz = gerar_massa(str(tmp_path))

    ids_transacoes = set()
    for arquivo in sorted(raiz.glob("transacoes_cartao_*.csv")):
        ids_transacoes.update(t["id_transacao"] for t in ler_csv(arquivo))

    estornos = ler_csv(raiz / "estornos.csv")
    orfaos = [e for e in estornos if e["id_transacao"] not in ids_transacoes]

    assert len(orfaos) == DEFEITOS["estornos_orfaos"]
    assert len(estornos) == DEFEITOS["estornos_validos"] + DEFEITOS["estornos_orfaos"]


def test_ids_de_estorno_sao_unicos(tmp_path):
    estornos = ler_csv(gerar_massa(str(tmp_path)) / "estornos.csv")
    ids = [e["id_estorno"] for e in estornos]
    assert len(ids) == len(set(ids))


# ---------------------------------------------------------------------------
# Funcoes de CPF
# ---------------------------------------------------------------------------

def test_cpf_gerado_e_valido():
    rng = random.Random(1)
    for _ in range(100):
        assert cpf_ok(cpf_valido(rng))


def test_cpf_com_digito_errado_tem_formato_certo():
    """Precisa passar no formato e falhar no digito — e o que separa as duas regras."""
    rng = random.Random(1)
    for _ in range(100):
        cpf = cpf_digito_errado(rng)
        assert len(cpf) == 11 and cpf.isdigit()
        assert not cpf_ok(cpf)


def test_defeitos_de_transacao_tem_um_valor_por_dia():
    for chave, valores in DEFEITOS_TRANSACAO.items():
        assert len(valores) == DIAS, f"{chave} nao tem {DIAS} valores"