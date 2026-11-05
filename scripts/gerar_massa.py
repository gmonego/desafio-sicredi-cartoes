"""Gera a massa fictícia da camada landing."""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from src.config import get_data_root
from src.utils.logging import etapa, get_logger

logger = get_logger(__name__)

SEMENTE = 20261103

DATAS_CARGA = [
    date(2026, 8, 14),
    date(2026, 9, 16),
    date(2026, 10, 15),
]
DIAS = len(DATAS_CARGA)

QTD_CLIENTES = 300
QTD_CONTAS = 340
QTD_CARTOES = 400


QTD_TRANSACOES = [1800, 2400, 1500]

# Defeitos de cadastro. Cada número tem um teste correspondente.
DEFEITOS = {
    "clientes_duplicados": 18,       # mesma chave, data_atualizacao diferente
    "clientes_cpf_invalido": 11,     # 11 dígitos, verificador errado
    "clientes_cpf_malformado": 7,    # falha no formato
    "clientes_uf_invalida": 6,       # severidade alerta, não rejeita
    "contas_sem_cliente": 9,         # id_cliente inexistente
    "contas_status_invalido": 8,
    "cartoes_sem_conta": 12,
    "cartoes_cancelados": 30,        # estado legítimo, exige tratamento na ouro
    "cartoes_bandeira_invalida": 7,  # severidade alerta
    "estornos_validos": 160,
    "estornos_orfaos": 14,
}

# Defeitos de transacao, um valor por carga.
DEFEITOS_TRANSACAO = {
    "duplicadas": [9, 12, 7],
    "valor_negativo": [6, 8, 5],
    "data_invalida": [3, 4, 3],
    "atrasadas": [11, 14, 9],
    "sem_cartao": [5, 4, 3],
}

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Felipe", "Gabriela",
         "Henrique", "Isabel", "Joao", "Karina", "Lucas", "Marina", "Nelson",
         "Olivia", "Paulo", "Renata", "Sergio", "Tatiana", "Vitor"]
SOBRENOMES = ["Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira",
              "Alves", "Pereira", "Lima", "Gomes", "Costa", "Ribeiro"]
CIDADES_UF = [("Porto Alegre", "RS"), ("Canoas", "RS"), ("Caxias do Sul", "RS"),
              ("Florianopolis", "SC"), ("Curitiba", "PR"), ("Sao Paulo", "SP"),
              ("Campinas", "SP"), ("Belo Horizonte", "MG")]
TIPOS_CONTA = ["CORRENTE", "POUPANCA", "SALARIO"]
STATUS_CONTA = ["ATIVA", "INATIVA", "BLOQUEADA", "ENCERRADA"]
STATUS_CONTA_RUINS = ["ativo", "PENDENTE", "", "N/A", "SUSPENSA"]
BANDEIRAS = ["VISA", "MASTERCARD", "ELO", "AMEX"]
BANDEIRAS_RUINS = ["DINERS", "HIPERCARD", "", "visa card"]
TIPOS_CARTAO = ["CREDITO", "DEBITO", "MULTIPLO"]
STATUS_CARTAO = ["ATIVO", "BLOQUEADO"]
UFS_RUINS = ["XX", "RioGrande", "", "1A"]
ESTABELECIMENTOS = [("5411", "Supermercado Central"), ("5411", "Mercado Bom Preco"),
                    ("5812", "Restaurante da Esquina"), ("5812", "Cantina Italiana"),
                    ("5541", "Posto Ipiranga Centro"), ("5912", "Farmacia Popular"),
                    ("5732", "Eletronicos Tech"), ("4121", "Transporte Urbano"),
                    ("7995", "Casa de Apostas"), ("5999", "Loja de Conveniencia")]
MOTIVOS_ESTORNO = ["COMPRA_NAO_RECONHECIDA", "DUPLICIDADE", "CANCELAMENTO_COMERCIO",
                   "VALOR_DIVERGENTE", "FRAUDE_CONFIRMADA"]


# ---------------------------------------------------------------------------
# Plantio de defeitos
# ---------------------------------------------------------------------------

def sortear_disjuntos(rng, linhas: list, quantidades: list) -> list:
    """Sorteia uma amostra única e a divide em fatias que não se cruzam."""
    amostra = rng.sample(linhas, sum(quantidades))

    fatias = []
    inicio = 0
    for quantidade in quantidades:
        fatias.append(amostra[inicio:inicio + quantidade])
        inicio += quantidade
    return fatias


# ---------------------------------------------------------------------------
# CPF
# ---------------------------------------------------------------------------

def digito_cpf(base: str, peso_inicial: int) -> str:
    """Calcula um dígito verificador de CPF."""
    soma = sum(int(d) * (peso_inicial - i) for i, d in enumerate(base))
    resto = soma % 11
    return "0" if resto < 2 else str(11 - resto)


def cpf_valido(rng) -> str:
    """CPF de 11 dígitos com verificadores corretos."""
    base = "".join(str(rng.randint(0, 9)) for _ in range(9))
    primeiro = digito_cpf(base, 10)
    segundo = digito_cpf(base + primeiro, 11)
    return base + primeiro + segundo


def cpf_digito_errado(rng) -> str:
    """CPF com formato certo e verificador errado."""
    cpf = cpf_valido(rng)
    primeiro_errado = str((int(cpf[9]) + 1) % 10)
    return cpf[:9] + primeiro_errado + cpf[10]


def cpf_malformado(rng) -> str:
    """CPF que falha na regra de formato."""
    return rng.choice(["123", "abcdefghijk", "111.222.333-44", "", "000000000000000"])


# ---------------------------------------------------------------------------
# Apoio
# ---------------------------------------------------------------------------

def escrever_csv(caminho: Path, linhas: list) -> None:
    """Grava as linhas em CSV usando as chaves do primeiro registro como cabeçalho."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=list(linhas[0]))
        escritor.writeheader()
        escritor.writerows(linhas)
    logger.info("landing/%-34s %7d linhas", caminho.name, len(linhas))


def momento(rng, dia: date) -> str:
    """Timestamp aleatório dentro de um dia."""
    instante = datetime(dia.year, dia.month, dia.day) + timedelta(
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
    )
    return instante.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Entidades
# ---------------------------------------------------------------------------

def gerar_clientes(rng, quantidade: int) -> list:
    """Clientes, com duplicatas versionadas e CPFs inválidos."""
    primeira_carga = DATAS_CARGA[0]
    linhas = []

    for i in range(1, quantidade + 1):
        cidade, uf = rng.choice(CIDADES_UF)
        linhas.append({
            "id_cliente": f"CLI{i:06d}",
            "nome": f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}",
            "cpf": cpf_valido(rng),
            "data_nascimento": date(
                rng.randint(1955, 2005), rng.randint(1, 12), rng.randint(1, 28)
            ).isoformat(),
            "cidade": cidade,
            "estado": uf,
            "data_atualizacao": momento(rng, primeira_carga - timedelta(days=60)),
        })

    # Duplicatas
    for original in rng.sample(linhas, DEFEITOS["clientes_duplicados"]):
        copia = dict(original)
        copia["cidade"], copia["estado"] = rng.choice(CIDADES_UF)
        copia["nome"] = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        copia["data_atualizacao"] = momento(rng, primeira_carga)
        linhas.append(copia)

    com_digito_errado, com_formato_ruim, com_uf_ruim = sortear_disjuntos(
        rng,
        linhas,
        [
            DEFEITOS["clientes_cpf_invalido"],
            DEFEITOS["clientes_cpf_malformado"],
            DEFEITOS["clientes_uf_invalida"],
        ],
    )

    for cliente in com_digito_errado:
        cliente["cpf"] = cpf_digito_errado(rng)

    for cliente in com_formato_ruim:
        cliente["cpf"] = cpf_malformado(rng)

    for cliente in com_uf_ruim:
        cliente["estado"] = rng.choice(UFS_RUINS)

    rng.shuffle(linhas)
    return linhas


def gerar_contas(rng, quantidade: int, ids_clientes: list) -> list:
    """Contas, com órfas e status fora do domínio."""
    primeira_carga = DATAS_CARGA[0]
    linhas = []

    for i in range(1, quantidade + 1):
        linhas.append({
            "id_conta": f"CTA{i:06d}",
            "id_cliente": rng.choice(ids_clientes),
            "tipo_conta": rng.choice(TIPOS_CONTA),
            "status_conta": rng.choice(STATUS_CONTA),
            "data_abertura": (
                primeira_carga - timedelta(days=rng.randint(60, 2000))
            ).isoformat(),
        })

    orfas, com_status_ruim = sortear_disjuntos(
        rng,
        linhas,
        [DEFEITOS["contas_sem_cliente"], DEFEITOS["contas_status_invalido"]],
    )

    for conta in orfas:
        conta["id_cliente"] = f"CLI9{rng.randint(10000, 99999)}"

    for conta in com_status_ruim:
        conta["status_conta"] = rng.choice(STATUS_CONTA_RUINS)

    rng.shuffle(linhas)
    return linhas


def gerar_cartoes(rng, quantidade: int, ids_contas: list) -> list:
    """Cartões, com órfãos, cancelados e bandeiras fora do domínio."""
    primeira_carga = DATAS_CARGA[0]
    linhas = []

    for i in range(1, quantidade + 1):
        linhas.append({
            "id_cartao": f"CAR{i:06d}",
            "id_conta": rng.choice(ids_contas),
            "bandeira": rng.choice(BANDEIRAS),
            "tipo_cartao": rng.choice(TIPOS_CARTAO),
            "status_cartao": rng.choice(STATUS_CARTAO),
            "data_emissao": (
                primeira_carga - timedelta(days=rng.randint(30, 1500))
            ).isoformat(),
            "data_atualizacao": momento(rng, primeira_carga - timedelta(days=30)),
        })

    orfaos, cancelados, com_bandeira_ruim = sortear_disjuntos(
        rng,
        linhas,
        [
            DEFEITOS["cartoes_sem_conta"],
            DEFEITOS["cartoes_cancelados"],
            DEFEITOS["cartoes_bandeira_invalida"],
        ],
    )

    for cartao in orfaos:
        cartao["id_conta"] = f"CTA9{rng.randint(10000, 99999)}"

    for cartao in cancelados:
        cartao["status_cartao"] = "CANCELADO"

    for cartao in com_bandeira_ruim:
        cartao["bandeira"] = rng.choice(BANDEIRAS_RUINS)

    rng.shuffle(linhas)
    return linhas


def gerar_transacoes(rng, ids_cartoes: list, dia: date, quantidade: int,
                     defeitos: dict) -> list:
    """Transações de uma carga."""
    linhas = []

    for i in range(quantidade):
        mcc, estabelecimento = rng.choice(ESTABELECIMENTOS)
        cidade, _ = rng.choice(CIDADES_UF)
        linhas.append({
            "id_transacao": f"TRX{dia.strftime('%Y%m%d')}{i:06d}",
            "id_cartao": rng.choice(ids_cartoes),
            "data_transacao": momento(rng, dia),
            "valor": f"{rng.uniform(5.0, 3500.0):.2f}",
            "nome_estabelecimento": estabelecimento,
            "mcc": mcc,
            "cidade_estabelecimento": cidade,
        })

    atrasadas, negativas, datas_invalidas, sem_cartao, duplicadas = sortear_disjuntos(
        rng,
        linhas,
        [
            defeitos["atrasadas"],
            defeitos["valor_negativo"],
            defeitos["data_invalida"],
            defeitos["sem_cartao"],
            defeitos["duplicadas"],
        ],
    )

    # Atrasadas
    for transacao in atrasadas:
        transacao["data_transacao"] = momento(rng, dia - timedelta(days=1))

    for transacao in negativas:
        transacao["valor"] = f"-{rng.uniform(5.0, 900.0):.2f}"

    for transacao in datas_invalidas:
        transacao["data_transacao"] = momento(rng, dia + timedelta(days=400))

    for transacao in sem_cartao:
        transacao["id_cartao"] = f"CAR9{rng.randint(10000, 99999)}"

    for transacao in duplicadas:
        linhas.append(dict(transacao))

    rng.shuffle(linhas)
    return linhas


def gerar_estornos(rng, ids_transacoes: list) -> list:
    """Estornos válidos e órfãos."""
    ultima_carga = DATAS_CARGA[-1]
    linhas = []

    for i, id_transacao in enumerate(
        rng.sample(ids_transacoes, DEFEITOS["estornos_validos"]), start=1
    ):
        linhas.append({
            "id_estorno": f"EST{i:06d}",
            "id_transacao": id_transacao,
            "data_estorno": momento(rng, ultima_carga + timedelta(days=3)),
            "motivo_estorno": rng.choice(MOTIVOS_ESTORNO),
        })

    proximo = DEFEITOS["estornos_validos"] + 1
    for i in range(proximo, proximo + DEFEITOS["estornos_orfaos"]):
        linhas.append({
            "id_estorno": f"EST{i:06d}",
            "id_transacao": f"TRX9999{rng.randint(10000, 99999)}",
            "data_estorno": momento(rng, ultima_carga + timedelta(days=3)),
            "motivo_estorno": rng.choice(MOTIVOS_ESTORNO),
        })

    rng.shuffle(linhas)
    return linhas


# ---------------------------------------------------------------------------
# Execucao
# ---------------------------------------------------------------------------

def gerar_massa(destino=None, escala: int = 1) -> Path:
    """Gera todos os arquivos da landing e devolve o diretório usado.

    Args:
        destino: diretório alternativo. Usado pelos testes para gerar em área
            temporária, sem tocar no diretório de trabalho.
        escala: multiplicador dos volumes base. Os defeitos permanecem em
            número absoluto
    """
    rng = random.Random(SEMENTE)

    raiz = Path(destino) if destino else Path(get_data_root()) / "landing"
    raiz.mkdir(parents=True, exist_ok=True)

    qtd_clientes = QTD_CLIENTES * escala
    qtd_contas = QTD_CONTAS * escala
    qtd_cartoes = QTD_CARTOES * escala

    with etapa(logger, f"geracao da massa (escala={escala})"):
        logger.info(
            "destino=%s | semente=%d | %d carga(s) entre %s e %s",
            raiz, SEMENTE, DIAS, DATAS_CARGA[0], DATAS_CARGA[-1],
        )

        clientes = gerar_clientes(rng, qtd_clientes)
        escrever_csv(raiz / "clientes.csv", clientes)

        contas = gerar_contas(rng, qtd_contas, [c["id_cliente"] for c in clientes])
        escrever_csv(raiz / "contas.csv", contas)

        cartoes = gerar_cartoes(rng, qtd_cartoes, [c["id_conta"] for c in contas])
        escrever_csv(raiz / "cartoes.csv", cartoes)

        ids_transacoes = []
        for indice, dia in enumerate(DATAS_CARGA):
            defeitos_da_carga = {
                chave: valores[indice]
                for chave, valores in DEFEITOS_TRANSACAO.items()
            }
            transacoes = gerar_transacoes(
                rng,
                [c["id_cartao"] for c in cartoes],
                dia,
                QTD_TRANSACOES[indice] * escala,
                defeitos_da_carga,
            )
            escrever_csv(raiz / f"transacoes_cartao_{dia:%Y%m%d}.csv", transacoes)
            ids_transacoes.extend(t["id_transacao"] for t in transacoes)

        escrever_csv(raiz / "estornos.csv", gerar_estornos(rng, ids_transacoes))

    return raiz


if __name__ == "__main__":
    gerar_massa()