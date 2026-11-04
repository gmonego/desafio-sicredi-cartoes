"""Gera a massa ficticia da camada landing.

O desafio pede pelo menos três dias de carga com problemas de qualidade e
duplicidade. Gero tudo com semente fixa e declaro a quantidade de cada
defeito numa constante, assim os testes das camadas podem afirmar números
exatos em vez de apenas conferir que alguma coisa foi rejeitada.

Nenhum dado real é usado.

Uso:
    docker compose run --rm pipeline python -m scripts.gerar_massa
"""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from src.config import get_data_root

SEMENTE = 20261103
DATA_INICIAL = date(2026, 10, 1)
DIAS = 3

QTD_CLIENTES = 200
QTD_CONTAS = 220
QTD_CARTOES = 260

# Volumes diferentes por dia: carga uniforme esconderia erro de filtro por data.
QTD_TRANSACOES = [400, 550, 300]

# Defeitos de cadastro. Cada numero tem um teste correspondente.
DEFEITOS = {
    "clientes_duplicados": 12,       # mesma chave, data_atualizacao diferente
    "clientes_cpf_invalido": 8,      # 11 digitos, verificador errado
    "clientes_cpf_malformado": 5,    # falha ja no formato
    "clientes_uf_invalida": 4,       # severidade alerta, nao rejeita
    "contas_sem_cliente": 7,         # id_cliente inexistente
    "contas_status_invalido": 6,
    "cartoes_sem_conta": 9,
    "cartoes_cancelados": 20,        # estado legitimo, exige tratamento na ouro
    "cartoes_bandeira_invalida": 5,  # severidade alerta
    "estornos_validos": 40,
    "estornos_orfaos": 9,
}

# Defeitos de transação, um valor por dia de carga. Declarados dia a dia para
# não precisar de logica de distribuição: o que esta escrito é o que é gerado.
DEFEITOS_TRANSACAO = {
    "duplicadas": [5, 6, 4],
    "valor_negativo": [4, 4, 3],
    "data_invalida": [2, 2, 2],
    "atrasadas": [6, 7, 5],
    "sem_cartao": [3, 2, 2],
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
# CPF
# ---------------------------------------------------------------------------

def digito_cpf(base: str, peso_inicial: int) -> str:
    """Calcula um digito verificador de CPF."""
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
    """CPF que falha ja na regra de formato."""
    return rng.choice(["123", "abcdefghijk", "111.222.333-44", "", "000000000000000"])


# ---------------------------------------------------------------------------
# Apoio
# ---------------------------------------------------------------------------

def escrever_csv(caminho: Path, linhas: list) -> None:
    """Grava as linhas em CSV usando as chaves do primeiro registro como cabecalho."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with open(caminho, "w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=list(linhas[0]))
        escritor.writeheader()
        escritor.writerows(linhas)
    print(f"  {caminho.name:34s} {len(linhas):6d} linhas")


def momento(rng, dia: date) -> str:
    """Timestamp aleatorio dentro de um dia."""
    instante = datetime(dia.year, dia.month, dia.day) + timedelta(
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
    )
    return instante.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Entidades
# ---------------------------------------------------------------------------

def gerar_clientes(rng) -> list:
    """Clientes, com duplicatas versionadas e CPFs invalidos."""
    linhas = []
    for i in range(1, QTD_CLIENTES + 1):
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
            "data_atualizacao": momento(rng, DATA_INICIAL - timedelta(days=30)),
        })

    # Duplicatas: mesma chave, versão mais nova, dados cadastrais diferentes.
    for original in rng.sample(linhas, DEFEITOS["clientes_duplicados"]):
        copia = dict(original)
        copia["cidade"], copia["estado"] = rng.choice(CIDADES_UF)
        copia["nome"] = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
        copia["data_atualizacao"] = momento(rng, DATA_INICIAL)
        linhas.append(copia)

    qtd_digito = DEFEITOS["clientes_cpf_invalido"]
    qtd_formato = DEFEITOS["clientes_cpf_malformado"]
    afetados = rng.sample(linhas, qtd_digito + qtd_formato)

    for cliente in afetados[:qtd_digito]:
        cliente["cpf"] = cpf_digito_errado(rng)

    for cliente in afetados[qtd_digito:]:
        cliente["cpf"] = cpf_malformado(rng)

    for cliente in rng.sample(linhas, DEFEITOS["clientes_uf_invalida"]):
        cliente["estado"] = rng.choice(UFS_RUINS)

    rng.shuffle(linhas)
    return linhas


def gerar_contas(rng, ids_clientes: list) -> list:
    """Contas, com órfãs e status fora do domínio.

    Conta órfã tem o campo preenchido e passa nas regras de qualidade: só a
    validação de integridade referencial detecta. Por isso as duas
    verificações são separadas no pipeline.
    """
    linhas = []
    for i in range(1, QTD_CONTAS + 1):
        linhas.append({
            "id_conta": f"CTA{i:06d}",
            "id_cliente": rng.choice(ids_clientes),
            "tipo_conta": rng.choice(TIPOS_CONTA),
            "status_conta": rng.choice(STATUS_CONTA),
            "data_abertura": (
                DATA_INICIAL - timedelta(days=rng.randint(60, 2000))
            ).isoformat(),
        })

    for conta in rng.sample(linhas, DEFEITOS["contas_sem_cliente"]):
        conta["id_cliente"] = f"CLI9{rng.randint(10000, 99999)}"

    for conta in rng.sample(linhas, DEFEITOS["contas_status_invalido"]):
        conta["status_conta"] = rng.choice(STATUS_CONTA_RUINS)

    rng.shuffle(linhas)
    return linhas


def gerar_cartoes(rng, ids_contas: list) -> list:
    """Cartões, com órfãos, cancelados e bandeiras fora do domínio."""
    linhas = []
    for i in range(1, QTD_CARTOES + 1):
        linhas.append({
            "id_cartao": f"CAR{i:06d}",
            "id_conta": rng.choice(ids_contas),
            "bandeira": rng.choice(BANDEIRAS),
            "tipo_cartao": rng.choice(TIPOS_CARTAO),
            "status_cartao": rng.choice(STATUS_CARTAO),
            "data_emissao": (
                DATA_INICIAL - timedelta(days=rng.randint(30, 1500))
            ).isoformat(),
            "data_atualizacao": momento(rng, DATA_INICIAL - timedelta(days=15)),
        })

    for cartao in rng.sample(linhas, DEFEITOS["cartoes_sem_conta"]):
        cartao["id_conta"] = f"CTA9{rng.randint(10000, 99999)}"

    for cartao in rng.sample(linhas, DEFEITOS["cartoes_cancelados"]):
        cartao["status_cartao"] = "CANCELADO"

    for cartao in rng.sample(linhas, DEFEITOS["cartoes_bandeira_invalida"]):
        cartao["bandeira"] = rng.choice(BANDEIRAS_RUINS)

    rng.shuffle(linhas)
    return linhas


def gerar_transacoes(rng, ids_cartoes: list, dia: date, quantidade: int,
                     defeitos: dict) -> list:
    """Transações de um dia de carga."""
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

    # Atrasadas: evento de ontem chegando no arquivo de hoje. É o caso que
    # impede a carga incremental de filtrar simplesmente por data_transacao.
    for transacao in rng.sample(linhas, defeitos["atrasadas"]):
        transacao["data_transacao"] = momento(rng, dia - timedelta(days=1))

    for transacao in rng.sample(linhas, defeitos["valor_negativo"]):
        transacao["valor"] = f"-{rng.uniform(5.0, 900.0):.2f}"

    for transacao in rng.sample(linhas, defeitos["data_invalida"]):
        transacao["data_transacao"] = momento(rng, dia + timedelta(days=400))

    for transacao in rng.sample(linhas, defeitos["sem_cartao"]):
        transacao["id_cartao"] = f"CAR9{rng.randint(10000, 99999)}"

    # Duplicata de transação repete a linha inteira, sem versão mais nova.
    # Diferente do cadastro, aqui não há o que desempatar pela data.
    for transacao in rng.sample(linhas, defeitos["duplicadas"]):
        linhas.append(dict(transacao))

    rng.shuffle(linhas)
    return linhas


def gerar_estornos(rng, ids_transacoes: list) -> list:
    """Estornos válidos e órfãos.

    O órfão aponta para transação inexistente. A validação precisa separá-lo
    sem descartar os legítimos, que afetam os indicadores financeiros da ouro.
    """
    linhas = []
    for i, id_transacao in enumerate(
        rng.sample(ids_transacoes, DEFEITOS["estornos_validos"]), start=1
    ):
        linhas.append({
            "id_estorno": f"EST{i:06d}",
            "id_transacao": id_transacao,
            "data_estorno": momento(rng, DATA_INICIAL + timedelta(days=3)),
            "motivo_estorno": rng.choice(MOTIVOS_ESTORNO),
        })

    proximo = DEFEITOS["estornos_validos"] + 1
    for i in range(proximo, proximo + DEFEITOS["estornos_orfaos"]):
        linhas.append({
            "id_estorno": f"EST{i:06d}",
            "id_transacao": f"TRX9999{rng.randint(10000, 99999)}",
            "data_estorno": momento(rng, DATA_INICIAL + timedelta(days=3)),
            "motivo_estorno": rng.choice(MOTIVOS_ESTORNO),
        })

    rng.shuffle(linhas)
    return linhas


# ---------------------------------------------------------------------------
# Execucao
# ---------------------------------------------------------------------------

def gerar_massa(destino=None) -> Path:
    """Gera todos os arquivos da landing e devolve o diretório usado."""
    # Instancia propria de Random: usar o random global deixaria a sequência
    # sujeita a qualquer outro código do processo.
    rng = random.Random(SEMENTE)

    raiz = Path(destino) if destino else Path(get_data_root()) / "landing"
    raiz.mkdir(parents=True, exist_ok=True)

    print(f"\nGerando massa em {raiz}  (semente={SEMENTE}, {DIAS} dias)\n")

    clientes = gerar_clientes(rng)
    escrever_csv(raiz / "clientes.csv", clientes)

    contas = gerar_contas(rng, [c["id_cliente"] for c in clientes])
    escrever_csv(raiz / "contas.csv", contas)

    cartoes = gerar_cartoes(rng, [c["id_conta"] for c in contas])
    escrever_csv(raiz / "cartoes.csv", cartoes)

    ids_transacoes = []
    for indice in range(DIAS):
        dia = DATA_INICIAL + timedelta(days=indice)
        defeitos_do_dia = {
            chave: valores[indice] for chave, valores in DEFEITOS_TRANSACAO.items()
        }
        transacoes = gerar_transacoes(
            rng,
            [c["id_cartao"] for c in cartoes],
            dia,
            QTD_TRANSACOES[indice],
            defeitos_do_dia,
        )
        escrever_csv(raiz / f"transacoes_cartao_{dia:%Y%m%d}.csv", transacoes)
        ids_transacoes.extend(t["id_transacao"] for t in transacoes)

    escrever_csv(raiz / "estornos.csv", gerar_estornos(rng, ids_transacoes))

    print()
    return raiz


if __name__ == "__main__":
    gerar_massa()