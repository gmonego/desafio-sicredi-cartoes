"""Testes da configuração declarativa."""

import pytest

from src.config import (
    get_camadas,
    get_config_version,
    get_entidade,
    load_conf,
    resolve_target,
)


def test_configuracao_carrega_com_versao_declarada():
    conf = load_conf()
    assert get_config_version(conf)
    assert conf["entidades"]


def test_camadas_essenciais_declaradas():
    camadas = get_camadas()
    for esperada in ("landing", "bronze", "silver", "gold", "quarantine"):
        assert esperada in camadas, f"camada {esperada} ausente"


def test_toda_entidade_tem_chave_de_negocio_e_tipos():
    """Sem chave de negocio não há deduplicação; sem tipos não há tipagem."""
    for nome, entidade in load_conf()["entidades"].items():
        assert entidade.get("chave_negocio"), f"{nome}: chave_negocio ausente"
        assert entidade.get("tipos"), f"{nome}: tipos ausentes"


def test_chave_de_negocio_existe_entre_os_tipos_declarados():
    """Pega o erro de digitar a chave com nome diferente do campo real."""
    for nome, entidade in load_conf()["entidades"].items():
        colunas = set(entidade["tipos"])
        for chave in entidade["chave_negocio"]:
            assert chave in colunas, f"{nome}: chave {chave!r} nao esta em tipos"


def test_coluna_versao_existe_entre_os_tipos_declarados():
    """A coluna de versao decide qual registro vence na deduplicacao."""
    for nome, entidade in load_conf()["entidades"].items():
        versao = entidade.get("coluna_versao")
        assert versao, f"{nome}: coluna_versao ausente"
        assert versao in entidade["tipos"], f"{nome}: {versao!r} nao esta em tipos"


def test_regras_de_qualidade_tem_campos_obrigatorios():
    severidades = {"critica", "alerta"}
    for nome, entidade in load_conf()["entidades"].items():
        for regra in entidade.get("regras_qualidade", []):
            assert "nome" in regra, f"{nome}: regra sem nome"
            assert "expressao" in regra, f"{nome}: regra {regra} sem expressao"
            assert regra.get("severidade") in severidades, (
                f"{nome}: severidade invalida em {regra.get('nome')}"
            )


def test_nomes_de_regra_sao_unicos_na_entidade():
    """O nome da regra identifica o motivo na quarentena."""
    for nome, entidade in load_conf()["entidades"].items():
        nomes = [r["nome"] for r in entidade.get("regras_qualidade", [])]
        assert len(nomes) == len(set(nomes)), f"{nome}: nomes de regra duplicados"


def test_toda_entidade_tem_ao_menos_uma_regra_critica():
    for nome, entidade in load_conf()["entidades"].items():
        severidades = [r["severidade"] for r in entidade.get("regras_qualidade", [])]
        assert "critica" in severidades, f"{nome}: nenhuma regra critica"


def test_integridade_referencial_aponta_para_entidade_e_coluna_existentes():
    conf = load_conf()
    entidades = conf["entidades"]
    for nome, entidade in entidades.items():
        for fk in entidade.get("integridade_referencial", []):
            referencia = fk["referencia"]
            assert referencia in entidades, (
                f"{nome}: FK aponta para entidade inexistente {referencia!r}"
            )
            assert fk["coluna"] in entidade["tipos"], (
                f"{nome}: coluna de FK {fk['coluna']!r} nao existe na entidade"
            )
            assert fk["coluna_referencia"] in entidades[referencia]["tipos"], (
                f"{nome}: coluna {fk['coluna_referencia']!r} nao existe "
                f"em {referencia}"
            )


def test_entidade_inexistente_levanta_erro_descritivo():
    with pytest.raises(ValueError, match="nao configurada"):
        get_entidade("entidade_que_nao_existe")


def test_resolve_target_usa_as_camadas_da_configuracao():
    assert resolve_target("silver", "clientes").endswith("/silver/clientes")