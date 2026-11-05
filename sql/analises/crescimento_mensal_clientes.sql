with movimento_mensal as (
    select
        id_cliente,
        nome_cliente,
        ano_mes,
        valor_liquido,
        qtd_transacoes
    from gold_resumo_cliente_mes
),

com_mes_anterior as (
    select
        id_cliente,
        nome_cliente,
        ano_mes,
        valor_liquido,
        qtd_transacoes,
        lag(valor_liquido) over (
            partition by id_cliente
            order by ano_mes
        )                                           as valor_mes_anterior,
        lag(ano_mes) over (
            partition by id_cliente
            order by ano_mes
        )                                           as mes_anterior
    from movimento_mensal
)

select
    id_cliente,
    nome_cliente,
    mes_anterior,
    ano_mes                                         as mes_atual,
    valor_mes_anterior,
    valor_liquido                                   as valor_mes_atual,
    valor_liquido - valor_mes_anterior              as variacao_absoluta,
    round(
        (valor_liquido - valor_mes_anterior) / valor_mes_anterior * 100,
        2
    )                                               as crescimento_percentual,
    qtd_transacoes
from com_mes_anterior
where valor_mes_anterior is not null
  and valor_mes_anterior > 0
  and (valor_liquido - valor_mes_anterior) / valor_mes_anterior > 0.50
order by crescimento_percentual desc