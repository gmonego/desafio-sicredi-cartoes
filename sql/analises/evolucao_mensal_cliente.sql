with evolucao as (
    select
        id_cliente,
        nome_cliente,
        ano_mes,
        valor_liquido,
        qtd_transacoes,
        ticket_medio,

        lag(valor_liquido)  over (partition by id_cliente order by ano_mes)
            as valor_mes_anterior,
        lead(valor_liquido) over (partition by id_cliente order by ano_mes)
            as valor_mes_seguinte,

        row_number() over (partition by id_cliente order by ano_mes)
            as numero_do_mes
    from gold_resumo_cliente_mes
)

select
    id_cliente,
    nome_cliente,
    ano_mes,
    numero_do_mes,
    valor_mes_anterior,
    valor_liquido                       as valor_mes_atual,
    valor_mes_seguinte,
    qtd_transacoes,
    ticket_medio,

    case
        when valor_mes_anterior is null then 'primeiro mes'
        when valor_liquido > valor_mes_anterior then 'crescimento'
        when valor_liquido < valor_mes_anterior then 'retracao'
        else 'estavel'
    end                                 as tendencia,

    case
        when valor_mes_anterior is null OR valor_mes_anterior = 0 then null
        else round(
            (valor_liquido - valor_mes_anterior) / valor_mes_anterior * 100, 2
        )
    end                                 as variacao_percentual

from evolucao
order by id_cliente, ano_mes