with total_por_cliente as (
    select
        id_cliente,
        nome_cliente,
        sum(valor_liquido)      as valor_total,
        sum(qtd_transacoes)     as qtd_transacoes,
        sum(valor_estornado)    as valor_estornado,
        count(distinct ano_mes) as meses_ativos
    from gold_resumo_cliente_mes
    group by id_cliente, nome_cliente
),

classificado as (
    select
        id_cliente,
        nome_cliente,
        valor_total,
        qtd_transacoes,
        valor_estornado,
        meses_ativos,
        round(valor_total / qtd_transacoes, 2)          as ticket_medio,
        ntile(4) over (order by valor_total desc)       as quartil,
        dense_rank() over (order by valor_total desc)   as posicao
    from total_por_cliente
)

select
    id_cliente,
    nome_cliente,
    quartil,
    case quartil
        when 1 then 'Q1 - maior movimentacao'
        when 2 then 'Q2 - movimentacao acima da mediana'
        when 3 then 'Q3 - movimentacao abaixo da mediana'
        when 4 then 'Q4 - menor movimentacao'
    end                             as faixa,
    posicao,
    valor_total,
    qtd_transacoes,
    ticket_medio,
    valor_estornado,
    meses_ativos
from classificado
order by quartil, posicao