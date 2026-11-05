with agregado as (
    select
        nome_estabelecimento,
        mcc,
        cidade_estabelecimento,
        sum(valor_liquido)                          as valor_liquido,
        sum(valor)                                  as valor_bruto,
        count(*)                                    as qtd_transacoes,
        sum(case when estornada then 1 else 0 end)  as qtd_estornadas,
        count(distinct id_cliente)                  as qtd_clientes
    from gold_transacoes_cartao
    group by nome_estabelecimento, mcc, cidade_estabelecimento
)

select
    nome_estabelecimento,
    mcc,
    cidade_estabelecimento,
    valor_liquido,
    valor_bruto,
    qtd_transacoes,
    qtd_estornadas,
    qtd_clientes,

    dense_rank() over (order by valor_liquido desc)   as rank_valor,
    dense_rank() over (order by qtd_transacoes desc)  as rank_quantidade,
    dense_rank() over (
        partition by cidade_estabelecimento
        order by valor_liquido desc
    )                                                 as rank_valor_na_cidade

from agregado