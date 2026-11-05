with por_cidade as (
    select
        cidade_estabelecimento,
        nome_estabelecimento,
        mcc,
        sum(valor_liquido)                          as valor_liquido,
        count(*)                                    as qtd_transacoes,
        count(distinct id_cliente)                  as qtd_clientes,
        sum(case when estornada then 1 else 0 end)  as qtd_estornadas
    from gold_transacoes_cartao
    group by cidade_estabelecimento, nome_estabelecimento, mcc
),

ranqueado as (
    select
        cidade_estabelecimento,
        nome_estabelecimento,
        mcc,
        valor_liquido,
        qtd_transacoes,
        qtd_clientes,
        qtd_estornadas,
        row_number() over (
            partition by cidade_estabelecimento
            order by valor_liquido desc, nome_estabelecimento
        )                                           as posicao,
        dense_rank() over (
            partition by cidade_estabelecimento
            order by valor_liquido desc
        )                                           as posicao_com_empate,
        sum(valor_liquido) over (
            partition by cidade_estabelecimento
        )                                           as valor_total_cidade
    from por_cidade
)

select
    cidade_estabelecimento,
    posicao,
    posicao_com_empate,
    nome_estabelecimento,
    mcc,
    valor_liquido,
    round(valor_liquido / valor_total_cidade * 100, 2) as participacao_na_cidade,
    qtd_transacoes,
    qtd_clientes,
    qtd_estornadas
from ranqueado
where posicao <= 3
order by cidade_estabelecimento, posicao