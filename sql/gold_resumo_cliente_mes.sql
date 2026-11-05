select
    id_cliente,
    nome_cliente,
    ano_mes,

    count(*)                                            as qtd_transacoes,
    sum(case when estornada then 1 else 0 end)          as qtd_estornadas,

    sum(valor)                                          as valor_bruto,
    sum(valor_liquido)                                  as valor_liquido,
    sum(valor) - sum(valor_liquido)                     as valor_estornado,

    count(distinct id_cartao)                           as qtd_cartoes_usados,
    count(distinct nome_estabelecimento)                as qtd_estabelecimentos,
    count(distinct mcc)                                 as qtd_categorias,

    case
        when count(*) - sum(case when estornada then 1 else 0 end) > 0
        then ROUND(
            sum(valor_liquido)
            / (count(*) - sum(case when estornada then 1 else 0 end)),
            2
        )
        else cast(0 as decimal(18,2))
    end                                                 as ticket_medio

from gold_transacoes_cartao
where id_cliente is not null
group by id_cliente, nome_cliente, ano_mes