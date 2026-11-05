with cartoes_completos as (
    select id_cartao, id_conta, bandeira, tipo_cartao, status_cartao,
           FALSE as restricao
    from silver_cartoes
    union all
    select q.id_cartao, q.id_conta, q.bandeira, q.tipo_cartao, q.status_cartao,
           TRUE as restricao
    from quarantine_cartoes q
    where not exists (
        select 1 from silver_cartoes s where s.id_cartao = q.id_cartao
    )
),

contas_completas as (
    select id_conta, id_cliente, tipo_conta, status_conta,
           FALSE as restricao
    from silver_contas
    union all
    select q.id_conta, q.id_cliente, q.tipo_conta, q.status_conta,
           TRUE as restricao
    from quarantine_contas q
    where not exists (
        select 1 from silver_contas s where s.id_conta = q.id_conta
    )
),

clientes_completos as (
    select id_cliente, nome, cidade, estado,
           FALSE as restricao
    from silver_clientes
    union all
    select q.id_cliente, q.nome, q.cidade, q.estado,
           TRUE as restricao
    from quarantine_clientes q
    where not exists (
        select 1 from silver_clientes s where s.id_cliente = q.id_cliente
    )
)

select
    t.id_transacao,
    t.data_transacao,
    cast(t.data_transacao as date)              as data,
    date_format(t.data_transacao, 'yyyy-MM')    as ano_mes,
    t.valor,
    t.valor_liquido,
    t.estornada,
    t.nome_estabelecimento,
    t.mcc,
    t.cidade_estabelecimento,

    ca.id_cartao,
    ca.bandeira,
    ca.tipo_cartao,
    ca.status_cartao,

    co.id_conta,
    co.tipo_conta,
    co.status_conta,

    cl.id_cliente,
    cl.nome                                     as nome_cliente,
    cl.cidade                                   as cidade_cliente,
    cl.estado                                   as estado_cliente,

    coalesce(ca.restricao, TRUE)
        or coalesce(co.restricao, TRUE)
        or coalesce(cl.restricao, TRUE)         as cadastro_com_restricao,

    t.data_referencia

from silver_transacoes t
left join cartoes_completos  ca on t.id_cartao   = ca.id_cartao
left join contas_completas   co on ca.id_conta   = co.id_conta
left join clientes_completos cl on co.id_cliente = cl.id_cliente