with rejeicoes as (
    select 'clientes' as entidade, explode(motivos_erro) as motivo from quarantine_clientes
    union all
    select 'contas', explode(motivos_erro) from quarantine_contas
    union all
    select 'cartoes', explode(motivos_erro) from quarantine_cartoes
    union all
    select 'transacoes', explode(motivos_erro) from quarantine_transacoes
    union all
    select 'estornos', explode(motivos_erro) from quarantine_estornos
),

alertas as (
    select 'clientes' as entidade, explode(alertas) as motivo from silver_clientes
    union all
    select 'contas', explode(alertas) from silver_contas
    union all
    select 'cartoes', explode(alertas) from silver_cartoes
    union all
    select 'transacoes', explode(alertas) from silver_transacoes
    union all
    select 'estornos', explode(alertas) from silver_estornos
)

select entidade, 'rejeicao' as tipo, motivo, count(*) as qtd_registros
from rejeicoes
group by entidade, motivo

union all

select entidade, 'alerta' as tipo, motivo, count(*) as qtd_registros
from alertas
group by entidade, motivo

order by entidade, tipo, qtd_registros desc