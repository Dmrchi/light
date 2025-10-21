{{ config(
    materialized='table',
    alias='analise_consumo_regional',
    schema='public_analytics'
) }}

-- Este modelo agrega o consumo mensal por estado e tipo de cliente

with consumo_agregado as (
    select
        t.ano,
        t.mes,
        e.estado,  -- Agora obtido de dim_estado
        c.tipo_cliente,
        sum(f.consumo_kwh) as consumo_total_kwh,
        avg(f.consumo_kwh) as consumo_medio_kwh

    from {{ ref('fato_medicoes_energia') }} f

    inner join {{ ref('dim_tempo') }} t
        on f.id_tempo = t.id_tempo

    inner join {{ ref('dim_clientes') }} c
        on f.id_cliente = c.id_cliente

    inner join {{ ref('dim_localizacao') }} l
        on f.id_localizacao = l.id_localizacao

    inner join {{ ref('dim_estado') }} e
        on l.id_estado = e.id_estado

    group by 1, 2, 3, 4
),

perdas_agregadas as (
    select
        t.ano,
        t.mes,
        e.estado,
        sum(p.perda_tecnica_kwh) as perda_tecnica_kwh,
        sum(p.perda_nao_tecnica_kwh) as perda_nao_tecnica_kwh
    from {{ ref('fato_perdas_energia') }} p

    inner join {{ ref('dim_tempo') }} t
        on p.id_tempo = t.id_tempo

    inner join {{ ref('dim_estado') }} e
        on p.id_estado = e.id_estado

    group by 1, 2, 3
)

select
    c.ano,
    c.mes,
    c.estado,
    c.tipo_cliente,
    c.consumo_total_kwh,
    c.consumo_medio_kwh,
    p.perda_tecnica_kwh,
    p.perda_nao_tecnica_kwh

from consumo_agregado c
left join perdas_agregadas p
    on c.ano = p.ano
    and c.mes = p.mes
    and c.estado = p.estado

order by c.ano, c.mes, c.estado, c.tipo_cliente
