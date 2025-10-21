{{ config(
    materialized='table',
    alias='analise_perdas_energia',
    schema='public_analytics'
) }}

-- Modelo analítico que resume as perdas por estado e tempo

select
    t.ano,
    t.mes,
    e.estado,
    sum(p.perda_tecnica_kwh) as perda_tecnica_total_kwh,
    sum(p.perda_nao_tecnica_kwh) as perda_nao_tecnica_total_kwh

from {{ ref('fato_perdas_energia') }} p

inner join {{ ref('dim_tempo') }} t
    on p.id_tempo = t.id_tempo

inner join {{ ref('dim_estado') }} e
    on p.id_estado = e.id_estado

group by 1, 2, 3
order by 1, 2, 3
