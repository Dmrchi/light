{{ config(
    materialized='table',
    alias='top_clientes_por_consumo'
) }}

with consumo_por_cliente as (
    select
        c.id_cliente,
        c.nome_cliente,
        l.cidade,
        d_est.estado,

        sum(f.consumo_kwh) as consumo_total_kwh

    from {{ ref('fato_medicoes_energia') }} f

    inner join {{ ref('dim_clientes') }} c
        on f.id_cliente = c.id_cliente

    inner join {{ ref('dim_localizacao') }} l
        on c.id_localizacao = l.id_localizacao


    inner join {{ ref('dim_estado') }} d_est
        on l.id_estado = d_est.id_estado

    -- 3. Agrupamento
    group by
        c.id_cliente,
        c.nome_cliente,
        l.cidade,
        d_est.estado
),

final_rankeado as (
    select
        *,
        row_number() over (order by consumo_total_kwh desc) as rank_consumo_geral
    from consumo_por_cliente
)

select * from final_rankeado