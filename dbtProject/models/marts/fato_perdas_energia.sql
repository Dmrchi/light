{{ config(
    materialized='table',
    alias='perdas_energia',
    unique_key=['id_tempo', 'id_estado']
) }}

with perdas_clean as (
    select
        data_perda,
        estado_bruto,
        perda_tecnica_kwh,
        perda_nao_tecnica_kwh
    from {{ ref('stg_perdas_energia') }}
),

-- Realiza os JOINs para obter as Chaves Estrangeiras (FKs)
final as (
    select
        -- Geração da PK (Artificial)
        -- A PK é gerada pela ordenação das chaves de negócio únicas
        row_number() over (order by t.id_tempo, e.id_estado) as id_perda,

        -- Foreign Keys (FKs)
        t.id_tempo,   -- FK para analytics.tempo
        e.id_estado,  -- FK para analytics.estado

        -- Métricas
        p.perda_tecnica_kwh,
        p.perda_nao_tecnica_kwh

    from perdas_clean p -- Alias 'p' é mais comum para 'perdas'

    -- JOIN para obter a chave id_tempo
    inner join {{ ref('dim_tempo') }} t
        on p.data_perda = t.data

    -- JOIN para obter a chave id_estado
    inner join {{ ref('dim_estado') }} e
        on p.estado_bruto = e.estado
)

select * from final
