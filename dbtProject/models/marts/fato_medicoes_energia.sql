{{ config(
    materialized='table',
    alias='medicoes_energia',
    unique_key='id_medicao_fato'
) }}

-- 1. Buscando os dados limpos do staging
-- Nota: stg_medicoes_energia deve ter apenas data_medicao, id_cliente e consumo_kwh.
with medicoes_staging as (
    select
        data_medicao,
        id_cliente, -- Chave de negócio para Cliente
        consumo_kwh
    from {{ ref('stg_medicoes_energia') }}
),

-- 2. Resolvendo as Foreign Keys (FKs) através de JOINS
final as (
    select
        -- PK artificial (Surrogate Key)
        row_number() over (order by t.id_tempo, c.id_localizacao) as id_medicao_fato,

        -- Foreign Keys (FKs)
        t.id_tempo,  -- FK da dim_tempo (resolvida por data_medicao)
        c.id_cliente as id_cliente, -- PK/FK de dim_clientes (Nome corrigido: id_cliente)
        c.id_localizacao, -- FK da dim_localizacao (já resolvida dentro de dim_clientes)
        -- Coluna c.id_estado removida pois não existe no dim_clientes.

        -- Métricas
        m.consumo_kwh

    from medicoes_staging m

    -- JOIN com DIM_TEMPO
    inner join {{ ref('dim_tempo') }} t
        on m.data_medicao = t.data

    -- **JOIN CHAVE**: Usamos DIM_CLIENTES para obter a localização resolvida (FKs de Localização)
    -- O JOIN usa c.id_cliente, que foi corrigido no passo anterior.
    inner join {{ ref('dim_clientes') }} c
        on m.id_cliente = c.id_cliente
)

select * from final
