{{ config(
    materialized='table',
    alias='dim_localizacao',
    unique_key='id_localizacao'
) }}

select
    -- 1. Geração da PK: id_localizacao
    -- CORREÇÃO: Substituímos dbt_utils pela função MD5()
    md5(cast(e.id_estado as text) || c.cidade_bruta) as id_localizacao,
    e.id_estado, -- FK para a tabela estado (analytics.dim_estado)
    c.cidade_bruta as cidade

from (
    -- Desduplica as combinações cidade/estado das fontes
    select distinct cidade_bruta, estado_bruto from {{ ref('stg_clientes') }}
    union all
    select distinct cidade_bruta, estado_bruto from {{ ref('stg_ocorrencias_tecnicas') }}
) c
inner join {{ ref('dim_estado') }} e
    on c.estado_bruto = e.estado
