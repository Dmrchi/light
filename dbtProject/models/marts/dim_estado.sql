{{ config(
    materialized='table',
    alias='dim_estado',
    unique_key='id_estado'
) }}

select distinct
    -- 1. Geração da PK estável: id_estado (Surrogate Key)
    -- CORREÇÃO: Usando a função MD5() nativa do PostgreSQL no lugar de dbt_utils
    md5(estado_bruto) as id_estado,
    estado_bruto as estado

from (
    -- 2. Seleciona todos os estados únicos das fontes
    select estado_bruto from {{ ref('stg_clientes') }}
    union all
    select estado_bruto from {{ ref('stg_perdas_energia') }}
    -- Você pode querer incluir stg_ocorrencias_tecnicas aqui também, se tiver estado_bruto
) as estados_unicos
where estado_bruto is not null
