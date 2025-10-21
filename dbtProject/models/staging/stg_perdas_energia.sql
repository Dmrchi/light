-- Este modelo limpa e padroniza os dados brutos de perdas de energia.
-- Ele será materializado como VIEW no esquema 'staging'.
select
    -- Garante que a data seja um tipo DATE e trata a sigla do estado
    -- CORREÇÃO: A coluna na fonte bruta é 'data_perda', não 'data_bruta'.
    cast(data_perda as date) as data_perda,
    upper(trim(estado)) as estado_bruto,
    cast(perda_tecnica_kwh as numeric(10, 2)) as perda_tecnica_kwh,
    cast(perda_nao_tecnica_kwh as numeric(10, 2)) as perda_nao_tecnica_kwh

from {{ source('source_data', 'perdas_energia_bruto') }}
