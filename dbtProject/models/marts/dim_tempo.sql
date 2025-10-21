{{ config(
    materialized='table',
    alias='tempo',
    unique_key='data'
) }}

-- Use uma lógica que gere todas as datas necessárias para o seu caso (ex: 2020 a 2025)
with dates as (
    -- Gera datas (use uma Macro mais robusta em produção, mas esta funciona)
    select generate_series('2020-01-01'::date, '2025-12-31'::date, '1 day'::interval) as date_day
),
final as (
    select
        cast(to_char(date_day, 'YYYYMMDD') as integer) as id_tempo, -- PK
        date_day as data,
        extract(year from date_day) as ano,
        extract(month from date_day) as mes,
        -- ... (Adicione todas as outras colunas de tempo)
        to_char(date_day, 'Day') as nome_dia_da_semana
    from dates
)
select * from final