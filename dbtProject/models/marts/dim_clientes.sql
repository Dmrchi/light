{{ config(
    materialized='table',
    alias='clientes',
    unique_key='id_cliente'
) }}

select
    s.id_cliente, -- Manter a PK original
    s.nome_cliente,
    l.id_localizacao, -- FK
    s.tipo_cliente::tipo_cliente_enum as tipo_cliente, -- Aplica o ENUM (Postgresql)
    s.data_adesao

from {{ ref('stg_clientes') }} s
inner join {{ ref('dim_localizacao') }} l
    on s.cidade_bruta = l.cidade -- Faz o JOIN para obter a FK
    -- A lógica de JOIN deve ser ajustada para incluir o estado se for necessário garantir unicidade