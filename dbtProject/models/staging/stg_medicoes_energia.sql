-- models/staging/stg_medicoes_energia.sql

-- Limpa e padroniza os dados brutos de medições de energia.

select
    -- PK/FKs
    cast(id_cliente as integer) as id_cliente,
    cast(data_medicao as date) as data_medicao, -- Será usado para JOIN com dim_tempo

    -- Fato/Métricas
    cast(consumo_kwh as numeric(10, 2)) as consumo_kwh,
    tipo_medicao -- O tipo ENUM será aplicado na camada marts

-- Assumindo que a tabela bruta se chama medicoes_energia_bruto no esquema public
from {{ source('source_data', 'medicoes_energia_bruto') }}
