-- models/staging/stg_ocorrencias_tecnicas.sql

-- Este modelo limpa e padroniza os dados brutos de ocorrências técnicas.
-- Ele será materializado como VIEW no esquema 'staging'.
select
    cast(id_ocorrencia as integer) as id_ocorrencia,
    cast(data_ocorrencia as timestamp) as data_ocorrencia,
    trim(lower(cidade)) as cidade_bruta,
    upper(trim(estado)) as estado_bruto,
    tipo_ocorrencia,
    cast(tempo_reparo_h as numeric(5, 2)) as tempo_reparo_h

from {{ source('source_data', 'ocorrencias_tecnicas_bruto') }}
