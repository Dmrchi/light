{{ config(
    materialized='table',
    alias='ocorrencias_tecnicas',
    unique_key='id_ocorrencia_fato'
) }}

with ocorrencias_clean as (
    select
        id_ocorrencia,
        data_ocorrencia,
        estado_bruto,
        cidade_bruta,
        tipo_ocorrencia,
        tempo_reparo_h
    from {{ ref('stg_ocorrencias_tecnicas') }}
),

-- 1. Resolve Foreign Keys
resolved_fks as (
    select
        -- PK artificial (Surrogate Key)
        row_number() over (order by s.id_ocorrencia, t.id_tempo) as id_ocorrencia_fato,

        -- Chaves Estrangeiras (FKs)
        s.id_ocorrencia, -- PK Original (opcionalmente mantida como informação)
        t.id_tempo,  -- FK da dim_tempo
        l.id_localizacao, -- FK da dim_localizacao

        -- Métricas e Atributos
        s.tipo_ocorrencia,
        s.tempo_reparo_h

    from ocorrencias_clean s

    -- JUNTA COM DIMENSÃO TEMPO
    inner join {{ ref('dim_tempo') }} t
        on s.data_ocorrencia = t.data -- Assumindo que a data_ocorrencia está no formato DATE

    -- JUNTA COM DIMENSÃO ESTADO (Para obter o id_estado necessário para a localização)
    inner join {{ ref('dim_estado') }} e
        on s.estado_bruto = e.estado

    -- JUNTA COM DIMENSÃO LOCALIZAÇÃO (Usando a chave composta: Cidade + ID_Estado)
    inner join {{ ref('dim_localizacao') }} l
        on s.cidade_bruta = l.cidade
        and e.id_estado = l.id_estado
)

select * from resolved_fks
