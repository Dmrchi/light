{{ config(
    materialized='table',
    alias='analise_ocorrencias_tecnicas',
    schema='public_analytics'
) }}

-- Modelo que cruza dados de ocorrências técnicas com localização e tempo

select
    f.id_ocorrencia_fato,
    f.id_tempo,
    t.ano,
    t.mes,
    l.cidade,
    e.estado,
    f.tipo_ocorrencia,
    f.tempo_reparo_h

from {{ ref('fato_ocorrencias_tecnicas') }} f

inner join {{ ref('dim_tempo') }} t
    on f.id_tempo = t.id_tempo

inner join {{ ref('dim_localizacao') }} l
    on f.id_localizacao = l.id_localizacao

inner join {{ ref('dim_estado') }} e
    on l.id_estado = e.id_estado

order by t.ano, t.mes, e.estado, l.cidade
