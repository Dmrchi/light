-- Limpa e padroniza os dados brutos de clientes.
select
    -- PK: id_cliente (inteiro)
    -- CORREÇÃO: Usamos 'id_cliente' assumindo que este é o nome da coluna na tabela bruta
    cast(id_cliente as integer) as id_cliente,

    -- Campos de texto padronizados (limpeza e minúsculas/maiúsculas)
    nome_cliente,
    trim(lower(cidade)) as cidade_bruta,
    upper(trim(estado)) as estado_bruto,

    -- Tipos e Datas
    tipo_cliente, -- O tipo ENUM será aplicado na camada marts
    cast(data_adesao as date) as data_adesao

-- Assumindo que a tabela bruta se chama clientes_bruto no esquema public
from {{ source('source_data', 'clientes_bruto') }}
