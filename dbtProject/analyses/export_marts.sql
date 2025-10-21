-- Este script lista as consultas a serem executadas para exportar os
-- dados dos modelos Mart (Transformados) para uso no Python/Power BI.
-- O script Python 'data_enrichment.py' executa estas consultas.

-- 1. Dados para Enriquecimento de Consumo
SELECT ano, mes, estado, tipo_cliente, consumo_total_kwh
FROM public_analytics.analise_consumo_regional
ORDER BY estado, tipo_cliente, ano, mes;

-- 2. Dados para Análise de Ocorrências
SELECT * FROM public_analytics.analise_ocorrencias_tecnicas;

-- 3. Dados para Análise de Perdas
SELECT * FROM public_analytics.analise_perdas_energia;
