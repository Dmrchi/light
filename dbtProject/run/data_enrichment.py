import pandas as pd
from sqlalchemy import create_engine
import logging

# Configuração de Log para acompanhar o processo no console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_USER = 'postgres'
DB_PASS = 'password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'lightdb'

# String de Conexão SQL Alchemy
# DB_URL = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
DB_URL = 'postgresql://postgres:password@localhost:5432/dblight'
try:
    engine = create_engine(DB_URL)
    print("Conexão com o banco de dados configurada. Tentando conectar...")
except ImportError:
    logging.error(
        "A biblioteca 'psycopg2' ou 'sqlalchemy' não está instalada. Execute: pip install pandas sqlalchemy psycopg2-binary")
    exit()


# --- 2. Função de Enriquecimento Principal (Consumo) ---

def extract_and_enrich_consumo(engine: create_engine) -> pd.DataFrame:
    """Extrai os dados de consumo regional e calcula a Média Móvel."""

    # Consulta SQL para extrair o Mart de Consumo Regional
    # CORREÇÃO AQUI: Mudança de 'public_analytics' para 'public_public_analytics'
    query_consumo = """
                    SELECT ano, \
                           mes, \
                           estado, \
                           tipo_cliente, \
                           consumo_total_kwh
                    FROM public_public_analytics.analise_consumo_regional
                    ORDER BY estado, tipo_cliente, ano, mes \
                    """
    df_consumo = pd.read_sql(query_consumo, engine)
    logging.info(f"Dados de consumo carregados. Linhas: {len(df_consumo)}")

    # Cálculo da Média Móvel (Rolling Mean) de 3 Meses
    # Este é o enriquecimento de dados exigido pelo Case (uso de Python).
    # Agrupamos por Estado e Tipo de Cliente antes de calcular a tendência.

    # 2.1. Cria a coluna de Média Móvel (3 meses)
    df_consumo['consumo_medio_movel_3m'] = df_consumo.groupby(['estado', 'tipo_cliente'])[
        'consumo_total_kwh'].transform(
        # window=3: Usa os últimos 3 meses (incluindo o atual)
        # min_periods=1: Se não houver 3 meses, usa o que tiver disponível
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    logging.info("Média móvel de 3 meses calculada e adicionada à coluna 'consumo_medio_movel_3m'.")
    return df_consumo


# --- 3. Função para Extrair os Demais Marts ---

def extract_other_marts(engine: create_engine):
    """Extrai os modelos Marts de Ocorrências e Perdas para arquivos CSV."""
    marts = {
        'analise_ocorrencias_tecnicas': 'ocorrencias_analise.csv',
        'analise_perdas_energia': 'perdas_analise.csv'
    }

    for table_name, file_name in marts.items():
        # CORREÇÃO AQUI: Mudança de 'public_analytics' para 'public_public_analytics'
        query = f"SELECT * FROM public_public_analytics.{table_name}"
        df = pd.read_sql(query, engine)
        df.to_csv(file_name, index=False)
        logging.info(f"Modelo {table_name} exportado para {file_name}. Linhas: {len(df)}")


# --- 4. Execução Principal ---
if __name__ == "__main__":
    try:
        logging.info("Iniciando o processo de enriquecimento de dados...")

        # A) Enriquecer e exportar Consumo
        df_consumo_enriched = extract_and_enrich_consumo(engine)
        OUTPUT_FILE_CONSUMO = 'consumo_enriquecido.csv'
        df_consumo_enriched.to_csv(OUTPUT_FILE_CONSUMO, index=False)
        logging.info(f"Dados enriquecidos de Consumo exportados para: {OUTPUT_FILE_CONSUMO}")

        # B) Exportar outros Marts (apenas extração simples)
        extract_other_marts(engine)

        logging.info("Processo de Python concluído. Três arquivos CSV estão prontos para o Power BI.")

    except Exception as e:
        logging.error(f"Ocorreu um erro fatal na extração ou enriquecimento dos dados.")
        logging.error(f"Detalhes: {e}")
        print(
            "\nERRO: Certifique-se de que o PostgreSQL está rodando, o dbt run foi executado e a senha no script está correta.")
