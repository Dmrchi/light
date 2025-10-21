import pandas as pd
from sqlalchemy import create_engine, text
import os
import glob
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DB_URL = 'postgresql://postgres:password@localhost:5432/dblight'
SCHEMA_RAW = 'public'  # Esquema para dados brutos/origem
# Mapeamento dos arquivos CSV para os nomes das tabelas brutas esperadas pelo dbt
FILE_TO_TABLE_MAP = {
    'clientes.csv': 'clientes_bruto',
    'medicoes_energia.csv': 'medicoes_energia_bruto',
    'ocorrencias_tecnicas.csv': 'ocorrencias_tecnicas_bruto',
    'perdas_energia.csv': 'perdas_energia_bruto'
}


def load_data_to_postgres():
    """Conecta ao PostgreSQL, cria os ENUMs e carrega todos os CSVs."""

    try:
        engine = create_engine(DB_URL)

        # 1. GARANTIR A EXISTÊNCIA DOS ENUMS (DDL)
        # CORREÇÃO: Removemos 'IF NOT EXISTS' e usamos um bloco DO $$...$$
        # para checar a existência no catálogo pg_type, tornando o DDL idempotente.
        DDL_COMMANDS = """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tipo_cliente_enum') THEN
                CREATE TYPE tipo_cliente_enum AS ENUM ('Residencial', 'Comercial', 'Industrial');
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'tipo_medicao_enum') THEN
                CREATE TYPE tipo_medicao_enum AS ENUM ('Normal', 'Estimada');
            END IF;
        END
        $$;
        """

        with engine.connect() as connection:
            logging.info("Conexão com o PostgreSQL estabelecida com sucesso.")

            # Executa o bloco DDL condicional
            connection.execute(text(DDL_COMMANDS))
            connection.commit()
            logging.info("Tipos ENUM (tipo_cliente_enum, tipo_medicao_enum) verificados/criados com idempotência.")

        # 2. CARREGAR E INSERIR OS DADOS
        for file_name, table_name in FILE_TO_TABLE_MAP.items():
            if not os.path.exists(file_name):
                logging.warning(f"Arquivo CSV não encontrado: {file_name}. Pulando o carregamento.")
                continue

            # Leitura do CSV
            df = pd.read_csv(file_name)
            logging.info(f"Lido o arquivo: {file_name}. Total de linhas: {len(df)}")

            # Limpeza rápida de nomes de colunas (melhor prática)
            df.columns = [c.lower().replace(' ', '_') for c in df.columns]

            # Inserção no PostgreSQL
            df.to_sql(
                name=table_name,
                con=engine,
                schema=SCHEMA_RAW,
                if_exists='replace',  # Substitui a tabela se já existir (ótimo para testes)
                index=False,  # Não cria índice para o índice do Pandas
                chunksize=10000  # Melhora a performance em grandes volumes
            )
            logging.info(f"Tabela {SCHEMA_RAW}.{table_name} carregada com sucesso.")

        logging.info("Todos os dados brutos foram carregados com sucesso no PostgreSQL.")

    except Exception as e:
        logging.error(f"Erro durante o carregamento dos dados: {e}")
        print(f"ERRO: Não foi possível conectar/carregar. Verifique o DB_URL e se os CSVs estão presentes.")


if __name__ == '__main__':
    load_data_to_postgres()



#import pandas as pd
#from sqlalchemy import create_engine
#import numpy as np

# 1. Configuração da Conexão (Ajuste conforme seu profiles.yml)
# Use o mesmo DBname, User e Senha configurados no dbt profiles.yml
#DB_URL = 'postgresql://root:@localhost:5432/lightdb'
#engine = create_engine(DB_URL)

# 2. Leitura dos CSVs (assumindo que estão na mesma pasta)
#clientes_raw = pd.read_csv('clientes.csv')
#medicoes_raw = pd.read_csv('medicoes_energia.csv')
#ocorrencias_raw = pd.read_csv('ocorrencias_tecnicas.csv')
#perdas_raw = pd.read_csv('perdas_energia.csv')