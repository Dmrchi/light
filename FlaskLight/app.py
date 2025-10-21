from flask import Flask
import pandas as pd
from sqlalchemy import text
from flask_sqlalchemy import SQLAlchemy
import logging
import urllib
app = Flask(__name__)

db_user = 'postgres'
db_pass = 'password'
db_host = 'localhost'
db_name = 'dblight'
db_port = '5432'

app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
logging.basicConfig(level=logging.INFO)
@app.route('/')
def index():
    return (
        "<h1>Aplicação Flask de Preparação de Dados!</h1>"
        "<p>Acesse /adicionar-localizacao-cidade-estado para adicionar cidade_estado à tabela de localização.</p>"
    )


@app.route('/adicionar-localizacao-cidade-estado')
def preparar_dim_localizacao():
    """
    Esta rota altera a tabela 'public_analytics.dim_localizacao' para:
    1. Adicionar a coluna 'chave_estado_cidade_key' (se não existir).
    2. Preencher esta coluna com a junção de 'cidade' e 'estado' (ex: 'Mendes_RJ').
    """
    logging.info("Iniciando preparação da 'dim_localizacao'...")

    # SQL (PostgreSQL) para adicionar a coluna.
    # 'IF NOT EXISTS' torna a operação segura para rodar múltiplas vezes.
    sql_alter_table = text("""
                           ALTER TABLE public_analytics.dim_localizacao
                               ADD COLUMN IF NOT EXISTS chave_estado_cidade_key VARCHAR (255);
                           """)

    # SQL (PostgreSQL) para preencher a nova coluna.
    # Usamos a sintaxe UPDATE...FROM... para fazer a junção.
    sql_update_data = text("""
                           UPDATE public_analytics.dim_localizacao AS l
                           SET chave_estado_cidade_key = l.cidade || '_' || e.estado FROM public_analytics.dim_estado AS e
                           WHERE l.id_estado = e.id_estado;
                           """)

    try:
        logging.info("Executando: ALTER TABLE...")
        db.session.execute(sql_alter_table)

        logging.info("Executando: UPDATE... FROM...")
        resultado = db.session.execute(sql_update_data)

        db.session.commit()

        linhas_afetadas = resultado.rowcount

        logging.info(f"Preparação concluída! {linhas_afetadas} linhas foram atualizadas.")

        return (
            f"<h1>Preparação Concluída!</h1>"
            f"<p>A tabela <strong>public_analytics.dim_localizacao</strong> foi preparada com sucesso.</p>"
            f"<p>{linhas_afetadas} linhas tiveram a coluna 'chave_estado_cidade_key' preenchida.</p>"
        )

    except Exception as e:
        db.session.rollback()
        logging.error(f"ERRO NA PREPARAÇÃO: {e}")
        return f"<h1>Ocorreu um erro ao preparar a tabela</h1><p>{e}</p>"


@app.route('/processar-cidades-sinalizar-duplicadas')
def processar_etl_completo():
    """
    Processo de ETL Não-Destrutivo (Melhor Prática):
    1. CRIA 'dim_localizacao_suja_com_status' (cópia + flags 'VALID'/'DUPLICATE_ERROR').
    2. CRIA a VIEW 'dim_localizacao_limpa' (filtrando apenas os 'VALID').
    3. ENRIQUECE 'clientes_bruto' usando a VIEW limpa e salva em 'clientes_enriquecido'.
    4. NÃO MODIFICA as tabelas originais.
    """

    # --- ETAPA 1: CRIAR A TABELA 'SUJA' COM STATUS (SQL) ---
    logging.info("ETAPA 1: Criando a tabela 'dim_localizacao_suja_com_status'...")

    # Este SQL é mais eficiente: Cria a tabela nova já com toda a lógica.
    sql_etapa_1 = text("""
                       -- 1. Apaga a tabela antiga, se existir
                       DROP TABLE IF EXISTS public_analytics.dim_localizacao_suja_com_status;

                       -- 2. Cria a nova tabela 'suja' com a lógica de 'flagging'
                       CREATE TABLE public_analytics.dim_localizacao_suja_com_status AS
                       WITH ranked_locations AS (
                           -- Junta a localização com o estado
                           SELECT l.*, -- Pega todas as colunas originais de 'l' (dim_localizacao)
                                  e.estado,
                                  -- Cria a chave composta
                                  l.cidade || '_' || e.estado AS chave_estado_cidade_key_calc,
                                  -- A 'Regra de Sobrevivência': Acha o primeiro (rn = 1) de cada grupo
                                  ROW_NUMBER()                   OVER(
                    PARTITION BY l.cidade, e.estado 
                    ORDER BY l.id_localizacao -- O "primeiro" é o que tem o menor/primeiro ID
                ) as rn
                           FROM public_analytics.dim_localizacao AS l
                                    JOIN
                                public_analytics.dim_estado AS e ON l.id_estado = e.id_estado)
                       -- 3. Seleciona as colunas para a tabela final
                       SELECT id_localizacao,
                              id_estado,
                              cidade,
                              estado, -- Adicionando o nome do estado
                              chave_estado_cidade_key_calc AS chave_estado_cidade_key,
                              -- Cria a coluna de Data Quality (DQ)
                              CASE
                                  WHEN rn = 1 THEN 'VALID'
                                  ELSE 'DUPLICATE_ERROR'
                                  END                      AS dq_status
                       FROM ranked_locations;
                       """)

    try:
        with db.session.begin():  # Inicia uma transação
            db.session.execute(sql_etapa_1)
        logging.info("ETAPA 1 concluída. Tabela 'dim_localizacao_suja_com_status' criada.")
    except Exception as e:
        db.session.rollback()
        logging.error(f"ERRO na ETAPA 1: {e}")
        return f"<h1>Ocorreu um erro na Etapa 1 (Criação da Tabela Suja)</h1><p>{e}</p>"

    # --- ETAPA 2: CRIAR A VIEW 'LIMPA' (SQL) ---
    logging.info("ETAPA 2: Criando a VIEW 'dim_localizacao_limpa'...")

    # Uma VIEW é um filtro salvo. Não ocupa espaço e está sempre atualizada.
    sql_etapa_2 = text("""
                       CREATE
                       OR REPLACE VIEW public_analytics.dim_localizacao_limpa AS
                       SELECT *
                       FROM public_analytics.dim_localizacao_suja_com_status
                       WHERE dq_status = 'VALID';
                       """)

    try:
        with db.session.begin():
            db.session.execute(sql_etapa_2)
        logging.info("ETAPA 2 concluída. VIEW 'dim_localizacao_limpa' criada.")
    except Exception as e:
        db.session.rollback()
        logging.error(f"ERRO na ETAPA 2: {e}")
        return f"<h1>Ocorreu um erro na Etapa 2 (Criação da VIEW)</h1><p>{e}</p>"

    # --- ETAPA 3: ENRIQUECER OS CLIENTES (Pandas) ---
    logging.info("ETAPA 3: Enriquecendo a tabela de clientes...")

    try:
        query_clientes = text("SELECT * FROM public.clientes_bruto")
        # Lemos da nova VIEW limpa
        query_local_limpa = text("SELECT * FROM public_analytics.dim_localizacao_limpa")

        with db.engine.connect() as conn:
            df_clientes = pd.read_sql(query_clientes, conn)
            df_local_limpa = pd.read_sql(query_local_limpa, conn)

        # Prepara a chave na tabela de clientes
        df_clientes['chave_estado_cidade_key'] = (
                df_clientes['cidade'].fillna('') + "_" + df_clientes['estado'].fillna('')
        )

        # Enriquece os clientes usando a VIEW limpa
        df_clientes_enriquecido = pd.merge(
            df_clientes,
            # Pegamos o 'id_localizacao' da VIEW limpa
            df_local_limpa[['id_localizacao', 'chave_estado_cidade_key']],
            on='chave_estado_cidade_key',
            how='left'
        )

        # Salva o resultado final em uma nova tabela
        nome_tabela_nova = 'clientes_enriquecido'
        schema_nome = 'public'

        df_clientes_enriquecido.to_sql(
            nome_tabela_nova,
            db.engine,
            schema=schema_nome,
            if_exists='replace',
            index=False
        )

        logging.info("ETAPA 3 concluída. Tabela 'clientes_enriquecido' criada.")

        return (
            f"<h1>ETL Completo!</h1>"
            f"<p>1. Tabela 'dim_localizacao_suja_com_status' criada com flags 'VALID'/'DUPLICATE_ERROR'.</p>"
            f"<p>2. VIEW 'dim_localizacao_limpa' criada (filtrando 'VALID').</p>"
            f"<p>3. Tabela 'clientes_enriquecido' criada e pronta para o Power BI.</p>"
            f"<p><b>SUAS TABELAS ORIGINAIS NÃO FORAM ALTERADAS.</b></p>"
        )

    except Exception as e:
        logging.error(f"ERRO na ETAPA 3: {e}")
        return f"<h1>Ocorreu um erro na Etapa 3 (Enriquecimento)</h1><p>{e}</p>"


@app.route('/remover-duplicatas')
def remover_duplicatas():
    """
    Processo DESTRUTIVO: Remove permanentemente os registros duplicados
    (aqueles que não são a 'primeira' ocorrência física) da tabela 'dim_localizacao'.
    Usa o 'ctid' do PostgreSQL para garantir a remoção precisa.
    """
    logging.warning("Iniciando processo DESTRUTIVO de remoção de duplicatas...")

    # SQL para identificar e apagar as linhas duplicadas usando o identificador de linha físico 'ctid'.
    sql_delete_duplicates = text("""
                                 DELETE
                                 FROM public_analytics.dim_localizacao
                                 WHERE ctid IN ( -- Usa o identificador de linha físico e único 'ctid'
                                     -- Subconsulta para encontrar os 'ctid' de todas as duplicatas
                                     SELECT ctid
                                     FROM (SELECT l.ctid, -- Seleciona o 'ctid' para identificar a linha física
                                                  -- A mesma lógica de 'ranking', mas agora ordenada por 'ctid' para ser determinística
                                                  ROW_NUMBER() OVER(
                        PARTITION BY TRIM(LOWER(l.cidade)), TRIM(LOWER(e.estado)), l.id_localizacao
                        ORDER BY l.ctid
                    ) as rn
                                           FROM public_analytics.dim_localizacao AS l
                                                    JOIN
                                                public_analytics.dim_estado AS e ON l.id_estado = e.id_estado) AS ranked_subquery
                                     -- O alvo do DELETE são todos os que não são a primeira linha física (rn > 1)
                                     WHERE rn > 1);
                                 """)

    try:
        with db.session.begin():
            logging.info("Executando: DELETE para remover duplicatas usando ctid...")
            resultado = db.session.execute(sql_delete_duplicates)

        linhas_afetadas = resultado.rowcount
        logging.info(f"Remoção concluída. {linhas_afetadas} linhas duplicadas foram apagadas.")
        return (
            f"<h1>Remoção Concluída!</h1>"
            f"<p><b>{linhas_afetadas}</b> linhas duplicadas foram permanentemente apagadas "
            f"da tabela 'public_analytics.dim_localizacao'.</p>"
            f"<p>A tabela agora está limpa e sem duplicatas físicas.</p>"
        )

    except Exception as e:
        db.session.rollback()
        logging.error(f"ERRO na remoção de duplicatas: {e}")
        return f"<h1>Ocorreu um erro ao remover duplicatas</h1><p>{e}</p>"


@app.route('/sincronizar-status')
def sincronizar_status():
    """
    Script focado em atualizar a 'dim_localizacao' com o 'dq_status'
    calculado na 'dim_localizacao_suja_com_status', usando o ID como chave.
    """
    logging.info("Iniciando a sincronização do DQ_Status...")

    # PostgreSQL para adicionar a coluna de status na tabela original, se não existir.
    sql_alter_table = text("""
                           ALTER TABLE public_analytics.dim_localizacao
                               ADD COLUMN IF NOT EXISTS dq_status VARCHAR (50);
                           """)

    # PostgreSQL para atualizar a tabela original com base na tabela 'suja'.
    sql_update_status = text("""
                             UPDATE public_analytics.dim_localizacao AS original
                             SET dq_status = suja.dq_status FROM 
            public_analytics.dim_localizacao_suja_com_status AS suja
                             WHERE
                                 original.id_localizacao = suja.id_localizacao;
                             """)

    try:
        with db.session.begin():  # Garante que as operações sejam atômicas
            logging.info("Executando: ALTER TABLE para garantir que a coluna 'dq_status' exista...")
            db.session.execute(sql_alter_table)

            logging.info("Executando: UPDATE para sincronizar o status...")
            resultado = db.session.execute(sql_update_status)

        linhas_afetadas = resultado.rowcount
        logging.info(f"Sincronização concluída. {linhas_afetadas} linhas foram atualizadas.")
        return (
            f"<h1>Sincronização Concluída!</h1>"
            f"<p>A coluna 'dq_status' na tabela 'public_analytics.dim_localizacao' "
            f"foi atualizada com sucesso para {linhas_afetadas} linhas.</p>"
        )

    except Exception as e:
        db.session.rollback()
        logging.error(f"ERRO na sincronização: {e}")
        return f"<h1>Ocorreu um erro na sincronização</h1><p>{e}</p>"
@app.route('/listar-duplicatas')
def listar_duplicatas():
    """
    Endpoint GET para listar todos os registros da dimensão de localização
    que foram sinalizados como 'DUPLICATE_ERROR' pelo processo de ETL.
    """
    logging.info("Requisição recebida para /listar-duplicatas...")

    # SQL para selecionar apenas os registros "sujos"
    # Nós ordenamos pela chave e pelo ID para que você possa ver
    # os grupos de duplicatas juntos.
    sql_query_duplicatas = text("""
                                SELECT *
                                FROM public_analytics.dim_localizacao_suja_com_status
                                WHERE dq_status = 'DUPLICATE_ERROR'
                                ORDER BY chave_estado_cidade_key, id_localizacao;
                                """)

    try:
        # Usamos o Pandas para ler o SQL e converter para HTML
        with db.engine.connect() as conn:
            df_duplicatas = pd.read_sql(sql_query_duplicatas, conn)

        if df_duplicatas.empty:
            logging.info("Nenhuma duplicata encontrada.")
            return (
                "<h1>Nenhum Registro Duplicado Encontrado</h1>"
                "<p>A tabela 'dim_localizacao_suja_com_status' não contém registros com o status 'DUPLICATE_ERROR'.</p>"
            )

        logging.info(f"Encontradas {len(df_duplicatas)} duplicatas. Exibindo...")

        # Converte o DataFrame do Pandas em uma tabela HTML bonita
        html_table = df_duplicatas.to_html(index=False, classes='table table-striped', border=1)

        return (
            f"<h1>Lista de Localizações Duplicadas (Sinalizadas como 'DUPLICATE_ERROR')</h1>"
            f"<p>Estes são os registros que foram identificados como duplicatas e <b>não</b> estão sendo usados no dashboard final.</p>"
            f"<hr>{html_table}"
        )

    except Exception as e:
        # Isso vai acontecer se a tabela ainda não tiver sido criada
        logging.error(f"ERRO ao listar duplicatas: {e}")
        return (
            f"<h1>Ocorreu um erro ao buscar duplicatas</h1>"
            f"<p>{e}</p>"
            f"<p><b>Dica:</b> Você já rodou a rota <a href='/processar-etl-completo'>/processar-etl-completo</a> pelo menos uma vez?</p>"
        )
if __name__ == '__main__':
    app.run(debug=True)