from flask import Flask
import pandas as pd
from sqlalchemy import text
from flask_sqlalchemy import SQLAlchemy
import logging
import random
from flask import Flask, jsonify
from faker import Faker
from datetime import date, timedelta, datetime
import os
import urllib
import calendar
from sqlalchemy.exc import OperationalError, ProgrammingError
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

#Inicializa o Faker aqui, no escopo global
fake = Faker('pt_BR')
DADOS_BASE = {}

MAP_ESTADOS_ID = {
    'BA': '5fc810cf62601df84b7923b9964c53e6',
    'MG': 'ba2a034f4d913f87fe07cad29368d114',
    'SP': '674769e3326f8cf937af4282f2815c02'
}


# --- Lógica de Setup ---
def setup_dimensoes_em_memoria():
    """
    Carrega localizações válidas do DB e tipos de cliente para a memória.
    Esta função AGORA será chamada no início da aplicação.
    """
    global DADOS_BASE
    logging.info("Iniciando setup: Carregando dimensões do BD...")

    sql_localizacoes = text("""
                            SELECT l.id_localizacao,
                                   l.id_estado,
                                   l.cidade,
                                   e.estado AS estado_sigla
                            FROM public_analytics.dim_localizacao AS l
                                     JOIN public_analytics.dim_estado AS e ON l.id_estado = e.id_estado
                            """)

    sql_tipos_cliente = text("""
                             SELECT DISTINCT tipo_cliente
                             FROM public.clientes_bruto
                             WHERE tipo_cliente IS NOT NULL;
                             """)

    try:
        # Abertura de conexão para as operações de leitura
        # Isso funciona porque estaremos dentro do app_context()
        with db.engine.connect() as conn:
            # 1. Carrega as Localizações Válidas do BD
            df_local = pd.read_sql(sql_localizacoes, conn)
            DADOS_BASE['localizacoes'] = df_local.to_dict('records')

            # 2. Carrega os Tipos de Cliente Distintos
            df_tipos = pd.read_sql(sql_tipos_cliente, conn)
            DADOS_BASE['tipos_cliente'] = df_tipos['tipo_cliente'].astype(str).unique().tolist()

            # Data de adesão em 2025
            DADOS_BASE['intervalo_data'] = (date(2025, 1, 1), date(2025, 1, 31))

            # Log de sucesso
            logging.info(
                f"✅ Setup concluído. {len(DADOS_BASE['localizacoes'])} localizações e {len(DADOS_BASE['tipos_cliente'])} tipos de cliente carregados.")

    except (OperationalError, ProgrammingError) as e:
        logging.error(f"ERRO FATAL no Setup: Falha na Conexão ou SQL.")
        logging.error(
            f"   Por favor, verifique se o PostgreSQL está rodando e se as tabelas (dim_localizacao, clientes_bruto) existem.")
        logging.error(f"   Detalhes do Erro: {e}")

        # Fallback de segurança
        DADOS_BASE['localizacoes'] = []
        DADOS_BASE['tipos_cliente'] = ['Comercial', 'Industrial']
        DADOS_BASE['intervalo_data'] = (date(2025, 1, 1), date(2025, 12, 31))

    except Exception as e:
        logging.error(f"ERRO INESPERADO no Setup: {e}")
        DADOS_BASE['localizacoes'] = []
        DADOS_BASE['tipos_cliente'] = ['Comercial', 'Industrial']
        DADOS_BASE['intervalo_data'] = (date(2025, 1, 1), date(2025, 12, 31))


# --- Lógica de Geração ---
def get_proximo_id_cliente(db_instance):
    """
    Busca o MAX(id_cliente) na tabela clientes_bruto e retorna + 1.
    """
    sql_max_id = text("SELECT MAX(id_cliente) AS ultimo_id FROM public.clientes_bruto;")

    try:
        with db_instance.engine.connect() as conn:
            resultado = conn.execute(sql_max_id).scalar()
            proximo_id = (resultado or 0) + 1
            logging.info(f"Próximo ID de cliente: {proximo_id}")
            return proximo_id
    except Exception as e:
        logging.error(f"ERRO ao buscar MAX(id_cliente): {e}")
        return random.randint(900000, 999999)


def criar_cliente_aleatorio(db_instance):
    """
    Cria um cliente combinando dados do Faker (nome, data)
    com dados coerentes do BD (localização, tipo_cliente) e id sequencial.
    """
    if not DADOS_BASE.get('localizacoes'):
        logging.error("Dados base não disponíveis. Setup falhou ou não encontrou localizações.")
        return None

    #1.Busca o ID sequencial
    proximo_id = get_proximo_id_cliente(db_instance)

    #2.Seleção aleatória de localização coerente
    local = random.choice(DADOS_BASE['localizacoes'])
    data_inicio, data_fim = DADOS_BASE['intervalo_data']

    #Gera a data aleatória em 2025
    data_adesao = fake.date_between(start_date=data_inicio, end_date=data_fim).strftime('%Y-%m-%d')

    novo_cliente = {
        "id_cliente": proximo_id,
        "id_localizacao": local['id_localizacao'],
        "id_estado": local['id_estado'],
        "nome_cliente": fake.name(),
        "cidade": local['cidade'],
        "estado_sigla": local['estado_sigla'],
        "tipo_cliente": random.choice(DADOS_BASE['tipos_cliente']),
        "data_adesao": data_adesao,
    }
    return novo_cliente

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


@app.route('/test-cliente-faker', methods=['GET'])
def get_cliente_faker():
    """Endpoint que retorna um cliente aleatório com integridade de localização."""
    cliente = criar_cliente_aleatorio(db)

    if cliente is None:
        return jsonify({"erro": "Falha na geração. O setup do DB (setup_dimensoes_em_memoria) pode ter falhado."}, 500)

    return jsonify(cliente)


@app.route('/adicionar-cliente-faker', methods=['GET'])
def adicionar_cliente_faker():
    """
    Cria um cliente aleatório E salva na tabela public.clientes_bruto.
    """
    # 1. Gera o cliente da mesma forma
    cliente_data = criar_cliente_aleatorio(db)

    if cliente_data is None:
        return jsonify({"erro": "Falha na geração. O setup do DB (setup_dimensoes_em_memoria) pode ter falhado."}, 500)

    # 2. Mapeia o dicionário para a tabela clientes_bruto (baseado na sua screenshot)
    #    - Remove id_localizacao/id_estado (que não estão em clientes_bruto)
    #    - Renomeia estado_sigla -> estado
    dados_para_inserir = {
        "id_cliente": cliente_data["id_cliente"],
        "nome_cliente": cliente_data["nome_cliente"],
        "cidade": cliente_data["cidade"],
        "estado": cliente_data["estado_sigla"],  # Renomeia a chave
        "tipo_cliente": cliente_data["tipo_cliente"],
        "data_adesao": cliente_data["data_adesao"]
    }

    # 3. SQL para inserção
    #    Usamos os nomes das colunas da tabela clientes_bruto
    sql_insert = text("""
                      INSERT INTO public.clientes_bruto
                          (id_cliente, nome_cliente, cidade, estado, tipo_cliente, data_adesao)
                      VALUES (:id_cliente, :nome_cliente, :cidade, :estado, :tipo_cliente, :data_adesao)
                      """)

    try:
        # 4. Executa a inserção dentro de uma transação
        #    db.session.begin() cuida do commit (se sucesso) e rollback (se erro)
        with db.session.begin():
            db.session.execute(sql_insert, dados_para_inserir)

        logging.info(f"Cliente {dados_para_inserir['id_cliente']} inserido com sucesso.")
        return jsonify({
            "status": "SUCESSO",
            "mensagem": "Cliente fake inserido na tabela clientes_bruto.",
            "cliente_inserido": dados_para_inserir
        })

    except Exception as e:
        logging.error(f"ERRO ao inserir cliente fake: {e}")
        return jsonify({
            "status": "FALHA",
            "mensagem": "Erro ao salvar cliente no banco de dados.",
            "detalhe": str(e)
        }, 500)


@app.route('/teste-conexao', methods=['GET'])
def teste_conexao():
    """
    Endpoint de diagnóstico que consulta o último registro da tabela clientes_bruto
    para testar a conexão com o PostgreSQL e a existência da tabela.
    """
    logging.info("Iniciando teste de conexão com o PostgreSQL...")
    sql_ultimo_registro = text("SELECT * FROM public.clientes_bruto ORDER BY id_cliente DESC LIMIT 1;")

    try:
        with db.engine.connect() as conn:
            resultado = pd.read_sql(sql_ultimo_registro, conn)

            if resultado.empty:
                return jsonify({
                    "status": "SUCESSO (Tabela Vazia)",
                    "mensagem": "Conexão com o PostgreSQL bem-sucedida, mas a tabela public.clientes_bruto não contém registros."
                })

            # Converte para dict (precisa converter tipos de dados do pandas/db)
            ultimo_registro = resultado.iloc[0].to_dict()
            # Converte tipos não-serializáveis (como datas, decimais, etc) para string
            ultimo_registro_serializado = {k: str(v) for k, v in ultimo_registro.items()}

            logging.info("Conexão e consulta de teste bem-sucedidas.")
            return jsonify({
                "status": "SUCESSO",
                "mensagem": "Conexão com o PostgreSQL e consulta à tabela public.clientes_bruto OK.",
                "ultimo_registro": ultimo_registro_serializado
            })

    except Exception as e:
        logging.error(f"ERRO DE CONEXÃO/CONSULTA: {e}")
        mensagem_erro = str(e)
        detalhe = f"Erro de banco de dados: {mensagem_erro}"

        if "fe_sendauth" in mensagem_erro or "password" in mensagem_erro:
            detalhe = "Verifique as credenciais (db_user, db_pass)."
        elif "could not connect to server" in mensagem_erro:
            detalhe = "Verifique se o host (db_host) e a porta (db_port) estão corretos e se o PostgreSQL está rodando."
        elif "relation" in mensagem_erro and "does not exist" in mensagem_erro:
            detalhe = "A tabela 'public.clientes_bruto' (ou outra consultada no setup) não existe."

        return jsonify({
            "status": "FALHA NA CONEXÃO",
            "mensagem": "Não foi possível conectar ao banco de dados ou a tabela não existe.",
            "detalhe": detalhe
        }, 500)


@app.route('/gerar-medicoes-lote', methods=['POST'])
def gerar_medicoes_lote():
    """
    Gera e insere dados mock em lote para a tabela medicoes_energia_bruto
    para clientes de 1001 a 1050 (9 meses cada).
    Usa o db.session do Flask-SQLAlchemy.
    """
    logging.info("Iniciando geração de medições em lote...")

    dados_para_inserir = []

    CLIENTE_ID_INICIAL = 1001
    CLIENTE_ID_FINAL = 1050
    MES_INICIAL = 2
    MES_FINAL = 10

    try:
        # --- CORREÇÃO AQUI ---
        # Inicia a transação ANTES de qualquer operação de DB
        with db.session.begin():

            # 1. Buscar o último id_medicao da tabela
            sql_max_id = text("SELECT MAX(id_medicao) FROM public.medicoes_energia_bruto")
            resultado = db.session.execute(sql_max_id).scalar()

            proximo_id_medicao = (resultado or 0) + 1

            logging.info(f"Iniciando geração. Próximo id_medicao: {proximo_id_medicao}")

            # 2. Gerar os dados mock em memória
            for id_cliente in range(CLIENTE_ID_INICIAL, CLIENTE_ID_FINAL + 1):
                for mes in range(MES_INICIAL, MES_FINAL + 1):
                    data_medicao_str = f"2025-{mes:02d}-01"
                    consumo_kwh_val = round(fake.pyfloat(min_value=600, max_value=3000, right_digits=2), 2)

                    dados_para_inserir.append({
                        "id_medicao": proximo_id_medicao,
                        "id_cliente": id_cliente,
                        "data_medicao": data_medicao_str,
                        "consumo_kwh": consumo_kwh_val,
                        "tipo_medicao": "Normal"
                    })
                    proximo_id_medicao += 1

            # 3. Inserir todos os dados em lote
            if not dados_para_inserir:
                logging.warning("Nenhum dado foi gerado para inserção.")
                # Retornar aqui não causará problemas,
                # o 'with' fará um rollback automático da transação vazia.
                return jsonify({"status": "aviso", "mensagem": "Nenhum dado foi gerado."}), 200

            logging.info(f"Pronto para inserir {len(dados_para_inserir)} registros em lote...")

            sql_insert = text("""
                              INSERT INTO public.medicoes_energia_bruto
                                  (id_medicao, id_cliente, data_medicao, consumo_kwh, tipo_medicao)
                              VALUES (:id_medicao, :id_cliente, :data_medicao, :consumo_kwh, :tipo_medicao)
                              """)

            # Executa a inserção DENTRO do mesmo 'with'
            db.session.execute(sql_insert, dados_para_inserir)

        # FIM DO 'with db.session.begin()'
        # Se chegou aqui, o commit foi feito automaticamente.

        total_inserido = len(dados_para_inserir)
        logging.info(f"Sucesso! {total_inserido} registros inseridos.")

        return jsonify({
            "status": "sucesso",
            "registros_inseridos": total_inserido,
            "ultimo_id_medicao_inserido": proximo_id_medicao - 1
        }), 201

    except Exception as e:
        # O 'with db.session.begin()' faz o rollback automaticamente em caso de exceção.
        logging.error(f"Erro na operação de inserção em lote: {e}")
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route('/gerar-medicoes-lote-nov-dez', methods=['POST'])
def gerar_medicoes_lote_nov_dez():
    """
    Gera e insere dados mock (NOV e DEZ) para a tabela medicoes_energia_bruto
    para clientes de 1001 a 1050.
    """
    logging.info("Iniciando geração de medições em lote (Nov/Dez)...")

    dados_para_inserir = []

    # --- Regras de Negócio (Nov/Dez) ---
    CLIENTE_ID_INICIAL = 1001
    CLIENTE_ID_FINAL = 1050
    MESES_PARA_GERAR = [11, 12]  # Apenas Novembro e Dezembro
    ANO_GERACAO = 2025
    CONSUMO_MIN = 400.0
    CONSUMO_MAX = 2000.0
    TIPO_MEDICAO_FIXO = "Estimada"  # Novo tipo
    # ------------------------------------

    try:
        # Inicia a transação única para SELECT MAX e INSERT
        with db.session.begin():

            # 1. Buscar o último id_medicao da tabela
            sql_max_id = text("SELECT MAX(id_medicao) FROM public.medicoes_energia_bruto")
            resultado = db.session.execute(sql_max_id).scalar()

            proximo_id_medicao = (resultado or 0) + 1

            logging.info(f"Iniciando geração (Nov/Dez). Próximo id_medicao: {proximo_id_medicao}")

            # 2. Gerar os dados mock em memória
            # 50 usuários (1001 a 1050)
            for id_cliente in range(CLIENTE_ID_INICIAL, CLIENTE_ID_FINAL + 1):
                # 2 meses (11 e 12)
                for mes in MESES_PARA_GERAR:
                    data_medicao_str = f"{ANO_GERACAO}-{mes:02d}-01"

                    # Usa o 'fake' global com a nova faixa de consumo
                    consumo_kwh_val = round(fake.pyfloat(min_value=CONSUMO_MIN, max_value=CONSUMO_MAX, right_digits=2),
                                            2)

                    dados_para_inserir.append({
                        "id_medicao": proximo_id_medicao,
                        "id_cliente": id_cliente,
                        "data_medicao": data_medicao_str,
                        "consumo_kwh": consumo_kwh_val,
                        "tipo_medicao": TIPO_MEDICAO_FIXO
                    })

                    # OBRIGATÓRIO: Incrementa o ID para o *próximo registro*
                    proximo_id_medicao += 1

            # 3. Inserir todos os dados em lote
            if not dados_para_inserir:
                logging.warning("Nenhum dado (Nov/Dez) foi gerado para inserção.")
                return jsonify({"status": "aviso", "mensagem": "Nenhum dado foi gerado."}), 200

            total_a_inserir = len(dados_para_inserir)  # Deve ser 100 (50 clientes * 2 meses)
            logging.info(f"Pronto para inserir {total_a_inserir} registros (Nov/Dez) em lote...")

            sql_insert = text("""
                              INSERT INTO public.medicoes_energia_bruto
                                  (id_medicao, id_cliente, data_medicao, consumo_kwh, tipo_medicao)
                              VALUES (:id_medicao, :id_cliente, :data_medicao, :consumo_kwh, :tipo_medicao)
                              """)

            # Executa a inserção DENTRO do mesmo 'with'
            db.session.execute(sql_insert, dados_para_inserir)

        # FIM DO 'with db.session.begin()' - Commit automático

        logging.info(f"Sucesso! {total_a_inserir} registros (Nov/Dez) inseridos.")

        return jsonify({
            "status": "sucesso",
            "registros_inseridos": total_a_inserir,
            "ultimo_id_medicao_inserido": proximo_id_medicao - 1
        }), 201

    except Exception as e:
        # Rollback automático em caso de exceção
        logging.error(f"Erro na operação de inserção em lote (Nov/Dez): {e}")
        return jsonify({"status": "erro", "mensagem": str(e)}), 500


@app.route('/gerar-perdas-lote-jan-jul', methods=['POST'])
def gerar_perdas_lote_jan_jul():
    """
    Gera e insere dados mock de PERDAS (Jan a Jul/2025) para BA, SP, MG.
    """
    logging.info("Iniciando geração de PERDAS de energia em lote (Jan-Jul)...")

    dados_para_inserir = []

    # --- Regras de Negócio (Perdas) ---
    ESTADOS_ALVO = ['BA', 'SP', 'MG']
    MES_INICIAL = 1
    MES_FINAL = 7
    ANO_GERACAO = 2025
    # ------------------------------------

    try:
        # Inicia a transação única para SELECT MAX e INSERT
        with db.session.begin():

            # 1. Buscar o último id_perda
            sql_max_id = text("SELECT MAX(id_perda) FROM public.perdas_energia_bruto")
            resultado = db.session.execute(sql_max_id).scalar()

            # Define o próximo ID, tratando caso a tabela esteja vazia
            proximo_id_perda = (resultado or 0) + 1

            logging.info(f"Iniciando geração de Perdas. Próximo id_perda: {proximo_id_perda}")

            # 2. Gerar os dados mock em memória
            # Loop pelos 3 estados
            for estado in ESTADOS_ALVO:
                # Loop pelos 7 meses (1 ao 7)
                for mes in range(MES_INICIAL, MES_FINAL + 1):
                    # Gera uma data com dia aleatório dentro do mês/ano corretos
                    primeiro_dia_mes = date(ANO_GERACAO, mes, 1)
                    # Pega o número do último dia do mês (ex: 31, 28, 31, 30...)
                    ultimo_dia_num = calendar.monthrange(ANO_GERACAO, mes)[1]
                    ultimo_dia_mes = date(ANO_GERACAO, mes, ultimo_dia_num)

                    # Usa o Faker para pegar uma data aleatória nesse intervalo
                    data_perda_obj = fake.date_between_dates(date_start=primeiro_dia_mes, date_end=ultimo_dia_mes)
                    data_perda_str = data_perda_obj.strftime('%Y-%m-%d')

                    # Gera perdas com 'random.randint' (pois 'random' já está importado no seu código)
                    perda_tec = random.randint(500, 1100)
                    perda_nao_tec = random.randint(400, 900)

                    dados_para_inserir.append({
                        "id_perda": proximo_id_perda,
                        "data_perda": data_perda_str,
                        "estado": estado,
                        "perda_tecnica_kwh": perda_tec,
                        "perda_nao_tecnica_kwh": perda_nao_tec
                    })

                    # OBRIGATÓRIO: Incrementa o ID para o *próximo registro*
                    proximo_id_perda += 1

            # 3. Inserir todos os dados em lote
            # Total deve ser 21 (3 estados * 7 meses)
            total_a_inserir = len(dados_para_inserir)
            if total_a_inserir == 0:
                logging.warning("Nenhum dado de Perda (Jan-Jul) foi gerado.")
                return jsonify({"status": "aviso", "mensagem": "Nenhum dado foi gerado."}), 200

            logging.info(f"Pronto para inserir {total_a_inserir} registros (Perdas Jan-Jul) em lote...")

            # SQL baseado no seu exemplo de INSERT
            sql_insert = text("""
                              INSERT INTO public.perdas_energia_bruto
                              (id_perda, data_perda, estado, perda_tecnica_kwh, perda_nao_tecnica_kwh)
                              VALUES (:id_perda, :data_perda, :estado, :perda_tecnica_kwh, :perda_nao_tecnica_kwh)
                              """)

            # Executa a inserção DENTRO do mesmo 'with'
            db.session.execute(sql_insert, dados_para_inserir)

        # FIM DO 'with db.session.begin()' - Commit automático

        logging.info(f"Sucesso! {total_a_inserir} registros (Perdas Jan-Jul) inseridos.")

        return jsonify({
            "status": "sucesso",
            "registros_inseridos": total_a_inserir,
            "ultimo_id_perda_inserido": proximo_id_perda - 1
        }), 201

    except Exception as e:
        # Rollback automático em caso de exceção
        logging.error(f"Erro na operação de inserção em lote (Perdas Jan-Jul): {e}")
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

if __name__ == '__main__':
    with app.app_context():
        logging.info("Executando setup inicial da aplicação...")
        setup_dimensoes_em_memoria()
    app.run(debug=True, use_reloader=False)

@app.before_request
def garantir_dados_carregados():
    if not DADOS_BASE.get('localizacoes'):
        logging.info("DADOS_BASE vazio — executando setup_dimensoes_em_memoria() novamente...")
        setup_dimensoes_em_memoria()