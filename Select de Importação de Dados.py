import pyodbc
import psycopg2
import psycopg2.extras # Necessário para execute_batch
import os
import logging
import time # Para medir o tempo de execução

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==== CONFIGURAÇÕES DE CONEXÃO (Use Variáveis de Ambiente para Segurança!) ====
# Para um ambiente de produção, é altamente recomendável usar variáveis de ambiente
# ou um sistema de gerenciamento de segredos para armazenar credenciais.
SQL_SERVER = os.getenv('DB_SQL_SERVER', '10.10.0.38')
SQL_DATABASE = os.getenv('DB_SQL_DATABASE', 'JAT_dtbTransporte')
SQL_USER = os.getenv('DB_SQL_USER', 'view_jat')
SQL_PASSWORD = os.getenv('DB_SQL_PASSWORD', 'jat@2024*')
ODBC_DRIVER = os.getenv('ODBC_DRIVER', '{ODBC Driver 17 for SQL Server}') # Verifique o nome exato!

PG_HOST = os.getenv('DB_PG_HOST', '10.10.0.223')
PG_DB = os.getenv('DB_PG_DB', 'JAT')
PG_USER = os.getenv('DB_PG_USER', 'postgres')
PG_PASSWORD = os.getenv('DB_PG_PASSWORD', 'jat@@2026')

# Tamanho do lote para inserções (otimiza performance em grandes volumes de dados)
BATCH_SIZE = 50000

# Nome da tabela de destino no PostgreSQL para os dados de CIOT/Parcela/Manifesto
PG_TABLE_NAME = "gestao_fretes.parcela_CIOT"

# --- Query SQL Server para Extração de Dados (sua nova query complexa) ---
# Esta query extrai informações detalhadas de manifestos, CIOTs e parcelas,
# incluindo cálculos e formatação de datas.
SQL_SERVER_EXTRACTION_QUERY = """
WITH MANIFESTO_INFO AS (
                -- Esta CTE busca as informações principais do manifesto, incluindo vl_Combustivel
                SELECT
                    A.id_Manifesto,
                    A.nr_CIOT,
                    C.ds_TipoVeiculo,
                    F.ds_TipoMovimento,
                    A.vl_Adicional, -- Certifique-se de que vl_Adicional também está aqui se for usado no cálculo
                    A.vl_Combustivel, -- Incluindo vl_Combustivel diretamente do manifesto
                    (A.vl_Entrega + A.vl_Adicional) AS Total_Frete,
                    B.cd_Placa,
                    -- Usamos ROW_NUMBER para pegar o manifesto mais recente por nr_CIOT
                    -- Priorizamos id_Manifesto DESC para o mais alto,
                    -- mas se houver uma coluna de data/hora de criação, seria melhor usá-la (ex: dt_CriacaoManifesto DESC)
                    ROW_NUMBER() OVER (PARTITION BY A.nr_CIOT ORDER BY A.id_Manifesto DESC) AS rn
                FROM tbdManifesto AS A
                INNER JOIN tbdVeiculo AS B ON A.id_Veiculo = B.id_Veiculo
                INNER JOIN tbdTipoVeiculo AS C ON B.id_TipoVeiculo = C.id_TipoVeiculo
                INNER JOIN tbdManifestoMovimento AS D ON A.id_Manifesto = D.id_Manifesto
                INNER JOIN tbdMovimento AS E ON D.id_Movimento = E.id_Movimento
                INNER JOIN tbdTipoMovimento AS F ON E.id_TipoMovimento = F.id_TipoMovimento
                WHERE A.dt_Inclusao >= '2025-01-01' AND nr_CIOT IS NOT NULL AND nr_CIOT <> ''
            ),
            LATEST_MANIFESTO AS (
                -- Seleciona apenas o manifesto mais recente e relevante para cada CIOT
                SELECT
                    id_Manifesto,
                    nr_CIOT,
                    ds_TipoVeiculo,
                    ds_TipoMovimento,
                    cd_Placa,
                    vl_Combustivel, -- Propagando vl_Combustivel
                    Total_Frete
                FROM MANIFESTO_INFO
                WHERE rn = 1
            )
            SELECT
                M.id_Manifesto,
                M.nr_CIOT,
                A.cd_Parcela,
                A.ds_Parcela,
                CONVERT(VARCHAR(10), A.dt_Parcela, 103) AS dt_Parcela, -- Formatado como string DD/MM/AAAA
                M.Total_Frete,
                -- Agora usando M.vl_Combustivel para o cálculo
                CASE
                    WHEN A.ds_Parcela = 'Saldo do Frete' THEN (A.vl_Parcela - ISNULL(M.vl_Combustivel, 0))
                    ELSE A.vl_Parcela
                END AS vl_Parcela,
                M.vl_Combustivel,
                A.cd_Status,
                CONVERT(VARCHAR(10), A.dt_Envio, 103) + ' ' + CONVERT(VARCHAR(8), A.hr_Envio, 108) AS dt_hr_Envio,
                CONVERT(VARCHAR(10), A.dt_Cancelamento, 103) + ' ' + CONVERT(VARCHAR(8), A.hr_Cancelamento, 108) AS dt_hr_Cancelamento,
                CONVERT(VARCHAR(10), A.dt_Exclusao, 103) + ' ' + CONVERT(VARCHAR(8), A.hr_Exclusao, 108) AS dt_hr_Exclusao,
                CONVERT(VARCHAR(10), A.dt_Liberacao, 103) + ' ' + CONVERT(VARCHAR(8), A.hr_Liberacao, 108) AS dt_hr_Liberacao,
                A.ds_Alteracao,
                E.ds_Banco
            FROM tbd_22_CIOTParcela AS A
            LEFT JOIN tbd_22_CIOT AS B ON A.id_CIOT = B.ID_CIOT
            LEFT JOIN LATEST_MANIFESTO AS M ON B.nr_CIOT = M.nr_CIOT
            LEFT JOIN tbdPessoa AS C ON B.id_Motorista = C.id_Pessoa
            LEFT JOIN tbdPessoa AS D ON B.id_Agregado = D.id_Pessoa
            LEFT JOIN tbdPessoaReferenciaBancaria AS E ON B.id_Agregado = E.id_Pessoa
            WHERE B.dt_Abertura >= '2024-01-01' AND M.id_Manifesto IS NOT NULL
            ORDER BY M.id_Manifesto, A.cd_Parcela;
"""

# Definição das colunas de destino e seus tipos no PostgreSQL
# A chave primária composta (id_Manifesto, cd_Parcela) garante a unicidade dos registros.
PG_COLUMN_DEFINITIONS = """
    id_manifesto INT,
    nr_ciot TEXT,
    cd_parcela TEXT,
    ds_parcela TEXT,
    dt_parcela_str TEXT,
    total_frete NUMERIC,
    vl_parcela NUMERIC,
    vl_combustivel NUMERIC,
    cd_status TEXT,
    dt_hr_envio_str TEXT,
    dt_hr_cancelamento_str TEXT,
    dt_hr_exclusao_str TEXT,
    dt_hr_liberacao_str TEXT,
    ds_alteracao TEXT,
    ds_banco TEXT,
    PRIMARY KEY (id_manifesto, cd_parcela)
"""

# Nomes das colunas na ordem exata da sua query SELECT do SQL Server
PG_COLUMN_NAMES = [
    "id_manifesto",
    "nr_ciot",
    "cd_parcela",
    "ds_parcela",
    "dt_parcela_str",
    "total_frete",
    "vl_parcela",
    "vl_combustivel",
    "cd_status",
    "dt_hr_envio_str",
    "dt_hr_cancelamento_str",
    "dt_hr_exclusao_str",
    "dt_hr_liberacao_str",
    "ds_alteracao",
    "ds_banco"
]

def importar_ciot_parcelas_e_manifestos():
    """
    Importa dados detalhados de CIOTs, parcelas e manifestos do SQL Server
    para a tabela gestao_fretes.parcela_CIOT no PostgreSQL.

    A tabela no PostgreSQL tem uma chave primária composta em (id_manifesto, cd_parcela).
    Novos pares são inseridos, e pares existentes são ignorados
    usando ON CONFLICT DO NOTHING.
    """
    sql_conn, pg_conn = None, None
    total_records_processed_from_sql = 0 # Total de registros lidos do SQL Server
    start_time = time.time()

    try:
        # 1. Conectar aos Bancos de Dados
        logging.info(f'Tentando conectar ao PostgreSQL: {PG_HOST}/{PG_DB}...')
        pg_conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
        pg_cursor = pg_conn.cursor()
        logging.info(f'✅ Conectado ao PostgreSQL: {PG_DB}.')

        logging.info(f'Tentando conectar ao SQL Server: {SQL_SERVER}/{SQL_DATABASE}...')
        sql_conn = pyodbc.connect(f'DRIVER={ODBC_DRIVER};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD}')
        sql_cursor = sql_conn.cursor()
        logging.info(f'✅ Conectado ao SQL Server: {SQL_DATABASE}.')

        # 2. Garantir Schema e Tabela no PostgreSQL (Criar se não existir)
        try:
            pg_cursor.execute("CREATE SCHEMA IF NOT EXISTS gestao_fretes;")
            pg_conn.commit()
            logging.info("Schema 'gestao_fretes' garantido no PostgreSQL.")
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao garantir o schema 'gestao_fretes': {e}")
            raise # Re-lança a exceção para interromper a execução

        try:
            pg_cursor.execute(f"SELECT to_regclass('{PG_TABLE_NAME}');")
            table_exists = pg_cursor.fetchone()[0]
            if not table_exists:
                pg_cursor.execute(f"CREATE TABLE {PG_TABLE_NAME} ({PG_COLUMN_DEFINITIONS});")
                pg_conn.commit()
                logging.info(f"Tabela '{PG_TABLE_NAME}' criada, pois não existia.")
            else:
                logging.info(f"Tabela '{PG_TABLE_NAME}' já existe. Prosseguindo com importação (apenas novos registros serão inseridos).")
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao verificar ou criar a tabela '{PG_TABLE_NAME}': {e}")
            raise # Re-lança a exceção para interromper a execução

        # 3. Extração de Dados do SQL Server
        logging.info(f'Executando consulta no SQL Server para buscar registros de CIOT/Parcela/Manifesto...')
        sql_extraction_start_time = time.time()
        try:
            sql_cursor.execute(SQL_SERVER_EXTRACTION_QUERY)
            records_batch = sql_cursor.fetchmany(BATCH_SIZE)
        except pyodbc.Error as e:
            logging.error(f"❌ Erro ao executar consulta no SQL Server: {e}")
            raise # Re-lança a exceção para interromper a execução
        
        sql_extraction_end_time = time.time()
        logging.info(f"Tempo de extração inicial do SQL Server: {sql_extraction_end_time - sql_extraction_start_time:.2f} segundos.")

        if not records_batch:
            logging.warning("Nenhum registro encontrado no SQL Server que atenda aos critérios para importação. Nenhuma inserção será realizada.")
            return # Sai da função se não houver dados

        logging.info(f'Primeiro registro de exemplo do SQL Server: {records_batch[0]}')
        logging.info(f'Número de colunas retornadas: {len(records_batch[0])}')

        if len(PG_COLUMN_NAMES) != len(records_batch[0]):
            logging.error(f"Erro: O número de colunas na sua query SQL ({len(records_batch[0])}) não corresponde ao número de colunas esperadas em PG_COLUMN_NAMES ({len(PG_COLUMN_NAMES)}). Verifique a query SQL e a lista PG_COLUMN_NAMES.")
            return # Sai da função em caso de incompatibilidade de colunas

        # 4. Inserção de Dados no PostgreSQL (em Lotes)
        placeholders = ', '.join(['%s'] * len(PG_COLUMN_NAMES))
        insert_query = f"""
        INSERT INTO {PG_TABLE_NAME} ({', '.join(PG_COLUMN_NAMES)})
        VALUES ({placeholders})
        ON CONFLICT (id_manifesto, cd_parcela) DO NOTHING;
        """
        logging.info(f"Iniciando a importação/alimentação dos dados para o PostgreSQL em lotes de {BATCH_SIZE} (usando ON CONFLICT DO NOTHING)...")

        batch_count = 0
        while records_batch:
            try:
                psycopg2.extras.execute_batch(pg_cursor, insert_query, records_batch, page_size=BATCH_SIZE)
                pg_conn.commit() # Confirma as inserções do lote
                
                # total_records_processed_from_sql conta os registros lidos do SQL Server
                total_records_processed_from_sql += len(records_batch)
                batch_count += 1
                logging.info(f'Lote #{batch_count} processado. {len(records_batch)} registros lidos do SQL Server. Total lido: {total_records_processed_from_sql}.')
            except psycopg2.Error as e_batch:
                logging.error(f"❌ Erro ao processar um lote no PostgreSQL: {e_batch}. Revertendo lote e continuando com o próximo.")
                pg_conn.rollback() # Reverte o lote com erro, mas permite que o script continue
            except Exception as e_general:
                logging.error(f"❗ Erro inesperado ao processar um lote: {e_general}. Revertendo lote e continuando com o próximo.")
                pg_conn.rollback()

            records_batch = sql_cursor.fetchmany(BATCH_SIZE) # Tenta buscar o próximo lote

        pg_conn.commit() # Commit final para quaisquer registros restantes (se houver)
        end_time = time.time()
        logging.info(f'🎉 Importação/Alimentação concluída: {total_records_processed_from_sql} registros lidos do SQL Server. Tempo total: {end_time - start_time:.2f} segundos.')
        logging.info("Para verificar o número exato de registros *inseridos*, compare a contagem de linhas da tabela no PostgreSQL antes e depois da execução do script.")

    except (pyodbc.Error, psycopg2.Error) as db_error:
        logging.error(f'❌ Erro crítico de Banco de Dados durante a execução: {db_error}')
        if pg_conn:
            pg_conn.rollback() # Reverte a transação em caso de erro crítico
            logging.info("Transação PostgreSQL revertida devido a erro crítico.")
    except Exception as e:
        logging.error(f'❗ Ocorreu um erro inesperado e crítico: {e}')
        if pg_conn:
            pg_conn.rollback()
            logging.info("Transação PostgreSQL revertida devido a erro crítico.")
    finally:
        # Fechar conexões e cursores de forma segura
        if sql_cursor:
            sql_cursor.close()
            logging.info('Cursor do SQL Server fechado.')
        if sql_conn:
            sql_conn.close()
            logging.info('Conexão com SQL Server fechada.')
        if pg_cursor:
            pg_cursor.close()
            logging.info('Cursor do PostgreSQL fechado.')
        if pg_conn:
            pg_conn.close()
            logging.info('Conexão com PostgreSQL fechada.')

if __name__ == '__main__':
    logging.info("Iniciando a execução do script de importação para CIOT/Parcela/Manifesto...")
    importar_ciot_parcelas_e_manifestos()
    logging.info("Execução do script finalizada.")