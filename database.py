import logging
import os
import duckdb
import polars as pl
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

# Configuração de logs padrão para auditoria
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WatchdogDatabase")

# Definição dos caminhos locais (Lakehouse Portátil)
DELTA_LAKE_PATH = os.path.abspath("data/delta_lake")

def save_to_delta_lake(df: pl.DataFrame):
    """
    Salva o DataFrame do Polars em formato Delta Lake usando partições.
    Isso confere propriedades ACID e histórico de versões (Time Travel).
    """
    logger.info(f"Salvando {df.shape[0]} registros no Delta Lake local...")
    
    # Certifica que a pasta existe antes de salvar
    os.makedirs(DELTA_LAKE_PATH, exist_ok=True)
    
    # Escreve os dados no Delta Lake particionando por 'status' para otimizar queries analíticas
    write_deltalake(
        table_or_uri=DELTA_LAKE_PATH,
        data=df.to_arrow(),  # Conversão ultra rápida via protocolo Apache Arrow (Zero-Copy)
        mode="append",       # Adiciona novos dados incrementalmente
        partition_by=["status"]
    )
    logger.info(f"Dados persistidos com sucesso em: {DELTA_LAKE_PATH}")

def run_analytical_query() -> pl.DataFrame:
    """
    Conecta o DuckDB diretamente sobre os arquivos Delta Lake para rodar agregar estatísticas em SQL.
    """
    logger.info("Inicializando conexão OLAP in-memory com DuckDB sobre o Delta Lake...")
    
    # Inicializa uma conexão isolada com o DuckDB
    con = duckdb.connect()
    
    # Registra a extensão do parquet (o DuckDB lê o Delta Lake nativamente via metadados parquet)
    query = f"""
        SELECT 
            status,
            COUNT(*) as total_transacoes,
            ROUND(SUM(amount), 2) as volume_total,
            ROUND(AVG(amount), 2) as ticket_medio
        FROM parquet_scan('{DELTA_LAKE_PATH}/**/*.parquet')
        GROUP BY status
        ORDER BY volume_total DESC
    """
    
    # Executa a query OLAP e joga o resultado direto de volta para um DataFrame do Polars
    logger.info("Executando agregação analítica agregada...")
    duckdb_result = con.execute(query).pl()
    
    return duckdb_result

if __name__ == "__main__":
    logger.info("=== TESTANDO ENGINE DE PERSISTÊNCIA & ANALYTICS ===")
    
    # 1. Simula a importação do seu pipeline de ingestão assíncrona
    from ingestion import process_pipeline_cycle
    import asyncio
    
    # Executa um ciclo de captura
    df_dados = asyncio.run(process_pipeline_cycle())
    
    # 2. Salva no Delta Lake
    save_to_delta_lake(df_dados)
    
    # 3. Executa queries com o DuckDB
    df_analítico = run_analytical_query()
    
    print("\n=== KPI'S ANALÍTICOS GERADOS PELO DUCKDB VIA DELTA LAKE ===")
    print(df_analítico)
