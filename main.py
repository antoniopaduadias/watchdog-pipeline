import asyncio
import logging
import time
import sys
import os

# Adiciona o diretório 'src' ao path para evitar erros de importação local
sys.path.append(os.path.abspath("src"))

from ingestion import process_pipeline_cycle
from quality import DataQualityWatchdog
from database import save_to_delta_lake, run_analytical_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("WatchdogOrchestrator")

async def run_pipeline_loop(iterations: int = 3):
    """
    Orquestra os componentes em uma execução sequencial e cíclica simulando produção.
    """
    logger.info("=== INICIALIZANDO ORQUESTRADOR CENTRAL WATCHDOG ===")
    
    for cycle in range(1, iterations + 1):
        logger.info(f"\n--- INICIANDO CICLO DE PROCESSAMENTO #{cycle} ---")
        
        # Passo 1: Ingestão Assíncrona e Paralela via Polars
        df_raw = await process_pipeline_cycle()
        
        # Passo 2: Data Quality Gateway
        dq = DataQualityWatchdog(df_raw)
        if not dq.validate_schema():
            logger.error(f"Ciclo #{cycle} abortado por quebra catastrófica de schema.")
            continue
            
        df_clean = dq.check_business_rules()
        
        # Passo 3: Armazenamento em Delta Lake de Alta Performance
        save_to_delta_lake(df_clean)
        
        # Passo 4: Motor de Analytics OLAP via DuckDB
        df_metrics = run_analytical_query()
        
        print(f"\n[Métricas Atualizadas do Lakehouse - Ciclo {cycle}]")
        print(df_metrics)
        
        # Intervalo entre os ciclos de streaming em lote
        if cycle < iterations:
            logger.info("Aguardando 5 segundos para o próximo ciclo de transações...")
            await asyncio.sleep(5)
            
    logger.info("\n=== EXECUÇÃO DO ORQUESTRADOR CONCLUÍDA COM SUCESSO ===")

if __name__ == "__main__":
    # Dispara a execução principal
    asyncio.run(run_pipeline_loop(iterations=3))
