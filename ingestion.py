import asyncio
import logging
import random
import time
from datetime import datetime
import polars as pl

# Configuração rigorosa de logs - Padrão de Engenharia de Produção
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("WatchdogIngestion")

async def fetch_transaction_batch(source_id: int) -> list[dict]:
    """
    Simula uma requisição assíncrona de I/O de alta velocidade (API ou Webhook).
    """
    logger.info(f"Iniciando chamada assíncrona para Fonte #{source_id}...")
    
    # Simula latência de rede variável do mundo real
    await asyncio.sleep(random.uniform(0.3, 1.2))
    
    batch_size = random.randint(2500, 7500)
    records = []
    status_options = ["COMPLETED", "PENDING", "FAILED", "SUSPICIOUS"]
    
    for _ in range(batch_size):
        records.append({
            "transaction_id": f"tx_{random.randint(1000000, 9999999)}",
            "account_source": f"acc_{random.randint(1000, 5000)}",
            "account_destination": f"acc_{random.randint(1000, 5000)}",
            "amount": round(random.uniform(5.0, 65000.0), 2),
            "timestamp": datetime.utcnow().isoformat(),
            "status": random.choice(status_options),
            "source_api": source_id
        })
        
    logger.info(f"Fonte #{source_id} retornou {batch_size} registros com sucesso.")
    return records

async def process_pipeline_cycle():
    """
    Orquestra a ingestão paralela concorrente e unifica os lotes usando Polars.
    """
    start_time = time.perf_counter()
    num_sources = 5
    
    logger.info("Disparando requisições assíncronas em paralelo com asyncio.gather...")
    
    # Executa todas as chamadas I/O de rede de forma simultânea
    tasks = [fetch_transaction_batch(i) for i in range(1, num_sources + 1)]
    results = await asyncio.gather(*tasks)
    
    # Achata a lista de locais em uma única estrutura de memória rápida
    all_records = [record for batch in results for record in batch]
    
    logger.info(f"Total de registros capturados em memória: {len(all_records)}")
    
    # PROCESSAMENTO ULTRA RÁPIDO COM POLARS (Eager Mode -> Lazy Evaluation)
    logger.info("Convertendo registros brutos e disparando otimizador de query do Polars...")
    df = pl.DataFrame(all_records)
    
    # Aplica transformações de tipos e filtros em nível de CPU de forma vetorizada
    df_transformed = (
        df.lazy()
        .with_columns([
            pl.col("timestamp").str.to_datetime(),
            (pl.col("amount") * 1.002).alias("amount_with_fee")  # Simulação de taxa operacional
        ])
        .filter(pl.col("amount") > 10.0)  # Descarta transações irrelevantes / ruído
        .collect()
    )
    
    execution_time = time.perf_counter() - start_time
    logger.info(f"Ciclo concluído. Shape final: {df_transformed.shape} | Tempo Total: {execution_time:.4f}s")
    
    return df_transformed

if __name__ == "__main__":
    logger.info("=== DISPARANDO MOTOR ASSÍNCRONO WATCHDOG (POLARS CORE) ===")
    df_final = asyncio.run(process_pipeline_cycle())
    print("\n--- Amostra dos Dados Processados com Polars (Top 5) ---")
    print(df_final.head(5))
