import logging
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("WatchdogQuality")

class DataQualityWatchdog:
    def __init__(self, df: pl.DataFrame):
        self.df = df

    def validate_schema(self) -> bool:
        """
        Garante que todas as colunas obrigatórias estão presentes no lote.
        """
        required_columns = {
            "transaction_id", "account_source", "account_destination", 
            "amount", "timestamp", "status", "source_api"
        }
        current_columns = set(self.df.columns)
        missing = required_columns - current_columns
        
        if missing:
            logger.error(f"[DATA QUALITY] Falha de Schema Drift! Colunas ausentes: {missing}")
            return False
        return True

    def check_business_rules(self) -> pl.DataFrame:
        """
        Aplica regras de negócio restritivas de forma vetorizada com Polars.
        Identifica registros inválidos (ex: valores negativos ouIDs vazios).
        """
        logger.info("[DATA QUALITY] Iniciando validação de regras de negócio vetorizadas...")
        
        # Cria máscaras de validação usando o motor rápido do Polars
        invalid_mask = (
            (pl.col("amount") <= 0) | 
            (pl.col("transaction_id").is_null()) |
            (pl.col("account_source") == pl.col("account_destination")) # Transação para si mesmo
        )
        
        # Filtra os dados inválidos para auditoria
        bad_data = self.df.filter(invalid_mask)
        good_data = self.df.filter(~invalid_mask)
        
        if bad_data.shape[0] > 0:
            logger.warning(f"[DATA QUALITY] Detectados {bad_data.shape[0]} registros corrompidos/inválidos!")
            # Aqui poderíamos salvar o bad_data em uma pasta de quarentena 'data/quarantine'
        else:
            logger.info("[DATA QUALITY] Sucesso! 100% dos registros estão em conformidade com as regras.")
            
        return good_data

if __name__ == "__main__":
    logger.info("=== TESTANDO MÓDULO DE DATA QUALITY ===")
    
    # Criando um dado intencionalmente corrompido para testar o Watchdog
    mock_corrupted_data = pl.DataFrame([
        {"transaction_id": "tx_111", "account_source": "acc_A", "account_destination": "acc_B", "amount": 500.0, "timestamp": "2026-09-25", "status": "COMPLETED", "source_api": 1},
        {"transaction_id": "tx_222", "account_source": "acc_C", "account_destination": "acc_C", "amount": 10.0, "timestamp": "2026-09-25", "status": "COMPLETED", "source_api": 1}, # Ruim (Origem == Destino)
        {"transaction_id": "tx_333", "account_source": "acc_D", "account_destination": "acc_E", "amount": -50.0, "timestamp": "2026-09-25", "status": "FAILED", "source_api": 1}   # Ruim (Valor negativo)
    ])
    
    dq = DataQualityWatchdog(mock_corrupted_data)
    if dq.validate_schema():
        clean_df = dq.check_business_rules()
        print("\n--- Dados Limpos Aprovados pelo Watchdog ---")
        print(clean_df)
