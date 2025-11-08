"""
Script Orquestrador do Pipeline de Dados Climáticos do INMET.

Executa as três etapas do pipeline em sequência:
1. Ingestão (Raw -> Bronze): Baixa os dados e salva em Parquet.
2. Processamento (Bronze -> Silver): Limpa, normaliza e enriquece os dados.
3. Transformação (Silver -> Gold): Agrega os dados para criar um Data Mart.

Para executar o pipeline completo, execute este script a partir da raiz do projeto:
`python run_pipeline.py`
"""

import logging
import time
import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.append(str(root_dir))

# Importa as funções principais de cada etapa do pipeline
from ingestion.run_ingestion_bronze import main_bronze
from transforms.run_processing_silver import main_silver
from transforms.run_transformation_gold import main_gold


# --- Configuração do Logger Principal ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PIPELINE-ORCHESTRATOR] - %(message)s'
)

def run_full_pipeline():
    """
    Orquestra a execução sequencial de todas as etapas do pipeline de dados.
    """
    pipeline_start_time = time.time()
    logging.info("=" * 80)
    logging.info(">>> INICIANDO EXECUÇÃO COMPLETA DO PIPELINE DE DADOS CLIMÁTICOS <<<")
    logging.info("=" * 80)

    try:
        # --- Etapa 1: Ingestão (Bronze) ---
        logging.info("-" * 80)
        logging.info(">>> [ETAPA 1/3] EXECUTANDO: Ingestão para a camada BRONZE...")
        stage_start_time = time.time()
        main_bronze()
        stage_duration = time.time() - stage_start_time
        logging.info(f">>> [ETAPA 1/3] CONCLUÍDA: Camada BRONZE finalizada em {stage_duration:.2f} segundos.")
        logging.info("-" * 80)

        # --- Etapa 2: Processamento (Silver) ---
        logging.info(">>> [ETAPA 2/3] EXECUTANDO: Processamento para a camada SILVER...")
        stage_start_time = time.time()
        main_silver()
        stage_duration = time.time() - stage_start_time
        logging.info(f">>> [ETAPA 2/3] CONCLUÍDA: Camada SILVER finalizada em {stage_duration:.2f} segundos.")
        logging.info("-" * 80)

        # --- Etapa 3: Transformação (Gold) ---
        logging.info(">>> [ETAPA 3/3] EXECUTANDO: Transformação para a camada GOLD...")
        stage_start_time = time.time()
        main_gold()
        stage_duration = time.time() - stage_start_time
        logging.info(f">>> [ETAPA 3/3] CONCLUÍDA: Camada GOLD finalizada em {stage_duration:.2f} segundos.")
        logging.info("-" * 80)

        logging.info("=" * 80)
        logging.info(">>> SUCESSO: Pipeline completo finalizado com êxito. <<<")
    except Exception as e:
        logging.error("=" * 80)
        logging.error(f"!!! FALHA CRÍTICA: O pipeline foi interrompido. Erro: {e} !!!", exc_info=True)
    finally:
        pipeline_duration = time.time() - pipeline_start_time
        logging.info(f"Tempo total de execução do pipeline: {pipeline_duration:.2f} segundos.")
        logging.info("=" * 80)

if __name__ == "__main__":
    run_full_pipeline()
