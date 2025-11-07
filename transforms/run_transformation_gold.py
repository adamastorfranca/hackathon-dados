"""
Script de Transformação (Silver -> Gold) para Dados Climáticos do INMET.

Responsabilidades:
1. Ler o dataset completo da camada Silver (dados horários e limpos).
2. Agregar os dados horários em métricas diárias por município.
   - Ex: Temperatura máxima/mínima, precipitação total, etc.
3. Padronizar a coluna 'municipio' para ser usada como chave de partição.
4. Renomear as colunas agregadas para nomes amigáveis ao negócio (BI).
5. Salvar o Data Mart (camada Gold) em formato Parquet, particionado por 'municipio'
   para otimizar a performance de consultas em ferramentas de visualização.
"""

import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import time

# --- Configuração ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [TRANSFORM-GOLD] - %(message)s'
)

# --- Configuração de Paths ---
PROJECT_ROOT = Path.cwd() 
SILVER_DATALAKE_PATH = PROJECT_ROOT / "data" / "silver" / "inmet_climate_data"
GOLD_DATALAKE_PATH = PROJECT_ROOT / "data" / "gold" / "dm_inmet_daily_metrics"

# --- Funções de Transformação ---

def load_silver_data(path: Path) -> pd.DataFrame:
    """Carrega o dataset da camada Silver."""
    logging.info(f"Iniciando leitura da camada Silver de: {path}")
    try:
        # Lendo o dataset particionado da Silver
        df = pd.read_parquet(path)
        logging.info(f"Camada Silver carregada. Total de {len(df)} registros horários.")
        
        # PRÉ-REQUISITO (da Etapa 2): 
        # Garantir que a coluna de data/hora local (America/Fortaleza) exista.
        if 'timestamp_local' not in df.columns:
            logging.error("Coluna 'timestamp_local' não encontrada na camada Silver. Abortando.")
            raise ValueError("A camada Silver deve conter 'timestamp_local'.")
            
        # Criamos a chave de agregação (a data)
        df['data_local'] = df['timestamp_local'].dt.date
        
        return df
    except Exception as e:
        logging.error(f"Falha ao ler dados da Silver: {e}", exc_info=True)
        raise

def aggregate_to_gold(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as regras de agregação para transformar dados horários (Silver)
    em métricas diárias (Gold).
    """
    logging.info("Iniciando agregação para a camada Gold...")
    
    try:
        if 'municipio' in df_silver.columns:
            logging.info("Padronizando a coluna 'municipio' para particionamento (ex: JOAO_PESSOA)...")
            df_silver['municipio'] = df_silver['municipio'].str.replace(' ', '_').str.upper()
        else:
            # Se 'municipio' não existir, o pipeline deve falhar, pois é uma chave de agregação.
            logging.error("Coluna 'municipio' não encontrada na camada Silver. Esta coluna é essencial.")
            raise ValueError("Coluna 'municipio' ausente na camada Silver.")
            
    except AttributeError as e:
        logging.error(f"Falha ao tentar padronizar a coluna 'municipio'. Verifique os dados da Silver. Erro: {e}")
        raise
    
    # Definição das regras de agregação
    agg_rules = {
        'temperatura_max_hora_ant_c': 'max',   # Temp. Máxima do dia
        'temperatura_min_hora_ant_c': 'min',   # Temp. Mínima do dia
        'precipitacao_total_horario_mm': 'sum'  # Precipitação Total do dia
    }
 
    grouping_keys = ['data_local', 'municipio']
    
    try:
        df_gold = df_silver.groupby(grouping_keys).agg(agg_rules).reset_index()
        
        # Renomeia as colunas para nomes mais claros para o BI
        df_gold.rename(columns={
            'temperatura_max_hora_ant_c': 'temp_maxima_diaria_c',
            'temperatura_min_hora_ant_c': 'temp_minima_diaria_c',
            'precipitacao_total_horario_mm': 'precipitacao_total_diaria_mm'
        }, inplace=True)
        
        logging.info(f"Agregação Gold concluída. {len(df_gold)} registros diários gerados.")
        return df_gold
        
    except Exception as e:
        logging.error(f"Falha durante a agregação (GROUP BY): {e}", exc_info=True)
        raise

def write_gold_dataset(df: pd.DataFrame, base_path: Path):
    """
    Escreve o Data Mart (Gold) em Parquet, particionado para performance
    otimizada no Metabase.
    """
    if df.empty:
        logging.warning("DataFrame Gold está vazio. Nenhum dado será escrito.")
        return

    try:
        df['data_local'] = pd.to_datetime(df['data_local']).dt.date

        table = pa.Table.from_pandas(df, preserve_index=False)
        
        partition_cols = ['municipio'] 
        
        logging.info(f"Escrevendo {len(df)} registros na camada Gold em: {base_path} (Particionado por {partition_cols})")
        
        pq.write_to_dataset(
            table,
            root_path=base_path,
            partition_cols=partition_cols,
            existing_data_behavior='delete_matching' # Garante idempotência
        )
        logging.info("Escrita da camada Gold concluída com sucesso.")
    
    except Exception as e:
        logging.error(f"Falha ao escrever o dataset Gold Parquet: {e}", exc_info=True)
        raise

# --- Orquestração ---

def main():
    """Orquestra a transformação Silver -> Gold."""
    pipeline_start_time = time.time()
    logging.info("--- Iniciando pipeline de transformação (Silver -> Gold) ---")
    
    try:
        # 1. Load
        df_silver = load_silver_data(SILVER_DATALAKE_PATH)
        
        if df_silver.empty:
            logging.warning("Dados da camada Silver estão vazios. Pipeline encerrado.")
            return

        # 2. Transform
        df_gold = aggregate_to_gold(df_silver)
        
        # 3. Write
        write_gold_dataset(df_gold, GOLD_DATALAKE_PATH)
        
        logging.info("--- Pipeline (Silver -> Gold) finalizado com sucesso ---")
    except Exception as e:
        logging.error(f"--- Pipeline (Silver -> Gold) falhou: {e} ---")
    finally:
        pipeline_duration = time.time() - pipeline_start_time
        logging.info(f"Tempo total de execução: {pipeline_duration:.2f} segundos")

if __name__ == "__main__":
    main()