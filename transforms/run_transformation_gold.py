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
from decimal import Decimal
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [TRANSFORM-GOLD] - %(message)s'
)

PROJECT_ROOT = Path.cwd()
SILVER_DATALAKE_PATH = PROJECT_ROOT / "data" / "silver" / "inmet_climate_data"
GOLD_DATALAKE_PATH = PROJECT_ROOT / "data" / "gold" / "dm_inmet_daily_metrics"

# --- Funções de Transformação ---

def load_silver_data(path: Path) -> pd.DataFrame:
    """Carrega o dataset da camada Silver."""
    """
    Carrega o dataset Parquet da camada Silver, valida a presença da coluna
    de timestamp e cria a coluna 'data_local' para agregação diária.
    """
    logging.info(f"Iniciando leitura da camada Silver: {path}")
    try:
        df = pd.read_parquet(path)
        logging.info(f"Camada Silver carregada. Total de {len(df)} registros horários.")
        
        # Validação: 'timestamp_local' é essencial para a agregação diária correta.
        if 'timestamp_local' not in df.columns:
            logging.error("Coluna 'timestamp_local' não encontrada na camada Silver. Abortando.")
            raise ValueError("A camada Silver deve conter 'timestamp_local'.")
            
        # Cria a coluna 'data_local' (ex: 2024-01-15), que servirá como chave para a agregação.
        df['data_local'] = df['timestamp_local'].dt.date
        
        return df
    except Exception as e:
        logging.error(f"Falha ao ler dados da Silver: {e}", exc_info=True)
        raise

def aggregate_to_gold(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica as regras de agregação para transformar dados horários (Silver)
    em um Data Mart de métricas diárias (Gold).
    """
    logging.info("Iniciando agregação para a camada Gold...")
    
    try:
        if 'municipio' in df_silver.columns:
            logging.info("Padronizando a coluna 'municipio' para particionamento (ex: JOAO_PESSOA)...")
            df_silver['municipio'] = df_silver['municipio'].str.replace(' ', '_').str.upper()
        else:
            logging.error("Coluna 'municipio' não encontrada na camada Silver. Esta coluna é essencial.")
            raise ValueError("Coluna 'municipio' ausente na camada Silver.")
            
    except AttributeError as e:
        logging.error(f"Falha ao tentar padronizar a coluna 'municipio'. Verifique os dados da Silver. Erro: {e}")
        raise

    all_named_agg_rules = {
        'temp_maxima_diaria_c': ('temperatura_max_hora_ant_c', 'max'),
        'temp_minima_diaria_c': ('temperatura_min_hora_ant_c', 'min'),
        'precipitacao_total_diaria_mm': ('precipitacao_total_horario_mm', 'sum'),
        'temp_media_diaria_c': ('temperatura_ar_bulbo_seco_horaria_c', 'mean'),
        'umidade_media_diaria_percentual': ('umidade_relativa_ar_horaria_percent', 'mean'),
        'radiacao_total_diaria_kj_m2': ('radiacao_global_kj_m2', 'sum'),
        'vento_velocidade_media_diaria_ms': ('vento_velocidade_horaria_ms', 'mean'),
        'vento_rajada_maxima_diaria_ms': ('vento_velocidade_horaria_ms', 'max')
    }

    # Filtra as regras de agregação para incluir apenas as colunas que realmente existem no DataFrame Silver.
    available_agg_rules = {}
    missing_cols = []
    
    for new_col_name, (source_col, agg_func) in all_named_agg_rules.items():
        if source_col in df_silver.columns:
            available_agg_rules[new_col_name] = pd.NamedAgg(column=source_col, aggfunc=agg_func)
        else:
            missing_cols.append(source_col)

    if missing_cols:
        logging.warning(f"As seguintes colunas não foram encontradas na Silver e não serão agregadas: {list(set(missing_cols))}")

    if not available_agg_rules:
        logging.error("Nenhuma coluna de métrica disponível para agregação. Pipeline não pode continuar.")
        return pd.DataFrame()

    grouping_keys = ['data_local', 'municipio']
    
    try:
        # Executa a agregação usando as regras definidas.
        # 'as_index=False' garante que as chaves de agrupamento ('data_local', 'municipio') permaneçam como colunas.
        df_gold = df_silver.groupby(grouping_keys, as_index=False).agg(
            **available_agg_rules
        )

        # Calcula métricas derivadas, como a amplitude térmica diária.
        if 'temp_maxima_diaria_c' in df_gold.columns and 'temp_minima_diaria_c' in df_gold.columns:
            logging.info("Calculando métrica derivada 'amplitude_termica_diaria_c'.")
            df_gold['amplitude_termica_diaria_c'] = df_gold['temp_maxima_diaria_c'] - df_gold['temp_minima_diaria_c']

        # Refatoração: Arredondar as colunas de métricas para um número ideal de casas decimais.
        logging.info("Arredondando valores das métricas para o número ideal de casas decimais.")
        rounding_map = {
            'temp_maxima_diaria_c': 2,
            'temp_minima_diaria_c': 2,
            'precipitacao_total_diaria_mm': 1,
            'temp_media_diaria_c': 2,
            'umidade_media_diaria_percentual': 2,
            'radiacao_total_diaria_kj_m2': 2,
            'vento_velocidade_media_diaria_ms': 2,
            'vento_rajada_maxima_diaria_ms': 2,
            'amplitude_termica_diaria_c': 2
        }

        for col, decimals in rounding_map.items():
            if col in df_gold.columns:
                df_gold[col] = df_gold[col].round(decimals)

        logging.info(f"Agregação Gold concluída. {len(df_gold)} registros diários gerados.")
        return df_gold
        
    except Exception as e:
        logging.error(f"Falha durante a agregação (GROUP BY): {e}", exc_info=True)
        raise

def write_gold_dataset(df: pd.DataFrame, base_path: Path):
    """
    Escreve o Data Mart (Gold) em Parquet, particionado para performance
    otimizada em ferramentas de BI.
    """
    if df.empty:
        logging.warning("DataFrame Gold está vazio. Nenhum dado será escrito.")
        return

    try:
        # Garante que a coluna de data está no formato datetime para extração
        df['data_local'] = pd.to_datetime(df['data_local'])

        logging.info("Criando colunas 'ano' e 'mes' para particionamento otimizado.")
        df['ano'] = df['data_local'].dt.year
        df['mes'] = df['data_local'].dt.month

        df['data_local'] = df['data_local'].dt.date

        logging.info("Definindo schema explícito para garantir a precisão dos dados no Parquet.")
        schema_fields = {
            'data_local': pa.date32(),
            'municipio': pa.string(),
            'temp_maxima_diaria_c': pa.decimal128(10, 2),
            'temp_minima_diaria_c': pa.decimal128(10, 2),
            'precipitacao_total_diaria_mm': pa.decimal128(10, 1),
            'temp_media_diaria_c': pa.decimal128(10, 2),
            'umidade_media_diaria_percentual': pa.decimal128(10, 2),
            'radiacao_total_diaria_kj_m2': pa.decimal128(12, 2),
            'vento_velocidade_media_diaria_ms': pa.decimal128(10, 2),
            'vento_rajada_maxima_diaria_ms': pa.decimal128(10, 2),
            'amplitude_termica_diaria_c': pa.decimal128(10, 2),
            'ano': pa.int32(),
            'mes': pa.int32()
        }

        for col_name in schema_fields.keys():
            if col_name not in df.columns:
                logging.warning(f"Coluna '{col_name}' esperada no schema não foi encontrada no DataFrame. Será adicionada com valores nulos.")
                df[col_name] = None

        logging.info("Convertendo colunas float para objetos Decimal do Python antes de criar a tabela Arrow...")
        for col_name, arrow_type in schema_fields.items():
            if isinstance(arrow_type, pa.Decimal128Type) and col_name in df.columns:
                # O tipo decimal do Arrow espera objetos `Decimal` para garantir a precisão, evitando a conversão direta de float.
                # A verificação `pd.notna(x) and isinstance(x, (int, float))` garante que a função `np.isfinite`
                # só seja chamada para tipos numéricos, evitando o TypeError com valores `None`.
                df[col_name] = df[col_name].apply(
                    lambda x: Decimal(str(x)) if pd.notna(x) and isinstance(x, (int, float)) and np.isfinite(x) else None
                )

        # Reordena as colunas do DataFrame para corresponder à ordem do schema e cria o schema final
        final_columns_order = [col for col in schema_fields.keys() if col in df.columns]
        df = df[final_columns_order]
        final_schema = pa.schema([pa.field(name, schema_fields[name]) for name in final_columns_order])
        table = pa.Table.from_pandas(df, schema=final_schema, preserve_index=False)
        
        partition_cols = ['ano', 'mes', 'municipio'] 
        
        logging.info(f"Escrevendo {len(df)} registros na camada Gold em: {base_path} (Particionado por {partition_cols})")
        
        pq.write_to_dataset(
            table,
            root_path=base_path,
            partition_cols=partition_cols,
            existing_data_behavior='overwrite_or_ignore'
        )
        logging.info("Escrita da camada Gold concluída com sucesso.")
    
    except Exception as e:
        logging.error(f"Falha ao escrever o dataset Gold Parquet: {e}", exc_info=True)
        raise

def main_gold():
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
        
        if df_gold.empty:
            logging.warning("DataFrame Gold resultante está vazio. Nenhum dado será escrito.")
            return

        # 3. Write
        write_gold_dataset(df_gold, GOLD_DATALAKE_PATH)
        
        logging.info("--- Pipeline (Silver -> Gold) finalizado com sucesso ---")
    except Exception as e:
        logging.error(f"--- Pipeline (Silver -> Gold) falhou: {e} ---")
    finally:
        pipeline_duration = time.time() - pipeline_start_time
        logging.info(f"Tempo total de execução: {pipeline_duration:.2f} segundos")

if __name__ == "__main__":
    main_gold()