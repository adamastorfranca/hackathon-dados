"""
Script de Processamento (Bronze -> Silver) para Dados Climáticos do INMET.

Responsabilidades:
1. Ler o dataset completo da camada Bronze (particionado por ano).
2. Normalizar os nomes das colunas (snake_case, sem acentos).
3. Criar um campo de timestamp unificado (data + hora).
4. Converter o timezone de UTC para 'America/Fortaleza'.
5. Garantir tipos de dados corretos (números como float, datas como timestamp).
6. Tratar valores inválidos (ex: '---' viram NaN) e aplicar regras de qualidade.
7. Deduplicar os registros com base na estação (source_file) e timestamp.
8. Salvar os dados limpos na camada Silver em Parquet, particionados 
   por 'partition_year' e 'partition_month' para performance analítica.
"""

import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import time

# --- Configuração ---

# Configura um logger profissional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PROCESSING-SILVER] - %(message)s'
)

# --- Constantes do Pipeline ---
PROJECT_ROOT = Path.cwd()  # Assume que o script é executado da raiz
BRONZE_DATALAKE_PATH = PROJECT_ROOT / "data" / "bronze" / "inmet_climate_data"
SILVER_DATALAKE_PATH = PROJECT_ROOT / "data" / "silver" / "inmet_climate_data"
TARGET_TIMEZONE = "America/Fortaleza"

# Mapeamento explícito das colunas conforme listado na sua descrição
COLUMN_RENAME_MAP = {
    'Data': 'data',
    'Hora UTC': 'hora_utc',
    'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': 'precipitacao_total_horario_mm',
    'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)': 'pressao_atm_estacao_horaria_mb',
    'PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)': 'pressao_atm_max_hora_ant_mb',
    'PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)': 'pressao_atm_min_hora_ant_mb',
    'RADIACAO GLOBAL (Kj/m²)': 'radiacao_global_kj_m2',
    'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': 'temperatura_ar_bulbo_seco_horaria_c',
    'TEMPERATURA DO PONTO DE ORVALHO (°C)': 'temperatura_ponto_orvalho_c',
    'TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)': 'temperatura_max_hora_ant_c',
    'TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)': 'temperatura_min_hora_ant_c',
    'TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)': 'temperatura_orvalho_max_hora_ant_c',
    'TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)': 'temperatura_orvalho_min_hora_ant_c',
    'UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)': 'umidade_rel_max_hora_ant_percent',
    'UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)': 'umidade_rel_min_hora_ant_percent',
    'UMIDADE RELATIVA DO AR, HORARIA (%)': 'umidade_relativa_ar_horaria_percent',
    'VENTO, DIREÇÃO HORARIA (gr) (° (gr))': 'vento_direcao_horaria_gr',
    'VENTO, RAJADA MAXIMA (m/s)': 'vento_rajada_maxima_ms',
    'VENTO, VELOCIDADE HORARIA (m/s)': 'vento_velocidade_horaria_ms',
    'municipio': 'municipio',
    'source_file': 'source_file',
    'partition_year': 'partition_year'
}

def load_bronze_dataset(bronze_path: Path) -> pd.DataFrame:
    """
    Carrega o dataset Parquet particionado da camada Bronze.
    """
    logging.info(f"Iniciando leitura do dataset Bronze em: {bronze_path}")
    try:
        # CORREÇÃO: O argumento 'use_pandas_metadata' foi removido
        # pois não é mais suportado nesta versão do pyarrow.
        table = pq.read_table(bronze_path)
        df = table.to_pandas()
        
        if df.empty:
            logging.warning("Dataset Bronze está vazio ou não foi encontrado.")
            return pd.DataFrame()
            
        logging.info(f"Dataset Bronze carregado. Total de {len(df)} registros.")
        return df
    except Exception as e:
        logging.error(f"Falha ao ler o dataset Bronze: {e}", exc_info=True)
        raise

def process_bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as regras de transformação da camada Silver.
    """
    
    # 1. Normalizar nomes de colunas (snake_case)
    #
    logging.info("Normalizando nomes de colunas...")
    # Garante que apenas colunas presentes no DF sejam renomeadas
    df.rename(columns=lambda c: COLUMN_RENAME_MAP.get(c, c), inplace=True)
    
    # Seleciona apenas as colunas que esperamos
    expected_cols = [col for col in COLUMN_RENAME_MAP.values() if col in df.columns]
    df = df[expected_cols]

    # 2. Converter tipos de dados (datas para timestamp)
    #
    logging.info("Criando timestamp e ajustando timezone...")
    try:
        # Limpa a coluna 'hora_utc': remove " UTC" e preenche com zeros à esquerda.
        # Ex: '0 UTC' -> '0000', '100 UTC' -> '0100'
        df['hora_utc'] = df['hora_utc'].astype(str).str.replace(' UTC', '', regex=False).str.zfill(4)

        
        # Trata o caso de 2400 (meia-noite) que o INMET usa
        df.loc[df['hora_utc'] == '2400', 'hora_utc'] = '0000'
        
        # Combina data e hora
        timestamp_str = df['data'].astype(str) + ' ' + df['hora_utc']
        
        # Converte para datetime e localiza como UTC
        df['timestamp_utc'] = pd.to_datetime(
            timestamp_str, 
            format='%Y/%m/%d %H%M',
            errors='coerce' # 'coerce' transforma datas inválidas em NaT
        )
        
        # Converte para o fuso horário de Fortaleza
        df['timestamp_local'] = df['timestamp_utc'].dt.tz_localize('UTC') \
                                                  .dt.tz_convert(TARGET_TIMEZONE)
                                                  
        # Remove registros que não puderam ser convertidos
        # Vamos adicionar um log para ver se algo for descartado
        original_count_timestamp = len(df)
        df.dropna(subset=['timestamp_local'], inplace=True)
        dropped_count = original_count_timestamp - len(df)
        
        if dropped_count > 0:
            logging.warning(f"Removidos {dropped_count} registros devido a timestamp inválido.")
        else:
            logging.info("Todos os registros tiveram timestamp convertido com sucesso.")

    except Exception as e:
        logging.error(f"Falha na conversão do timestamp: {e}", exc_info=True)
        # Se a conversão de tempo falhar, é um erro crítico
        raise

    # 3. Tratar valores faltantes e inválidos (Tipos Numéricos)
    #
    logging.info("Convertendo tipos numéricos e aplicando regras de qualidade...")
    
    numeric_cols = [
        'precipitacao_total_horario_mm', 'pressao_atm_estacao_horaria_mb',
        'pressao_atm_max_hora_ant_mb', 'pressao_atm_min_hora_ant_mb',
        'radiacao_global_kj_m2', 'temperatura_ar_bulbo_seco_horaria_c',
        'temperatura_ponto_orvalho_c', 'temperatura_max_hora_ant_c',
        'temperatura_min_hora_ant_c', 'temperatura_orvalho_max_hora_ant_c',
        'temperatura_orvalho_min_hora_ant_c', 'umidade_rel_max_hora_ant_percent',
        'umidade_rel_min_hora_ant_percent', 'umidade_relativa_ar_horaria_percent',
        'vento_direcao_horaria_gr', 'vento_rajada_maxima_ms',
        'vento_velocidade_horaria_ms'
    ]

    for col in numeric_cols:
        if col in df.columns:
            # Força a conversão para numérico. Valores inválidos (ex: '---') viram NaT
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Aplica regras de qualidade (Data Quality)
    #
    if 'umidade_relativa_ar_horaria_percent' in df.columns:
        df.loc[~df['umidade_relativa_ar_horaria_percent'].between(0, 100), 'umidade_relativa_ar_horaria_percent'] = pd.NA
    if 'precipitacao_total_horario_mm' in df.columns:
        df.loc[df['precipitacao_total_horario_mm'] < 0, 'precipitacao_total_horario_mm'] = pd.NA
    
    temp_cols = [c for c in df.columns if 'temperatura' in c]
    for t_col in temp_cols:
        # Define temperaturas fora de um range plausível (-20 a 50 C) como Nulas
        df.loc[~df[t_col].between(-20, 50), t_col] = pd.NA

    # 4. Deduplicar registros
    #
    logging.info("Removendo registros duplicados...")
    # Chave de negócio: um registro é único pela estação (source_file) e pelo timestamp
    original_count = len(df)
    df.drop_duplicates(
        subset=['timestamp_local', 'source_file'], 
        keep='first', # Mantém o primeiro registro encontrado
        inplace=True
    )
    logging.info(f"Removidas {original_count - len(df)} duplicatas.")

    # 5. Adicionar colunas de partição e selecionar colunas finais
    logging.info("Adicionando colunas de partição para a camada Silver...")
    df['partition_year'] = df['timestamp_utc'].dt.year
    df['partition_month'] = df['timestamp_utc'].dt.month
    
    # Limpa colunas usadas na transformação
    df_silver = df.drop(columns=['data', 'hora_utc', 'timestamp_utc'], errors='ignore')
    
    return df_silver


def write_silver_dataset(
    df: pd.DataFrame,
    base_path: Path,
    partition_cols: list[str]
):
    """
    Escreve o DataFrame Silver em um dataset Parquet particionado.
    """
    if df.empty:
        logging.info("DataFrame Silver está vazio, nenhuma escrita necessária.")
        return

    try:
        # Garante que o diretório exista
        base_path.mkdir(parents=True, exist_ok=True)
        
        table = pa.Table.from_pandas(df, preserve_index=False)
        
        logging.info(f"Escrevendo {len(df)} registros Silver em {base_path} particionado por {partition_cols}")
        
        pq.write_to_dataset(
            table,
            root_path=base_path,
            partition_cols=partition_cols,
            # Garante que a escrita seja idempotente para as partições
            existing_data_behavior='delete_matching'
        )
        logging.info(f"Escrita Silver concluída com sucesso.")
    
    except Exception as e:
        logging.error(f"Falha ao escrever o dataset Parquet Silver: {e}", exc_info=True)


def main():
    """
    Função principal de orquestração do pipeline (Bronze -> Silver).
    """
    pipeline_start_time = time.time()
    logging.info("--- Iniciando pipeline de processamento (Bronze -> Silver) ---")
    
    try:
        # 1. Carregar Dados
        bronze_df = load_bronze_dataset(BRONZE_DATALAKE_PATH)
        
        if bronze_df.empty:
            logging.info("Nenhum dado na camada Bronze para processar.")
            return

        # 2. Processar Dados
        silver_df = process_bronze_to_silver(bronze_df)
        
        # 3. Escrever Dados
        write_silver_dataset(
            silver_df,
            SILVER_DATALAKE_PATH,
            partition_cols=['partition_year', 'partition_month']
        )

    except Exception as e:
        logging.error(f"Pipeline Silver falhou: {e}", exc_info=True)
        
    finally:
        pipeline_duration = time.time() - pipeline_start_time
        logging.info("--- Pipeline de processamento (Bronze -> Silver) finalizado ---")
        logging.info(f"Tempo total de execução: {pipeline_duration:.2f} segundos")


if __name__ == "__main__":
    main()