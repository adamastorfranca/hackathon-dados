"""
Script de Ingestão (Raw -> Bronze) para Dados Climáticos do INMET.

Responsabilidades:
1. Baixar arquivos ZIP de dados climáticos (últimos 5 anos) em paralelo.
2. Ler os ZIPs em memória, sem descompactar em disco.
3. Filtrar arquivos CSV de interesse (ex: '_A320_').
4. Processar os CSVs em streams:
   - Pular metadados (cabeçalho).
   - Ler dados principais com delimitador (';'), decimal (',') e encoding ('latin-1').
5. Salvar os dados na camada Bronze em formato Parquet, usando pyarrow.
6. Particionar os dados por 'partition_year' para otimizar queries futuras.
"""

import logging
import io
import zipfile
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import List, Generator, Tuple, Optional
import time
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuração ---

# Configura um logger profissional
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [INGESTION-BRONZE] - %(message)s'
)

# --- Constantes do Pipeline ---
BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
CURRENT_YEAR = date.today().year
YEARS_TO_PROCESS = range(CURRENT_YEAR - 4, CURRENT_YEAR + 1)
FILE_FILTER_KEY = "_A320_"  # Filtro para arquivos de João Pessoa (PB)
MAX_WORKERS = 5            # Limita o paralelismo para não sobrecarregar a fonte

# Define o caminho base do projeto
PROJECT_ROOT = Path.cwd()  # Assume que o script é executado da raiz
BRONZE_DATALAKE_PATH = PROJECT_ROOT / "data" / "bronze" / "inmet_climate_data"

# Constantes específicas do formato de arquivo do INMET
FILE_ENCODING = 'latin-1'
FILE_DELIMITER = ';'
DECIMAL_SEPARATOR = ','
METADATA_ROWS_TO_SKIP = 8  # Linhas de cabeçalho (metadados) a serem puladas


def download_zip_file(session: requests.Session, url: str) -> Optional[io.BytesIO]:
    """
    Baixa um arquivo ZIP de uma URL e o retorna como um buffer em memória.
    Retorna None se o download falhar.
    """
    try:
        response = session.get(url)
        response.raise_for_status()  # Lança exceção para status HTTP 4xx/5xx
        logging.info(f"Sucesso no download de: {url}")
        return io.BytesIO(response.content)
    except requests.exceptions.RequestException as e:
        logging.error(f"Falha ao baixar {url}: {e}")
        return None


def stream_filtered_files_from_zip(
    zip_buffer: io.BytesIO, file_filter: str
) -> Generator[Tuple[str, io.TextIOWrapper], None, None]:
    """
    Faz o streaming de arquivos de dentro de um ZIP em memória que
    correspondem ao filtro, decodificando-os.
    Isso evita descompactar o ZIP inteiro em disco.
    """
    with zipfile.ZipFile(zip_buffer, 'r') as zip_f:
        for file_name in zip_f.namelist():
            # Filtra pelo nome do arquivo (ex: _A320_) e extensão
            if file_filter in file_name and file_name.lower().endswith('.csv'):
                logging.debug(f"Arquivo encontrado no ZIP: {file_name}")
                with zip_f.open(file_name) as binary_file:
                    # Envolve o arquivo binário para decodificar com o encoding correto
                    yield file_name, io.TextIOWrapper(binary_file, encoding=FILE_ENCODING)


def process_csv_stream(file_stream: io.TextIOWrapper, file_name: str) -> pd.DataFrame:
    """
    Processa um stream de arquivo CSV, pulando metadados e lendo os dados.
    """
    try:
        # 1. Pular linhas de metadados para posicionar o stream no cabeçalho
        for _ in range(METADATA_ROWS_TO_SKIP):
            file_stream.readline()

        # 2. Ler os dados principais
        # O stream agora está posicionado na linha do cabeçalho dos dados
        df = pd.read_csv(
            file_stream,
            delimiter=FILE_DELIMITER,
            decimal=DECIMAL_SEPARATOR,
            on_bad_lines='warn'  # Loga linhas com problemas em vez de falhar
        )

        # 3. Limpar colunas vazias de trailing delimiters
        unnamed_cols = [col for col in df.columns if 'Unnamed:' in col]
        if unnamed_cols:
            df.drop(columns=unnamed_cols, inplace=True, errors='ignore')
        
        logging.debug(f"Processado {file_name} em DataFrame. Linhas: {len(df)}")
        return df

    except Exception as e:
        logging.warning(f"Erro ao processar {file_name}: {e}. Pulando arquivo.")
        return pd.DataFrame()


def write_bronze_dataset(
    df: pd.DataFrame,
    base_path: Path,
    partition_cols: List[str]
):
    """
    Escreve um DataFrame em um dataset Parquet particionado usando PyArrow.
    Esta abordagem é otimizada para performance e escalabilidade.
    """
    if df.empty:
        logging.info("DataFrame está vazio, nenhuma escrita necessária.")
        return

    try:
        # Converte para uma Tabela Arrow antes de escrever
        table = pa.Table.from_pandas(df, preserve_index=False)
        
        logging.info(f"Escrevendo {len(df)} registros em {base_path} particionado por {partition_cols}")
        
        # Usa a função otimizada do PyArrow para escrita particionada
        pq.write_to_dataset(
            table,
            root_path=base_path,
            partition_cols=partition_cols,
            # CORREÇÃO AQUI: 
            # 'delete_matching' garante que, se a partição (ano) existir, 
            # ela será atomicamente substituída.
            existing_data_behavior='delete_matching'
            # 'use_legacy_dataset' foi removido para suprimir o warning
        )
        logging.info(f"Escrita em {base_path} concluída com sucesso.")
    
    except Exception as e:
        logging.error(f"Falha ao escrever o dataset Parquet em {base_path}: {e}", exc_info=True)


def process_year(year: int, session: requests.Session) -> int:
    """
    Encapsula toda a lógica de ETL para um único ano:
    Download -> Processamento -> Salvamento.
    Retorna o número de registros ingeridos.
    """
    logging.info(f"Iniciando processamento para o ano: {year}")
    url = BASE_URL.format(year=year)
    
    zip_buffer = download_zip_file(session, url)
    if not zip_buffer:
        logging.warning(f"Download para o ano {year} falhou. Pulando.")
        return 0
    
    dfs_for_this_year: List[pd.DataFrame] = []
    for file_name, file_stream in stream_filtered_files_from_zip(zip_buffer, FILE_FILTER_KEY):
        df = process_csv_stream(file_stream, file_name)
        if not df.empty:
            df['source_file'] = file_name
            dfs_for_this_year.append(df)

    if not dfs_for_this_year:
        logging.warning(f"Nenhum dado encontrado para '{FILE_FILTER_KEY}' no ano {year}.")
        return 0

    # Consolida os dados APENAS para este ano (seguro para a memória)
    try:
        year_df = pd.concat(dfs_for_this_year, ignore_index=True)
        # Adiciona a coluna que será usada para o particionamento físico
        year_df['partition_year'] = year
        
        logging.info(f"Ano {year} consolidado com {len(year_df)} registros.")
    except Exception as e:
        logging.error(f"Falha ao concatenar dados para o ano {year}: {e}")
        return 0

    # PONTO CRÍTICO: A escrita ocorre dentro da thread, por partição.
    # Isso evita o gargalo de memória de juntar todos os anos no final.
    write_bronze_dataset(
        year_df,
        BRONZE_DATALAKE_PATH,
        partition_cols=['partition_year']
    )
    
    return len(year_df)


def main():
    """
    Função principal de orquestração do pipeline de ingestão.
    """
    pipeline_start_time = time.time()
    logging.info("--- Iniciando pipeline de ingestão (Raw -> Bronze) ---")
    logging.info(f"Anos a serem processados: {list(YEARS_TO_PROCESS)}")
    logging.info(f"Destino do Datalake: {BRONZE_DATALAKE_PATH}")
    
    # Garante que o diretório raiz do Datalake exista
    BRONZE_DATALAKE_PATH.mkdir(parents=True, exist_ok=True)
    
    total_records_ingested = 0
    
    # --- Download, Processamento e Escrita em Paralelo ---
    with requests.Session() as session:
        # max_workers limita para não sobrecarregar o servidor de origem (I/O bound)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            
            future_to_year = {
                executor.submit(process_year, year, session): year
                for year in YEARS_TO_PROCESS
            }
            
            for future in as_completed(future_to_year):
                year = future_to_year[future]
                try:
                    # Coleta o resultado da função (número de registros)
                    records_processed = future.result()
                    if records_processed > 0:
                        logging.info(f"Ano {year} finalizado com sucesso. Registros: {records_processed}")
                        total_records_ingested += records_processed
                    else:
                        logging.info(f"Ano {year} finalizado sem novos registros.")
                        
                except Exception as e:
                    # Captura exceções que podem ter ocorrido dentro da thread
                    logging.error(f"Falha crítica no processamento do ano {year}: {e}", exc_info=True)

    pipeline_duration = time.time() - pipeline_start_time
    logging.info("--- Pipeline de ingestão (Raw -> Bronze) finalizado ---")
    logging.info(f"Total de registros ingeridos: {total_records_ingested}")
    logging.info(f"Tempo total de execução: {pipeline_duration:.2f} segundos")


if __name__ == "__main__":
    main()