"""
Testes para o script de ingestão da camada Bronze (ingestion/run_ingestion_bronze.py).

Este módulo contém testes unitários e de integração para garantir a robustez
e a correção do pipeline de ingestão de dados climáticos do INMET.

Testes Unitários:
- `test_download_zip_file`: Valida o download bem-sucedido de um arquivo ZIP.
- `test_download_zip_file_error`: Garante que falhas de download são tratadas corretamente.
- `test_stream_filtered_files_from_zip`: Testa a capacidade de ler e filtrar arquivos
  CSV de dentro de um ZIP em memória.
- `test_process_csv_stream`: Verifica a extração de metadados (município) e a leitura
  correta dos dados de um stream de CSV.
- `test_process_csv_stream_error_handling`: Confirma que arquivos CSV malformados
  são ignorados sem quebrar o pipeline.
- `test_write_bronze_dataset`: Testa se a função de escrita em Parquet é chamada.
- `test_write_bronze_dataset_empty_df`: Garante que o pipeline não falha ao tentar
  escrever um DataFrame vazio.
- `test_process_year`: Testa a orquestração de um ano completo (download, processamento,
  escrita), usando mocks para isolar a lógica.

Testes de Integração:
- `test_full_pipeline_integration`: Teste marcado como 'integration' que realiza uma
  chamada real à API do INMET para validar a lógica contra dados reais.
"""

import pytest
import io
import pandas as pd
import zipfile
from pathlib import Path # Importa Path para manipulação de caminhos
from unittest.mock import Mock, patch, MagicMock
from requests.exceptions import RequestException
from ingestion.run_ingestion_bronze import ( # Importa as funções a serem testadas
    download_zip_file,
    stream_filtered_files_from_zip,
    process_csv_stream,
    process_year,
    write_bronze_dataset
)

# --- Fixtures: Dados de Teste Reutilizáveis ---

@pytest.fixture
def sample_csv_content():
    """Fixture que fornece um conteúdo de CSV em memória, simulando um arquivo real do INMET."""
    return """
REGIAO:;NE
UF:;PB
ESTACAO:;JOAO PESSOA
CODIGO (WMO):;A320
LATITUDE:;-7,16527777
LONGITUDE:;-34,81555555
ALTITUDE:;33,5
DATA DE FUNDACAO:;21/07/07
Data;Hora;Temp. Inst.;Temp. Max.;Temp. Min.;
01/01/2025;0000;25.3;26.1;24.8;
01/01/2025;0100;25.1;25.9;24.7;
""".strip()

@pytest.fixture
def mock_zip_file(sample_csv_content):
    """Fixture que cria um arquivo ZIP em memória contendo o CSV de amostra."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('INMET_NE_PB_A320_2025.CSV', sample_csv_content)
    zip_buffer.seek(0)
    return zip_buffer

def test_download_zip_file():
    """Testa o cenário de sucesso do download de um arquivo ZIP."""
    # Configura um mock para a sessão de requests
    mock_session = Mock() 
    mock_response = Mock()
    mock_response.content = b"zip content"
    mock_session.get.return_value = mock_response
    
    result = download_zip_file(mock_session, "http://test.url")
    
    assert isinstance(result, io.BytesIO)
    # Verifica se o método 'get' foi chamado com a URL correta
    mock_session.get.assert_called_once_with("http://test.url")

def test_download_zip_file_error():
    """Testa o tratamento de erro quando o download falha."""
    mock_session = Mock()
    # Simula uma exceção de request
    mock_session.get.side_effect = RequestException("Download failed")
    
    result = download_zip_file(mock_session, "http://test.url")
    
    assert result is None

def test_stream_filtered_files_from_zip(mock_zip_file):
    """Testa se a função consegue encontrar e decodificar o arquivo correto dentro do ZIP."""
    files = list(stream_filtered_files_from_zip(mock_zip_file, "_NE_PB_")) # O filtro deve corresponder ao arquivo
    
    assert len(files) == 1
    assert files[0][0] == "INMET_NE_PB_A320_2025.CSV"
    assert isinstance(files[0][1], io.TextIOWrapper)

def test_process_csv_stream(sample_csv_content):
    file_stream = io.StringIO(sample_csv_content)
    """Verifica se a função processa um stream de CSV, extrai metadados e lê os dados corretamente."""
    df = process_csv_stream(file_stream, "test_file.csv")
    
    assert isinstance(df, pd.DataFrame)
    assert 'municipio' in df.columns
    assert df['municipio'].iloc[0] == "JOAO PESSOA"
    assert len(df) == 2  # Deve haver duas linhas de dados

@patch('pyarrow.parquet.write_to_dataset')
def test_write_bronze_dataset(mock_write):
    """Testa se a função de escrita em Parquet é chamada corretamente."""
    # Cria um DataFrame de exemplo
    df = pd.DataFrame({
        'data': ['2025-01-01', '2025-01-02'],
        'temperatura': [25.5, 26.0],
        'partition_year': [2025, 2025]
    })
    
    write_bronze_dataset(
        df,
        Path('test/path'),
        partition_cols=['partition_year']
    )
    
    # Garante que a função de escrita do pyarrow foi chamada uma vez
    mock_write.assert_called_once()

@patch('requests.Session')
def test_process_year(mock_session, mock_zip_file):
    """Testa a orquestração da lógica de processamento para um único ano."""
    mock_session_instance = MagicMock()
    mock_session.return_value = mock_session_instance

    # Simula o download para retornar o ZIP em memória (mock_zip_file)
    mock_session_instance.get.return_value = MagicMock(content=mock_zip_file.getvalue())

    # Usa patch para isolar a função de escrita, focando o teste na lógica de orquestração
    with patch('ingestion.run_ingestion_bronze.write_bronze_dataset') as mock_write:
        records = process_year(2025, mock_session_instance)
        
        assert isinstance(records, int)
        assert records >= 0
        # Verifica se a função de escrita foi chamada, indicando que o processamento ocorreu
        mock_write.assert_called_once()

def test_process_csv_stream_error_handling():
    """Garante que a função lida com arquivos CSV malformados e retorna um DataFrame vazio."""
    malformed_content = "invalid;csv;content\nwith;wrong;format"
    file_stream = io.StringIO(malformed_content)
    
    df = process_csv_stream(file_stream, "test_file.csv")
    assert df.empty

def test_write_bronze_dataset_empty_df():
    """Testa se a função de escrita não levanta exceção ao receber um DataFrame vazio."""
    df = pd.DataFrame()
    write_bronze_dataset(
        df,
        Path('test/path'),
        partition_cols=['partition_year']
    )
    # O teste passa se nenhuma exceção for levantada

@pytest.mark.integration
def test_full_pipeline_integration():
    """
    Teste de integração que executa a lógica de `process_year` com uma
    chamada real ao servidor do INMET. Marcado para ser executado separadamente.
    """
    import requests
    from ingestion.run_ingestion_bronze import process_year
    
    with requests.Session() as session:
        records = process_year(2025, session)
        assert records > 0