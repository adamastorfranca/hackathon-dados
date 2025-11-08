"""
Testes para o script de processamento da camada Silver (transforms/run_processing_silver.py).

Este módulo contém testes unitários para garantir a robustez e a correção do
pipeline de processamento de dados da camada Bronze para a Silver.

Testes Unitários:
- `test_load_bronze_dataset`: Valida o carregamento de dados da Bronze.
- `test_load_bronze_dataset_error`: Garante que falhas na leitura são tratadas.
- `test_column_renaming`: Verifica se as colunas são renomeadas corretamente para o padrão snake_case.
- `test_timestamp_creation_and_conversion`: Testa a criação do timestamp unificado e a conversão de timezone.
- `test_numeric_type_conversion`: Garante que colunas numéricas são convertidas corretamente e valores inválidos ('---') viram NaN.
- `test_data_quality_rules`: Testa a aplicação de regras de qualidade, como limites para umidade e temperatura.
- `test_deduplication`: Verifica se registros duplicados são removidos com base na chave de negócio.
- `test_main_silver_orchestration`: Testa a função principal de orquestração, garantindo que as etapas (leitura, processamento, escrita) são chamadas na ordem correta.
"""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from transforms.run_processing_silver import (
    load_bronze_dataset,
    process_bronze_to_silver,
    write_silver_dataset,
    main_silver
)

# --- Fixtures: Dados de Teste Reutilizáveis ---

@pytest.fixture
def sample_bronze_df():
    """
    Fixture que fornece um DataFrame em memória, simulando dados da camada Bronze
    com diversos casos de teste (duplicatas, valores inválidos, etc.).
    """
    data = {
        'Data': ['2023/01/01', '2023/01/01', '2023/01/01', '2023/01/02'], # Datas para teste
        'Hora UTC': ['0400 UTC', '0500 UTC', '0500 UTC', '0000 UTC'], # Horas ajustadas para evitar sobreposição de timezone
        'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)': ['0,1', '---', '0,3', '-5'], # Inclui inválido e negativo
        'UMIDADE RELATIVA DO AR, HORARIA (%)': ['80', '105', '85', '90'], # Inclui valor fora do range
        'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)': ['25,5', '26,0', '26,1', '99'], # Inclui valor fora do range
        'municipio': ['JOAO PESSOA', 'JOAO PESSOA', 'JOAO PESSOA', 'CAMPINA GRANDE'],
        'source_file': ['file1.csv', 'file1.csv', 'file1.csv', 'file2.csv'], # Chave para duplicata
        'partition_year': [2023, 2023, 2023, 2023]
    }
    return pd.DataFrame(data)


# --- Testes das Funções ---

@patch('pyarrow.parquet.read_table')
def test_load_bronze_dataset(mock_read_table, sample_bronze_df):
    """Testa o carregamento bem-sucedido de dados da camada Bronze."""
    mock_read_table.return_value.to_pandas.return_value = sample_bronze_df
    
    df = load_bronze_dataset(Path('fake/path'))
    
    assert not df.empty
    assert len(df) == 4
    mock_read_table.assert_called_once()

@patch('pyarrow.parquet.read_table', side_effect=Exception("Leitura falhou"))
def test_load_bronze_dataset_error(mock_read_table):
    """Garante que uma exceção na leitura da Bronze é propagada."""
    with pytest.raises(Exception, match="Leitura falhou"):
        load_bronze_dataset(Path('fake/path'))

def test_column_renaming(sample_bronze_df):
    """Verifica se as colunas são renomeadas para o padrão snake_case."""
    df = process_bronze_to_silver(sample_bronze_df.copy())
    
    assert 'precipitacao_total_horario_mm' in df.columns
    assert 'umidade_relativa_ar_horaria_percent' in df.columns
    assert 'Data' not in df.columns # Coluna original não deve mais existir

def test_timestamp_creation_and_conversion(sample_bronze_df):
    """Testa a criação do timestamp e a conversão para o timezone alvo."""
    df = process_bronze_to_silver(sample_bronze_df.copy())
    
    assert 'timestamp_local' in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df['timestamp_local'])
    assert str(df['timestamp_local'].dt.tz) == 'America/Fortaleza'
    # Verifica se o timestamp foi criado corretamente (00:00 UTC -> 21:00 do dia anterior em Fortaleza)
    assert df['timestamp_local'].iloc[0].hour == 1
    assert df['timestamp_local'].iloc[0].day == 1

def test_numeric_type_conversion(sample_bronze_df):
    """Garante que colunas numéricas são convertidas e valores inválidos ('---') viram NaN."""
    df = process_bronze_to_silver(sample_bronze_df.copy())
    
    # '0,1' deve virar 0.1
    assert df['precipitacao_total_horario_mm'].iloc[0] == 0.1
    # '---' deve virar NaN (representado como pd.NA)
    assert pd.isna(df['precipitacao_total_horario_mm'].iloc[1])

def test_data_quality_rules(sample_bronze_df):
    """Testa a aplicação de regras de qualidade (valores fora de um range plausível)."""
    df = process_bronze_to_silver(sample_bronze_df.copy())
    
    # Após o processamento, não deve haver valores de umidade fora do range [0, 100]
    # (valores inválidos devem ser nulos)
    umidade_validos = df['umidade_relativa_ar_horaria_percent'].dropna()
    assert umidade_validos.between(0, 100).all()
    
    # Não deve haver valores de temperatura fora do range [-20, 50]
    temperatura_validos = df['temperatura_ar_bulbo_seco_horaria_c'].dropna()
    assert temperatura_validos.between(-20, 50).all()
    
    # Não deve haver valores de precipitação negativos
    precipitacao_validos = df['precipitacao_total_horario_mm'].dropna()
    assert (precipitacao_validos >= 0).all()

def test_deduplication(sample_bronze_df):
    """Verifica se a remoção de duplicatas funciona com base na chave de negócio."""
    # O DataFrame de entrada tem 4 linhas, uma delas é duplicata
    df = process_bronze_to_silver(sample_bronze_df.copy())
    
    # Após a deduplicação, devem restar 3 linhas
    assert len(df) == 3

@patch('transforms.run_processing_silver.load_bronze_dataset')
@patch('transforms.run_processing_silver.write_silver_dataset')
def test_main_silver_orchestration(mock_write, mock_load, sample_bronze_df):
    """
    Testa a função de orquestração `main_silver`, garantindo que as funções
    de leitura, processamento e escrita são chamadas.
    """
    # Configura os mocks
    mock_load.return_value = sample_bronze_df
    
    # Executa a função principal
    main_silver()
    
    # Verifica se as funções foram chamadas
    mock_load.assert_called_once()
    mock_write.assert_called_once()
    
    # Verifica se o DataFrame passado para a escrita não está vazio
    written_df = mock_write.call_args[0][0]
    assert isinstance(written_df, pd.DataFrame)
    assert not written_df.empty

@patch('transforms.run_processing_silver.load_bronze_dataset')
@patch('transforms.run_processing_silver.write_silver_dataset')
def test_main_silver_with_empty_bronze(mock_write, mock_load):
    """Testa o comportamento do pipeline quando a camada Bronze está vazia."""
    # Configura o mock para retornar um DataFrame vazio
    mock_load.return_value = pd.DataFrame()
    
    # Executa a função principal
    main_silver()
    
    # Garante que a função de escrita não foi chamada
    mock_load.assert_called_once()
    mock_write.assert_not_called()