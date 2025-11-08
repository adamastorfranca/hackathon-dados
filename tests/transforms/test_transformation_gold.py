"""
Testes para o script de transformação da camada Gold (transforms/run_transformation_gold.py).

Este módulo contém testes unitários para garantir a robustez e a correção do
pipeline de agregação que transforma os dados da camada Silver em um Data Mart
diário na camada Gold.

Testes Unitários:
- `test_load_silver_data`: Valida a leitura de dados da Silver e a criação da
  coluna 'data_local'.
- `test_load_silver_data_missing_timestamp`: Garante que o pipeline falha se a
  coluna 'timestamp_local' estiver ausente.
- `test_aggregate_to_gold_core_metrics`: Testa a agregação principal, verificando
  se as métricas diárias (max, min, sum, mean) são calculadas corretamente.
- `test_aggregate_to_gold_derived_metric`: Confirma o cálculo de métricas derivadas,
  como a amplitude térmica.
- `test_aggregate_to_gold_rounding`: Verifica se os valores numéricos são
  arredondados para a precisão correta.
- `test_aggregate_to_gold_missing_source_columns`: Garante que o pipeline lida
  corretamente com a ausência de colunas de métricas na origem.
- `test_aggregate_to_gold_missing_municipio`: Assegura que uma exceção é levantada
  se a coluna 'municipio' estiver faltando.
- `test_write_gold_dataset`: Testa a lógica de escrita, incluindo a criação de
  colunas de partição e a conversão para o schema do PyArrow.
- `test_main_gold_orchestration`: Valida a orquestração do pipeline principal,
  garantindo que as funções de carga, transformação e escrita são chamadas.
"""

import pytest
import pandas as pd
import pyarrow as pa
from pathlib import Path
from unittest.mock import patch, MagicMock
from decimal import Decimal

from transforms.run_transformation_gold import (
    load_silver_data,
    aggregate_to_gold,
    write_gold_dataset,
    main_gold
)

# --- Fixtures: Dados de Teste Reutilizáveis ---

@pytest.fixture
def sample_silver_df():
    """
    Fixture que fornece um DataFrame em memória, simulando os dados
    horários e limpos da camada Silver.
    """
    data = {
        'timestamp_local': pd.to_datetime([
            '2025-04-01 12:00:00', '2025-04-01 13:00:00',  # Dia 1 - JP
            '2025-04-01 12:00:00', '2025-04-01 13:00:00',  # Dia 1 - Patos
            '2025-04-02 12:00:00'                         # Dia 2 - JP
        ]),
        'municipio': [
            'JOAO PESSOA', 'JOAO PESSOA',
            'PATOS', 'PATOS',
            'JOAO PESSOA'
        ],
        'temperatura_max_hora_ant_c': [30.0, 32.5, 35.0, 36.0, 31.0],
        'temperatura_min_hora_ant_c': [29.0, 30.5, 33.0, 34.0, 30.0],
        'precipitacao_total_horario_mm': [0.0, 5.2, 0.0, 0.0, 10.0],
        'temperatura_ar_bulbo_seco_horaria_c': [29.5, 31.5, 34.0, 35.0, 30.5],
        'umidade_relativa_ar_horaria_percent': [80.0, 75.0, 50.0, 48.0, 90.0]
    }
    return pd.DataFrame(data)


@patch('pandas.read_parquet')
def test_load_silver_data(mock_read_parquet, sample_silver_df):
    """Testa se a função carrega dados e cria a coluna 'data_local' corretamente."""
    mock_read_parquet.return_value = sample_silver_df.copy()
    
    df = load_silver_data(Path("fake/path"))
    
    assert 'data_local' in df.columns
    assert df['data_local'].iloc[0] == pd.to_datetime('2025-04-01').date()
    mock_read_parquet.assert_called_once()


@patch('pandas.read_parquet')
def test_load_silver_data_missing_timestamp(mock_read_parquet):
    """Testa se a função levanta um erro se 'timestamp_local' estiver ausente."""
    mock_read_parquet.return_value = pd.DataFrame({'col1': [1]})
    
    with pytest.raises(ValueError, match="A camada Silver deve conter 'timestamp_local'"):
        load_silver_data(Path("fake/path"))


def test_aggregate_to_gold_core_metrics(sample_silver_df):
    """Testa o cálculo correto das métricas de agregação diárias."""
    # Pre-processamento: Adiciona a coluna 'data_local', simulando a etapa de carga.
    df_silver_processed = sample_silver_df.copy()
    df_silver_processed['data_local'] = df_silver_processed['timestamp_local'].dt.date
    
    df_gold = aggregate_to_gold(df_silver_processed)
    
    # Filtra para um dia e município específico para validação
    jp_day1 = df_gold[(df_gold['municipio'] == 'JOAO_PESSOA') & (df_gold['data_local'] == pd.to_datetime('2025-04-01').date())]
    
    assert not jp_day1.empty
    assert jp_day1['temp_maxima_diaria_c'].iloc[0] == 32.5  # max(30.0, 32.5)
    assert jp_day1['temp_minima_diaria_c'].iloc[0] == 29.0  # min(29.0, 30.5)
    assert jp_day1['precipitacao_total_diaria_mm'].iloc[0] == 5.2  # sum(0.0, 5.2)
    assert jp_day1['temp_media_diaria_c'].iloc[0] == 30.5  # mean(29.5, 31.5)
    assert jp_day1['umidade_media_diaria_percentual'].iloc[0] == 77.5 # mean(80.0, 75.0)


def test_aggregate_to_gold_derived_metric(sample_silver_df):
    """Testa o cálculo da métrica derivada 'amplitude_termica_diaria_c'."""
    # Pre-processamento: Adiciona a coluna 'data_local' antes de agregar.
    df_silver_processed = sample_silver_df.copy()
    df_silver_processed['data_local'] = df_silver_processed['timestamp_local'].dt.date

    df_gold = aggregate_to_gold(df_silver_processed)
    jp_day1 = df_gold[(df_gold['municipio'] == 'JOAO_PESSOA') & (df_gold['data_local'] == pd.to_datetime('2025-04-01').date())]
    
    # 32.5 (max) - 29.0 (min) = 3.5
    assert jp_day1['amplitude_termica_diaria_c'].iloc[0] == 3.5


def test_aggregate_to_gold_rounding(sample_silver_df):
    """Testa se as métricas são arredondadas para a precisão correta."""
    # Adiciona um valor que resultará em mais casas decimais
    df_silver_processed = sample_silver_df.copy()
    df_silver_processed.loc[len(df_silver_processed)] = df_silver_processed.iloc[0]
    df_silver_processed.loc[len(df_silver_processed)-1, 'precipitacao_total_horario_mm'] = 0.123

    # Pre-processamento: Adiciona a coluna 'data_local' antes de agregar.
    df_silver_processed['data_local'] = df_silver_processed['timestamp_local'].dt.date

    df_gold = aggregate_to_gold(df_silver_processed)
    jp_day1 = df_gold[(df_gold['municipio'] == 'JOAO_PESSOA') & (df_gold['data_local'] == pd.to_datetime('2025-04-01').date())]

    # Precipitação deve ter 1 casa decimal: sum(0.0, 5.2, 0.123) = 5.323 -> round(1) -> 5.3
    assert jp_day1['precipitacao_total_diaria_mm'].iloc[0] == 5.3


def test_aggregate_to_gold_missing_source_columns(sample_silver_df):
    """Garante que a agregação funciona mesmo se colunas de métricas estiverem ausentes."""
    df_silver_processed = sample_silver_df.copy()
    df_silver_processed = df_silver_processed.drop(columns=['precipitacao_total_horario_mm'])
    
    # Pre-processamento: Adiciona a coluna 'data_local' antes de agregar.
    df_silver_processed['data_local'] = df_silver_processed['timestamp_local'].dt.date
    df_gold = aggregate_to_gold(df_silver_processed)
    
    assert 'precipitacao_total_diaria_mm' not in df_gold.columns
    assert 'temp_maxima_diaria_c' in df_gold.columns # Outras colunas devem existir


def test_aggregate_to_gold_missing_municipio(sample_silver_df):
    """Testa se a função levanta um erro se 'municipio' estiver ausente."""
    df_no_municipio = sample_silver_df.drop(columns=['municipio'])
    # Pre-processamento: Adiciona 'data_local' para isolar o teste do erro de 'municipio'.
    df_no_municipio['data_local'] = df_no_municipio['timestamp_local'].dt.date
    with pytest.raises(ValueError, match="Coluna 'municipio' ausente na camada Silver."):
        aggregate_to_gold(df_no_municipio)


@patch('pyarrow.parquet.write_to_dataset')
@patch('pyarrow.Table')
def test_write_gold_dataset(mock_arrow_table, mock_write_to_dataset, sample_silver_df):
    """Testa a lógica de escrita, incluindo particionamento e conversão de schema."""
    # Pre-processamento: Adiciona a coluna 'data_local' antes de agregar.
    df_silver_processed = sample_silver_df.copy()
    df_silver_processed['data_local'] = df_silver_processed['timestamp_local'].dt.date
    df_gold = aggregate_to_gold(df_silver_processed)
    
    write_gold_dataset(df_gold, Path("fake/gold/path"))
    
    # Verifica se a conversão para tabela Arrow foi chamada.
    # O mock é na classe `pyarrow.Table` para lidar com o `classmethod` `from_pandas`.
    mock_arrow_table.from_pandas.assert_called_once()
    
    # Verifica se a função de escrita foi chamada com os argumentos corretos
    mock_write_to_dataset.assert_called_once()
    call_args, call_kwargs = mock_write_to_dataset.call_args
    # O primeiro argumento posicional para write_to_dataset deve ser a tabela mockada.
    assert call_args[0] == mock_arrow_table.from_pandas.return_value
    assert call_kwargs['root_path'] == Path("fake/gold/path")
    assert call_kwargs['partition_cols'] == ['ano', 'mes', 'municipio']

    # Verifica se a conversão para Decimal foi tentada
    df_passed_to_arrow = mock_arrow_table.from_pandas.call_args[0][0]
    # O valor original era 32.5, após a conversão deve ser um objeto Decimal
    assert isinstance(df_passed_to_arrow['temp_maxima_diaria_c'].iloc[0], Decimal)


@patch('transforms.run_transformation_gold.load_silver_data')
@patch('transforms.run_transformation_gold.aggregate_to_gold')
@patch('transforms.run_transformation_gold.write_gold_dataset')
def test_main_gold_orchestration(mock_write, mock_aggregate, mock_load, sample_silver_df):
    """Testa a orquestração do pipeline, garantindo que as funções são chamadas."""
    mock_load.return_value = sample_silver_df
    mock_aggregate.return_value = pd.DataFrame({'col': [1]}) # Retorna um DF não vazio
    
    main_gold()
    
    mock_load.assert_called_once()
    mock_aggregate.assert_called_once()
    mock_write.assert_called_once()