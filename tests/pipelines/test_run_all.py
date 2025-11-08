"""
Testes para o script orquestrador do pipeline (pipelines/run_all.py).

Este módulo contém testes para garantir que o orquestrador executa as etapas
do pipeline (Bronze, Silver, Gold) na sequência correta e lida com falhas
de forma apropriada.

Testes:
- `test_run_full_pipeline_success_order`: Valida se todas as etapas são chamadas
  na ordem correta (Bronze -> Silver -> Gold) em um cenário de sucesso.
- `test_run_full_pipeline_stops_on_failure`: Garante que o pipeline para a
  execução se uma das etapas falhar, impedindo que as etapas subsequentes
  sejam executadas.
"""

import pytest
from unittest.mock import patch, call, Mock

from pipelines.run_all import run_full_pipeline


def test_run_full_pipeline_success_order():
    """
    Testa a orquestração bem-sucedida, garantindo que as etapas
    são chamadas na ordem correta: Bronze -> Silver -> Gold.
    """
    # Cria um mock "gerenciador" para registrar a ordem das chamadas
    manager = Mock()

    # Usa o gerenciador para substituir as funções principais de cada etapa
    with patch('pipelines.run_all.main_bronze', manager.main_bronze), \
         patch('pipelines.run_all.main_silver', manager.main_silver), \
         patch('pipelines.run_all.main_gold', manager.main_gold):

        run_full_pipeline()

    # Define a sequência de chamadas esperada
    expected_calls = [
        call.main_bronze(),
        call.main_silver(),
        call.main_gold()
    ]

    # Compara a lista de chamadas reais com a lista esperada
    assert manager.mock_calls == expected_calls


@patch('pipelines.run_all.main_gold')
@patch('pipelines.run_all.main_silver')
@patch('pipelines.run_all.main_bronze')
def test_run_full_pipeline_stops_on_failure(mock_bronze, mock_silver, mock_gold):
    """
    Testa se o pipeline para a execução se a primeira etapa (Bronze) falhar.
    """
    # Configura o mock da etapa Bronze para levantar uma exceção
    mock_bronze.side_effect = Exception("Falha na etapa Bronze")

    run_full_pipeline()

    mock_bronze.assert_called_once()
    mock_silver.assert_not_called()
    mock_gold.assert_not_called()