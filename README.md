# Pipeline de Dados Climáticos do INMET (Paraíba)

![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)
![Framework](https://img.shields.io/badge/pytest-✓-green.svg)
![License](https://img.shields.io/badge/license-MIT-purple.svg)

## 📖 Visão Geral

Este projeto implementa um pipeline de engenharia de dados completo (*End-to-End*) para ingestão, processamento e transformação de dados climáticos históricos do **Instituto Nacional de Meteorologia (INMET)**, com foco específico nas estações meteorológicas do estado da **Paraíba**.

**Fonte de Dados:** Os dados são obtidos oficialmente em [portal.inmet.gov.br/dadoshistoricos](https://portal.inmet.gov.br/dadoshistoricos). O pipeline é configurado para ingerir automaticamente uma janela móvel dos últimos 5 anos, começando do ano atual.

**Objetivo:** Fornecer uma base de dados analítica confiável, auditável e performática para estudos climáticos, seguindo as melhores práticas de arquitetura de dados moderna (Arquitetura Medalhão).

---

## 🎓 Sobre o Projeto (Hackathon UNIESP)
Este projeto foi desenvolvido como parte do Hackathon de Finalização do MBA em Engenharia e Ciência de Dados da UNIESP - Centro Universitário (João Pessoa - PB).
Coordenador do MBA: Dr. Marcelo Fernandes.
Criador e Avaliador do Hackathon: Pablo Santos (Head de Engenharia de Dados na Radix).
O desafio proposto foi construir uma solução robusta de engenharia de dados que demonstrasse domínio prático sobre conceitos avançados de construção de Data Lakes, pipelines de ETL e qualidade de dados.

## 🏗 Arquitetura do Pipeline
O projeto segue a arquitetura Medallion (Databricks), dividindo os dados em três camadas lógicas com níveis crescentes de qualidade e agregação. Todas as camadas utilizam o formato Parquet com compressão Snappy, garantindo alta performance de leitura/escrita e eficiência de armazenamento.

### Detalhamento das Camadas e Estratégia de Particionamento
A estratégia de particionamento foi escolhida para otimizar as consultas mais frequentes em cada estágio do ciclo de vida do dado.

| Camada | Função | Formato & Particionamento (Performance) | Principais Tratamentos |
|:---:|---|---|---|
| **Bronze** | Repositório de dados brutos (Raw), garantindo a reprodutibilidade. | Parquet. Particionado por ano (`partition_year=YYYY`). | Leitura de ZIPs em memória, filtro de arquivos de interesse (`_NE_PB_`), conversão de CSV para Parquet. |
| **Silver** | Dados limpos, enriquecidos, tipados e deduplicados. | Parquet. Particionado por ano e mês (`partition_year=YYYY/partition_month=M`). | Normalização de colunas (`snake_case`), tipagem forte, tratamento de nulos, conversão de timezone (`America/Fortaleza`), deduplicação. |
| **Gold** | Dados agregados e prontos para consumo analítico (BI/ML). | Parquet. Particionado por município (`municipio=NOME`). | Criação de Data Mart com agregações diárias (máximas, mínimas, médias), cálculo de métricas derivadas (ex: amplitude térmica). |

## 📂 Estrutura do Projeto
A organização de pastas foi pensada para separar responsabilidades de ingestão, transformação, orquestração e testes.

```sh
inmet_climate_pipeline/
├── .github/workflows/
│   └── ci.yml              # Definição do Pipeline de CI/CD (GitHub Actions)
├── data/                   # Armazenamento local dos dados (ignorado pelo git)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docs/                   # Documentação complementar
├── ingestion/              # Scripts de ingestão (Raw -> Bronze)
│   └── run_ingestion_bronze.py
├── pipelines/              # Orquestrador do fluxo completo
│   └── run_all.py
├── tests/                  # Suíte de testes automatizados (Unitários e Integração)
│   ├── ingestion/
│   ├── pipelines/
│   └── transforms/
├── transforms/             # Scripts de transformação (Bronze->Silver, Silver->Gold)
│   ├── run_processing_silver.py
│   └── run_transformation_gold.py
├── .gitignore
├── pytest.ini              # Configuração do Pytest
├── README.md               # Este arquivo
└── requirements.txt        # Dependências do projeto
```

## 🚀 Começando

Siga os passos abaixo para configurar e executar o projeto em seu ambiente local.

### Pré-requisitos
*   **Python 3.10+**
*   **Git**
*   `pip` (gerenciador de pacotes Python)

### Instalação

1.  **Clone o repositório:**
```bash
git clone [https://github.com/adamastorfranca/hackathon-dados.git](https://github.com/adamastorfranca/hackathon-dados.git)
cd hackathon-dados
```

Instale as dependências:
```bash
pip install -r requirements.txt
```

## ⚙️ Execução do Pipeline
Você pode executar o pipeline completo ou cada estágio individualmente.
➤ Pipeline Completo (Recomendado)
Para rodar o fluxo de ponta a ponta (Bronze → Silver → Gold) em sequência, utilize o script orquestrador:
```bash
python pipelines/run_all.py
```

Este script garante que se uma etapa falhar, as subsequentes não serão iniciadas, mantendo a integridade dos dados.
➤ Execução por Estágios
Caso precise reprocessar apenas uma camada específica:
Ingestão (Raw → Bronze):
Baixa os dados dos últimos 5 anos e salva em formato bruto.
```bash
python ingestion/run_ingestion_bronze.py
```

Processamento (Bronze → Silver):
Lê a camada Bronze, aplica limpeza, tipagem e deduplicação.
```bash
python transforms/run_processing_silver.py
```

Transformação (Silver → Gold):
Gera as agregações diárias por município para consumo final.
```bash
python transforms/run_transformation_gold.py
```

## 🧪 Testes e Qualidade de Código
O projeto adota uma filosofia rigorosa de testes para garantir que alterações no código não quebrem o pipeline de dados. Utilizamos o pytest.
Executando os Testes
Para rodar toda a suíte de testes (unitários e de integração):
```bash
pytest
```
