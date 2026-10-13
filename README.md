# yfinance-stock-dataset

Coleta de dados históricos de ações brasileiras e americanas usando a biblioteca `yfinance`, com arquitetura de medalhão (bronze → silver → gold) em tabelas Delta no Databricks.

## Tickers incluídos

**Brasileiras (B3):** PETR4.SA, VALE3.SA, ITUB4.SA, BBAS3.SA, WEGE3.SA, B3SA3.SA

**Americanas (NASDAQ/NYSE):** AAPL, MSFT, NVDA, GOOGL, AMZN, META

## Pipeline (arquitetura de medalhão)

1. **Ingestão** — O módulo [src/ingest.py](#file-2145476656331878) baixa os dados dos últimos 5 anos via `yfinance` e salva em formato Parquet (raw).
2. **Bronze** — O módulo [src/persist.py](#file-2145476656331879) carrega o Parquet e salva os dados brutos na tabela Delta `main.default.bronze_acoes`.
3. **Silver** — O módulo [src/transform.py](#file-2145476656331882) lê a tabela bronze, limpa os dados (remove nulos e duplicatas), adiciona métricas derivadas (daily_return, volatility, typical_price, intraday_change) e salva na tabela Delta `main.default.silver_acoes`.
4. **Gold** — O módulo [src/load.py](#file-82629887395478) lê a tabela silver e cria agregações: resumo por ticker (retorno total, volatilidade média, ranking) e resumo mensal (abertura, fechamento, retorno mensal, volume médio).

### Tabelas Delta (Unity Catalog)

| Camada | Tabela | Descrição |
| --- | --- | --- |
| Bronze | `main.default.bronze_acoes` | Dados brutos do yfinance |
| Silver | `main.default.silver_acoes` | Dados limpos + métricas derivadas |
| Gold | `main.default.gold_resumo_ticker` | Resumo por ticker: retorno total, ranking, preço médio, volatilidade, volume total (12 tickers) |
| Gold | `main.default.gold_resumo_mensal` | Resumo mensal por ticker: abertura, fechamento, retorno mensal, volatilidade, volume médio (732 registros) |

## Estrutura

```
yfinance-stock-dataset/
├── src/
│   ├── __init__.py      # Inicialização do pacote
│   ├── ingest.py        # Ingestão via yfinance -> Parquet (raw)
│   ├── persist.py       # Parquet -> Delta bronze
│   ├── transform.py     # Delta bronze -> limpeza + métricas -> Delta silver
│   └── load.py          # Delta silver -> agregações + KPIs -> Delta gold
├── main.py              # Entry point do pipeline
├── notebook/            # Notebooks de análise
│   └── Análise Exploratoria dos Dados.py  # EDA usando camada Gold
├── requirements.txt     # Dependências do projeto
├── data/                # Arquivos Parquet (ignorados pelo Git)
├── .gitignore
└── README.md
```

## Como executar

No Databricks, execute o `main.py`:

```bash
python main.py
```

Ou importe e execute os módulos individualmente:

```python
from src.ingest import executar as ingest
from src.persist import executar as bronze
from src.transform import executar as silver
from src.load import executar as gold

ingest()   # yfinance -> Parquet
bronze()   # Parquet -> Delta bronze
silver()   # Delta bronze -> Delta silver
gold()     # Delta silver -> Delta gold
```

## Análise Exploratória

O notebook `notebook/Análise Exploratoria dos Dados` realiza a análise exploratória dos dados consumindo **exclusivamente** as tabelas da camada Gold:

- Evolução do preço de fechamento mensal por ticker
- Volume total de negociação
- Distribuição dos retornos mensais e estatísticas (média, desvio, Sharpe aproximado)
- Matriz de correlação entre preços de fechamento mensais
- Volatilidade média mensal ao longo do tempo
- Retorno mensal (%) por ticker