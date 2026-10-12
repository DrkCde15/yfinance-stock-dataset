# yfinance-stock-dataset

Coleta de dados históricos de ações brasileiras e americanas usando a biblioteca `yfinance`, com arquitetura de medalhão (bronze → silver) em tabelas Delta no Databricks.

## Tickers incluídos

**Brasileiras (B3):** PETR4.SA, VALE3.SA, ITUB4.SA, BBAS3.SA, WEGE3.SA, B3SA3.SA

**Americanas (NASDAQ/NYSE):** AAPL, MSFT, NVDA, GOOGL, AMZN, META

## Pipeline (arquitetura de medalhão)

1. **Ingestão** — O módulo [src/ingest.py](#file-2145476656331878) baixa os dados dos últimos 5 anos via `yfinance` e salva em formato Parquet (raw).
2. **Bronze** — O módulo [src/persist.py](#file-2145476656331879) carrega o Parquet e salva os dados brutos na tabela Delta `main.default.bronze_acoes`.
3. **Silver** — O módulo [src/transform.py](#file-2145476656331882) lê a tabela bronze, limpa os dados (remove nulos e duplicatas), adiciona métricas derivadas (daily_return, volatility, typical_price, intraday_change) e salva na tabela Delta `main.default.silver_acoes`.

### Tabelas Delta (Unity Catalog)

| Camada | Tabela | Descrição |
| --- | --- | --- |
| Bronze | `main.default.bronze_acoes` | Dados brutos do yfinance |
| Silver | `main.default.silver_acoes` | Dados limpos + métricas derivadas |

## Estrutura

```
yfinance-stock-dataset/
├── src/
│   ├── __init__.py      # Inicialização do pacote
│   ├── ingest.py        # Ingestão via yfinance -> Parquet (raw)
│   ├── persist.py       # Parquet -> Delta bronze
│   └── transform.py     # Delta bronze -> limpeza + métricas -> Delta silver
├── main.py              # Entry point do pipeline
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

ingest()   # yfinance -> Parquet
bronze()   # Parquet -> Delta bronze
silver()   # Delta bronze -> Delta silver
```