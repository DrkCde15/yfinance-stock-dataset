# yfinance-stock-dataset

Coleta de dados históricos de ações brasileiras e americanas usando a biblioteca `yfinance` e armazenamento em tabela Delta no Databricks.

## Tickers incluídos

**Brasileiras (B3):** PETR4.SA, VALE3.SA, ITUB4.SA, BBAS3.SA, WEGE3.SA, B3SA3.SA

**Americanas (NASDAQ/NYSE):** AAPL, MSFT, NVDA, GOOGL, AMZN, META

## Pipeline

1. **Ingestão** — O notebook [Ingest](#notebook-2001482278915205) baixa os dados dos últimos 5 anos via `yfinance`, consolida todas as ações em um único DataFrame e salva em formato Parquet.
2. **Persistência** — Os dados são convertidos e salvos na tabela Delta `main.default.acoes_ultimos_5_anos` no Unity Catalog.

## Estrutura

```
yfinance-stock-dataset/
├── Ingest              # Notebook de ingestão e persistência
├── data/               # Arquivos Parquet (ignorados pelo Git)
└── README.md
```

## Como executar

1. Abra o notebook **Ingest** no Databricks.
2. Execute a célula 1 para baixar os dados e gerar o arquivo Parquet.
3. Execute a célula 2 para visualizar os dados.
4. Execute a célula 3 para salvar os dados na tabela Delta.