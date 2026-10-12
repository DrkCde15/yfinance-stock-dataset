"""Camada silver: transformação e limpeza de dados de ações."""

import pandas as pd
from pyspark.sql import SparkSession

# Nomes das tabelas Delta no Unity Catalog
TABELA_BRONZE = "main.default.bronze_acoes"
TABELA_SILVER = "main.default.silver_acoes"

# SparkSession
spark = SparkSession.builder.getOrCreate()


def ler_bronze(tabela: str = TABELA_BRONZE) -> pd.DataFrame:
    """Lê a tabela Delta bronze e converte para pandas.

    Args:
        tabela: Nome da tabela Delta bronze.

    Returns:
        DataFrame pandas.
    """
    spark_df = spark.table(tabela)
    df = spark_df.toPandas()
    print(f"Registros lidos da bronze: {len(df)}")
    return df


def limpar_dados(df: pd.DataFrame) -> pd.DataFrame:
    """Remove registros inválidos e garante tipos corretos.

    Args:
        df: DataFrame bruto.

    Returns:
        DataFrame limpo.
    """
    # Remove linhas com OHLCV nulo
    colunas_essenciais = ["open", "high", "low", "close", "volume"]
    df = df.dropna(subset=colunas_essenciais)

    # Garante que date é datetime
    df["date"] = pd.to_datetime(df["date"])

    # Remove duplicatas por (ticker, date)
    df = df.drop_duplicates(subset=["ticker", "date"])

    return df


def adicionar_metricas(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona colunas derivadas úteis para análise.

    Args:
        df: DataFrame limpo.

    Returns:
        DataFrame com colunas adicionais.
    """
    # Ordena por ticker e date para calcular métricas
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Variação diária (close anterior vs close atual) por ticker
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    # Volatilidade intradia (high - low)
    df["volatility"] = df["high"] - df["low"]

    # Preço típico
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3

    # Variação percentual intradia
    df["intraday_change"] = (df["close"] - df["open"]) / df["open"]

    return df


def salvar_silver(df: pd.DataFrame, tabela: str = TABELA_SILVER) -> str:
    """Converte DataFrame pandas para Spark e salva como tabela Delta silver.

    Args:
        df: DataFrame pandas transformado.
        tabela: Nome da tabela Delta silver.

    Returns:
        Nome da tabela salva.
    """
    spark_df = spark.createDataFrame(df)

    spark_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tabela)

    print(f"Registros salvos na silver: {spark_df.count()}")
    print(f"Tabela: {tabela}")
    return tabela


def executar(tabela_bronze: str = TABELA_BRONZE, tabela_silver: str = TABELA_SILVER) -> str:
    """Executa a transformação completa: lê bronze, limpa, adiciona métricas e salva silver.

    Args:
        tabela_bronze: Nome da tabela Delta bronze.
        tabela_silver: Nome da tabela Delta silver.

    Returns:
        Nome da tabela silver salva.
    """
    df = ler_bronze(tabela_bronze)

    df = limpar_dados(df)
    print(f"Registros após limpeza: {len(df)}")

    df = adicionar_metricas(df)
    print(f"Colunas finais: {list(df.columns)}")

    return salvar_silver(df, tabela=tabela_silver)