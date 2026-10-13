"""Camada gold: agregações e KPIs prontos para análise."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Nomes das tabelas Delta no Unity Catalog
TABELA_SILVER = "main.default.silver_acoes"
TABELA_GOLD_RESUMO_TICKER = "main.default.gold_resumo_ticker"
TABELA_GOLD_RESUMO_MENSAL = "main.default.gold_resumo_mensal"

# SparkSession
spark = SparkSession.builder.getOrCreate()


def criar_resumo_ticker(tabela_silver: str = TABELA_SILVER, tabela_gold: str = TABELA_GOLD_RESUMO_TICKER) -> str:
    """Agrega métricas por ticker: retorno total, volatilidade média, volume total, min/max.

    Args:
        tabela_silver: Nome da tabela Delta silver.
        tabela_gold: Nome da tabela Delta gold de saída.

    Returns:
        Nome da tabela gold salva.
    """
    df = spark.table(tabela_silver)

    resumo = df.groupBy("ticker").agg(
        F.count("*").alias("total_registros"),
        F.min("close").alias("preco_minimo"),
        F.max("close").alias("preco_maximo"),
        F.avg("close").alias("preco_medio"),
        F.sum("volume").alias("volume_total"),
        F.avg("daily_return").alias("retorno_diario_medio"),
        F.avg("volatility").alias("volatilidade_media"),
        F.avg("intraday_change").alias("variacao_intradia_media"),
        F.first("close").alias("primeiro_fechamento"),
        F.last("close").alias("ultimo_fechamento"),
    )

    # Retorno total (%)
    resumo = resumo.withColumn(
        "retorno_total_pct",
        F.round((F.col("ultimo_fechamento") - F.col("primeiro_fechamento")) / F.col("primeiro_fechamento") * 100, 2),
    )

    # Ranking por retorno total
    window = Window.orderBy(F.col("retorno_total_pct").desc())
    resumo = resumo.withColumn("ranking_retorno", F.rank().over(window))

    resumo.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tabela_gold)

    print(f"Resumo por ticker salvo: {tabela_gold} ({resumo.count()} tickers)")
    return tabela_gold


def criar_resumo_mensal(tabela_silver: str = TABELA_SILVER, tabela_gold: str = TABELA_GOLD_RESUMO_MENSAL) -> str:
    """Agrega métricas mensais por ticker: abertura, fechamento, volume médio, retorno mensal.

    Args:
        tabela_silver: Nome da tabela Delta silver.
        tabela_gold: Nome da tabela Delta gold de saída.

    Returns:
        Nome da tabela gold salva.
    """
    df = spark.table(tabela_silver)

    # Extrai ano e mês
    df = df.withColumn("ano", F.year("date")).withColumn("mes", F.month("date"))

    resumo = df.groupBy("ticker", "ano", "mes").agg(
        F.first("open").alias("abertura"),
        F.last("close").alias("fechamento"),
        F.max("high").alias("maxima"),
        F.min("low").alias("minima"),
        F.avg("volume").alias("volume_medio"),
        F.avg("daily_return").alias("retorno_diario_medio"),
        F.avg("volatility").alias("volatilidade_media"),
    )

    # Retorno mensal (%)
    resumo = resumo.withColumn(
        "retorno_mensal_pct",
        F.round((F.col("fechamento") - F.col("abertura")) / F.col("abertura") * 100, 2),
    )

    # Ordena por ticker e data
    resumo = resumo.orderBy("ticker", "ano", "mes")

    resumo.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tabela_gold)

    print(f"Resumo mensal salvo: {tabela_gold} ({resumo.count()} registros)")
    return tabela_gold


def executar(tabela_silver: str = TABELA_SILVER) -> None:
    """Executa a camada gold completa: cria resumo por ticker e resumo mensal.

    Args:
        tabela_silver: Nome da tabela Delta silver (fonte).
    """
    criar_resumo_ticker(tabela_silver=tabela_silver)
    criar_resumo_mensal(tabela_silver=tabela_silver)