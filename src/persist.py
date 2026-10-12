"""Camada bronze: persistência de dados brutos em tabela Delta."""

from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession

# Nome da tabela Delta bronze no Unity Catalog
TABELA_BRONZE = "main.default.bronze_acoes"

# SparkSession
spark = SparkSession.builder.getOrCreate()


def carregar_parquet(caminho: Path = Path("data/acoes_ultimos_5_anos.parquet")) -> pd.DataFrame:
    """Carrega dados de um arquivo Parquet.

    Args:
        caminho: Caminho do arquivo Parquet.

    Returns:
        DataFrame pandas.
    """
    df = pd.read_parquet(caminho)
    print(f"Registros carregados: {len(df)}")
    return df


def salvar_delta(df: pd.DataFrame, tabela: str = TABELA_BRONZE) -> str:
    """Converte DataFrame pandas para Spark e salva como tabela Delta.

    Args:
        df: DataFrame pandas a ser salvo.
        tabela: Nome da tabela Delta no Unity Catalog.

    Returns:
        Nome da tabela salva.
    """
    spark_df = spark.createDataFrame(df)

    spark_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(tabela)

    print(f"Registros salvos na tabela Delta: {spark_df.count()}")
    print(f"Tabela: {tabela}")
    return tabela


def executar(caminho_parquet: Path = Path("data/acoes_ultimos_5_anos.parquet")) -> str:
    """Executa a persistência completa: carrega Parquet e salva em Delta.

    Args:
        caminho_parquet: Caminho do arquivo Parquet.

    Returns:
        Nome da tabela Delta salva.
    """
    df = carregar_parquet(caminho_parquet)
    return salvar_delta(df, tabela=TABELA_BRONZE)