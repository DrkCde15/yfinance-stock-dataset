# Databricks notebook source
# /// script
# [tool.databricks.environment]
# dependencies = [
#   "yfinance",
# ]
# ///
%pip install yfinance

# COMMAND ----------
"""Pipeline de ingestão e persistência de dados de ações.

Executa o pipeline completo (arquitetura de medalhão):
    1. Baixa dados históricos via yfinance (Parquet)
    2. Salva dados brutos na camada bronze (Delta)
    3. Transforma, limpa e salva na camada silver (Delta)
    4. Agrega e salva KPIs na camada gold (Delta)
"""

from src.ingest import executar as executar_ingest
from src.persist import executar as executar_bronze
from src.transform import executar as executar_silver
from src.load import executar as executar_gold


if __name__ == "__main__":
    # Etapa 1: Ingestão (yfinance -> Parquet)
    print("=== Ingestão de dados ===")
    executar_ingest(periodo="5y")

    # Etapa 2: Bronze (Parquet -> Delta bronze)
    print("\n=== Camada Bronze ===")
    executar_bronze()

    # Etapa 3: Silver (Delta bronze -> limpeza + métricas -> Delta silver)
    print("\n=== Camada Silver ===")
    executar_silver()

    # Etapa 4: Gold (Delta silver -> agregações + KPIs -> Delta gold)
    print("\n=== Camada Gold ===")
    executar_gold()

    print("\nPipeline concluído!")