"""Ingestão de dados históricos de ações via yfinance."""

from pathlib import Path
import pandas as pd
import yfinance as yf

# Tickers brasileiros e americanos
TICKERS = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA",
    "BBAS3.SA", "WEGE3.SA", "B3SA3.SA",
    "AAPL", "MSFT", "NVDA", "GOOGL",
    "AMZN", "META",
]

# Caminho do arquivo de saída
PASTA_DATA = Path("data")
ARQUIVO_SAIDA = PASTA_DATA / "acoes_ultimos_5_anos.parquet"


def baixar_dados(tickers: list[str] | None = None, periodo: str = "5y") -> pd.DataFrame:
    """Baixa dados históricos de ações via yfinance e consolida em um DataFrame.

    Args:
        tickers: Lista de tickers para baixar. Usa TICKERS padrão se None.
        periodo: Período de dados (ex: "5y", "1y", "6mo").

    Returns:
        DataFrame consolidado com colunas: date, open, high, low, close, volume, ticker.
    """
    tickers = tickers or TICKERS

    df = yf.download(
        tickers,
        period=periodo,
        interval="1d",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    frames = []
    for ticker in tickers:
        try:
            dados = df[ticker].copy()
            dados = dados.dropna(how="all")
            if dados.empty:
                continue
            dados = dados.reset_index()
            dados["ticker"] = ticker
            frames.append(dados)
        except KeyError:
            print(f"Sem dados disponíveis: {ticker}")

    if not frames:
        raise ValueError("Nenhum dado foi obtido.")

    resultado = pd.concat(frames, ignore_index=True)
    resultado.columns = [str(col).lower().replace(" ", "_") for col in resultado.columns]
    return resultado


def salvar_parquet(df: pd.DataFrame, caminho: Path | None = None) -> Path:
    """Salva o DataFrame em formato Parquet.

    Args:
        df: DataFrame a ser salvo.
        caminho: Caminho do arquivo. Usa ARQUIVO_SAIDA padrão se None.

    Returns:
        Caminho do arquivo salvo.
    """
    caminho = caminho or ARQUIVO_SAIDA
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(caminho, index=False)
    print(f"Arquivo criado: {caminho}")
    print(f"Total de registros: {len(df)}")
    return caminho


def executar(periodo: str = "5y") -> pd.DataFrame:
    """Executa a ingestão completa: baixa dados e salva em Parquet.

    Args:
        periodo: Período de dados (ex: "5y", "1y", "6mo").

    Returns:
        DataFrame consolidado.
    """
    df = baixar_dados(periodo=periodo)
    salvar_parquet(df)
    return df