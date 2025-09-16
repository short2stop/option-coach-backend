from fastapi import FastAPI, Query
import httpx
import yfinance as yf
from typing import List
import os

# Load Polygon API key from environment
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

app = FastAPI(title="Option Coach Backend")

POLYGON_BASE = "https://api.polygon.io/v2"

# ----- Polygon Helpers -----
async def fetch_polygon_stock(ticker: str):
    """Try Polygon for stock ticker (previous day data)."""
    url = f"{POLYGON_BASE}/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if "results" in data and data["results"]:
                result = data["results"][0]
                return {
                    "ticker": ticker.upper(),
                    "asset_type": "equity",
                    "source": "polygon",
                    "close": result["c"],
                    "open": result["o"],
                    "high": result["h"],
                    "low": result["l"],
                    "volume": result.get("v"),
                }
    return None

async def fetch_polygon_crypto(symbol: str):
    """Try Polygon for crypto (BTCUSD, ETHUSD, etc.)."""
    pair = f"X:{symbol.upper()}"
    url = f"{POLYGON_BASE}/aggs/ticker/{pair}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            if "results" in data and data["results"]:
                result = data["results"][0]
                return {
                    "ticker": symbol.upper(),
                    "asset_type": "crypto",
                    "source": "polygon",
                    "close": result["c"],
                    "open": result["o"],
                    "high": result["h"],
                    "low": result["l"],
                    "volume": result.get("v"),
                }
    return None

# ----- Yahoo Fallback -----
def fetch_yahoo(ticker: str):
    """Fallback to Yahoo Finance for equities."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
        if not hist.empty:
            latest = hist.iloc[-1]
            return {
                "ticker": ticker.upper(),
                "asset_type": "equity",
                "source": "yahoo",
                "close": float(latest["Close"]),
                "open": float(latest["Open"]),
                "high": float(latest["High"]),
                "low": float(latest["Low"]),
                "volume": int(latest["Volume"]),
            }
    except Exception as e:
        return {"ticker": ticker.upper(), "error": f"Yahoo fail: {str(e)}"}
    return {"ticker": ticker.upper(), "error": "No data available"}

# ----- API Endpoint -----
@app.get("/market_data")
async def market_data(tickers: List[str] = Query(...)):
    """
    Fetch latest market data for multiple tickers.
    Stocks: AAPL, MSFT
    Crypto: BTCUSD, ETHUSD
    Example: /market_data?tickers=AAPL&tickers=BTCUSD
    """
    results = []
    for ticker in tickers:
        data = None
        if ticker.upper().endswith("USD"):  # crypto
            data = await fetch_polygon_crypto(ticker)
        else:  # equity
            data = await fetch_polygon_stock(ticker)
            if not data:
                data = fetch_yahoo(ticker)
        results.append(data or {"ticker": ticker.upper(), "error": "No data available"})
    return {"results": results}

# ----- Test Root -----
@app.get("/")
def read_root():
    return {"message": "Backend is working with Polygon + Yahoo!"}
