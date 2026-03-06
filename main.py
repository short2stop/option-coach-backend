from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Literal, Dict, Any
import asyncio
import json
import os
import httpx
import yfinance as yf

app = FastAPI(title="Option Coach Backend")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE = "https://api.polygon.io/v2"

with open("constituents.json") as f:
    INDEX_MAP = json.load(f)

ALL_UNIVERSES = ["sp500", "dow30", "nasdaq100", "russell2000", "crypto"]


class ScreenRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "all"]
    horizon: Literal["1d", "1w", "1mo"] = "1mo"
    tickers: Optional[List[str]] = None


async def fetch_polygon_stock(ticker: str):
    url = f"{POLYGON_BASE}/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("results"):
                r = data["results"][0]
                return {
                    "ticker": ticker.upper(),
                    "asset_type": "equity",
                    "source": "polygon",
                    "close": r["c"],
                    "open": r["o"],
                    "high": r["h"],
                    "low": r["l"],
                    "volume": r.get("v"),
                }
    return None


async def fetch_polygon_crypto(symbol: str):
    pair = f"X:{symbol.upper()}"
    url = f"{POLYGON_BASE}/aggs/ticker/{pair}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("results"):
                r = data["results"][0]
                return {
                    "ticker": symbol.upper(),
                    "asset_type": "crypto",
                    "source": "polygon",
                    "close": r["c"],
                    "open": r["o"],
                    "high": r["h"],
                    "low": r["l"],
                    "volume": r.get("v"),
                }
    return None


def fetch_yahoo(ticker: str):
    try:
        hist = yf.Ticker(ticker).history(period="1d")
        if not hist.empty:
            x = hist.iloc[-1]
            return {
                "ticker": ticker.upper(),
                "asset_type": "equity",
                "source": "yahoo",
                "close": float(x["Close"]),
                "open": float(x["Open"]),
                "high": float(x["High"]),
                "low": float(x["Low"]),
                "volume": int(x["Volume"]),
            }
    except Exception as e:
        return {"ticker": ticker.upper(), "error": f"Yahoo fail: {str(e)}"}
    return {"ticker": ticker.upper(), "error": "No data available"}


async def fetch_ticker(ticker: str):
    if ticker.upper().endswith("USD"):
        data = await fetch_polygon_crypto(ticker)
        return data or {"ticker": ticker.upper(), "error": "No crypto data available"}
    data = await fetch_polygon_stock(ticker)
    return data or fetch_yahoo(ticker)


def chunked(items: List[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


async def fetch_batch(batch: List[str]) -> List[Dict[str, Any]]:
    tasks = [fetch_ticker(t) for t in batch]
    return await asyncio.gather(*tasks)


async def fetch_with_isolation(batch: List[str], skipped: List[str]) -> List[Dict[str, Any]]:
    sizes = [100, 50, 25, 10, 5, 1]
    for size in sizes:
        if len(batch) <= size:
            try:
                return await fetch_batch(batch)
            except Exception:
                if len(batch) == 1:
                    skipped.append(batch[0])
                    return [{"ticker": batch[0], "error": "Skipped after isolation"}]
                out = []
                mid = len(batch) // 2
                out.extend(await fetch_with_isolation(batch[:mid], skipped))
                out.extend(await fetch_with_isolation(batch[mid:], skipped))
                return out

    try:
        return await fetch_batch(batch)
    except Exception:
        results = []
        for t in batch:
            try:
                results.extend(await fetch_with_isolation([t], skipped))
            except Exception:
                skipped.append(t)
                results.append({"ticker": t, "error": "Skipped after repeated failure"})
        return results


async def process_universe(name: str) -> Dict[str, Any]:
    tickers = INDEX_MAP.get(name, [])
    if not tickers:
        return {"universe": name, "results": [], "skipped": [], "errors": ["Unknown or empty universe"]}

    skipped = []
    results = []

    for batch in chunked(tickers, 100):
        try:
            batch_results = await fetch_with_isolation(batch, skipped)
            results.extend(batch_results)
        except Exception as e:
            try:
                batch_results = await fetch_with_isolation(batch, skipped)
                results.extend(batch_results)
            except Exception:
                for t in batch:
                    skipped.append(t)
                    results.append({"ticker": t, "error": f"Batch failed twice: {str(e)}"})

    return {
        "universe": name,
        "results": results,
        "skipped": sorted(set(skipped)),
    }


@app.get("/")
def read_root():
    return {"message": "Option Coach Backend is running."}


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/screen")
async def screen(req: ScreenRequest):
    universes = ALL_UNIVERSES if req.universe == "all" else [req.universe]

    output = []
    for universe in universes:
        output.append(await process_universe(universe))

    return {
        "horizon": req.horizon,
        "universes": output,
    }
