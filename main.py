from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import httpx
import yfinance as yf
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Option Coach Backend")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
POLYGON_BASE = "https://api.polygon.io/v2"
CONSTITUENTS_FILE = Path(__file__).with_name("constituents.json")

ALL_UNIVERSES = ["sp500", "dow30", "nasdaq100", "russell2000", "crypto"]
CONCURRENCY_LIMIT = int(os.getenv("OPTION_COACH_CONCURRENCY", "20"))


def load_constituents() -> Dict[str, List[str]]:
    """
    Load index ticker lists from constituents.json.

    The old version crashed at import time if constituents.json was missing.
    This keeps the server bootable and reports the file problem through /health
    and /screen instead.
    """
    if not CONSTITUENTS_FILE.exists():
        return {}

    try:
        with CONSTITUENTS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}

    cleaned: Dict[str, List[str]] = {}
    for universe, tickers in data.items():
        if isinstance(tickers, list):
            cleaned[universe] = [
                str(t).strip().upper()
                for t in tickers
                if str(t).strip()
            ]
    return cleaned


INDEX_MAP = load_constituents()


class ScreenRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "all"]
    horizon: Literal["1d", "1w", "1mo"] = "1mo"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper().replace("/", "").replace("-", "")


def is_crypto_ticker(ticker: str) -> bool:
    # Polygon crypto aggregate tickers look like X:BTCUSD, X:ETHUSD, etc.
    return ticker.upper().endswith("USD") and len(ticker) > 3


async def fetch_polygon_stock(client: httpx.AsyncClient, ticker: str) -> Optional[Dict[str, Any]]:
    if not POLYGON_API_KEY:
        return None

    url = f"{POLYGON_BASE}/aggs/ticker/{ticker}/prev"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}

    try:
        resp = await client.get(url, params=params)
        if resp.status_code in {401, 403}:
            return {"ticker": ticker, "error": "Polygon authentication failed. Check POLYGON_API_KEY."}
        if resp.status_code == 429:
            return {"ticker": ticker, "error": "Polygon rate limit hit."}
        if resp.status_code != 200:
            return {"ticker": ticker, "error": f"Polygon HTTP {resp.status_code}: {resp.text[:200]}"}

        data = resp.json()
        results = data.get("results") or []
        if not results:
            return None

        r = results[0]
        return {
            "ticker": ticker,
            "asset_type": "equity",
            "source": "polygon",
            "close": r.get("c"),
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "volume": r.get("v"),
        }
    except Exception as e:
        return {"ticker": ticker, "error": f"Polygon stock request failed: {e}"}


async def fetch_polygon_crypto(client: httpx.AsyncClient, symbol: str) -> Optional[Dict[str, Any]]:
    if not POLYGON_API_KEY:
        return None

    normalized = normalize_ticker(symbol)
    pair = f"X:{normalized}"
    url = f"{POLYGON_BASE}/aggs/ticker/{pair}/prev"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}

    try:
        resp = await client.get(url, params=params)
        if resp.status_code in {401, 403}:
            return {"ticker": normalized, "error": "Polygon authentication failed. Check POLYGON_API_KEY."}
        if resp.status_code == 429:
            return {"ticker": normalized, "error": "Polygon rate limit hit."}
        if resp.status_code != 200:
            return {"ticker": normalized, "error": f"Polygon HTTP {resp.status_code}: {resp.text[:200]}"}

        data = resp.json()
        results = data.get("results") or []
        if not results:
            return None

        r = results[0]
        return {
            "ticker": normalized,
            "asset_type": "crypto",
            "source": "polygon",
            "close": r.get("c"),
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "volume": r.get("v"),
        }
    except Exception as e:
        return {"ticker": normalized, "error": f"Polygon crypto request failed: {e}"}


def fetch_yahoo_sync(ticker: str) -> Dict[str, Any]:
    """
    yfinance is synchronous, so this function is called with asyncio.to_thread().
    """
    try:
        hist = yf.Ticker(ticker).history(period="5d", interval="1d")
        if hist.empty:
            return {"ticker": ticker, "error": "No Yahoo data available"}

        x = hist.iloc[-1]
        return {
            "ticker": ticker,
            "asset_type": "equity",
            "source": "yahoo",
            "close": float(x["Close"]),
            "open": float(x["Open"]),
            "high": float(x["High"]),
            "low": float(x["Low"]),
            "volume": int(x["Volume"]) if not str(x["Volume"]) == "nan" else None,
        }
    except Exception as e:
        return {"ticker": ticker, "error": f"Yahoo request failed: {e}"}


async def fetch_yahoo(ticker: str) -> Dict[str, Any]:
    return await asyncio.to_thread(fetch_yahoo_sync, ticker)


async def fetch_ticker(
    client: httpx.AsyncClient,
    ticker: str,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    ticker = normalize_ticker(ticker)

    async with semaphore:
        if not ticker:
            return {"ticker": ticker, "error": "Blank ticker"}

        if is_crypto_ticker(ticker):
            data = await fetch_polygon_crypto(client, ticker)
            return data or {"ticker": ticker, "asset_type": "crypto", "error": "No crypto data available"}

        polygon_result = await fetch_polygon_stock(client, ticker)

        # If Polygon explicitly errors, return that error unless Yahoo can rescue missing data.
        if polygon_result and "error" not in polygon_result:
            return polygon_result

        yahoo_result = await fetch_yahoo(ticker)
        if "error" not in yahoo_result:
            yahoo_result["fallback_reason"] = polygon_result.get("error") if isinstance(polygon_result, dict) else "Polygon unavailable"
            return yahoo_result

        if polygon_result and "error" in polygon_result:
            return {
                "ticker": ticker,
                "error": f"{polygon_result['error']} | {yahoo_result['error']}",
            }

        return yahoo_result


def tickers_for_request(req: ScreenRequest, universe: str) -> List[str]:
    if req.tickers:
        return sorted({normalize_ticker(t) for t in req.tickers if normalize_ticker(t)})

    tickers = INDEX_MAP.get(universe, [])
    return sorted({normalize_ticker(t) for t in tickers if normalize_ticker(t)})


async def process_universe(
    req: ScreenRequest,
    universe: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    tickers = tickers_for_request(req, universe)

    if not tickers:
        errors = [f"Unknown or empty universe: {universe}"]
        if not INDEX_MAP and not req.tickers:
            errors.append(f"Could not load {CONSTITUENTS_FILE}")
        return {"universe": universe, "results": [], "skipped": [], "errors": errors}

    tasks = [fetch_ticker(client, ticker, semaphore) for ticker in tickers]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    results: List[Dict[str, Any]] = []
    skipped: List[str] = []

    for ticker, result in zip(tickers, raw_results):
        if isinstance(result, Exception):
            skipped.append(ticker)
            results.append({"ticker": ticker, "error": str(result)})
        else:
            if result.get("error"):
                skipped.append(ticker)
            results.append(result)

    return {
        "universe": universe,
        "count": len(results),
        "results": results,
        "skipped": sorted(set(skipped)),
    }


@app.get("/")
def read_root() -> Dict[str, str]:
    return {"message": "Option Coach Backend is running."}


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "polygon_api_key_loaded": bool(POLYGON_API_KEY),
        "constituents_file": str(CONSTITUENTS_FILE),
        "constituents_loaded": bool(INDEX_MAP),
        "universes_loaded": sorted(INDEX_MAP.keys()),
        "concurrency_limit": CONCURRENCY_LIMIT,
    }


@app.post("/screen")
async def screen(req: ScreenRequest) -> Dict[str, Any]:
    universes = ALL_UNIVERSES if req.universe == "all" else [req.universe]

    if not req.tickers and not INDEX_MAP:
        raise HTTPException(
            status_code=500,
            detail=f"No constituents loaded. Put constituents.json next to main.py or pass explicit tickers.",
        )

    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=20.0)
    semaphore = asyncio.Semaphore(3)

    async with httpx.AsyncClient(timeout=timeout) as client:
        output = [
            await process_universe(req=req, universe=universe, client=client, semaphore=semaphore)
            for universe in universes
        ]

    return {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "universes": output,
    }
