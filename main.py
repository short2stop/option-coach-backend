from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Option Coach Backend")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
POLYGON_BASE = "https://api.polygon.io/v2"
CONSTITUENTS_FILE = Path(__file__).with_name("constituents.json")

ALL_UNIVERSES = ["sp500", "dow30", "nasdaq100", "russell2000", "crypto"]
CACHE_MAX_AGE_SECONDS = int(os.getenv("OPTION_COACH_CACHE_SECONDS", str(60 * 60 * 12)))

# In-memory cache. This is enough for Render web service runtime.
# The morning cron can hit /refresh so GPT/user requests read cached data.
CACHE: Dict[str, Any] = {
    "generated_at": None,
    "market_date": None,
    "stock_rows": {},
    "crypto_rows": {},
    "errors": [],
}

CACHE_LOCK = asyncio.Lock()


def load_constituents() -> Dict[str, List[str]]:
    if not CONSTITUENTS_FILE.exists():
        return {}
    try:
        with CONSTITUENTS_FILE.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}

    cleaned: Dict[str, List[str]] = {}
    for universe, tickers in raw.items():
        if isinstance(tickers, list):
            cleaned[universe] = [str(t).strip().upper() for t in tickers if str(t).strip()]
    return cleaned


INDEX_MAP = load_constituents()


class ScreenRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "all"]
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False, description="Force a cache refresh before returning data")


def normalize_ticker(ticker: str) -> str:
    return ticker.strip().upper()


def polygon_stock_key(ticker: str) -> str:
    # Polygon stock symbols commonly keep dot class notation, e.g. BRK.B.
    return normalize_ticker(ticker)


def yahoo_style_key(ticker: str) -> str:
    return normalize_ticker(ticker).replace(".", "-")


def is_crypto_ticker(ticker: str) -> bool:
    t = normalize_ticker(ticker)
    return t.endswith("USD") and len(t) > 3


def cache_age_seconds() -> Optional[float]:
    generated_at = CACHE.get("generated_at")
    if not generated_at:
        return None
    return (datetime.now(timezone.utc) - generated_at).total_seconds()


def cache_is_fresh() -> bool:
    age = cache_age_seconds()
    return age is not None and age <= CACHE_MAX_AGE_SECONDS and bool(CACHE.get("stock_rows"))


def recent_market_dates(days_back: int = 10) -> List[str]:
    # Try recent weekdays. Polygon will simply return no rows for market holidays.
    dates: List[str] = []
    d = datetime.now(timezone.utc).date()
    for i in range(1, days_back + 1):
        candidate = d - timedelta(days=i)
        if candidate.weekday() < 5:
            dates.append(candidate.isoformat())
    return dates


async def fetch_grouped_stocks(client: httpx.AsyncClient) -> tuple[Dict[str, Dict[str, Any]], Optional[str], List[str]]:
    errors: List[str] = []
    if not POLYGON_API_KEY:
        return {}, None, ["POLYGON_API_KEY is not loaded"]

    for market_date in recent_market_dates():
        url = f"{POLYGON_BASE}/aggs/grouped/locale/us/market/stocks/{market_date}"
        params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}
        try:
            resp = await client.get(url, params=params)
        except Exception as e:
            errors.append(f"Grouped stocks request failed for {market_date}: {e}")
            continue

        if resp.status_code in {401, 403}:
            return {}, None, ["Polygon authentication failed. Check POLYGON_API_KEY in Render."]
        if resp.status_code == 429:
            return {}, None, ["Polygon rate limit hit while fetching grouped stocks."]
        if resp.status_code != 200:
            errors.append(f"Polygon grouped stocks HTTP {resp.status_code} for {market_date}: {resp.text[:200]}")
            continue

        data = resp.json()
        rows = data.get("results") or []
        if not rows:
            errors.append(f"No grouped stock rows returned for {market_date}")
            continue

        mapped: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            ticker = str(r.get("T", "")).upper()
            if not ticker:
                continue
            mapped[ticker] = {
                "ticker": ticker,
                "asset_type": "equity",
                "source": "polygon_grouped",
                "close": r.get("c"),
                "open": r.get("o"),
                "high": r.get("h"),
                "low": r.get("l"),
                "volume": r.get("v"),
            }
        return mapped, market_date, errors

    return {}, None, errors or ["Could not fetch grouped stock data"]


async def fetch_polygon_crypto_one(client: httpx.AsyncClient, symbol: str) -> Dict[str, Any]:
    ticker = normalize_ticker(symbol)
    pair = f"X:{ticker}"
    url = f"{POLYGON_BASE}/aggs/ticker/{pair}/prev"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}

    try:
        resp = await client.get(url, params=params)
        if resp.status_code == 429:
            return {"ticker": ticker, "error": "Polygon crypto rate limit hit."}
        if resp.status_code in {401, 403}:
            return {"ticker": ticker, "error": "Polygon authentication failed."}
        if resp.status_code != 200:
            return {"ticker": ticker, "error": f"Polygon crypto HTTP {resp.status_code}: {resp.text[:200]}"}
        data = resp.json()
        rows = data.get("results") or []
        if not rows:
            return {"ticker": ticker, "error": "No Polygon crypto data available"}
        r = rows[0]
        return {
            "ticker": ticker,
            "asset_type": "crypto",
            "source": "polygon",
            "close": r.get("c"),
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "volume": r.get("v"),
        }
    except Exception as e:
        return {"ticker": ticker, "error": f"Polygon crypto request failed: {e}"}


async def fetch_crypto_rows(client: httpx.AsyncClient) -> Dict[str, Dict[str, Any]]:
    crypto_tickers = INDEX_MAP.get("crypto", [])
    if not crypto_tickers:
        return {}

    # Crypto list is small. Keep it intentionally sequential to avoid rate limits.
    rows: Dict[str, Dict[str, Any]] = {}
    for ticker in crypto_tickers:
        result = await fetch_polygon_crypto_one(client, ticker)
        rows[normalize_ticker(ticker)] = result
        await asyncio.sleep(1)
    return rows


async def refresh_cache() -> Dict[str, Any]:
    async with CACHE_LOCK:
        timeout = httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=30.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            stock_rows, market_date, stock_errors = await fetch_grouped_stocks(client)
            crypto_rows = await fetch_crypto_rows(client)

        CACHE["generated_at"] = datetime.now(timezone.utc)
        CACHE["market_date"] = market_date
        CACHE["stock_rows"] = stock_rows
        CACHE["crypto_rows"] = crypto_rows
        CACHE["errors"] = stock_errors

        return cache_status()


def cache_status() -> Dict[str, Any]:
    age = cache_age_seconds()
    return {
        "cached": bool(CACHE.get("stock_rows") or CACHE.get("crypto_rows")),
        "fresh": cache_is_fresh(),
        "generated_at": CACHE["generated_at"].isoformat() if CACHE.get("generated_at") else None,
        "age_seconds": round(age, 1) if age is not None else None,
        "market_date": CACHE.get("market_date"),
        "stock_count": len(CACHE.get("stock_rows") or {}),
        "crypto_count": len(CACHE.get("crypto_rows") or {}),
        "errors": CACHE.get("errors") or [],
    }


def requested_tickers(req: ScreenRequest, universe: str) -> List[str]:
    if req.tickers:
        return sorted({normalize_ticker(t) for t in req.tickers if normalize_ticker(t)})
    return sorted({normalize_ticker(t) for t in INDEX_MAP.get(universe, []) if normalize_ticker(t)})


def result_for_ticker(ticker: str) -> Dict[str, Any]:
    t = normalize_ticker(ticker)
    if is_crypto_ticker(t):
        return (CACHE.get("crypto_rows") or {}).get(t) or {"ticker": t, "error": "No cached crypto data"}

    stock_rows: Dict[str, Dict[str, Any]] = CACHE.get("stock_rows") or {}
    # Try direct Polygon symbol first. If needed, try common class-share variants.
    for key in [polygon_stock_key(t), t.replace("-", "."), t.replace(".", "-")]:
        if key in stock_rows:
            row = dict(stock_rows[key])
            row["requested_ticker"] = t
            return row

    return {"ticker": t, "error": "No cached stock data for ticker"}


def build_universe_response(req: ScreenRequest, universe: str) -> Dict[str, Any]:
    tickers = requested_tickers(req, universe)
    if not tickers:
        return {"universe": universe, "count": 0, "results": [], "skipped": [], "errors": [f"Unknown or empty universe: {universe}"]}

    results = [result_for_ticker(t) for t in tickers]
    skipped = sorted({r.get("ticker", "") for r in results if r.get("error")})
    return {
        "universe": universe,
        "count": len(results),
        "results": results,
        "skipped": skipped,
    }


@app.get("/")
def read_root() -> Dict[str, str]:
    return {"message": "Option Coach Backend is running."}


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "polygon_api_key_loaded": bool(POLYGON_API_KEY),
        "constituents_loaded": bool(INDEX_MAP),
        "universes_loaded": sorted(INDEX_MAP.keys()),
        "cache": cache_status(),
    }


@app.post("/refresh")
async def refresh() -> Dict[str, Any]:
    if not INDEX_MAP:
        raise HTTPException(status_code=500, detail="No constituents loaded. Put constituents.json next to main.py.")
    return await refresh_cache()


@app.post("/screen")
async def screen(req: ScreenRequest) -> Dict[str, Any]:
    if not INDEX_MAP and not req.tickers:
        raise HTTPException(status_code=500, detail="No constituents loaded. Put constituents.json next to main.py or pass explicit tickers.")

    if req.refresh or not cache_is_fresh():
        await refresh_cache()

    universes = ALL_UNIVERSES if req.universe == "all" else [req.universe]
    return {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": cache_status(),
        "universes": [build_universe_response(req, universe) for universe in universes],
    }
