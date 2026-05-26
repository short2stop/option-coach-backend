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


class SignalsRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "all"] = "sp500"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False, description="Force a cache refresh before returning signals")
    limit: int = Field(default=10, ge=1, le=50, description="Maximum number of ranked candidates to return")
    min_price: float = Field(default=5.0, ge=0.0, description="Minimum underlying price")
    min_volume: float = Field(default=500000.0, ge=0.0, description="Minimum share volume")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def percentile_rank(value: float, values: List[float]) -> float:
    if not values:
        return 50.0
    ordered = sorted(values)
    below = sum(1 for x in ordered if x <= value)
    return 100.0 * below / len(ordered)


def classify_structure(score: float, day_change_pct: float, range_pct: float, close_location: float) -> str:
    if score >= 85 and range_pct >= 3.0:
        return "aggressive call debit spread or small-risk momentum calls"
    if score >= 75 and close_location >= 70:
        return "bullish call debit spread"
    if score >= 65:
        return "balanced bullish vertical spread"
    return "watchlist only; wait for confirmation"


def classify_risk(range_pct: float, day_change_pct: float, price: float) -> str:
    abs_move = abs(day_change_pct)
    if range_pct >= 5 or abs_move >= 4:
        return "very high"
    if range_pct >= 3 or abs_move >= 2.5:
        return "high"
    if range_pct >= 1.5:
        return "moderate"
    return "lower"


def setup_label(score: float, day_change_pct: float, close_location: float) -> str:
    if score >= 85:
        return "aggressive momentum breakout"
    if day_change_pct > 1.5 and close_location >= 65:
        return "trend continuation"
    if close_location >= 80:
        return "strong close / relative strength"
    if day_change_pct > 0:
        return "constructive bullish setup"
    return "low-priority watchlist setup"


def build_signal_rows(req: SignalsRequest, universe: str) -> Dict[str, Any]:
    screen_req = ScreenRequest(
        universe=universe, horizon=req.horizon, tickers=req.tickers, refresh=False
    )
    raw = build_universe_response(screen_req, universe)
    valid_rows: List[Dict[str, Any]] = []

    for row in raw.get("results", []):
        if row.get("error"):
            continue
        close = safe_float(row.get("close"))
        open_ = safe_float(row.get("open"))
        high = safe_float(row.get("high"))
        low = safe_float(row.get("low"))
        volume = safe_float(row.get("volume"))
        if close < req.min_price or volume < req.min_volume or open_ <= 0 or high <= 0 or low <= 0:
            continue
        valid_rows.append(row)

    volumes = [safe_float(r.get("volume")) for r in valid_rows]
    dollar_volumes = [safe_float(r.get("close")) * safe_float(r.get("volume")) for r in valid_rows]

    candidates: List[Dict[str, Any]] = []
    for row in valid_rows:
        ticker = str(row.get("requested_ticker") or row.get("ticker") or "").upper()
        close = safe_float(row.get("close"))
        open_ = safe_float(row.get("open"))
        high = safe_float(row.get("high"))
        low = safe_float(row.get("low"))
        volume = safe_float(row.get("volume"))
        day_change_pct = ((close - open_) / open_) * 100 if open_ else 0.0
        day_range = max(high - low, 0.0)
        range_pct = (day_range / close) * 100 if close else 0.0
        close_location = ((close - low) / day_range) * 100 if day_range > 0 else 50.0
        volume_rank = percentile_rank(volume, volumes)
        dollar_volume_rank = percentile_rank(close * volume, dollar_volumes)

        momentum_score = clamp(50 + day_change_pct * 8 + (close_location - 50) * 0.35)
        volatility_score = clamp(range_pct * 16)
        liquidity_score = clamp((volume_rank * 0.35) + (dollar_volume_rank * 0.65))
        trend_quality = clamp((momentum_score * 0.65) + (close_location * 0.35))
        overall_score = clamp(
            momentum_score * 0.40
            + trend_quality * 0.25
            + liquidity_score * 0.20
            + volatility_score * 0.15
        )

        # Trade plan uses the current cached daily range as an ATR proxy until
        # historical bars are added. This gives consistent risk units without
        # pretending to have full ATR/IV yet.
        risk_per_share = max(day_range * 0.45, close * 0.012)
        entry = round(close, 2)
        stop = round(max(close - risk_per_share, 0.01), 2)
        target_1 = round(close + risk_per_share * 1.5, 2)
        target_2 = round(close + risk_per_share * 2.5, 2)
        reward_risk = round((target_2 - entry) / max(entry - stop, 0.01), 2)

        candidates.append(
            {
                "ticker": ticker,
                "asset_type": row.get("asset_type"),
                "source": row.get("source"),
                "close": round(close, 4),
                "open": round(open_, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "volume": round(volume, 0),
                "day_change_pct": round(day_change_pct, 2),
                "range_pct": round(range_pct, 2),
                "close_location_pct": round(close_location, 1),
                "momentum_score": round(momentum_score, 1),
                "volatility_score": round(volatility_score, 1),
                "liquidity_score": round(liquidity_score, 1),
                "trend_quality_score": round(trend_quality, 1),
                "overall_score": round(overall_score, 1),
                "setup": setup_label(overall_score, day_change_pct, close_location),
                "risk_profile": classify_risk(range_pct, day_change_pct, close),
                "ideal_option_structure": classify_structure(overall_score, day_change_pct, range_pct, close_location),
                "trade_plan": {
                    "bias": "bullish" if overall_score >= 60 else "neutral/watchlist",
                    "entry": entry,
                    "stop": stop,
                    "target_1": target_1,
                    "target_2": target_2,
                    "risk_per_share": round(entry - stop, 2),
                    "reward_risk_to_target_2": reward_risk,
                    "invalidates_below": stop,
                },
                "notes": [
                    "Scores are based on cached Polygon OHLCV grouped data.",
                    "Risk plan uses daily range as a temporary ATR proxy until historical bars are added.",
                ],
            }
        )

    candidates.sort(key=lambda x: x.get("overall_score", 0), reverse=True)
    return {
        "universe": universe,
        "candidate_count": len(candidates),
        "returned": min(req.limit, len(candidates)),
        "signals": candidates[: req.limit],
        "skipped": raw.get("skipped", []),
    }


@app.post("/signals")
async def signals(req: SignalsRequest) -> Dict[str, Any]:
    if not INDEX_MAP and not req.tickers:
        raise HTTPException(status_code=500, detail="No constituents loaded. Put constituents.json next to main.py or pass explicit tickers.")

    if req.refresh or not cache_is_fresh():
        await refresh_cache()

    universes = ALL_UNIVERSES if req.universe == "all" else [req.universe]
    return {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": cache_status(),
        "methodology": {
            "version": "signals_v1_ohlcv",
            "inputs": ["open", "high", "low", "close", "volume", "dollar volume", "close location"],
            "limitations": [
                "No multi-day historical bars yet.",
                "No options chain, implied volatility, Greeks, earnings calendar, or live news yet.",
                "Stops/targets use daily range as an ATR proxy until historical bars are added.",
            ],
        },
        "universes": [build_signal_rows(req, universe) for universe in universes],
    }
