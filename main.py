from __future__ import annotations

import asyncio
import json
import os
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Option Coach Backend - Signals v2")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
POLYGON_BASE = "https://api.polygon.io/v2"
CONSTITUENTS_FILE = Path(__file__).with_name("constituents.json")

ALL_UNIVERSES = ["sp500", "dow30", "nasdaq100", "russell2000", "crypto"]
CACHE_MAX_AGE_SECONDS = int(os.getenv("OPTION_COACH_CACHE_SECONDS", str(60 * 60 * 12)))

DATA_DIR = Path(os.getenv("OPTION_COACH_DATA_DIR", "/var/data"))
TRACKED_SIGNALS_FILE = DATA_DIR / "tracked_signals.json"
TRACKING_MAX_DAYS = int(os.getenv("OPTION_COACH_TRACKING_MAX_DAYS", "7"))
TRACKING_TOP_N = int(os.getenv("OPTION_COACH_TRACKING_TOP_N", "10"))

EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS", "").strip()
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "").strip()
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT", "").strip()
EMAIL_SMTP_HOST = os.getenv("EMAIL_SMTP_HOST", "smtp.gmail.com").strip()
EMAIL_SMTP_PORT = int(os.getenv("EMAIL_SMTP_PORT", "465"))

# In-memory cache. This is enough for Render web service runtime.
# The morning cron can hit /refresh so GPT/user requests read cached data.
CACHE: Dict[str, Any] = {
    "generated_at": None,
    "market_date": None,
    "stock_rows": {},
    "crypto_rows": {},
    "history_rows": {},
    "intraday_rows": {},
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


async def fetch_grouped_stocks_for_date(client: httpx.AsyncClient, market_date: str) -> tuple[Dict[str, Dict[str, Any]], List[str]]:
    errors: List[str] = []
    if not POLYGON_API_KEY:
        return {}, ["POLYGON_API_KEY is not loaded"]

    url = f"{POLYGON_BASE}/aggs/grouped/locale/us/market/stocks/{market_date}"
    params = {"adjusted": "true", "apiKey": POLYGON_API_KEY}
    try:
        resp = await client.get(url, params=params)
    except Exception as e:
        return {}, [f"Grouped stocks request failed for {market_date}: {e}"]

    if resp.status_code in {401, 403}:
        return {}, ["Polygon authentication failed. Check POLYGON_API_KEY in Render."]
    if resp.status_code == 429:
        return {}, [f"Polygon rate limit hit while fetching grouped stocks for {market_date}."]
    if resp.status_code != 200:
        return {}, [f"Polygon grouped stocks HTTP {resp.status_code} for {market_date}: {resp.text[:200]}"]

    data = resp.json()
    rows = data.get("results") or []
    if not rows:
        return {}, [f"No grouped stock rows returned for {market_date}"]

    mapped: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ticker = str(r.get("T", "")).upper()
        if not ticker:
            continue
        mapped[ticker] = {
            "ticker": ticker,
            "date": market_date,
            "close": r.get("c"),
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "volume": r.get("v"),
        }
    return mapped, errors


def recent_weekday_dates(days_back: int = 60) -> List[str]:
    dates: List[str] = []
    d = datetime.now(timezone.utc).date()
    for i in range(1, days_back + 1):
        candidate = d - timedelta(days=i)
        if candidate.weekday() < 5:
            dates.append(candidate.isoformat())
    return dates


async def fetch_historical_grouped_stocks(
    client: httpx.AsyncClient,
    market_dates: List[str],
    max_market_days: int = 30,
) -> tuple[Dict[str, List[Dict[str, Any]]], List[str]]:
    """Fetch recent market-wide grouped stock bars.

    This uses one Polygon grouped request per market day instead of one request per ticker.
    For signals, this is the scalable way to build ATR/RSI/moving averages across a universe.
    """
    history: Dict[str, List[Dict[str, Any]]] = {}
    errors: List[str] = []
    fetched_days = 0

    for market_date in market_dates:
        rows, day_errors = await fetch_grouped_stocks_for_date(client, market_date)
        if rows:
            fetched_days += 1
            for ticker, row in rows.items():
                history.setdefault(ticker, []).append(row)
        else:
            errors.extend(day_errors)

        # Keep this intentionally gentle for free/lower Polygon plans.
        await asyncio.sleep(0.25)
        if fetched_days >= max_market_days:
            break

    # Chronological order: oldest -> newest.
    for ticker in list(history.keys()):
        history[ticker] = sorted(history[ticker], key=lambda x: x.get("date", ""))

    if fetched_days == 0:
        errors.append("No historical grouped market days were fetched.")
    return history, errors


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



async def fetch_intraday_snapshot(
    client: httpx.AsyncClient,
    ticker: str,
) -> Dict[str, Any]:
    if not POLYGON_API_KEY:
        return {"ticker": ticker, "error": "Polygon API key missing"}

    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
    params = {"apiKey": POLYGON_API_KEY}

    try:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return {"ticker": ticker, "error": f"Snapshot HTTP {resp.status_code}: {resp.text[:200]}"}

        data = resp.json()
        ticker_data = data.get("ticker", {})
        day = ticker_data.get("day", {})
        prev_day = ticker_data.get("prevDay", {})

        current_price = safe_float(day.get("c"))
        open_price = safe_float(day.get("o"))
        high_price = safe_float(day.get("h"))
        low_price = safe_float(day.get("l"))
        volume = safe_float(day.get("v"))
        day_vwap = safe_float(day.get("vw"))
        prev_close = safe_float(prev_day.get("c"))
        intraday_change_pct = safe_float(ticker_data.get("todaysChangePerc"))

        distance_from_high_pct = 0.0
        if high_price > 0 and current_price > 0:
            distance_from_high_pct = ((current_price - high_price) / high_price) * 100

        above_open = bool(current_price and open_price and current_price >= open_price)
        above_vwap = bool(current_price and day_vwap and current_price >= day_vwap)
        intraday_confirmed = intraday_change_pct > 0 and above_open and distance_from_high_pct > -2.0

        if intraday_confirmed:
            entry_status = "active_candidate"
            reason = "Bullish intraday confirmation passed"
        elif intraday_change_pct <= -1.0:
            entry_status = "exclude_from_aggressive_calls"
            reason = "Stock is down more than 1% intraday; do not chase bullish calls"
        else:
            entry_status = "watchlist_only"
            reason = "Bullish setup has not confirmed intraday"

        return {
            "ticker": ticker,
            "current_price": round(current_price, 2),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "volume": round(volume, 0),
            "day_vwap": round(day_vwap, 2),
            "prev_close": round(prev_close, 2),
            "intraday_change_pct": round(intraday_change_pct, 2),
            "distance_from_high_pct": round(distance_from_high_pct, 2),
            "above_open": above_open,
            "above_vwap": above_vwap,
            "intraday_confirmed": intraday_confirmed,
            "entry_status": entry_status,
            "reason": reason,
        }
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}


async def fetch_intraday_snapshots_for_tickers(
    tickers: List[str],
    max_concurrency: int = 8,
) -> Dict[str, Dict[str, Any]]:
    if not tickers:
        return {}

    timeout = httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=25.0)
    semaphore = asyncio.Semaphore(max_concurrency)

    async with httpx.AsyncClient(timeout=timeout) as client:
        async def fetch_one(ticker: str) -> tuple[str, Dict[str, Any]]:
            async with semaphore:
                result = await fetch_intraday_snapshot(client, ticker)
                await asyncio.sleep(0.05)
                return ticker, result

        tasks = [fetch_one(t) for t in tickers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    mapped: Dict[str, Dict[str, Any]] = {}
    for item in results:
        if isinstance(item, Exception):
            continue
        ticker, result = item
        mapped[ticker] = result
    return mapped


def calculate_intraday_quality_score(intraday: Dict[str, Any], signal: Dict[str, Any]) -> float:
    """Score live confirmation quality from 0-100.

    This is intentionally separate from the historical score so the API can show
    whether a ticker is strong historically, strong intraday, or both.
    """
    change_pct = safe_float(intraday.get("intraday_change_pct"))
    distance_from_high = safe_float(intraday.get("distance_from_high_pct"))
    above_open = bool(intraday.get("above_open"))
    above_vwap = bool(intraday.get("above_vwap"))

    history = signal.get("history", {}) or {}
    volume_anom = safe_float(history.get("volume_anomaly_ratio"), 1.0)

    score = 50.0

    # Positive intraday trend. Cap the contribution so one extreme move does not dominate.
    score += min(max(change_pct, -5.0), 5.0) * 6.0

    # Reward stocks holding close to high of day. distance_from_high is usually <= 0.
    if distance_from_high >= -0.25:
        score += 18
    elif distance_from_high >= -0.50:
        score += 14
    elif distance_from_high >= -1.00:
        score += 9
    elif distance_from_high >= -2.00:
        score += 4
    else:
        score -= min(abs(distance_from_high), 8.0) * 3.0

    if above_open:
        score += 8
    else:
        score -= 10

    if above_vwap:
        score += 10
    else:
        score -= 15

    # Reuse historical volume anomaly until true intraday relative-volume is added.
    if volume_anom >= 1.5:
        score += 8
    elif volume_anom >= 1.2:
        score += 5
    elif volume_anom < 0.75:
        score -= 5

    return clamp(score)


def apply_intraday_confirmation(signal: Dict[str, Any], intraday: Dict[str, Any]) -> Dict[str, Any]:
    original_score = safe_float(signal.get("overall_score"))

    if not intraday or intraday.get("error"):
        signal["intraday"] = intraday or {"error": "No intraday snapshot available"}
        signal["historical_score"] = round(original_score, 1)
        signal["intraday_quality_score"] = None
        signal["intraday_score_adjustment"] = 0.0
        signal["intraday_confirmed"] = None
        signal["entry_status"] = "historical_only"
        signal["action"] = "wait_for_live_confirmation"
        signal["notes"].append("No live intraday confirmation was available; treat as historical setup only.")
        return signal

    signal["intraday"] = intraday
    signal["intraday_confirmed"] = intraday.get("intraday_confirmed")
    signal["entry_status"] = intraday.get("entry_status")
    signal["action"] = "eligible_for_bullish_calls" if intraday.get("intraday_confirmed") else "watchlist_only"
    signal["historical_score"] = round(original_score, 1)

    intraday_quality = calculate_intraday_quality_score(intraday, signal)
    signal["intraday_quality_score"] = round(intraday_quality, 1)

    if not intraday.get("intraday_confirmed"):
        change_pct = safe_float(intraday.get("intraday_change_pct"))
        distance_from_high = safe_float(intraday.get("distance_from_high_pct"))

        penalty = 12.0
        if change_pct <= -0.5:
            penalty = 18.0
        if change_pct <= -1.0:
            penalty = 25.0
        if change_pct <= -2.0:
            penalty = 35.0
        if not intraday.get("above_vwap"):
            penalty += 7.0
        if distance_from_high < -3.0:
            penalty += 5.0

        adjusted_score = clamp(original_score - penalty)
        signal["intraday_score_adjustment"] = round(-penalty, 1)
        signal["overall_score"] = round(adjusted_score, 1)
        signal["setup"] = "historical bullish setup; intraday confirmation failed"
        signal["ideal_option_structure"] = "watchlist only; wait for reclaim before bullish calls"
        signal["trade_plan"]["bias"] = "watchlist_only"
        signal["trade_plan"]["entry_rule"] = "Do not enter unless price reclaims intraday open/VWAP and turns positive on the day."
        signal["notes"].append(intraday.get("reason", "Failed bullish intraday confirmation."))
    else:
        # Confirmed names get a modest boost based on live quality, not just a binary pass.
        boost = max(0.0, (intraday_quality - 70.0) * 0.35)
        boost = min(boost, 12.0)
        adjusted_score = clamp(original_score + boost)

        signal["intraday_score_adjustment"] = round(boost, 1)
        signal["overall_score"] = round(adjusted_score, 1)
        signal["trade_plan"]["entry_rule"] = "Active only while intraday confirmation remains valid. Prefer entries above VWAP and near high-of-day continuation."
        signal["notes"].append("Live intraday confirmation boosted ranking quality.")

    return signal


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
        timeout = httpx.Timeout(connect=10.0, read=45.0, write=10.0, pool=45.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            stock_rows, market_date, stock_errors = await fetch_grouped_stocks(client)
            history_rows, history_errors = await fetch_historical_grouped_stocks(
                client, recent_weekday_dates(days_back=70), max_market_days=30
            )
            crypto_rows = await fetch_crypto_rows(client)

        CACHE["generated_at"] = datetime.now(timezone.utc)
        CACHE["market_date"] = market_date
        CACHE["stock_rows"] = stock_rows
        CACHE["crypto_rows"] = crypto_rows
        CACHE["history_rows"] = history_rows
        CACHE["errors"] = stock_errors + history_errors

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
        "history_ticker_count": len(CACHE.get("history_rows") or {}),
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
        "email_configured": bool(EMAIL_ADDRESS and EMAIL_PASSWORD and EMAIL_RECIPIENT),
        "tracking_file": str(TRACKED_SIGNALS_FILE),
        "tracking_records": len(load_tracked_signals()) if TRACKED_SIGNALS_FILE.exists() else 0,
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



def history_for_ticker(ticker: str) -> List[Dict[str, Any]]:
    t = normalize_ticker(ticker)
    history_rows: Dict[str, List[Dict[str, Any]]] = CACHE.get("history_rows") or {}
    for key in [polygon_stock_key(t), t.replace("-", "."), t.replace(".", "-")]:
        if key in history_rows:
            return history_rows[key]
    return []


def simple_sma(values: List[float], window: int) -> Optional[float]:
    if len(values) < window:
        return None
    subset = values[-window:]
    return sum(subset) / window


def calc_atr(bars: List[Dict[str, Any]], window: int = 14) -> Optional[float]:
    if len(bars) < 2:
        return None
    trs: List[float] = []
    prev_close = safe_float(bars[0].get("close"))
    for bar in bars[1:]:
        high = safe_float(bar.get("high"))
        low = safe_float(bar.get("low"))
        close = safe_float(bar.get("close"))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        if tr > 0:
            trs.append(tr)
        prev_close = close
    if not trs:
        return None
    subset = trs[-window:]
    return sum(subset) / len(subset)


def calc_rsi(closes: List[float], window: int = 14) -> Optional[float]:
    if len(closes) < window + 1:
        return None
    gains: List[float] = []
    losses: List[float] = []
    for i in range(-window, 0):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / window
    avg_loss = sum(losses) / window
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def pct_change_from_n(closes: List[float], days: int) -> Optional[float]:
    if len(closes) <= days or closes[-days - 1] <= 0:
        return None
    return ((closes[-1] - closes[-days - 1]) / closes[-days - 1]) * 100


def volume_anomaly(bars: List[Dict[str, Any]], window: int = 20) -> Optional[float]:
    if len(bars) < 2:
        return None
    volumes = [safe_float(b.get("volume")) for b in bars if safe_float(b.get("volume")) > 0]
    if len(volumes) < 2:
        return None
    current = volumes[-1]
    prior = volumes[-window-1:-1] if len(volumes) > window else volumes[:-1]
    if not prior:
        return None
    avg = sum(prior) / len(prior)
    return current / avg if avg > 0 else None


def trend_alignment_score(close: float, sma5: Optional[float], sma10: Optional[float], sma20: Optional[float]) -> float:
    score = 50.0
    if sma5 and close > sma5:
        score += 15
    if sma10 and close > sma10:
        score += 15
    if sma20 and close > sma20:
        score += 20
    if sma5 and sma10 and sma5 > sma10:
        score += 10
    if sma10 and sma20 and sma10 > sma20:
        score += 10
    return clamp(score)


def rsi_score(rsi: Optional[float]) -> float:
    if rsi is None:
        return 50.0
    # Best bullish momentum zone: strong but not too extended.
    if 55 <= rsi <= 70:
        return 100.0
    if 70 < rsi <= 80:
        return 85.0
    if 45 <= rsi < 55:
        return 65.0
    if 80 < rsi <= 90:
        return 55.0
    if 35 <= rsi < 45:
        return 40.0
    return 25.0

async def build_signal_rows(req: SignalsRequest, universe: str) -> Dict[str, Any]:
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

    snapshot_tickers = [
        str(r.get("requested_ticker") or r.get("ticker") or "").upper()
        for r in valid_rows
        if str(r.get("requested_ticker") or r.get("ticker") or "").upper()
    ]
    intraday_rows = await fetch_intraday_snapshots_for_tickers(snapshot_tickers)
    CACHE["intraday_rows"] = intraday_rows

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

        bars = history_for_ticker(ticker)
        closes = [safe_float(b.get("close")) for b in bars if safe_float(b.get("close")) > 0]
        atr14 = calc_atr(bars, 14)
        atr_pct = (atr14 / close) * 100 if atr14 and close else None
        rsi14 = calc_rsi(closes, 14)
        sma5 = simple_sma(closes, 5)
        sma10 = simple_sma(closes, 10)
        sma20 = simple_sma(closes, 20)
        momentum_5d = pct_change_from_n(closes, 5)
        momentum_20d = pct_change_from_n(closes, 20)
        vol_anom = volume_anomaly(bars, 20)

        intraday_momentum_score = clamp(50 + day_change_pct * 8 + (close_location - 50) * 0.35)
        historical_momentum_score = clamp(
            50
            + (momentum_5d or 0) * 4.0
            + (momentum_20d or 0) * 1.8
            + (rsi_score(rsi14) - 50) * 0.45
        )
        momentum_score = clamp(intraday_momentum_score * 0.45 + historical_momentum_score * 0.55)
        volatility_score = clamp(((atr_pct if atr_pct is not None else range_pct) * 18) + min((vol_anom or 1) - 1, 2) * 12)
        liquidity_score = clamp((volume_rank * 0.35) + (dollar_volume_rank * 0.65))
        trend_quality = clamp(
            trend_alignment_score(close, sma5, sma10, sma20) * 0.45
            + close_location * 0.25
            + rsi_score(rsi14) * 0.20
            + clamp((momentum_20d or 0) * 4 + 50) * 0.10
        )
        overall_score = clamp(
            momentum_score * 0.35
            + trend_quality * 0.30
            + liquidity_score * 0.20
            + volatility_score * 0.15
        )

        # Use ATR when available. Fall back to current day range for symbols without sufficient history.
        risk_unit = atr14 if atr14 and atr14 > 0 else max(day_range * 0.75, close * 0.015)
        risk_per_share = max(risk_unit * 0.75, close * 0.01)
        entry = round(close, 2)
        stop = round(max(close - risk_per_share, 0.01), 2)
        target_1 = round(close + risk_per_share * 1.5, 2)
        target_2 = round(close + risk_per_share * 2.5, 2)
        reward_risk = round((target_2 - entry) / max(entry - stop, 0.01), 2)

        signal = {
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
                "history": {
                    "bars": len(bars),
                    "atr14": round(atr14, 4) if atr14 is not None else None,
                    "atr14_pct": round(atr_pct, 2) if atr_pct is not None else None,
                    "rsi14": round(rsi14, 1) if rsi14 is not None else None,
                    "sma5": round(sma5, 4) if sma5 is not None else None,
                    "sma10": round(sma10, 4) if sma10 is not None else None,
                    "sma20": round(sma20, 4) if sma20 is not None else None,
                    "momentum_5d_pct": round(momentum_5d, 2) if momentum_5d is not None else None,
                    "momentum_20d_pct": round(momentum_20d, 2) if momentum_20d is not None else None,
                    "volume_anomaly_ratio": round(vol_anom, 2) if vol_anom is not None else None,
                },
                "momentum_score": round(momentum_score, 1),
                "volatility_score": round(volatility_score, 1),
                "liquidity_score": round(liquidity_score, 1),
                "trend_quality_score": round(trend_quality, 1),
                "overall_score": round(overall_score, 1),
                "setup": setup_label(overall_score, day_change_pct, close_location),
                "risk_profile": classify_risk(atr_pct if atr_pct is not None else range_pct, day_change_pct, close),
                "ideal_option_structure": classify_structure(overall_score, day_change_pct, atr_pct if atr_pct is not None else range_pct, close_location),
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
                    "Scores use cached Polygon grouped OHLCV plus recent grouped historical bars.",
                    "Stops/targets use ATR(14) when available; otherwise a range-based fallback is used.",
                ],
            }

        signal = apply_intraday_confirmation(signal, intraday_rows.get(ticker, {}))
        candidates.append(signal)

    candidates.sort(
        key=lambda x: (
            1 if x.get("intraday_confirmed") is True else 0,
            safe_float(x.get("intraday_quality_score")),
            x.get("overall_score", 0),
            x.get("historical_score", 0),
        ),
        reverse=True,
    )
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
    payload = {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": cache_status(),
        "methodology": {
            "version": "signals_v5_feedback_tracking",
            "inputs": [
                "open", "high", "low", "close", "volume", "dollar volume",
                "30 market days of grouped historical bars", "ATR(14)", "RSI(14)",
                "SMA(5/10/20)", "5d momentum", "20d momentum", "volume anomaly",
                "live intraday snapshot", "intraday confirmation", "intraday ranking boost",
                "VWAP/HOD confirmation", "recommendation tracking", "target-before-stop grading"
            ],
            "limitations": [
                "No options chain, implied volatility, Greeks, earnings calendar, or live news yet.",
                "Option structures are inferred from underlying behavior until options-chain data is added.",
                "Daily-bar grading cannot determine exact intraday order if stop and target hit on the same daily candle.",
            ],
        },
        "universes": [await build_signal_rows(req, universe) for universe in universes],
    }
    payload["tracking"] = track_signal_recommendations(payload)
    return payload



class DailyReportRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "all"] = "sp500"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    limit: int = Field(default=10, ge=1, le=25, description="Number of ranked setups to include in the email")
    refresh: bool = Field(default=False, description="Refresh cache before building the report")
    subject_prefix: str = Field(default="Option Coach Daily Report", description="Email subject prefix")
    send_email: bool = Field(default=True, description="If false, return the report without sending email")


def email_recipients() -> List[str]:
    return [x.strip() for x in EMAIL_RECIPIENT.split(",") if x.strip()]


def signal_report_text(payload: Dict[str, Any]) -> str:
    cache = payload.get("cache", {})
    lines = []
    lines.append("Option Coach Daily Report")
    lines.append(f"Market date: {cache.get('market_date')}")
    lines.append(f"Cache generated: {cache.get('generated_at')}")
    perf = payload.get("performance", {}) or {}
    if perf:
        lines.append("")
        lines.append("Performance Feedback")
        lines.append(f"Tracked total: {perf.get('tracked_total')} | Closed: {perf.get('closed_total')} | Open: {perf.get('open_total')}")
        lines.append(f"Clean win rate: {perf.get('clean_win_rate_pct')}% | Any target rate: {perf.get('any_target_rate_pct')}%")
        lines.append(f"Clean wins: {perf.get('clean_wins')} | Messy wins after stop: {perf.get('messy_wins_target_after_stop')} | Losses: {perf.get('losses_stop_before_target')}")
    lines.append("")

    for universe_block in payload.get("universes", []):
        lines.append(f"Universe: {universe_block.get('universe')} | Candidates: {universe_block.get('candidate_count')} | Returned: {universe_block.get('returned')}")
        lines.append("")
        for i, sig in enumerate(universe_block.get("signals", []), start=1):
            hist = sig.get("history", {}) or {}
            trade = sig.get("trade_plan", {}) or {}
            lines.append(f"{i}. {sig.get('ticker')} — {sig.get('setup')} | Score {sig.get('overall_score')}")
            lines.append(f"   Close: {sig.get('close')} | RSI14: {hist.get('rsi14')} | ATR14: {hist.get('atr14')} ({hist.get('atr14_pct')}%)")
            lines.append(f"   5d: {hist.get('momentum_5d_pct')}% | 20d: {hist.get('momentum_20d_pct')}% | Volume anomaly: {hist.get('volume_anomaly_ratio')}x")
            lines.append(f"   Entry: {trade.get('entry')} | Stop: {trade.get('stop')} | T1: {trade.get('target_1')} | T2: {trade.get('target_2')} | R/R: {trade.get('reward_risk_to_target_2')}")
            lines.append(f"   Structure: {sig.get('ideal_option_structure')} | Risk: {sig.get('risk_profile')}")
            lines.append("")
        skipped = universe_block.get("skipped") or []
        if skipped:
            lines.append(f"Skipped: {', '.join(skipped)}")
            lines.append("")
    return "\n".join(lines)


def esc(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def signal_report_html(payload: Dict[str, Any]) -> str:
    cache = payload.get("cache", {})
    methodology = payload.get("methodology", {})
    rows_html = []
    for universe_block in payload.get("universes", []):
        rows_html.append(f"<h2>{esc(universe_block.get('universe', '')).upper()} Signals</h2>")
        rows_html.append(
            f"<p><strong>Candidates:</strong> {esc(universe_block.get('candidate_count'))} &nbsp; "
            f"<strong>Returned:</strong> {esc(universe_block.get('returned'))}</p>"
        )
        rows_html.append("""
        <table>
          <thead>
            <tr>
              <th>#</th><th>Ticker</th><th>Setup</th><th>Score</th><th>Close</th>
              <th>RSI</th><th>ATR%</th><th>20d Mom</th><th>Vol Anom</th>
              <th>Entry</th><th>Stop</th><th>T1</th><th>T2</th><th>R/R</th><th>Structure</th>
            </tr>
          </thead><tbody>
        """)
        for i, sig in enumerate(universe_block.get("signals", []), start=1):
            hist = sig.get("history", {}) or {}
            trade = sig.get("trade_plan", {}) or {}
            rows_html.append(
                "<tr>"
                f"<td>{i}</td>"
                f"<td><strong>{esc(sig.get('ticker'))}</strong></td>"
                f"<td>{esc(sig.get('setup'))}<br><small>{esc(sig.get('risk_profile'))}</small></td>"
                f"<td>{esc(sig.get('overall_score'))}</td>"
                f"<td>{esc(sig.get('close'))}</td>"
                f"<td>{esc(hist.get('rsi14'))}</td>"
                f"<td>{esc(hist.get('atr14_pct'))}</td>"
                f"<td>{esc(hist.get('momentum_20d_pct'))}%</td>"
                f"<td>{esc(hist.get('volume_anomaly_ratio'))}x</td>"
                f"<td>{esc(trade.get('entry'))}</td>"
                f"<td>{esc(trade.get('stop'))}</td>"
                f"<td>{esc(trade.get('target_1'))}</td>"
                f"<td>{esc(trade.get('target_2'))}</td>"
                f"<td>{esc(trade.get('reward_risk_to_target_2'))}</td>"
                f"<td>{esc(sig.get('ideal_option_structure'))}</td>"
                "</tr>"
            )
        rows_html.append("</tbody></table>")
        skipped = universe_block.get("skipped") or []
        if skipped:
            rows_html.append(f"<p><strong>Skipped:</strong> {esc(', '.join(skipped))}</p>")

    errors = cache.get("errors") or []
    errors_html = "" if not errors else "<ul>" + "".join(f"<li>{esc(e)}</li>" for e in errors[:10]) + "</ul>"
    if len(errors) > 10:
        errors_html += f"<p><em>{len(errors) - 10} additional cache warnings omitted.</em></p>"

    return f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <style>
        body {{ font-family: Arial, sans-serif; color: #1f2937; line-height: 1.45; }}
        .card {{ border: 1px solid #e5e7eb; border-radius: 10px; padding: 16px; margin: 16px 0; }}
        table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
        th, td {{ border: 1px solid #e5e7eb; padding: 7px; vertical-align: top; }}
        th {{ background: #f3f4f6; text-align: left; }}
        small {{ color: #6b7280; }}
      </style>
    </head>
    <body>
      <h1>Option Coach Daily Report</h1>
      <div class="card">
        <p><strong>Market date:</strong> {esc(cache.get('market_date'))}</p>
        <p><strong>Cache generated:</strong> {esc(cache.get('generated_at'))}</p>
        <p><strong>Cache fresh:</strong> {esc(cache.get('fresh'))} | <strong>History tickers:</strong> {esc(cache.get('history_ticker_count'))}</p>
        <p><strong>Methodology:</strong> {esc(methodology.get('version'))}</p>
      </div>
      <div class="card">
        <h2>Performance Feedback</h2>
        <p><strong>Tracked:</strong> {esc((payload.get('performance') or {}).get('tracked_total'))}
        | <strong>Closed:</strong> {esc((payload.get('performance') or {}).get('closed_total'))}
        | <strong>Open:</strong> {esc((payload.get('performance') or {}).get('open_total'))}</p>
        <p><strong>Clean win rate:</strong> {esc((payload.get('performance') or {}).get('clean_win_rate_pct'))}%
        | <strong>Any target rate:</strong> {esc((payload.get('performance') or {}).get('any_target_rate_pct'))}%</p>
        <p><strong>Clean wins:</strong> {esc((payload.get('performance') or {}).get('clean_wins'))}
        | <strong>Messy wins after stop:</strong> {esc((payload.get('performance') or {}).get('messy_wins_target_after_stop'))}
        | <strong>Losses:</strong> {esc((payload.get('performance') or {}).get('losses_stop_before_target'))}</p>
      </div>
      {''.join(rows_html)}
      <div class="card">
        <h2>System Notes</h2>
        <p>This report is generated from the backend quantitative signal engine using cached Polygon grouped stock data and recent historical bars.</p>
        <p>Current limitations: no options chains, Greeks, implied volatility, earnings calendar, unusual options flow, or live news yet.</p>
        <h3>Cache warnings</h3>
        {errors_html or '<p>None.</p>'}
      </div>
    </body>
    </html>
    """


def send_email_report(subject: str, html_body: str, text_body: str) -> Dict[str, Any]:
    recipients = email_recipients()
    if not EMAIL_ADDRESS or not EMAIL_PASSWORD or not recipients:
        raise HTTPException(
            status_code=500,
            detail="Email is not configured. Set EMAIL_ADDRESS, EMAIL_PASSWORD, and EMAIL_RECIPIENT in Render.",
        )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(EMAIL_SMTP_HOST, EMAIL_SMTP_PORT, context=context) as server:
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.sendmail(EMAIL_ADDRESS, recipients, msg.as_string())
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email report: {e}")

    return {"sent": True, "recipients": recipients, "recipient_count": len(recipients)}


@app.post("/daily_report")
async def daily_report(req: DailyReportRequest) -> Dict[str, Any]:
    if not INDEX_MAP:
        raise HTTPException(status_code=500, detail="No constituents loaded. Put constituents.json next to main.py.")

    if req.refresh or not cache_is_fresh():
        await refresh_cache()

    performance_summary = await evaluate_tracked_signals()

    signals_req = SignalsRequest(
        universe=req.universe,
        horizon=req.horizon,
        limit=req.limit,
        refresh=False,
    )
    universes = ALL_UNIVERSES if signals_req.universe == "all" else [signals_req.universe]
    payload = {
        "horizon": signals_req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": cache_status(),
        "performance": performance_summary,
        "methodology": {
            "version": "signals_v5_feedback_tracking",
            "inputs": [
                "open", "high", "low", "close", "volume", "dollar volume",
                "30 market days of grouped historical bars", "ATR(14)", "RSI(14)",
                "SMA(5/10/20)", "5d momentum", "20d momentum", "volume anomaly", "live intraday snapshot", "intraday confirmation", "intraday ranking boost", "VWAP/HOD confirmation"
            ],
            "limitations": [
                "No options chain, implied volatility, Greeks, earnings calendar, or live news yet.",
                "Option structures are inferred from underlying behavior until options-chain data is added.",
            ],
        },
        "universes": [await build_signal_rows(signals_req, universe) for universe in universes],
    }
    payload["tracking"] = track_signal_recommendations(payload)

    subject_date = payload.get("cache", {}).get("market_date") or datetime.now(timezone.utc).date().isoformat()
    subject = f"{req.subject_prefix} — {str(req.universe).upper()} — {subject_date}"
    text_body = signal_report_text(payload)
    html_body = signal_report_html(payload)

    email_status = {"sent": False, "recipients": email_recipients(), "recipient_count": len(email_recipients())}
    if req.send_email:
        email_status = send_email_report(subject, html_body, text_body)

    return {
        "ok": True,
        "email": email_status,
        "subject": subject,
        "cache": payload.get("cache"),
        "performance": performance_summary,
        "tracking": payload.get("tracking"),
        "summary": {
            "universe": req.universe,
            "horizon": req.horizon,
            "limit": req.limit,
            "top_tickers": [
                s.get("ticker")
                for u in payload.get("universes", [])
                for s in (u.get("signals") or [])[: req.limit]
            ][: req.limit],
        },
        "preview_text": text_body[:4000],
    }
