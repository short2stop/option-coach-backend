from __future__ import annotations

import asyncio
import json
import os
import re
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parsedate_to_datetime

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="Option Coach Backend - Signals v2")

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
BENZINGA_API_KEY = os.getenv("BENZINGA_API_KEY", "").strip()
POLYGON_BASE = "https://api.polygon.io/v2"
BENZINGA_BASE = "https://api.benzinga.com"
CONSTITUENTS_FILE = Path(__file__).with_name("constituents.json")

ALL_UNIVERSES = ["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist"]
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
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"]
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
        day = ticker_data.get("day", {}) or {}
        minute = ticker_data.get("min", {}) or {}
        prev_day = ticker_data.get("prevDay", {}) or {}
        last_trade = ticker_data.get("lastTrade", {}) or {}

        prev_close = safe_float(prev_day.get("c"))
        todays_change = safe_float(ticker_data.get("todaysChange"))
        intraday_change_pct = safe_float(ticker_data.get("todaysChangePerc"))

        # Polygon snapshots sometimes return today's percent change while day.c/o/h/l/vw
        # are still zero/empty. Fall back aggressively so live entries do not become stale.
        current_price = (
            safe_float(day.get("c"))
            or safe_float(minute.get("c"))
            or safe_float(last_trade.get("p"))
        )

        if current_price <= 0 and prev_close > 0 and todays_change != 0:
            current_price = prev_close + todays_change

        if current_price <= 0 and prev_close > 0 and intraday_change_pct != 0:
            current_price = prev_close * (1 + intraday_change_pct / 100)

        open_price = safe_float(day.get("o")) or safe_float(minute.get("o")) or prev_close or current_price
        high_price = safe_float(day.get("h")) or safe_float(minute.get("h")) or max(open_price, current_price)
        low_price = safe_float(day.get("l")) or safe_float(minute.get("l")) or min(open_price, current_price)

        volume = (
            safe_float(day.get("v"))
            or safe_float(minute.get("av"))
            or safe_float(minute.get("v"))
        )

        day_vwap = safe_float(day.get("vw")) or safe_float(minute.get("vw")) or current_price

        if intraday_change_pct == 0 and prev_close > 0 and current_price > 0:
            intraday_change_pct = ((current_price - prev_close) / prev_close) * 100

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



def recalculate_trade_plan_from_live_price(signal: Dict[str, Any], intraday: Dict[str, Any]) -> Dict[str, Any]:
    """Recalculate entry, stop, and targets from live intraday price."""
    trade = signal.get("trade_plan", {}) or {}
    history = signal.get("history", {}) or {}

    old_entry = safe_float(trade.get("entry"))
    live_price = safe_float(intraday.get("current_price"))

    if live_price <= 0:
        signal["live_price_used_for_trade_plan"] = False
        signal["stale_entry_warning"] = True
        signal["stale_entry_reason"] = "No valid live current_price was available; trade_plan still uses historical close."
        return signal

    atr14 = safe_float(history.get("atr14"))
    day_high = safe_float(intraday.get("high"))
    day_low = safe_float(intraday.get("low"))
    day_range = max(day_high - day_low, 0.0)

    risk_unit = atr14 if atr14 > 0 else max(day_range * 0.75, live_price * 0.015)
    risk_per_share = max(risk_unit * 0.75, live_price * 0.01)

    new_entry = round(live_price, 2)
    new_stop = round(max(live_price - risk_per_share, 0.01), 2)
    new_target_1 = round(live_price + risk_per_share * 1.5, 2)
    new_target_2 = round(live_price + risk_per_share * 2.5, 2)
    new_reward_risk = round((new_target_2 - new_entry) / max(new_entry - new_stop, 0.01), 2)

    stale_entry_diff_pct = None
    stale_entry_warning = False

    if old_entry > 0:
        stale_entry_diff_pct = ((live_price - old_entry) / old_entry) * 100
        stale_entry_warning = abs(stale_entry_diff_pct) >= 1.0

    signal["historical_trade_plan"] = dict(trade)
    signal["live_price_used_for_trade_plan"] = True
    signal["stale_entry_warning"] = stale_entry_warning
    signal["stale_entry_diff_pct"] = round(stale_entry_diff_pct, 2) if stale_entry_diff_pct is not None else None

    if stale_entry_warning:
        signal["stale_entry_reason"] = (
            "Historical entry differed from live current price by more than 1%; "
            "entry/stop/targets were recalculated from live price."
        )
    else:
        signal["stale_entry_reason"] = None

    trade["entry"] = new_entry
    trade["stop"] = new_stop
    trade["target_1"] = new_target_1
    trade["target_2"] = new_target_2
    trade["risk_per_share"] = round(new_entry - new_stop, 2)
    trade["reward_risk_to_target_2"] = new_reward_risk
    trade["invalidates_below"] = new_stop
    trade["entry_basis"] = "live_intraday_current_price"
    trade["previous_historical_entry"] = round(old_entry, 2) if old_entry > 0 else None

    signal["trade_plan"] = trade
    return signal


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
    signal = recalculate_trade_plan_from_live_price(signal, intraday)
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


# -------------------------
# Benzinga capability test layer
# -------------------------

BENZINGA_TIMEOUT = httpx.Timeout(connect=8.0, read=15.0, write=8.0, pool=15.0)


def compact_benzinga_payload_summary(data: Any) -> Dict[str, Any]:
    """Return a compact summary of a Benzinga response without exposing raw data."""
    if isinstance(data, list):
        first = data[0] if data else {}
        return {
            "data_type": "list",
            "record_count": len(data),
            "sample_keys": sorted(list(first.keys()))[:20] if isinstance(first, dict) else [],
        }
    if isinstance(data, dict):
        # Some Benzinga endpoints wrap rows in common keys.
        possible_rows = None
        for key in ["data", "results", "items", "ratings", "earnings", "option_activity"]:
            if isinstance(data.get(key), list):
                possible_rows = data.get(key)
                break
        return {
            "data_type": "dict",
            "record_count": len(possible_rows) if isinstance(possible_rows, list) else (1 if data else 0),
            "sample_keys": sorted(list(data.keys()))[:20],
        }
    if isinstance(data, str):
        return {
            "data_type": "text",
            "record_count": 0 if data.strip() in {"", "<result/>"} else 1,
            "sample_keys": [],
        }
    return {"data_type": type(data).__name__, "record_count": 0, "sample_keys": []}


async def benzinga_get(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Small safe Benzinga GET wrapper.

    This deliberately returns only compact metadata. It never returns the API key
    and should not return full raw Benzinga payloads.
    """
    if not BENZINGA_API_KEY:
        return {
            "ok": False,
            "enabled": False,
            "status_code": None,
            "status": "missing_api_key",
            "error": "BENZINGA_API_KEY is not loaded",
            "summary": {"data_type": None, "record_count": 0, "sample_keys": []},
        }

    query = dict(params or {})
    query["token"] = BENZINGA_API_KEY
    url = f"{BENZINGA_BASE}{path}"

    try:
        resp = await client.get(url, params=query, headers={"accept": "application/json"})
    except Exception as e:
        return {
            "ok": False,
            "enabled": False,
            "status_code": None,
            "status": "request_failed",
            "error": str(e),
            "summary": {"data_type": None, "record_count": 0, "sample_keys": []},
        }

    status_code = resp.status_code
    text_preview = resp.text[:250] if resp.text else ""

    if status_code in {401, 403}:
        return {
            "ok": False,
            "enabled": False,
            "status_code": status_code,
            "status": "unauthorized_or_not_entitled",
            "error": text_preview,
            "summary": {"data_type": None, "record_count": 0, "sample_keys": []},
        }

    if status_code == 404:
        return {
            "ok": False,
            "enabled": False,
            "status_code": status_code,
            "status": "not_found_or_not_available",
            "error": text_preview,
            "summary": {"data_type": None, "record_count": 0, "sample_keys": []},
        }

    if status_code == 429:
        return {
            "ok": False,
            "enabled": True,
            "status_code": status_code,
            "status": "rate_limited",
            "error": text_preview,
            "summary": {"data_type": None, "record_count": 0, "sample_keys": []},
        }

    if status_code < 200 or status_code >= 300:
        return {
            "ok": False,
            "enabled": False,
            "status_code": status_code,
            "status": "http_error",
            "error": text_preview,
            "summary": {"data_type": None, "record_count": 0, "sample_keys": []},
        }

    try:
        data = resp.json()
    except Exception:
        data = resp.text.strip()

    summary = compact_benzinga_payload_summary(data)
    record_count = safe_float(summary.get("record_count"))

    if record_count > 0:
        status = "enabled_with_data"
    else:
        # 200 with no rows usually means the product is reachable, but that
        # this ticker/date/window did not return rows.
        status = "reachable_no_data"

    return {
        "ok": True,
        "enabled": True,
        "status_code": status_code,
        "status": status,
        "error": None,
        "summary": summary,
    }


BENZINGA_CAPABILITY_TESTS: Dict[str, Dict[str, Any]] = {
    # High priority for the trading engine.
    "news": {
        "path": "/api/v2/news",
        "params": {"pageSize": 3, "displayOutput": "abstract"},
        "ticker_param": "tickers",
    },
    "wiim": {
        # Benzinga documents WIIMs inside the News API family. Channel names may vary by subscription.
        "path": "/api/v2/news",
        "params": {"pageSize": 3, "displayOutput": "abstract", "channels": "WIIM"},
        "ticker_param": "tickers",
    },
    "newsquantified": {
        "path": "/api/v2/newsquantified",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "unusual_options": {
        "path": "/api/v1/signal/option_activity",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "block_trades": {
        "path": "/api/v1/signal/block_trade",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "market_movers": {
        "path": "/api/v1/market/movers",
        "params": {"limit": 3},
        "ticker_param": None,
    },
    "analyst_ratings": {
        "path": "/api/v2.1/calendar/ratings",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "ratings_firms": {
        "path": "/api/v2.1/calendar/ratings/firms",
        "params": {"limit": 3},
        "ticker_param": None,
    },
    "consensus_ratings": {
        "path": "/api/v1/consensus-ratings",
        "params": {},
        "ticker_param": "ticker",
    },
    # Calendar / risk products. If a candidate path is not included with the account,
    # the capability endpoint marks it unavailable and the backend continues normally.
    "earnings": {
        "path": "/api/v2.1/calendar/earnings",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "guidance": {
        "path": "/api/v2.1/calendar/guidance",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "offerings": {
        "path": "/api/v2.1/calendar/offerings",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "halts": {
        # Retest with likely signal/calendar halt variants in /test_benzinga_failed_capabilities.
        "path": "/api/v1/signal/halt",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "fda": {
        "path": "/api/v2.1/calendar/fda",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "mna": {
        "path": "/api/v2.1/calendar/ma",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "dividends": {
        "path": "/api/v2.1/calendar/dividends",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "splits": {
        "path": "/api/v2.1/calendar/splits",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "short_interest": {
        "path": "/api/v1/shortinterest",
        "params": {"limit": 3},
        "ticker_param": "symbols",
    },
    "insider_transactions": {
        "path": "/api/v1/sec/insider_transactions/transactions",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
    "government_trades": {
        "path": "/api/v1/government_trades",
        "params": {"limit": 3},
        "ticker_param": "tickers",
    },
}


# Step 2B: expanded retest paths for the four APIs that initially failed.
# This endpoint is intentionally diagnostic only; it does not change recommendations.
BENZINGA_FAILED_CAPABILITY_RETESTS: Dict[str, List[Dict[str, Any]]] = {
    "halts": [
        {"path": "/api/v1/signal/halt", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/signal/halts", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/signal/halt_resume", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v2.1/calendar/halt", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v2.1/calendar/halts", "params": {"limit": 3}, "ticker_param": "tickers"},
    ],
    "short_interest": [
        {"path": "/api/v1/shortinterest", "params": {"limit": 3}, "ticker_param": "symbols"},
        {"path": "/api/v1/shortinterest", "params": {"limit": 3}, "ticker_param": "symbol"},
        {"path": "/api/v1/short_interest", "params": {"limit": 3}, "ticker_param": "symbols"},
        {"path": "/api/v1/short-interest", "params": {"limit": 3}, "ticker_param": "symbols"},
    ],
    "insider_transactions": [
        {"path": "/api/v1/sec/insider_transactions/transactions", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/sec/insider_transactions/filings", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/sec/insider_transactions/owners", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/sec/insider-transactions", "params": {"limit": 3}, "ticker_param": "tickers"},
    ],
    "government_trades": [
        {"path": "/api/v1/government_trades", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/government_trades/house", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/government_trades/senate", "params": {"limit": 3}, "ticker_param": "tickers"},
        {"path": "/api/v1/government-trades", "params": {"limit": 3}, "ticker_param": "tickers"},
    ],
}


def choose_best_benzinga_attempt(attempts: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not attempts:
        return {}
    for status in ["enabled_with_data", "reachable_no_data", "rate_limited"]:
        for attempt in attempts:
            if attempt.get("status") == status:
                return attempt
    for attempt in attempts:
        if attempt.get("enabled"):
            return attempt
    return attempts[0]


async def fetch_benzinga_news_for_ticker(ticker: str, page_size: int = 5) -> Dict[str, Any]:
    """Compact one-ticker Benzinga News API check used by /test_benzinga."""
    ticker = normalize_ticker(ticker)
    timeout = BENZINGA_TIMEOUT
    async with httpx.AsyncClient(timeout=timeout) as client:
        if not BENZINGA_API_KEY:
            return {
                "benzinga_available": False,
                "ticker": ticker,
                "headline_count": 0,
                "top_headline": None,
                "top_headline_url": None,
                "created": None,
                "updated": None,
                "channels": [],
                "stocks": [],
                "importance_rank": None,
                "error": "BENZINGA_API_KEY is not loaded",
            }

        params = {
            "tickers": ticker,
            "pageSize": max(1, min(page_size, 10)),
            "displayOutput": "abstract",
        }
        query = dict(params)
        query["token"] = BENZINGA_API_KEY
        try:
            resp = await client.get(
                f"{BENZINGA_BASE}/api/v2/news",
                params=query,
                headers={"accept": "application/json"},
            )
        except Exception as e:
            return {
                "benzinga_available": False,
                "ticker": ticker,
                "headline_count": 0,
                "top_headline": None,
                "top_headline_url": None,
                "created": None,
                "updated": None,
                "channels": [],
                "stocks": [],
                "importance_rank": None,
                "error": str(e),
            }

        if resp.status_code != 200:
            return {
                "benzinga_available": False,
                "ticker": ticker,
                "headline_count": 0,
                "top_headline": None,
                "top_headline_url": None,
                "created": None,
                "updated": None,
                "channels": [],
                "stocks": [],
                "importance_rank": None,
                "error": f"HTTP {resp.status_code}: {resp.text[:250]}",
            }

        try:
            data = resp.json()
        except Exception:
            return {
                "benzinga_available": False,
                "ticker": ticker,
                "headline_count": 0,
                "top_headline": None,
                "top_headline_url": None,
                "created": None,
                "updated": None,
                "channels": [],
                "stocks": [],
                "importance_rank": None,
                "error": f"Non-JSON response: {resp.text[:250]}",
            }

    headlines = data if isinstance(data, list) else []
    top = headlines[0] if headlines else {}
    channels = [c.get("name") for c in (top.get("channels") or []) if isinstance(c, dict)] if top else []
    stocks = [s.get("name") for s in (top.get("stocks") or []) if isinstance(s, dict)] if top else []

    return {
        "benzinga_available": True,
        "ticker": ticker,
        "headline_count": len(headlines),
        "top_headline": top.get("title") if top else None,
        "top_headline_url": top.get("url") if top else None,
        "created": top.get("created") if top else None,
        "updated": top.get("updated") if top else None,
        "channels": channels[:10],
        "stocks": stocks[:20],
        "importance_rank": top.get("importance_rank") if top else None,
        "error": None,
    }


@app.get("/test_benzinga")
async def test_benzinga(ticker: str = "NVDA") -> Dict[str, Any]:
    """Quick single-endpoint Benzinga News API test. Does not expose the API key."""
    return await fetch_benzinga_news_for_ticker(ticker=ticker, page_size=5)


@app.get("/test_benzinga_capabilities")
async def test_benzinga_capabilities(ticker: str = "NVDA") -> Dict[str, Any]:
    """Check which Benzinga product endpoints appear reachable for this API key.

    This is intentionally a lightweight smoke test. A 200 with zero rows is labeled
    "reachable_no_data" because the account may have access even if the chosen ticker
    and time window do not return data.
    """
    ticker = normalize_ticker(ticker)
    if not BENZINGA_API_KEY:
        return {
            "benzinga_api_key_loaded": False,
            "ticker": ticker,
            "capabilities": {},
            "enabled_count": 0,
            "disabled_count": len(BENZINGA_CAPABILITY_TESTS),
            "note": "BENZINGA_API_KEY is not loaded in this Render service.",
        }

    results: Dict[str, Any] = {}
    timeout = BENZINGA_TIMEOUT
    async with httpx.AsyncClient(timeout=timeout) as client:
        for name, cfg in BENZINGA_CAPABILITY_TESTS.items():
            params = dict(cfg.get("params") or {})
            ticker_param = cfg.get("ticker_param")
            if ticker_param:
                params[ticker_param] = ticker
            result = await benzinga_get(client, cfg["path"], params=params)
            results[name] = {
                "enabled": bool(result.get("enabled")),
                "status": result.get("status"),
                "status_code": result.get("status_code"),
                "record_count": (result.get("summary") or {}).get("record_count"),
                "data_type": (result.get("summary") or {}).get("data_type"),
                "sample_keys": (result.get("summary") or {}).get("sample_keys"),
                "path": cfg["path"],
                "error": result.get("error"),
            }
            await asyncio.sleep(0.05)

    enabled = [k for k, v in results.items() if v.get("enabled")]
    disabled = [k for k, v in results.items() if not v.get("enabled")]

    return {
        "benzinga_api_key_loaded": True,
        "ticker": ticker,
        "capabilities": results,
        "enabled_count": len(enabled),
        "disabled_count": len(disabled),
        "enabled": enabled,
        "disabled": disabled,
        "note": (
            "This is a smoke test. 'reachable_no_data' means the endpoint returned HTTP 200 "
            "but no rows for the test request; it may still be usable with different filters."
        ),
    }


@app.get("/test_benzinga_failed_capabilities")
async def test_benzinga_failed_capabilities(ticker: str = "AAPL") -> Dict[str, Any]:
    """Retest the four Benzinga APIs that failed in the first capability smoke test.

    This tries multiple likely URL/path variants for each API and returns every attempt
    plus the best working candidate. It does not expose the API key.
    """
    ticker = normalize_ticker(ticker)
    if not BENZINGA_API_KEY:
        return {
            "benzinga_api_key_loaded": False,
            "ticker": ticker,
            "retests": {},
            "recovered": [],
            "still_disabled": list(BENZINGA_FAILED_CAPABILITY_RETESTS.keys()),
            "note": "BENZINGA_API_KEY is not loaded in this Render service.",
        }

    output: Dict[str, Any] = {}
    timeout = BENZINGA_TIMEOUT
    async with httpx.AsyncClient(timeout=timeout) as client:
        for capability, candidates in BENZINGA_FAILED_CAPABILITY_RETESTS.items():
            attempts: List[Dict[str, Any]] = []
            for cfg in candidates:
                params = dict(cfg.get("params") or {})
                ticker_param = cfg.get("ticker_param")
                if ticker_param:
                    params[ticker_param] = ticker
                result = await benzinga_get(client, cfg["path"], params=params)
                summary = result.get("summary") or {}
                attempts.append({
                    "path": cfg.get("path"),
                    "ticker_param": ticker_param,
                    "enabled": bool(result.get("enabled")),
                    "status": result.get("status"),
                    "status_code": result.get("status_code"),
                    "record_count": summary.get("record_count"),
                    "data_type": summary.get("data_type"),
                    "sample_keys": summary.get("sample_keys"),
                    "error": result.get("error"),
                })
                await asyncio.sleep(0.05)

            best = choose_best_benzinga_attempt(attempts)
            output[capability] = {
                "best_enabled": bool(best.get("enabled")),
                "best_status": best.get("status"),
                "best_path": best.get("path"),
                "best_ticker_param": best.get("ticker_param"),
                "best_record_count": best.get("record_count"),
                "attempts": attempts,
            }

    recovered = [k for k, v in output.items() if v.get("best_enabled")]
    still_disabled = [k for k, v in output.items() if not v.get("best_enabled")]

    return {
        "benzinga_api_key_loaded": True,
        "ticker": ticker,
        "retests": output,
        "recovered": recovered,
        "still_disabled": still_disabled,
        "note": (
            "Use best_path values that show enabled or reachable_no_data in the next integration. "
            "reachable_no_data means the endpoint exists but did not return rows for this ticker/window."
        ),
    }



@app.get("/")
def read_root() -> Dict[str, str]:
    return {"message": "Option Coach Backend is running."}


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "polygon_api_key_loaded": bool(POLYGON_API_KEY),
        "benzinga_api_key_loaded": bool(BENZINGA_API_KEY),
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
    include_news_catalysts: bool = Field(default=True, description="Include separate Polygon news catalyst scoring when available")
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "sp500"
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
            "version": "signals_v9_news_catalysts",
            "inputs": [
                "open", "high", "low", "close", "volume", "dollar volume",
                "30 market days of grouped historical bars", "ATR(14)", "RSI(14)",
                "SMA(5/10/20)", "5d momentum", "20d momentum", "volume anomaly", "live intraday snapshot", "intraday confirmation", "intraday ranking boost", "VWAP/HOD confirmation", "recommendation tracking", "target-before-stop grading", "live-price entry recalculation", "snapshot minute/last-trade fallback"
            ],
            "limitations": [
                "No options chain, implied volatility, Greeks, earnings calendar, or live news yet.",
                "Option structures are inferred from underlying behavior until options-chain data is added.",
                "Daily-bar grading cannot determine exact intraday order if stop and target hit on the same daily candle.",
                "Live trade plans use intraday current_price when available; historical grouped close is retained separately.",
            ],
        },
        "universes": [await build_signal_rows(req, universe) for universe in universes],
    }
    if getattr(req, "include_news_catalysts", True):
        payload = await enrich_payload_with_news_catalysts(payload)
    payload["tracking"] = track_signal_recommendations(payload)
    return payload



# -------------------------
# Feedback / performance tracking
# -------------------------

def ensure_data_dir() -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def load_tracked_signals() -> List[Dict[str, Any]]:
    ensure_data_dir()
    if not TRACKED_SIGNALS_FILE.exists():
        return []
    try:
        with TRACKED_SIGNALS_FILE.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, list) else []
    except Exception:
        return []


def save_tracked_signals(records: List[Dict[str, Any]]) -> None:
    ensure_data_dir()
    tmp = TRACKED_SIGNALS_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)
    tmp.replace(TRACKED_SIGNALS_FILE)


def tracking_key(record: Dict[str, Any]) -> str:
    return "|".join([
        str(record.get("created_date", "")),
        str(record.get("ticker", "")),
        str(record.get("entry", "")),
        str(record.get("stop", "")),
        str(record.get("target_1", "")),
    ])


def signal_to_tracking_record(sig: Dict[str, Any], universe: str, horizon: str, rank: int) -> Dict[str, Any]:
    trade = sig.get("trade_plan", {}) or {}
    hist = sig.get("history", {}) or {}
    intra = sig.get("intraday", {}) or {}
    now = datetime.now(timezone.utc)
    return {
        "id": str(uuid4()),
        "created_at": now.isoformat(),
        "created_date": now.date().isoformat(),
        "universe": universe,
        "horizon": horizon,
        "rank": rank,
        "ticker": sig.get("ticker"),
        "setup": sig.get("setup"),
        "risk_profile": sig.get("risk_profile"),
        "entry_status": sig.get("entry_status"),
        "action": sig.get("action"),
        "entry": safe_float(trade.get("entry")),
        "stop": safe_float(trade.get("stop")),
        "target_1": safe_float(trade.get("target_1")),
        "target_2": safe_float(trade.get("target_2")),
        "historical_score": safe_float(sig.get("historical_score", sig.get("overall_score"))),
        "overall_score": safe_float(sig.get("overall_score")),
        "intraday_quality_score": safe_float(sig.get("intraday_quality_score")),
        "intraday_score_adjustment": safe_float(sig.get("intraday_score_adjustment")),
        "rsi14": hist.get("rsi14"),
        "atr14": hist.get("atr14"),
        "momentum_5d_pct": hist.get("momentum_5d_pct"),
        "momentum_20d_pct": hist.get("momentum_20d_pct"),
        "volume_anomaly_ratio": hist.get("volume_anomaly_ratio"),
        "intraday_change_pct": intra.get("intraday_change_pct"),
        "above_vwap": intra.get("above_vwap"),
        "distance_from_high_pct": intra.get("distance_from_high_pct"),
        "status": "open",
        "outcome": "no_hit_yet",
        "first_hit": None,
        "target_hit_at": None,
        "stop_hit_at": None,
        "target_hit_after_stop": False,
        "max_gain_pct": None,
        "max_drawdown_pct": None,
        "last_evaluated_at": None,
        "evaluation_days": 0,
        "bars_checked": 0,
        "notes": [],
    }


def track_signal_recommendations(payload: Dict[str, Any]) -> Dict[str, Any]:
    records = load_tracked_signals()
    existing = {tracking_key(r) for r in records}
    added = 0

    for universe_block in payload.get("universes", []):
        universe = universe_block.get("universe")
        for rank, sig in enumerate((universe_block.get("signals") or [])[:TRACKING_TOP_N], start=1):
            record = signal_to_tracking_record(sig, universe, payload.get("horizon", "1d"), rank)
            key = tracking_key(record)
            if key not in existing:
                records.append(record)
                existing.add(key)
                added += 1

    cutoff = datetime.now(timezone.utc) - timedelta(days=730)
    cleaned = []
    for r in records:
        try:
            created = datetime.fromisoformat(str(r.get("created_at")).replace("Z", "+00:00"))
        except Exception:
            created = datetime.now(timezone.utc)
        if r.get("status") != "closed" or created >= cutoff:
            cleaned.append(r)

    save_tracked_signals(cleaned)
    return {"tracked_total": len(cleaned), "tracked_added": added}


async def fetch_daily_bars_for_tracking(client: httpx.AsyncClient, ticker: str, from_date: str, to_date: str) -> List[Dict[str, Any]]:
    if not POLYGON_API_KEY:
        return []
    url = f"https://api.polygon.io/v2/aggs/ticker/{polygon_stock_key(ticker)}/range/1/day/{from_date}/{to_date}"
    params = {"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": POLYGON_API_KEY}
    try:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return []
        rows = resp.json().get("results") or []
    except Exception:
        return []

    bars: List[Dict[str, Any]] = []
    for r in rows:
        ts = r.get("t")
        try:
            date = datetime.fromtimestamp(ts / 1000, timezone.utc).date().isoformat() if ts else None
        except Exception:
            date = None
        bars.append({
            "date": date,
            "open": safe_float(r.get("o")),
            "high": safe_float(r.get("h")),
            "low": safe_float(r.get("l")),
            "close": safe_float(r.get("c")),
            "volume": safe_float(r.get("v")),
        })
    return bars


def grade_record_with_bars(record: Dict[str, Any], bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    entry = safe_float(record.get("entry"))
    stop = safe_float(record.get("stop"))
    target = safe_float(record.get("target_1"))
    if entry <= 0 or stop <= 0 or target <= 0:
        record["outcome"] = "cannot_grade_missing_levels"
        return record

    first_hit = record.get("first_hit")
    stop_hit_at = record.get("stop_hit_at")
    target_hit_at = record.get("target_hit_at")
    max_high = entry
    min_low = entry
    bars_checked = 0

    for bar in bars:
        high = safe_float(bar.get("high"))
        low = safe_float(bar.get("low"))
        date = bar.get("date")
        if high <= 0 or low <= 0:
            continue
        bars_checked += 1
        max_high = max(max_high, high)
        min_low = min(min_low, low)
        hit_target = high >= target
        hit_stop = low <= stop

        if hit_target and not target_hit_at:
            target_hit_at = date
        if hit_stop and not stop_hit_at:
            stop_hit_at = date

        # With daily bars, same-day order is unknown if both levels hit.
        if not first_hit:
            if hit_target and hit_stop:
                first_hit = "ambiguous_same_day"
            elif hit_target:
                first_hit = "target"
            elif hit_stop:
                first_hit = "stop"

    record["bars_checked"] = bars_checked
    record["max_gain_pct"] = round(((max_high - entry) / entry) * 100, 2)
    record["max_drawdown_pct"] = round(((min_low - entry) / entry) * 100, 2)
    record["target_hit_at"] = target_hit_at
    record["stop_hit_at"] = stop_hit_at
    record["first_hit"] = first_hit
    record["target_hit_after_stop"] = bool(target_hit_at and stop_hit_at and first_hit == "stop")

    if first_hit == "target":
        record["status"] = "closed"
        record["outcome"] = "clean_win_target_before_stop"
    elif first_hit == "ambiguous_same_day":
        record["status"] = "closed"
        record["outcome"] = "ambiguous_target_and_stop_same_day"
    elif first_hit == "stop" and target_hit_at:
        record["status"] = "closed"
        record["outcome"] = "messy_win_target_after_stop"
    elif first_hit == "stop":
        record["status"] = "open_after_stop"
        record["outcome"] = "stop_hit_first_monitoring_for_later_target"
    else:
        record["status"] = "open"
        record["outcome"] = "no_hit_yet"

    try:
        created = datetime.fromisoformat(str(record.get("created_at")).replace("Z", "+00:00"))
        record["evaluation_days"] = (datetime.now(timezone.utc).date() - created.date()).days
    except Exception:
        record["evaluation_days"] = None

    if record.get("evaluation_days") is not None and record["evaluation_days"] >= TRACKING_MAX_DAYS and record["status"] in {"open", "open_after_stop"}:
        record["status"] = "closed"
        record["outcome"] = "loss_stop_before_target" if first_hit == "stop" else "expired_no_target_or_stop"

    record["last_evaluated_at"] = datetime.now(timezone.utc).isoformat()
    return record


async def evaluate_tracked_signals() -> Dict[str, Any]:
    records = load_tracked_signals()
    if not records:
        return build_performance_summary(records, updated=0)

    updated = 0
    today = datetime.now(timezone.utc).date()
    timeout = httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=25.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        for record in records:
            if record.get("status") == "closed":
                continue
            ticker = str(record.get("ticker") or "").upper()
            if not ticker:
                continue
            try:
                created = datetime.fromisoformat(str(record.get("created_at")).replace("Z", "+00:00")).date()
            except Exception:
                continue
            bars = await fetch_daily_bars_for_tracking(client, ticker, created.isoformat(), today.isoformat())
            if not bars:
                continue
            before = json.dumps(record, sort_keys=True, default=str)
            grade_record_with_bars(record, bars)
            after = json.dumps(record, sort_keys=True, default=str)
            if before != after:
                updated += 1
            await asyncio.sleep(0.05)

    save_tracked_signals(records)
    return build_performance_summary(records, updated=updated)


def build_performance_summary(records: List[Dict[str, Any]], updated: int = 0) -> Dict[str, Any]:
    closed = [r for r in records if r.get("status") == "closed"]
    open_records = [r for r in records if r.get("status") != "closed"]
    clean = [r for r in closed if r.get("outcome") == "clean_win_target_before_stop"]
    messy = [r for r in closed if r.get("outcome") == "messy_win_target_after_stop"]
    ambiguous = [r for r in closed if r.get("outcome") == "ambiguous_target_and_stop_same_day"]
    losses = [r for r in closed if r.get("outcome") == "loss_stop_before_target"]
    expired = [r for r in closed if r.get("outcome") == "expired_no_target_or_stop"]
    total = len(closed)
    clean_rate = round(len(clean) / total * 100, 1) if total else None
    any_target_rate = round((len(clean) + len(messy) + len(ambiguous)) / total * 100, 1) if total else None

    def avg(field: str):
        vals = [safe_float(r.get(field)) for r in closed if r.get(field) is not None]
        return round(sum(vals) / len(vals), 2) if vals else None

    def group_by(field: str) -> List[Dict[str, Any]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in closed:
            groups.setdefault(str(r.get(field) or "unknown"), []).append(r)
        output = []
        for name, rows in sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True):
            cw = sum(1 for r in rows if r.get("outcome") == "clean_win_target_before_stop")
            at = sum(1 for r in rows if r.get("outcome") in {"clean_win_target_before_stop", "messy_win_target_after_stop", "ambiguous_target_and_stop_same_day"})
            output.append({
                field: name,
                "closed": len(rows),
                "clean_wins": cw,
                "clean_win_rate_pct": round(cw / len(rows) * 100, 1) if rows else None,
                "any_target_rate_pct": round(at / len(rows) * 100, 1) if rows else None,
            })
        return output[:10]

    return {
        "tracked_total": len(records),
        "updated": updated,
        "closed_total": total,
        "open_total": len(open_records),
        "clean_wins": len(clean),
        "messy_wins_target_after_stop": len(messy),
        "ambiguous_target_and_stop_same_day": len(ambiguous),
        "losses_stop_before_target": len(losses),
        "expired_no_target_or_stop": len(expired),
        "clean_win_rate_pct": clean_rate,
        "any_target_rate_pct": any_target_rate,
        "avg_max_gain_pct": avg("max_gain_pct"),
        "avg_max_drawdown_pct": avg("max_drawdown_pct"),
        "by_setup": group_by("setup"),
        "by_entry_status": group_by("entry_status"),
        "by_risk_profile": group_by("risk_profile"),
        "recent_closed": closed[-10:],
        "recent_open": open_records[-10:],
    }


@app.post("/performance")
async def performance() -> Dict[str, Any]:
    return await evaluate_tracked_signals()





# -------------------------
# News catalyst layer
# -------------------------

NEWS_LOOKBACK_HOURS = int(os.getenv("OPTION_COACH_NEWS_LOOKBACK_HOURS", "24"))
NEWS_MAX_ARTICLES_PER_TICKER = int(os.getenv("OPTION_COACH_NEWS_MAX_ARTICLES_PER_TICKER", "5"))


def news_since_iso(hours: int = NEWS_LOOKBACK_HOURS) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


def score_news_text(title: str, description: str = "") -> Dict[str, Any]:
    text_blob = f"{title} {description}".lower()

    bullish_terms = {
        "beats": 16,
        "beat": 12,
        "raises guidance": 20,
        "raises outlook": 18,
        "record revenue": 16,
        "surges": 10,
        "jumps": 8,
        "rallies": 8,
        "upgrade": 12,
        "upgraded": 12,
        "price target raised": 14,
        "partnership": 12,
        "contract": 14,
        "order": 12,
        "acquisition": 10,
        "ai": 10,
        "artificial intelligence": 10,
        "nvidia": 14,
        "trillion-dollar": 18,
        "data center": 10,
        "defense": 8,
        "approval": 14,
        "patent": 8,
        "funding": 8,
        "buyback": 12,
        "dividend increase": 10,
    }

    bearish_terms = {
        "misses": -18,
        "miss": -12,
        "cuts guidance": -22,
        "cuts outlook": -20,
        "downgrade": -14,
        "downgraded": -14,
        "offering": -18,
        "public offering": -20,
        "dilution": -18,
        "investigation": -12,
        "lawsuit": -12,
        "recall": -14,
        "bankruptcy": -25,
        "going concern": -22,
        "resigns": -10,
        "halt": -18,
        "halts": -18,
        "delisting": -25,
        "loss widens": -14,
        "revenue falls": -12,
    }

    raw_score = 0
    matched_positive: List[str] = []
    matched_negative: List[str] = []

    for term, points in bullish_terms.items():
        if term in text_blob:
            raw_score += points
            matched_positive.append(term)

    for term, points in bearish_terms.items():
        if term in text_blob:
            raw_score += points
            matched_negative.append(term)

    catalyst_score = clamp(50 + raw_score, 0, 100)

    if catalyst_score >= 75:
        label = "strong_positive_catalyst"
    elif catalyst_score >= 60:
        label = "positive_catalyst"
    elif catalyst_score <= 25:
        label = "strong_negative_catalyst"
    elif catalyst_score <= 40:
        label = "negative_catalyst"
    else:
        label = "neutral_or_no_clear_catalyst"

    return {
        "score": round(catalyst_score, 1),
        "label": label,
        "matched_positive_terms": matched_positive[:10],
        "matched_negative_terms": matched_negative[:10],
    }


async def fetch_polygon_news_for_ticker(client: httpx.AsyncClient, ticker: str) -> List[Dict[str, Any]]:
    if not POLYGON_API_KEY:
        return []

    url = "https://api.polygon.io/v2/reference/news"
    params = {
        "ticker": polygon_stock_key(ticker),
        "published_utc.gte": news_since_iso(),
        "order": "desc",
        "limit": NEWS_MAX_ARTICLES_PER_TICKER,
        "apiKey": POLYGON_API_KEY,
    }

    try:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return []
        rows = resp.json().get("results") or []
    except Exception:
        return []

    articles: List[Dict[str, Any]] = []
    for r in rows[:NEWS_MAX_ARTICLES_PER_TICKER]:
        title = str(r.get("title") or "")
        description = str(r.get("description") or "")
        scoring = score_news_text(title, description)
        publisher = r.get("publisher") or {}

        articles.append({
            "title": title,
            "publisher": publisher.get("name"),
            "published_utc": r.get("published_utc"),
            "article_url": r.get("article_url"),
            "description": description[:280] if description else None,
            "score": scoring.get("score"),
            "label": scoring.get("label"),
            "matched_positive_terms": scoring.get("matched_positive_terms"),
            "matched_negative_terms": scoring.get("matched_negative_terms"),
        })

    return articles


def combine_news_articles(ticker: str, articles: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not articles:
        return {
            "ticker": ticker,
            "lookback_hours": NEWS_LOOKBACK_HOURS,
            "article_count": 0,
            "news_catalyst_score": 0,
            "news_catalyst_label": "no_recent_news_found",
            "top_headlines": [],
            "positive_terms": [],
            "negative_terms": [],
            "note": "No Polygon news articles found in the configured lookback window.",
        }

    weighted_total = 0.0
    weight_sum = 0.0
    for i, article in enumerate(articles):
        score = safe_float(article.get("score"), 50.0)
        weight = max(1.0, len(articles) - i)
        weighted_total += score * weight
        weight_sum += weight

    combined_score = weighted_total / weight_sum if weight_sum else 50.0

    pos_terms: List[str] = []
    neg_terms: List[str] = []
    for article in articles:
        pos_terms.extend(article.get("matched_positive_terms") or [])
        neg_terms.extend(article.get("matched_negative_terms") or [])

    if combined_score >= 75:
        label = "strong_positive_catalyst"
    elif combined_score >= 60:
        label = "positive_catalyst"
    elif combined_score <= 25:
        label = "strong_negative_catalyst"
    elif combined_score <= 40:
        label = "negative_catalyst"
    else:
        label = "neutral_or_mixed_news"

    return {
        "ticker": ticker,
        "lookback_hours": NEWS_LOOKBACK_HOURS,
        "article_count": len(articles),
        "news_catalyst_score": round(combined_score, 1),
        "news_catalyst_label": label,
        "top_headlines": articles[:3],
        "positive_terms": sorted(set(pos_terms))[:12],
        "negative_terms": sorted(set(neg_terms))[:12],
    }


async def enrich_payload_with_news_catalysts(payload: Dict[str, Any]) -> Dict[str, Any]:
    tickers: List[str] = []

    for universe_block in payload.get("universes", []) or []:
        for sig in universe_block.get("signals", []) or []:
            ticker = str(sig.get("ticker") or "").upper()
            if ticker and not is_crypto_ticker(ticker):
                tickers.append(ticker)

    tickers = sorted(set(tickers))

    if not tickers:
        payload["news_catalyst_summary"] = {
            "enabled": bool(POLYGON_API_KEY),
            "lookback_hours": NEWS_LOOKBACK_HOURS,
            "tickers_checked": 0,
            "top_news_catalysts": [],
        }
        return payload

    timeout = httpx.Timeout(connect=10.0, read=20.0, write=10.0, pool=20.0)
    semaphore = asyncio.Semaphore(6)
    news_map: Dict[str, Dict[str, Any]] = {}

    async with httpx.AsyncClient(timeout=timeout) as client:
        async def one(ticker: str) -> None:
            async with semaphore:
                articles = await fetch_polygon_news_for_ticker(client, ticker)
                news_map[ticker] = combine_news_articles(ticker, articles)
                await asyncio.sleep(0.05)

        await asyncio.gather(*(one(t) for t in tickers), return_exceptions=True)

    for universe_block in payload.get("universes", []) or []:
        for sig in universe_block.get("signals", []) or []:
            ticker = str(sig.get("ticker") or "").upper()
            catalyst = news_map.get(ticker) or combine_news_articles(ticker, [])
            sig["news_catalyst"] = catalyst

            technical = safe_float(sig.get("overall_score"))
            news_score = safe_float(catalyst.get("news_catalyst_score"))
            sig["technical_plus_news_score"] = round(
                clamp(technical * 0.82 + news_score * 0.18) if news_score > 0 else technical,
                1,
            )

    top = sorted(
        [v for v in news_map.values() if safe_float(v.get("news_catalyst_score")) > 0],
        key=lambda x: safe_float(x.get("news_catalyst_score")),
        reverse=True,
    )[:10]

    payload["news_catalyst_summary"] = {
        "enabled": bool(POLYGON_API_KEY),
        "lookback_hours": NEWS_LOOKBACK_HOURS,
        "tickers_checked": len(tickers),
        "top_news_catalysts": top,
        "note": "News catalyst score is separate from technical ranking. Use it to explain/confirm why a setup may be moving, especially premarket.",
    }

    return payload



# -------------------------
# Premarket signal layer
# -------------------------

class PremarketRequest(BaseModel):
    include_news_catalysts: bool = Field(default=True, description="Include separate Polygon news catalyst scoring when available")
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "sp500"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False, description="Refresh daily cache before returning premarket signals")
    limit: int = Field(default=10, ge=1, le=50, description="Maximum number of ranked candidates to return")
    min_price: float = Field(default=0.0, ge=0.0, description="Minimum underlying price")
    min_volume: float = Field(default=0.0, ge=0.0, description="Minimum prior-day share volume")
    min_premarket_volume: float = Field(default=0.0, ge=0.0, description="Minimum premarket share volume")


def premarket_window_utc_ms() -> tuple[int, int]:
    today = datetime.now(timezone.utc).date()
    start = datetime(today.year, today.month, today.day, 8, 0, tzinfo=timezone.utc)
    end = datetime(today.year, today.month, today.day, 14, 30, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


async def fetch_premarket_bars(client: httpx.AsyncClient, ticker: str) -> List[Dict[str, Any]]:
    if not POLYGON_API_KEY:
        return []

    today = datetime.now(timezone.utc).date().isoformat()
    url = f"https://api.polygon.io/v2/aggs/ticker/{polygon_stock_key(ticker)}/range/5/minute/{today}/{today}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 5000,
        "apiKey": POLYGON_API_KEY,
    }

    start_ms, end_ms = premarket_window_utc_ms()

    try:
        resp = await client.get(url, params=params)
        if resp.status_code != 200:
            return []
        rows = resp.json().get("results") or []
    except Exception:
        return []

    bars: List[Dict[str, Any]] = []
    for r in rows:
        ts = r.get("t")
        if ts is None or ts < start_ms or ts > end_ms:
            continue
        bars.append({
            "t": ts,
            "open": safe_float(r.get("o")),
            "high": safe_float(r.get("h")),
            "low": safe_float(r.get("l")),
            "close": safe_float(r.get("c")),
            "volume": safe_float(r.get("v")),
            "vwap": safe_float(r.get("vw")),
        })
    return bars


def classify_premarket_setup(gap_pct: float, distance_from_high_pct: float, position_pct: float, premarket_volume: float, range_pct: float) -> tuple[str, str]:
    if premarket_volume <= 0:
        return "no_premarket_data", "wait_for_open_confirmation"
    if gap_pct > 0 and distance_from_high_pct >= -1.0 and position_pct >= 70:
        if gap_pct >= 8:
            return "extended_gap_and_hold", "wait_for_opening_range_break_or_pullback"
        return "gap_and_hold", "buy_opening_range_break_or_vwap_reclaim"
    if gap_pct > 0 and position_pct < 40:
        return "gap_and_fade", "avoid_or_wait_for_reclaim"
    if gap_pct < 0 and position_pct >= 70:
        return "red_to_green_attempt", "wait_for_prior_close_reclaim"
    if range_pct >= 5 and distance_from_high_pct < -2:
        return "premarket_exhaustion", "avoid_chasing"
    return "neutral_premarket", "wait_for_open_confirmation"


def build_premarket_metrics(ticker: str, prev_close: float, bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not bars or prev_close <= 0:
        return {
            "ticker": ticker,
            "premarket_available": False,
            "premarket_price": None,
            "gap_pct": None,
            "premarket_high": None,
            "premarket_low": None,
            "premarket_volume": 0,
            "distance_from_premarket_high_pct": None,
            "premarket_range_pct": None,
            "premarket_position_pct": None,
            "premarket_setup": "no_premarket_data",
            "opening_strategy": "wait_for_open_confirmation",
        }

    price = safe_float(bars[-1].get("close"))
    high = max(safe_float(b.get("high")) for b in bars)
    low = min(safe_float(b.get("low")) for b in bars)
    volume = sum(safe_float(b.get("volume")) for b in bars)

    gap_pct = ((price - prev_close) / prev_close) * 100 if prev_close else 0.0
    rng = max(high - low, 0.0)
    range_pct = (rng / price) * 100 if price else 0.0
    distance_from_high_pct = ((price - high) / high) * 100 if high else 0.0
    position_pct = ((price - low) / rng) * 100 if rng > 0 else 50.0

    setup, strategy = classify_premarket_setup(gap_pct, distance_from_high_pct, position_pct, volume, range_pct)

    return {
        "ticker": ticker,
        "premarket_available": True,
        "premarket_price": round(price, 4),
        "gap_pct": round(gap_pct, 2),
        "premarket_high": round(high, 4),
        "premarket_low": round(low, 4),
        "premarket_volume": round(volume, 0),
        "distance_from_premarket_high_pct": round(distance_from_high_pct, 2),
        "premarket_range_pct": round(range_pct, 2),
        "premarket_position_pct": round(position_pct, 1),
        "premarket_setup": setup,
        "opening_strategy": strategy,
    }


def premarket_quality_score(metrics: Dict[str, Any], signal: Dict[str, Any]) -> float:
    if not metrics.get("premarket_available"):
        return 0.0

    gap = safe_float(metrics.get("gap_pct"))
    dist = safe_float(metrics.get("distance_from_premarket_high_pct"))
    pos = safe_float(metrics.get("premarket_position_pct"))
    vol = safe_float(metrics.get("premarket_volume"))
    rng = safe_float(metrics.get("premarket_range_pct"))
    hist = safe_float(signal.get("overall_score"))

    score = 40.0

    if 0.5 <= gap <= 6:
        score += 20
    elif 6 < gap <= 10:
        score += 10
    elif gap > 10:
        score -= 5
    elif gap < -1:
        score -= 10

    if dist >= -0.5:
        score += 20
    elif dist >= -1.0:
        score += 14
    elif dist >= -2.0:
        score += 6
    else:
        score -= 10

    if pos >= 80:
        score += 12
    elif pos >= 65:
        score += 8
    elif pos < 40:
        score -= 10

    if vol >= 1000000:
        score += 12
    elif vol >= 250000:
        score += 8
    elif vol >= 50000:
        score += 4
    else:
        score -= 5

    if rng > 10:
        score -= 8

    score += (hist - 50) * 0.15
    return clamp(score)


def recalculate_trade_plan_from_premarket_price(signal: Dict[str, Any], metrics: Dict[str, Any]) -> Dict[str, Any]:
    trade = signal.get("trade_plan", {}) or {}
    history = signal.get("history", {}) or {}

    price = safe_float(metrics.get("premarket_price"))
    if price <= 0:
        return signal

    old_entry = safe_float(trade.get("entry"))
    atr14 = safe_float(history.get("atr14"))
    high = safe_float(metrics.get("premarket_high"))
    low = safe_float(metrics.get("premarket_low"))
    rng = max(high - low, 0.0)

    risk_unit = atr14 if atr14 > 0 else max(rng * 0.75, price * 0.015)
    risk_per_share = max(risk_unit * 0.75, price * 0.01)

    entry = round(price, 2)
    stop = round(max(entry - risk_per_share, 0.01), 2)
    target_1 = round(entry + risk_per_share * 1.5, 2)
    target_2 = round(entry + risk_per_share * 2.5, 2)

    signal["historical_trade_plan"] = dict(trade)
    signal["premarket_price_used_for_trade_plan"] = True
    signal["stale_entry_diff_pct"] = round(((entry - old_entry) / old_entry) * 100, 2) if old_entry > 0 else None
    signal["stale_entry_warning"] = bool(old_entry > 0 and abs(((entry - old_entry) / old_entry) * 100) >= 1.0)

    trade["entry"] = entry
    trade["stop"] = stop
    trade["target_1"] = target_1
    trade["target_2"] = target_2
    trade["risk_per_share"] = round(entry - stop, 2)
    trade["reward_risk_to_target_2"] = round((target_2 - entry) / max(entry - stop, 0.01), 2)
    trade["invalidates_below"] = stop
    trade["entry_basis"] = "premarket_price"
    trade["previous_historical_entry"] = round(old_entry, 2) if old_entry > 0 else None
    trade["opening_strategy"] = metrics.get("opening_strategy")

    signal["trade_plan"] = trade
    return signal


async def build_premarket_rows(req: PremarketRequest, universe: str) -> Dict[str, Any]:
    signals_req = SignalsRequest(
        universe=universe,
        horizon=req.horizon,
        tickers=req.tickers,
        refresh=False,
        limit=min(max(req.limit, 1), 50),
        min_price=req.min_price,
        min_volume=req.min_volume,
    )

    base_block = await build_signal_rows(signals_req, universe)
    signals = base_block.get("signals", [])

    timeout = httpx.Timeout(connect=10.0, read=25.0, write=10.0, pool=25.0)
    semaphore = asyncio.Semaphore(8)

    async with httpx.AsyncClient(timeout=timeout) as client:
        async def one(sig: Dict[str, Any]) -> Dict[str, Any]:
            ticker = str(sig.get("ticker") or "").upper()
            async with semaphore:
                prev_close = safe_float(sig.get("close"))
                bars = await fetch_premarket_bars(client, ticker)
                metrics = build_premarket_metrics(ticker, prev_close, bars)
                sig["premarket"] = metrics
                sig["premarket_quality_score"] = round(premarket_quality_score(metrics, sig), 1)

                if safe_float(metrics.get("premarket_volume")) < req.min_premarket_volume:
                    sig["premarket_excluded_reason"] = "Below min_premarket_volume"
                else:
                    sig = recalculate_trade_plan_from_premarket_price(sig, metrics)

                await asyncio.sleep(0.05)
                return sig

        updated = await asyncio.gather(*(one(s) for s in signals), return_exceptions=True)

    candidates = [x for x in updated if isinstance(x, dict) and not x.get("premarket_excluded_reason")]
    candidates.sort(
        key=lambda x: (
            safe_float(x.get("premarket_quality_score")),
            safe_float(x.get("overall_score")),
        ),
        reverse=True,
    )

    return {
        "universe": universe,
        "candidate_count": len(candidates),
        "returned": min(req.limit, len(candidates)),
        "signals": candidates[:req.limit],
        "skipped": base_block.get("skipped", []),
    }


@app.post("/premarket_signals")
async def premarket_signals(req: PremarketRequest) -> Dict[str, Any]:
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
            "version": "premarket_signals_v1",
            "inputs": [
                "prior daily OHLCV",
                "historical ATR/RSI/momentum",
                "5-minute premarket aggregates",
                "premarket gap",
                "premarket high/low",
                "premarket volume",
                "distance from premarket high",
                "premarket range position"
            ],
            "limitations": [
                "Premarket liquidity can be thin and misleading.",
                "Opening volatility can invalidate premarket levels quickly.",
                "Options chains, IV, Greeks, earnings, news, and flow are not included yet."
            ],
        },
        "universes": [await build_premarket_rows(req, universe) for universe in universes],
    }

    if getattr(req, "include_news_catalysts", True):
        payload = await enrich_payload_with_news_catalysts(payload)
    return payload




# -------------------------
# End-of-day / overnight risk layer
# -------------------------

class OvernightRiskRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "sp500"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False, description="Refresh cache before overnight risk check")
    limit: int = Field(default=10, ge=1, le=50, description="Maximum number of candidates to return")
    min_price: float = Field(default=0.0, ge=0.0)
    min_volume: float = Field(default=0.0, ge=0.0)
    include_news_catalysts: bool = Field(default=True, description="Include separate Polygon news catalyst scoring when available")


def close_location_from_intraday(intraday: Dict[str, Any]) -> Optional[float]:
    current = safe_float(intraday.get("current_price"))
    high = safe_float(intraday.get("high"))
    low = safe_float(intraday.get("low"))
    rng = high - low
    if current <= 0 or high <= 0 or low <= 0 or rng <= 0:
        return None
    return ((current - low) / rng) * 100


def distance_from_low_pct(intraday: Dict[str, Any]) -> Optional[float]:
    current = safe_float(intraday.get("current_price"))
    low = safe_float(intraday.get("low"))
    if current <= 0 or low <= 0:
        return None
    return ((current - low) / low) * 100


def compute_overnight_risk(signal: Dict[str, Any]) -> Dict[str, Any]:
    intraday = signal.get("intraday", {}) or {}
    news = signal.get("news_catalyst", {}) or {}

    current = safe_float(intraday.get("current_price"))
    change_pct = safe_float(intraday.get("intraday_change_pct"))
    distance_hod = safe_float(intraday.get("distance_from_high_pct"))
    above_vwap = bool(intraday.get("above_vwap"))
    above_open = bool(intraday.get("above_open"))
    intraday_confirmed = bool(intraday.get("intraday_confirmed"))

    close_location = close_location_from_intraday(intraday)
    dist_lod = distance_from_low_pct(intraday)

    news_score = safe_float(news.get("news_catalyst_score"))
    news_label = str(news.get("news_catalyst_label") or "")

    score = 50.0
    risk_flags: List[str] = []
    support_factors: List[str] = []

    if current <= 0:
        return {
            "overnight_hold_score": 0,
            "overnight_action": "cannot_assess_no_live_price",
            "after_hours_dip_risk": "unknown",
            "confidence_pct": 0,
            "closing_strength": None,
            "distance_from_lod_pct": None,
            "risk_flags": ["No valid live current price available."],
            "support_factors": [],
            "reason": "Cannot assess overnight risk without a valid live price.",
        }

    if above_vwap:
        score += 14
        support_factors.append("Holding above VWAP")
    else:
        score -= 20
        risk_flags.append("Below VWAP")

    if above_open:
        score += 8
        support_factors.append("Above intraday open")
    else:
        score -= 10
        risk_flags.append("Below intraday open")

    if intraday_confirmed:
        score += 12
        support_factors.append("Intraday confirmation still valid")
    else:
        score -= 12
        risk_flags.append("Intraday confirmation failed or weak")

    if close_location is not None:
        if close_location >= 80:
            score += 16
            support_factors.append("Trading in top 20% of intraday range")
        elif close_location >= 60:
            score += 8
            support_factors.append("Trading in upper half of intraday range")
        elif close_location <= 30:
            score -= 18
            risk_flags.append("Trading in bottom 30% of intraday range")
        elif close_location <= 45:
            score -= 8
            risk_flags.append("Weak close-location inside intraday range")

    if distance_hod >= -0.75:
        score += 12
        support_factors.append("Within 0.75% of high of day")
    elif distance_hod >= -1.5:
        score += 6
        support_factors.append("Still near high of day")
    elif distance_hod <= -4:
        score -= 16
        risk_flags.append("Fading far from high of day")
    elif distance_hod <= -2.5:
        score -= 8
        risk_flags.append("Meaningfully off high of day")

    if change_pct >= 3:
        score += 10
        support_factors.append("Strong positive intraday move")
    elif change_pct >= 1:
        score += 5
        support_factors.append("Positive intraday move")
    elif change_pct <= -3:
        score -= 18
        risk_flags.append("Large negative intraday move")
    elif change_pct <= -1:
        score -= 8
        risk_flags.append("Negative intraday move")

    if news_score >= 75:
        score += 12
        support_factors.append("Strong positive news catalyst")
    elif news_score >= 60:
        score += 6
        support_factors.append("Positive news catalyst")
    elif 0 < news_score <= 40:
        score -= 10
        risk_flags.append("Negative or weak news catalyst")

    if "negative" in news_label:
        score -= 8
        risk_flags.append("News catalyst label is negative")

    if change_pct >= 8 and distance_hod < -1.5 and news_score < 60:
        score -= 10
        risk_flags.append("Extended intraday move fading without strong news support")

    score = clamp(score)

    if score >= 78:
        action = "hold_overnight"
        dip_risk = "lower"
    elif score >= 62:
        action = "hold_partial_position"
        dip_risk = "moderate"
    elif score >= 45:
        action = "take_profits_before_close"
        dip_risk = "elevated"
    else:
        action = "exit_before_close"
        dip_risk = "high"

    if action == "hold_overnight":
        reason = "Closing structure is strong enough to justify overnight exposure."
    elif action == "hold_partial_position":
        reason = "Setup remains constructive, but overnight gap risk justifies reducing exposure."
    elif action == "take_profits_before_close":
        reason = "Risk/reward into the close is no longer clearly favorable."
    else:
        reason = "Weak closing structure or risk flags argue against holding overnight."

    return {
        "overnight_hold_score": round(score, 1),
        "overnight_action": action,
        "after_hours_dip_risk": dip_risk,
        "confidence_pct": round(score, 1),
        "closing_strength": round(close_location, 1) if close_location is not None else None,
        "distance_from_lod_pct": round(dist_lod, 2) if dist_lod is not None else None,
        "risk_flags": risk_flags,
        "support_factors": support_factors,
        "reason": reason,
    }


async def build_overnight_risk_rows(req: OvernightRiskRequest, universe: str) -> Dict[str, Any]:
    signals_req = SignalsRequest(
        universe=universe,
        horizon=req.horizon,
        tickers=req.tickers,
        refresh=False,
        limit=req.limit,
        min_price=req.min_price,
        min_volume=req.min_volume,
    )

    base_block = await build_signal_rows(signals_req, universe)
    payload = {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": cache_status(),
        "universes": [base_block],
    }

    # News enrichment exists in the news-catalyst backend. Fall back gracefully if absent.
    if req.include_news_catalysts and "enrich_payload_with_news_catalysts" in globals():
        payload = await enrich_payload_with_news_catalysts(payload)

    block = payload.get("universes", [{}])[0]
    for sig in block.get("signals", []) or []:
        sig["overnight_risk"] = compute_overnight_risk(sig)

    signals = block.get("signals", []) or []
    signals.sort(
        key=lambda x: safe_float((x.get("overnight_risk") or {}).get("overnight_hold_score")),
        reverse=True,
    )

    block["signals"] = signals[:req.limit]
    block["returned"] = min(req.limit, len(signals))
    return block


@app.post("/overnight_risk")
async def overnight_risk(req: OvernightRiskRequest) -> Dict[str, Any]:
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
            "version": "overnight_risk_v1",
            "inputs": [
                "live current price",
                "VWAP",
                "intraday open",
                "distance from HOD",
                "distance from LOD",
                "close location in intraday range",
                "intraday confirmation",
                "news catalyst score",
            ],
            "classification": [
                "hold_overnight",
                "hold_partial_position",
                "take_profits_before_close",
                "exit_before_close",
            ],
            "limitations": [
                "Does not yet include earnings calendar.",
                "Does not yet include options-chain IV crush risk.",
                "After-hours liquidity can change rapidly after the assessment.",
            ],
        },
        "universes": [await build_overnight_risk_rows(req, universe) for universe in universes],
    }

    all_signals: List[Dict[str, Any]] = []
    for block in payload.get("universes", []) or []:
        all_signals.extend(block.get("signals", []) or [])

    payload["overnight_summary"] = {
        "hold_overnight": [
            s.get("ticker") for s in all_signals
            if (s.get("overnight_risk") or {}).get("overnight_action") == "hold_overnight"
        ],
        "hold_partial_position": [
            s.get("ticker") for s in all_signals
            if (s.get("overnight_risk") or {}).get("overnight_action") == "hold_partial_position"
        ],
        "take_profits_before_close": [
            s.get("ticker") for s in all_signals
            if (s.get("overnight_risk") or {}).get("overnight_action") == "take_profits_before_close"
        ],
        "exit_before_close": [
            s.get("ticker") for s in all_signals
            if (s.get("overnight_risk") or {}).get("overnight_action") == "exit_before_close"
        ],
        "top_hold_scores": [
            {
                "ticker": s.get("ticker"),
                "score": (s.get("overnight_risk") or {}).get("overnight_hold_score"),
                "action": (s.get("overnight_risk") or {}).get("overnight_action"),
            }
            for s in sorted(
                all_signals,
                key=lambda x: safe_float((x.get("overnight_risk") or {}).get("overnight_hold_score")),
                reverse=True,
            )[:10]
        ],
    }

    return payload





# -------------------------
# After-hours dip-buy order ticket layer
# -------------------------

class AfterHoursDipOrderRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "watchlist"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False, description="Refresh cache before generating after-hours dip tickets")
    limit: int = Field(default=10, ge=1, le=50)
    min_price: float = Field(default=0.0, ge=0.0)
    min_volume: float = Field(default=0.0, ge=0.0)
    include_news_catalysts: bool = Field(default=True)
    max_position_dollars: float = Field(default=500.0, ge=0.0, description="Maximum dollars per ticker for share quantity estimates")
    dip_levels_pct: List[float] = Field(default=[1.0, 2.0, 3.0], description="Dip percentages below current price for limit order tickets")
    require_above_vwap: bool = Field(default=True, description="Only create buy tickets if price is above VWAP")
    require_intraday_confirmed: bool = Field(default=True, description="Only create buy tickets if intraday confirmation is valid")
    allow_very_high_risk: bool = Field(default=False, description="If false, very-high-risk names are capped or skipped unless strong technical conditions exist")


def compute_after_hours_dip_order_plan(signal: Dict[str, Any], req: AfterHoursDipOrderRequest) -> Dict[str, Any]:
    ticker = str(signal.get("ticker") or "").upper()
    intraday = signal.get("intraday", {}) or {}
    trade = signal.get("trade_plan", {}) or {}
    news = signal.get("news_catalyst", {}) or {}
    history = signal.get("history", {}) or {}

    current = safe_float(intraday.get("current_price"))
    vwap = safe_float(intraday.get("day_vwap"))
    above_vwap = bool(intraday.get("above_vwap"))
    intraday_confirmed = bool(intraday.get("intraday_confirmed"))
    distance_hod = safe_float(intraday.get("distance_from_high_pct"))
    change_pct = safe_float(intraday.get("intraday_change_pct"))
    atr_pct = safe_float(history.get("atr14_pct"))
    volume_anomaly = safe_float(history.get("volume_anomaly_ratio"))
    risk_profile = str(signal.get("risk_profile") or "").lower()
    news_score = safe_float(news.get("news_catalyst_score"))

    skip_reasons: List[str] = []
    warnings: List[str] = []
    support_factors: List[str] = []

    if current <= 0:
        skip_reasons.append("No valid live current price.")
    if req.require_above_vwap and not above_vwap:
        skip_reasons.append("Current price is not above VWAP.")
    if req.require_intraday_confirmed and not intraday_confirmed:
        skip_reasons.append("Intraday confirmation is not valid.")
    if vwap <= 0:
        warnings.append("VWAP unavailable or invalid.")
    if "very high" in risk_profile and not req.allow_very_high_risk:
        warnings.append("Very-high-risk stock; use smaller size or skip unless intentionally speculative.")
    if current < 5:
        warnings.append("Low-priced stock under $5; after-hours liquidity and spreads may be poor.")
    if news_score == 0:
        warnings.append("No recent news catalyst found; dip-buy thesis is technical only.")
    if volume_anomaly > 0 and volume_anomaly < 1:
        warnings.append("Volume anomaly below 1.0; today's move may not have above-average participation.")
    if atr_pct >= 6:
        warnings.append("High ATR%; use wider stops and smaller sizing.")

    if above_vwap:
        support_factors.append("Above VWAP")
    if intraday_confirmed:
        support_factors.append("Intraday confirmation valid")
    if distance_hod >= -1:
        support_factors.append("Near high of day")
    if news_score >= 60:
        support_factors.append("Positive news catalyst")
    if change_pct > 0:
        support_factors.append("Positive intraday trend")

    tickets: List[Dict[str, Any]] = []

    if not skip_reasons:
        for pct in req.dip_levels_pct:
            pct = abs(safe_float(pct))
            if pct <= 0:
                continue

            limit_price = round(current * (1 - pct / 100), 2)

            # Do not place dip buys below VWAP unless the user intentionally disables VWAP requirement.
            if req.require_above_vwap and vwap > 0 and limit_price < vwap:
                ticket_status = "watch_only_below_vwap"
                ticket_note = "Limit is below VWAP; this should require VWAP reclaim before buying."
            else:
                ticket_status = "eligible_manual_ticket"
                ticket_note = "Review spread/liquidity before placing."

            estimated_shares = int(req.max_position_dollars // limit_price) if limit_price > 0 and req.max_position_dollars > 0 else None
            estimated_notional = round(estimated_shares * limit_price, 2) if estimated_shares else None

            tickets.append({
                "ticker": ticker,
                "side": "BUY",
                "order_type": "LIMIT",
                "time_in_force": "DAY_PLUS_EXTENDED_HOURS",
                "limit_price": limit_price,
                "dip_pct_from_current": pct,
                "estimated_shares": estimated_shares,
                "estimated_notional": estimated_notional,
                "status": ticket_status,
                "note": ticket_note,
            })

    # Suggested cancel / invalidation level for all dip tickets.
    stop = safe_float(trade.get("stop"))
    invalidates_below = safe_float(trade.get("invalidates_below")) or stop
    if invalidates_below <= 0 and vwap > 0:
        invalidates_below = round(vwap * 0.99, 2)

    if skip_reasons:
        action = "do_not_place_after_hours_dip_orders"
    elif warnings and ("very-high-risk" in " ".join(warnings).lower() or current < 5):
        action = "manual_review_small_size_only"
    else:
        action = "eligible_for_manual_after_hours_dip_tickets"

    return {
        "ticker": ticker,
        "current_price": round(current, 4) if current else None,
        "vwap": round(vwap, 4) if vwap else None,
        "intraday_change_pct": round(change_pct, 2),
        "distance_from_hod_pct": round(distance_hod, 2),
        "above_vwap": above_vwap,
        "intraday_confirmed": intraday_confirmed,
        "risk_profile": signal.get("risk_profile"),
        "news_catalyst_score": news_score,
        "action": action,
        "support_factors": support_factors,
        "warnings": warnings,
        "skip_reasons": skip_reasons,
        "cancel_if_below": round(invalidates_below, 2) if invalidates_below else None,
        "manual_order_tickets": tickets,
        "important_note": "These are manual limit-order tickets only. This endpoint does not submit orders to Schwab or any broker.",
    }


async def build_after_hours_dip_order_rows(req: AfterHoursDipOrderRequest, universe: str) -> Dict[str, Any]:
    signals_req = SignalsRequest(
        universe=universe,
        horizon=req.horizon,
        tickers=req.tickers,
        refresh=False,
        limit=req.limit,
        min_price=req.min_price,
        min_volume=req.min_volume,
    )

    base_block = await build_signal_rows(signals_req, universe)

    payload = {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": cache_status(),
        "universes": [base_block],
    }

    if req.include_news_catalysts and "enrich_payload_with_news_catalysts" in globals():
        payload = await enrich_payload_with_news_catalysts(payload)

    block = payload.get("universes", [{}])[0]
    for sig in block.get("signals", []) or []:
        sig["after_hours_dip_order_plan"] = compute_after_hours_dip_order_plan(sig, req)

    signals = block.get("signals", []) or []
    signals.sort(
        key=lambda x: (
            1 if (x.get("after_hours_dip_order_plan") or {}).get("action") == "eligible_for_manual_after_hours_dip_tickets" else 0,
            safe_float(x.get("technical_plus_news_score") or x.get("overall_score")),
            safe_float(x.get("intraday_quality_score")),
        ),
        reverse=True,
    )

    block["signals"] = signals[:req.limit]
    block["returned"] = min(req.limit, len(signals))
    return block


@app.post("/after_hours_dip_orders")
async def after_hours_dip_orders(req: AfterHoursDipOrderRequest) -> Dict[str, Any]:
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
            "version": "after_hours_dip_orders_v1",
            "purpose": "Generate manual Schwab-style limit order tickets for after-hours dip buys.",
            "inputs": [
                "live current price",
                "VWAP",
                "intraday confirmation",
                "distance from HOD",
                "technical score",
                "news catalyst score",
                "risk profile",
                "volume anomaly",
            ],
            "risk_controls": [
                "Limit orders only.",
                "No market orders.",
                "No broker submission.",
                "Can require price above VWAP.",
                "Can require intraday confirmation.",
                "Flags low-priced and very-high-risk stocks.",
            ],
            "limitations": [
                "Does not check Schwab symbol eligibility.",
                "Does not verify live bid/ask spread.",
                "Does not submit or cancel orders.",
                "After-hours liquidity may be extremely thin.",
            ],
        },
        "universes": [await build_after_hours_dip_order_rows(req, universe) for universe in universes],
    }

    all_plans: List[Dict[str, Any]] = []
    for block in payload.get("universes", []) or []:
        for sig in block.get("signals", []) or []:
            plan = sig.get("after_hours_dip_order_plan") or {}
            all_plans.append({
                "ticker": sig.get("ticker"),
                "action": plan.get("action"),
                "ticket_count": len(plan.get("manual_order_tickets") or []),
                "warnings": plan.get("warnings") or [],
                "skip_reasons": plan.get("skip_reasons") or [],
            })

    payload["after_hours_dip_order_summary"] = {
        "eligible": [p for p in all_plans if p.get("action") == "eligible_for_manual_after_hours_dip_tickets"],
        "manual_review": [p for p in all_plans if p.get("action") == "manual_review_small_size_only"],
        "do_not_place": [p for p in all_plans if p.get("action") == "do_not_place_after_hours_dip_orders"],
        "important_note": "Review every ticket manually in Schwab. Use limit orders only. This backend does not place live trades.",
    }

    return payload





# -------------------------
# Compact premarket brief layer
# -------------------------

class PremarketBriefRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "watchlist"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False)
    limit: int = Field(default=10, ge=1, le=25)
    min_price: float = Field(default=0.0, ge=0.0)
    min_volume: float = Field(default=0.0, ge=0.0)
    min_premarket_volume: float = Field(default=0.0, ge=0.0)
    include_news_catalysts: bool = Field(default=True)


def compact_premarket_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    pre = sig.get("premarket", {}) or {}
    trade = sig.get("trade_plan", {}) or {}
    news = sig.get("news_catalyst", {}) or {}

    headline = None
    top_headlines = news.get("top_headlines") or []
    if top_headlines:
        h = top_headlines[0] or {}
        headline = {
            "title": h.get("title"),
            "publisher": h.get("publisher"),
            "published_utc": h.get("published_utc"),
            "score": h.get("score"),
            "label": h.get("label"),
        }

    gap = safe_float(pre.get("gap_pct"))
    dist_high = safe_float(pre.get("distance_from_premarket_high_pct"))
    pre_vol = safe_float(pre.get("premarket_volume"))

    if pre.get("premarket_setup") in {"gap_and_hold", "red_to_green_attempt"} and dist_high >= -1.0:
        action = "watch_opening_range_break"
    elif pre.get("premarket_setup") == "extended_gap_and_hold":
        action = "wait_for_pullback_or_opening_range_break"
    elif pre.get("premarket_setup") in {"gap_and_fade", "premarket_exhaustion"}:
        action = "avoid_at_open"
    elif gap > 0 and pre_vol > 0:
        action = "wait_for_open_confirmation"
    else:
        action = "watch_only"

    return {
        "ticker": sig.get("ticker"),
        "action": action,
        "premarket_price": pre.get("premarket_price"),
        "gap_pct": pre.get("gap_pct"),
        "premarket_volume": pre.get("premarket_volume"),
        "premarket_high": pre.get("premarket_high"),
        "premarket_low": pre.get("premarket_low"),
        "distance_from_premarket_high_pct": pre.get("distance_from_premarket_high_pct"),
        "premarket_position_pct": pre.get("premarket_position_pct"),
        "premarket_setup": pre.get("premarket_setup"),
        "opening_strategy": pre.get("opening_strategy"),
        "entry": trade.get("entry"),
        "entry_basis": trade.get("entry_basis"),
        "stop": trade.get("stop"),
        "target_1": trade.get("target_1"),
        "target_2": trade.get("target_2"),
        "risk_reward": trade.get("reward_risk_to_target_2"),
        "invalidates_below": trade.get("invalidates_below"),
        "overall_score": sig.get("overall_score"),
        "premarket_quality_score": sig.get("premarket_quality_score"),
        "news_catalyst_score": news.get("news_catalyst_score", 0),
        "news_catalyst_label": news.get("news_catalyst_label"),
        "technical_plus_news_score": sig.get("technical_plus_news_score") or sig.get("overall_score"),
        "top_headline": headline,
        "risk_profile": sig.get("risk_profile"),
        "setup": sig.get("setup"),
    }


async def build_premarket_brief_rows(req: PremarketBriefRequest, universe: str) -> Dict[str, Any]:
    pre_req = PremarketRequest(
        universe=universe,
        horizon=req.horizon,
        tickers=req.tickers,
        refresh=False,
        limit=req.limit,
        min_price=req.min_price,
        min_volume=req.min_volume,
        min_premarket_volume=req.min_premarket_volume,
    )

    if hasattr(pre_req, "include_news_catalysts"):
        pre_req.include_news_catalysts = req.include_news_catalysts

    block = await build_premarket_rows(pre_req, universe)
    signals = block.get("signals", []) or []
    compact = [compact_premarket_signal(sig) for sig in signals]

    compact.sort(
        key=lambda x: (
            safe_float(x.get("technical_plus_news_score")),
            safe_float(x.get("premarket_quality_score")),
            safe_float(x.get("premarket_volume")),
        ),
        reverse=True,
    )

    return {
        "universe": universe,
        "candidate_count": block.get("candidate_count", len(compact)),
        "returned": min(req.limit, len(compact)),
        "signals": compact[:req.limit],
        "skipped": (block.get("skipped") or [])[:25],
    }


@app.post("/premarket_brief")
async def premarket_brief(req: PremarketBriefRequest) -> Dict[str, Any]:
    if not INDEX_MAP and not req.tickers:
        raise HTTPException(status_code=500, detail="No constituents loaded. Put constituents.json next to main.py or pass explicit tickers.")

    if req.refresh or not cache_is_fresh():
        await refresh_cache()

    universes = ALL_UNIVERSES if req.universe == "all" else [req.universe]
    cache = cache_status()

    universe_blocks = [await build_premarket_brief_rows(req, universe) for universe in universes]

    all_rows: List[Dict[str, Any]] = []
    for block in universe_blocks:
        all_rows.extend(block.get("signals", []) or [])

    all_rows.sort(
        key=lambda x: (
            safe_float(x.get("technical_plus_news_score")),
            safe_float(x.get("premarket_quality_score")),
            safe_float(x.get("premarket_volume")),
        ),
        reverse=True,
    )

    return {
        "horizon": req.horizon,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "cache": {
            "cached": cache.get("cached"),
            "fresh": cache.get("fresh"),
            "generated_at": cache.get("generated_at"),
            "market_date": cache.get("market_date"),
            "stale_data_warning": stale.get("stale_data_warning"),
            "stale_data_reason": stale.get("stale_data_reason"),
            "market_date_age_calendar_days": stale.get("market_date_age_calendar_days"),
            "errors": (cache.get("errors") or [])[:5],
        },
        "methodology": {
            "version": "premarket_brief_v1",
            "purpose": "Compact premarket scan response for GPT Action size limits.",
        },
        "universes": universe_blocks,
        "summary": {
            "top_candidates": [
                {
                    "ticker": r.get("ticker"),
                    "action": r.get("action"),
                    "gap_pct": r.get("gap_pct"),
                    "premarket_volume": r.get("premarket_volume"),
                    "score": r.get("technical_plus_news_score"),
                    "news_score": r.get("news_catalyst_score"),
                }
                for r in all_rows[:10]
            ],
            "avoid_at_open": [
                r.get("ticker") for r in all_rows
                if r.get("action") == "avoid_at_open"
            ][:10],
        },
    }

# -------------------------
# Benzinga catalyst + premarket recommendation layer v2
# -------------------------

BENZINGA_LOOKBACK_HOURS = int(os.getenv("BENZINGA_LOOKBACK_HOURS", "24"))
BENZINGA_MAX_ROWS = int(os.getenv("BENZINGA_MAX_ROWS", "10"))


def compute_stale_data_warning(cache: Dict[str, Any]) -> Dict[str, Any]:
    market_date = cache.get("market_date")
    generated_at = cache.get("generated_at")
    age_days = None
    warning = False
    reason = None
    try:
        md = datetime.strptime(str(market_date)[:10], "%Y-%m-%d").date() if market_date else None
        if generated_at:
            gd = datetime.fromisoformat(str(generated_at).replace("Z", "+00:00")).date()
        else:
            gd = datetime.now(timezone.utc).date()
        if md:
            age_days = (gd - md).days
            if age_days > 3:
                warning = True
                reason = f"Polygon grouped market_date is {age_days} calendar days behind generated_at. Verify live/pre-market prices before trading."
    except Exception:
        pass
    errors = cache.get("errors") or []
    if any("No grouped stock rows returned" in str(e) for e in errors) and age_days is not None and age_days > 1:
        warning = True
        reason = reason or "Polygon grouped rows were unavailable for at least one recent date. Verify freshness before trading."
    return {
        "stale_data_warning": warning,
        "stale_data_reason": reason,
        "market_date_age_calendar_days": age_days,
    }


def benzinga_extract_rows(data: Any) -> List[Dict[str, Any]]:
    """Extract the likely row list from common Benzinga response shapes."""
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in [
            "data", "results", "items", "news", "ratings", "earnings", "guidance",
            "offerings", "fda", "ma", "dividends", "splits", "option_activity",
            "shortInterestData", "insider_transactions", "transactions", "block_trades",
            "blocks", "result", "halt_resume", "halts",
        ]:
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
            if isinstance(val, dict):
                # Movers often wrap another list under result.*
                nested = benzinga_extract_rows(val)
                if nested:
                    return nested
        # If it is a single object, keep it as one row.
        if data:
            return [data]
    return []


def _split_symbol_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(_split_symbol_list(item))
        return out
    if isinstance(value, dict):
        out: List[str] = []
        for key in [
            "ticker", "tickers", "symbol", "symbols", "name", "underlying",
            "underlying_symbol", "root_symbol", "security_symbol", "stock",
            "target_ticker", "acquirer_ticker", "company_ticker",
        ]:
            if key in value:
                out.extend(_split_symbol_list(value.get(key)))
        return out
    text = str(value).upper().replace(";", ",").replace("|", ",")
    return [x.strip() for x in text.split(",") if x.strip()]


def _option_symbol_matches_ticker(symbol: str, ticker: str) -> bool:
    symbol = str(symbol or "").upper().strip()
    ticker = normalize_ticker(ticker)
    if not symbol or not ticker:
        return False
    if symbol == ticker:
        return True
    # OCC-style option symbols often begin with the root, then a YYMMDD date.
    # Example: AAPL260821C00250000. Avoid treating CAT as a match for CATH, etc.
    return bool(re.match(rf"^{re.escape(ticker)}\d{{6}}[CP]", symbol))


def benzinga_row_mentions_ticker(row: Dict[str, Any], ticker: str) -> bool:
    """Strict local ticker filter for Benzinga rows.

    Several Benzinga endpoints return broad data even when a ticker query parameter
    is supplied. Recommendations must not score another ticker's options flow,
    earnings, offerings, FDA, M&A, or ratings as if it belonged to the requested
    symbol. This function only accepts explicit ticker/stock/symbol matches.
    """
    ticker = normalize_ticker(ticker)
    if not isinstance(row, dict) or not ticker:
        return False

    explicit_fields = [
        "ticker", "tickers", "symbol", "symbols", "stock", "stocks",
        "underlying", "underlying_symbol", "root_symbol", "security_symbol",
        "option_symbol", "target_ticker", "acquirer_ticker", "company_ticker",
    ]

    candidates: List[str] = []
    for key in explicit_fields:
        if key in row:
            candidates.extend(_split_symbol_list(row.get(key)))

    # Some calendars use slightly different names. Add a shallow scan for
    # keys that clearly identify tickers/symbols without full-text matching.
    for key, value in row.items():
        lk = str(key).lower()
        if any(token in lk for token in ["ticker", "symbol", "underlying"]):
            candidates.extend(_split_symbol_list(value))

    for candidate in candidates:
        c = str(candidate or "").upper().strip()
        if c == ticker or _option_symbol_matches_ticker(c, ticker):
            return True

    return False


def filter_benzinga_rows_for_ticker(rows: List[Dict[str, Any]], ticker: str) -> List[Dict[str, Any]]:
    return [row for row in rows if benzinga_row_mentions_ticker(row, ticker)]


TICKER_COMPANY_HINTS = {
    "AAPL": ["APPLE"],
    "TSLA": ["TESLA"],
    "CAT": ["CATERPILLAR"],
    "JPM": ["JPMORGAN", "JP MORGAN", "J.P. MORGAN"],
    "MRK": ["MERCK"],
    "INTC": ["INTEL"],
    "UNH": ["UNITEDHEALTH", "UNITED HEALTH", "UNITEDHEALTH GROUP"],
    "ADI": ["ANALOG DEVICES"],
    "KLAC": ["KLA", "KLA CORP", "KLA CORPORATION"],
    "LRCX": ["LAM RESEARCH"],
    "DFLI": ["DRAGONFLY", "DRAGONFLY ENERGY"],
    "KRKNF": ["KRAKEN", "KRAKEN ROBOTICS"],
}


def benzinga_text_blob(row: Dict[str, Any]) -> str:
    parts = []
    for key in ["title", "headline", "teaser", "body", "description", "comments"]:
        val = row.get(key)
        if val:
            parts.append(str(val))
    return " ".join(parts)


def benzinga_row_is_direct_news(row: Dict[str, Any], ticker: str) -> bool:
    """Return True when a news/WIIM row is directly about the ticker.

    Benzinga news can include a ticker in the stocks metadata for broad sector/market
    stories. Those are useful context, but they should not become the top headline or
    score as a ticker-specific catalyst unless the headline/teaser/body directly names
    the ticker or a known company name.
    """
    ticker = normalize_ticker(ticker)
    if not row or not ticker:
        return False
    text = benzinga_text_blob(row).upper()
    if re.search(rf"\b{re.escape(ticker)}\b", text):
        return True
    for hint in TICKER_COMPANY_HINTS.get(ticker, []):
        if hint and hint.upper() in text:
            return True
    return False


def filter_direct_news_rows(rows: List[Dict[str, Any]], ticker: str) -> List[Dict[str, Any]]:
    return [row for row in rows if benzinga_row_is_direct_news(row, ticker)]


def benzinga_row_date(row: Dict[str, Any]) -> Optional[datetime]:
    for key in ["date", "created", "updated", "time", "event_date", "report_date", "announced", "announce_date"]:
        if row.get(key):
            dt = parse_benzinga_date(row.get(key))
            if dt:
                return dt
    return None


def benzinga_row_is_current_event(row: Dict[str, Any], past_days: int = 5, future_days: int = 30) -> bool:
    dt = benzinga_row_date(row)
    if not dt:
        return False
    delta = (dt.date() - datetime.now(timezone.utc).date()).days
    return -past_days <= delta <= future_days


def mna_row_has_explicit_role(row: Dict[str, Any], ticker: str) -> bool:
    """Strict M&A role check.

    Benzinga M&A rows may include broad ticker metadata or company-name text. For
    trading-risk purposes, only treat M&A as actionable when the row explicitly
    names the requested ticker in an acquirer/target/buyer/seller ticker or symbol
    field. Generic fields like ``ticker``/``tickers`` or company names are context,
    not enough to create an M&A risk flag.
    """
    ticker = normalize_ticker(ticker)
    if not ticker or not isinstance(row, dict):
        return False

    strict_role_fields = [
        "acquirer_ticker", "target_ticker", "buyer_ticker", "seller_ticker",
        "parent_ticker", "company_ticker", "deal_ticker", "acquired_ticker",
        "acquirer_symbol", "target_symbol", "buyer_symbol", "seller_symbol",
        "parent_symbol", "company_symbol",
    ]
    for key in strict_role_fields:
        if key not in row:
            continue
        val = row.get(key)
        # Do not let company names like "JPMorgan Chase" or "Caterpillar"
        # count here. These fields must carry ticker-like symbols.
        for candidate in _split_symbol_list(val):
            c = str(candidate or "").upper().strip()
            if c == ticker:
                return True
    return False


async def benzinga_raw_get(
    client: httpx.AsyncClient,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Internal Benzinga GET wrapper that returns parsed data for scoring.

    Never expose this raw payload directly to GPT Action responses.
    """
    if not BENZINGA_API_KEY:
        return {"ok": False, "status_code": None, "error": "BENZINGA_API_KEY is not loaded", "data": None}

    query = dict(params or {})
    query["token"] = BENZINGA_API_KEY
    url = f"{BENZINGA_BASE}{path}"

    try:
        resp = await client.get(url, params=query, headers={"accept": "application/json"})
    except Exception as e:
        return {"ok": False, "status_code": None, "error": str(e), "data": None}

    if resp.status_code < 200 or resp.status_code >= 300:
        return {"ok": False, "status_code": resp.status_code, "error": resp.text[:200], "data": None}

    try:
        data = resp.json()
    except Exception:
        data = resp.text.strip()

    return {"ok": True, "status_code": resp.status_code, "error": None, "data": data}


def minutes_since_benzinga_time(value: Any) -> Optional[int]:
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(str(value))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        mins = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds() / 60
        return max(0, int(mins))
    except Exception:
        return None


def classify_catalyst_type(text: str, channels: List[str]) -> str:
    blob = f"{text} {' '.join(channels)}".lower()
    if any(x in blob for x in ["rating", "upgrade", "downgrade", "price target", "analyst"]):
        return "analyst"
    if any(x in blob for x in ["earnings", "eps", "revenue", "guidance"]):
        return "earnings"
    if any(x in blob for x in ["fda", "pdufa", "clinical", "trial", "approval"]):
        return "fda"
    if any(x in blob for x in ["merger", "acquisition", "m&a", "buyout", "deal"]):
        return "mna"
    if any(x in blob for x in ["offering", "dilution", "shelf", "warrant"]):
        return "offering"
    if any(x in blob for x in ["lawsuit", "investigation", "legal"]):
        return "legal"
    if any(x in blob for x in ["product", "launch", "partnership", "contract", "order"]):
        return "product_contract"
    if any(x in blob for x in ["cpi", "fed", "inflation", "rates", "macro"]):
        return "macro"
    return "news" if text.strip() else "none"


def _text_has_term(blob: str, term: str) -> bool:
    """Match catalyst terms without false hits like 'miss' inside 'dismisses'."""
    term = term.lower().strip()
    if not term:
        return False
    if " " in term or "-" in term:
        return term in blob
    return bool(re.search(rf"\b{re.escape(term)}\b", blob))


def score_rows_text(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"score": 0.0, "label": "no_data", "positive_terms": [], "negative_terms": []}

    positive_terms = [
        "upgrade", "upgraded", "price target raised", "raises price target", "raises guidance",
        "beats", "beat", "earnings beat", "contract", "partnership", "fda approval",
        "approval", "buy rating", "strong demand", "record revenue", "launches", "winner",
        "long ideas", "surges", "jumps", "rallies", "acquisition", "order", "selected",
        "dismisses all litigation claims", "dismissed all litigation claims", "court dismisses",
    ]
    negative_terms = [
        "downgrade", "downgraded", "price target cut", "cuts price target", "cuts guidance",
        "misses", "miss", "offering", "dilution", "investigation", "bankruptcy",
        "sell rating", "trading lower", "turnaround taking longer", "halt", "delisting",
        "loss widens", "revenue falls", "recall",
    ]

    raw = 0.0
    matched_pos: List[str] = []
    matched_neg: List[str] = []
    for i, row in enumerate(rows[:BENZINGA_MAX_ROWS]):
        title = str(row.get("title") or row.get("headline") or row.get("teaser") or "")
        body = str(row.get("body") or row.get("description") or row.get("comments") or "")
        blob = f"{title} {body}".lower()
        weight = max(1.0, BENZINGA_MAX_ROWS - i) / BENZINGA_MAX_ROWS
        for term in positive_terms:
            if _text_has_term(blob, term):
                raw += 12 * weight
                matched_pos.append(term)
        for term in negative_terms:
            if _text_has_term(blob, term):
                raw -= 15 * weight
                matched_neg.append(term)

        # Legal headlines need context. A dismissed claim can be positive for the defendant;
        # a fresh lawsuit/investigation is negative. Avoid blindly penalizing every legal item.
        if "lawsuit" in blob or "litigation" in blob:
            if any(x in blob for x in ["dismisses", "dismissed", "non-infringement", "no infringement"]):
                raw += 6 * weight
                matched_pos.append("legal risk reduced")
            elif any(x in blob for x in ["files lawsuit", "sues", "investigation", "probe"]):
                raw -= 10 * weight
                matched_neg.append("legal risk")

        importance = safe_float(row.get("importance_rank"))
        if importance >= 2:
            raw += 3 * weight

    score = clamp(50 + raw)
    if score >= 75:
        label = "strong_positive"
    elif score >= 60:
        label = "positive"
    elif score <= 25:
        label = "strong_negative"
    elif score <= 40:
        label = "negative"
    else:
        label = "neutral"
    return {
        "score": round(score, 1),
        "label": label,
        "positive_terms": sorted(set(matched_pos))[:8],
        "negative_terms": sorted(set(matched_neg))[:8],
    }

def summarize_news_rows(ticker: str, rows: List[Dict[str, Any]], *, context_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    direct_rows = rows[:BENZINGA_MAX_ROWS]
    context_rows = (context_rows or rows)[:BENZINGA_MAX_ROWS]
    scoring = score_rows_text(direct_rows)
    top = direct_rows[0] if direct_rows else {}
    channels = [str(c.get("name") or c) for c in (top.get("channels") or []) if c]
    title = top.get("title") if direct_rows else None
    created = (top.get("created") or top.get("updated")) if direct_rows else None
    recency = minutes_since_benzinga_time(created)

    one_hour_count = 0
    for row in context_rows:
        mins = minutes_since_benzinga_time(row.get("created") or row.get("updated"))
        if mins is not None and mins <= 60:
            one_hour_count += 1

    return {
        "news_catalyst_score": scoring["score"],
        "news_catalyst_label": scoring["label"],
        "top_headline": title,
        "top_headline_url": top.get("url") if direct_rows else None,
        "headline_count_1h": one_hour_count,
        "headline_count_24h": len(context_rows),
        "news_recency_minutes": recency,
        "catalyst_type": classify_catalyst_type(str(title or ""), channels),
        "channels": channels[:5],
        "importance_rank": top.get("importance_rank") if direct_rows else None,
        "positive_terms": scoring["positive_terms"],
        "negative_terms": scoring["negative_terms"],
        "direct_headline_count_24h": len(direct_rows),
        "broad_context_headline_count_24h": max(0, len(context_rows) - len(direct_rows)),
    }


def summarize_wiim_rows(ticker: str, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = rows[:5]
    if not rows:
        return {"why_moving": None, "wiim_catalyst_type": None, "wiim_score": 0, "wiim_is_ticker_specific": False}

    direct_rows = filter_direct_news_rows(rows, ticker)
    chosen = direct_rows[0] if direct_rows else rows[0]
    title = str(chosen.get("title") or chosen.get("teaser") or "")
    is_direct = bool(direct_rows)
    scoring = score_rows_text(direct_rows if direct_rows else [])
    # Broad sector WIIM is useful context, but should not be scored as a ticker catalyst.
    score = scoring["score"] if is_direct else 0
    return {
        "why_moving": title[:240] if title else None,
        "wiim_catalyst_type": classify_catalyst_type(title, [str(c.get("name") or c) for c in (chosen.get("channels") or [])]),
        "wiim_score": score,
        "wiim_is_ticker_specific": is_direct,
    }


def summarize_newsquantified_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {
            "news_sentiment_score": 0,
            "news_relevance_score": 0,
            "news_impact_score": 0,
            "news_trending_score": 0,
        }

    # NewsQuantified field names vary. Use robust numeric hints when present.
    row = rows[0]
    nums = []
    for key in ["Sentiment", "sentiment", "NewsSentiment", "news_sentiment", "Comments"]:
        val = safe_float(row.get(key), None) if row.get(key) is not None else None
        if val is not None:
            nums.append(val)
    # Price reaction fields can imply impact/trending when non-zero.
    reaction_vals = []
    for key in ["30_Seconds%", "1_Minute%", "5_Minutes%", "10_Minutes%", "30_Minutes%", "60_Minutes%"]:
        if row.get(key) is not None:
            reaction_vals.append(abs(safe_float(row.get(key))))
    impact = clamp(50 + min(sum(reaction_vals), 10) * 5) if reaction_vals else 50
    trend = clamp(50 + min(safe_float(row.get("Curr_Vol")) / max(safe_float(row.get("Close_Vol")), 1), 5) * 8) if row.get("Curr_Vol") else 50
    sentiment = clamp(50 + (sum(nums) / len(nums) if nums else 0))
    relevance = 65 if rows else 0
    return {
        "news_sentiment_score": round(sentiment, 1),
        "news_relevance_score": round(relevance, 1),
        "news_impact_score": round(impact, 1),
        "news_trending_score": round(trend, 1),
    }


def summarize_options_activity(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {
            "options_flow_score": 0,
            "flow_direction": "none",
            "dominant_contracts": [],
            "call_put_ratio": None,
            "premium_bought": None,
            "largest_trade_premium": None,
            "flow_confirms_technical_signal": None,
        }

    call_count = 0
    put_count = 0
    premiums: List[float] = []
    contracts: List[Dict[str, Any]] = []

    for row in rows[:20]:
        blob = json.dumps(row, default=str).lower()
        if "call" in blob:
            call_count += 1
        if "put" in blob:
            put_count += 1
        premium = 0.0
        for key in ["cost_basis", "premium", "trade_value", "value", "notional", "size"]:
            premium = max(premium, safe_float(row.get(key)))
        if premium > 0:
            premiums.append(premium)
        contracts.append({
            "symbol": row.get("option_symbol") or row.get("symbol") or row.get("ticker"),
            "type": row.get("put_call") or row.get("option_type") or row.get("type"),
            "strike": row.get("strike_price") or row.get("strike"),
            "expiration": row.get("date_expiration") or row.get("expiration"),
            "premium": round(premium, 2) if premium else None,
            "sentiment": row.get("sentiment"),
        })

    if call_count > put_count * 1.25:
        direction = "bullish"
    elif put_count > call_count * 1.25:
        direction = "bearish"
    elif call_count or put_count:
        direction = "mixed"
    else:
        direction = "none"

    total = call_count + put_count
    call_put_ratio = round(call_count / max(put_count, 1), 2) if total else None
    base = 45 + min(total, 20) * 2
    if direction == "bullish":
        score = base + 15
    elif direction == "bearish":
        score = base - 25
    elif direction == "mixed":
        score = base
    else:
        score = 0

    return {
        "options_flow_score": round(clamp(score), 1),
        "flow_direction": direction,
        "dominant_contracts": contracts[:3],
        "call_put_ratio": call_put_ratio,
        "premium_bought": round(sum(premiums), 2) if premiums else None,
        "largest_trade_premium": round(max(premiums), 2) if premiums else None,
        "flow_confirms_technical_signal": True if direction == "bullish" else (False if direction == "bearish" else None),
    }



def parse_benzinga_date(value: Any) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    # Common Benzinga calendar date formats include YYYY-MM-DD and RFC-style dates.
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text[:10], fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def days_until_benzinga_date(value: Any) -> Optional[int]:
    dt = parse_benzinga_date(value)
    if not dt:
        return None
    return (dt.date() - datetime.now(timezone.utc).date()).days


def benzinga_event_is_near(value: Any, max_days: int) -> bool:
    days = days_until_benzinga_date(value)
    return days is not None and 0 <= days <= max_days

def summarize_calendar_rows(rows: List[Dict[str, Any]], kind: str, ticker: Optional[str] = None) -> Dict[str, Any]:
    has = bool(rows)
    top = rows[0] if rows else {}
    blob = json.dumps(top, default=str).lower()

    if kind == "ratings":
        scoring = score_rows_text(rows)
        return {
            "analyst_catalyst_score": scoring["score"] if has else 0,
            "latest_rating_action": top.get("action") or top.get("rating_action") or top.get("importance") or top.get("action_pt"),
            "rating_current": top.get("rating_current") or top.get("current_rating"),
            "rating_prior": top.get("rating_prior") or top.get("previous_rating"),
            "price_target_current": top.get("pt_current") or top.get("price_target") or top.get("price_target_current"),
            "price_target_prior": top.get("pt_prior") or top.get("price_target_prior"),
            "price_target_change_pct": None,
        }

    if kind == "earnings":
        earnings_date = top.get("date") or top.get("report_date") if has else None
        days_to_earnings = days_until_benzinga_date(earnings_date) if earnings_date else None
        # Only flag actionable earnings risk when the event is near. A stale or far-future
        # calendar row should not automatically punish today's premarket setup.
        earnings_risk = days_to_earnings is not None and 0 <= days_to_earnings <= 10
        return {
            "earnings_date": earnings_date,
            "earnings_time": top.get("time") or top.get("period") if has else "unknown",
            "days_to_earnings": days_to_earnings,
            "eps_estimate": top.get("eps_est") or top.get("eps_estimate"),
            "eps_actual": top.get("eps") or top.get("eps_actual"),
            "eps_surprise_pct": top.get("eps_surprise_percent") or top.get("eps_surprise_pct"),
            "revenue_estimate": top.get("revenue_est") or top.get("revenue_estimate"),
            "revenue_actual": top.get("revenue") or top.get("revenue_actual"),
            "revenue_surprise_pct": top.get("revenue_surprise_percent") or top.get("revenue_surprise_pct"),
            "earnings_risk_flag": earnings_risk,
        }

    if kind == "guidance":
        direction = "none"
        if "raise" in blob or "above" in blob:
            direction = "raised"
        elif "lower" in blob or "below" in blob or "cut" in blob:
            direction = "lowered"
        elif has:
            direction = "reaffirmed"
        return {
            "guidance_direction": direction,
            "guidance_surprise_score": 70 if direction == "raised" else (30 if direction == "lowered" else (50 if has else 0)),
        }

    if kind == "offerings":
        # Offerings are only actionable if the row is ticker-specific and current/recent.
        # Do not penalize large-cap names for stale or broad offering-calendar rows.
        actionable_rows = [r for r in rows if benzinga_row_is_current_event(r, past_days=7, future_days=14)]
        actionable = bool(actionable_rows)
        top2 = actionable_rows[0] if actionable_rows else top
        return {
            "offering_flag": actionable,
            "offering_price": top2.get("price") or top2.get("offering_price"),
            "offering_proceeds": top2.get("proceeds") or top2.get("amount"),
            "dilution_risk_score": 80 if actionable else 0,
        }

    if kind == "fda":
        fda_date = top.get("date") if has else None
        # FDA rows are mainly actionable for biotech/pharma names and near-dated events.
        fda_risk = has and benzinga_event_is_near(fda_date, 30)
        return {
            "fda_event_risk": fda_risk,
            "fda_event_date": fda_date,
            "fda_event_type": top.get("type") or top.get("event_type"),
            "fda_catalyst_score": 65 if fda_risk else 0,
        }

    if kind == "mna":
        # M&A risk must be much stricter than news context. Only use rows where
        # the ticker is explicitly listed in an acquirer/target/buyer/seller ticker
        # or symbol field. Broad M&A calendar rows should not distort premarket
        # technicals for every Dow/S&P name.
        actionable_statuses = ["announced", "definitive", "pending", "completed", "active"]
        strict_rows = [r for r in rows if ticker and mna_row_has_explicit_role(r, ticker)]
        actionable_rows: List[Dict[str, Any]] = []
        for r in strict_rows:
            status_text = str(r.get("deal_status") or r.get("status") or "").lower()
            deal_price = r.get("deal_price") or r.get("price")
            deal_value = r.get("deal_value") or r.get("value")
            status_ok = any(x in status_text for x in actionable_statuses)
            dated_ok = benzinga_row_is_current_event(r, past_days=30, future_days=180) or bool(deal_price) or bool(deal_value)
            if status_ok and dated_ok:
                actionable_rows.append(r)

        top2 = actionable_rows[0] if actionable_rows else (strict_rows[0] if strict_rows else {})
        status = top2.get("deal_status") or top2.get("status") if top2 else None
        deal_price = top2.get("deal_price") or top2.get("price") if top2 else None
        deal_value = top2.get("deal_value") or top2.get("value") if top2 else None
        actionable_mna = bool(actionable_rows)
        return {
            "mna_flag": actionable_mna,
            "deal_status": status,
            "deal_price": deal_price,
            "deal_value": deal_value,
        }

    if kind == "halts":
        return {
            "halt_flag": has,
            "halt_reason": top.get("reason") or top.get("halt_reason"),
            "resume_time": top.get("resume_time") or top.get("resumption_time"),
            "halt_risk_score": 90 if has else 0,
        }

    return {f"{kind}_flag": has}


def summarize_short_interest(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"short_interest_pct_float": None, "days_to_cover": None, "short_interest_change_pct": None, "short_squeeze_score": 0}
    row = rows[0]
    pct = safe_float(row.get("shortPercentOfFloat") or row.get("short_interest_pct_float") or row.get("shortFloat"))
    days = safe_float(row.get("daysToCover") or row.get("days_to_cover"))
    change = safe_float(row.get("changePercent") or row.get("short_interest_change_pct"))
    score = 0.0
    if pct >= 20:
        score += 45
    elif pct >= 10:
        score += 30
    elif pct > 0:
        score += 15
    if days >= 5:
        score += 25
    elif days >= 2:
        score += 12
    if change > 10:
        score += 15
    return {"short_interest_pct_float": pct or None, "days_to_cover": days or None, "short_interest_change_pct": change or None, "short_squeeze_score": round(clamp(score), 1)}


def summarize_insider_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"insider_activity_score": 0, "recent_insider_buying": False, "recent_insider_selling": False}
    blob = json.dumps(rows[:10], default=str).lower()
    buying = any(x in blob for x in ["buy", "purchase", "acquisition"])
    selling = any(x in blob for x in ["sell", "sale", "disposition"])
    score = 65 if buying and not selling else (35 if selling and not buying else 50)
    return {"insider_activity_score": score, "recent_insider_buying": buying, "recent_insider_selling": selling}


def summarize_block_trades(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"block_trade_score": 0, "block_trade_direction": "unclear", "largest_block_notional": None, "block_trade_count": 0}
    notionals = []
    for row in rows[:20]:
        notional = 0.0
        for key in ["notional", "trade_value", "value", "amount"]:
            notional = max(notional, safe_float(row.get(key)))
        notionals.append(notional)
    return {
        "block_trade_score": round(clamp(45 + min(len(rows), 20) * 2), 1),
        "block_trade_direction": "unclear",
        "largest_block_notional": round(max(notionals), 2) if notionals else None,
        "block_trade_count": len(rows),
    }


async def fetch_benzinga_catalyst_summary(
    client: httpx.AsyncClient,
    ticker: str,
    include_options_flow: bool = True,
    include_risk_events: bool = True,
) -> Dict[str, Any]:
    ticker = normalize_ticker(ticker)
    if not ticker or is_crypto_ticker(ticker):
        return {"enabled": bool(BENZINGA_API_KEY), "ticker": ticker, "final_benzinga_score": 0, "risk_flags": ["Benzinga equity catalyst not available for this symbol."]}

    async def get_rows(path: str, params: Dict[str, Any], *, strict_ticker_filter: bool = True) -> List[Dict[str, Any]]:
        result = await benzinga_raw_get(client, path, params)
        if not result.get("ok"):
            return []
        rows = benzinga_extract_rows(result.get("data"))
        if strict_ticker_filter:
            rows = filter_benzinga_rows_for_ticker(rows, ticker)
        return rows

    # Core catalyst endpoints. Keep limits compact to avoid slow scans.
    # News/WIIM are first filtered by Benzinga stock metadata, then tightened again
    # to direct ticker/company mentions for scoring and top-headline selection.
    news_context_rows = await get_rows("/api/v2/news", {"tickers": ticker, "pageSize": 10, "displayOutput": "abstract"})
    wiim_context_rows = await get_rows("/api/v2/news", {"tickers": ticker, "pageSize": 5, "displayOutput": "abstract", "channels": "WIIM"})
    news_rows = filter_direct_news_rows(news_context_rows, ticker)
    wiim_rows = wiim_context_rows
    nq_rows = await get_rows("/api/v2/newsquantified", {"tickers": ticker, "limit": 10})
    ratings_rows = await get_rows("/api/v2.1/calendar/ratings", {"tickers": ticker, "limit": 10})
    earnings_rows = await get_rows("/api/v2.1/calendar/earnings", {"tickers": ticker, "limit": 5}) if include_risk_events else []
    guidance_rows = await get_rows("/api/v2.1/calendar/guidance", {"tickers": ticker, "limit": 5}) if include_risk_events else []
    offerings_rows_raw = await get_rows("/api/v2.1/calendar/offerings", {"tickers": ticker, "limit": 5}) if include_risk_events else []
    offerings_rows = [r for r in offerings_rows_raw if benzinga_row_is_current_event(r, past_days=7, future_days=14)]
    fda_rows = await get_rows("/api/v2.1/calendar/fda", {"tickers": ticker, "limit": 5}) if include_risk_events else []
    mna_rows_raw = await get_rows("/api/v2.1/calendar/ma", {"tickers": ticker, "limit": 5}) if include_risk_events else []
    mna_rows = [r for r in mna_rows_raw if mna_row_has_explicit_role(r, ticker)]
    halt_rows = await get_rows("/api/v1/signal/halt_resume", {"tickers": ticker, "limit": 5}) if include_risk_events else []
    short_rows = await get_rows("/api/v1/shortinterest", {"symbols": ticker, "limit": 5}) if include_risk_events else []
    insider_rows = await get_rows("/api/v1/sec/insider_transactions/transactions", {"tickers": ticker, "limit": 10}) if include_risk_events else []
    block_rows = await get_rows("/api/v1/signal/block_trade", {"tickers": ticker, "limit": 10})
    option_rows = await get_rows("/api/v1/signal/option_activity", {"tickers": ticker, "limit": 20}) if include_options_flow else []

    news = summarize_news_rows(ticker, news_rows, context_rows=news_context_rows)
    wiim = summarize_wiim_rows(ticker, wiim_rows)
    nq = summarize_newsquantified_rows(nq_rows)
    options = summarize_options_activity(option_rows)
    analyst = summarize_calendar_rows(ratings_rows, "ratings")
    earnings = summarize_calendar_rows(earnings_rows, "earnings")
    guidance = summarize_calendar_rows(guidance_rows, "guidance")
    offerings = summarize_calendar_rows(offerings_rows, "offerings", ticker)
    fda = summarize_calendar_rows(fda_rows, "fda")
    mna = summarize_calendar_rows(mna_rows, "mna", ticker)
    halts = summarize_calendar_rows(halt_rows, "halts")
    short_interest = summarize_short_interest(short_rows)
    insider = summarize_insider_rows(insider_rows)
    blocks = summarize_block_trades(block_rows)

    support_factors: List[str] = []
    risk_flags: List[str] = []

    if news.get("news_catalyst_score", 0) >= 60:
        support_factors.append("Positive Benzinga news catalyst")
    if wiim.get("wiim_score", 0) >= 60:
        support_factors.append("Benzinga WIIM supports move")
    if options.get("flow_direction") == "bullish":
        support_factors.append("Bullish unusual options flow")
    if analyst.get("analyst_catalyst_score", 0) >= 60:
        support_factors.append("Positive analyst catalyst")
    if guidance.get("guidance_direction") == "raised":
        support_factors.append("Raised guidance")
    if short_interest.get("short_squeeze_score", 0) >= 50:
        support_factors.append("Short-squeeze potential")
    if blocks.get("block_trade_score", 0) >= 55:
        support_factors.append("Block trade activity detected")
    if insider.get("recent_insider_buying"):
        support_factors.append("Recent insider buying detected")

    if news.get("news_catalyst_label") in {"negative", "strong_negative"}:
        risk_flags.append("Negative Benzinga news catalyst")
    if options.get("flow_direction") == "bearish":
        risk_flags.append("Bearish unusual options flow")
    if offerings.get("offering_flag"):
        risk_flags.append("Offering/dilution risk")
    if halts.get("halt_flag"):
        risk_flags.append("Halt/resume risk")
    if earnings.get("earnings_risk_flag"):
        risk_flags.append("Earnings event risk")
    if fda.get("fda_event_risk"):
        risk_flags.append("FDA/binary biotech event risk")
    if mna.get("mna_flag"):
        risk_flags.append("M&A/deal event may distort technicals")
    if guidance.get("guidance_direction") == "lowered":
        risk_flags.append("Lowered guidance")
    if insider.get("recent_insider_selling") and not insider.get("recent_insider_buying"):
        risk_flags.append("Recent insider selling detected")

    # Build a weighted score only from sources that actually returned ticker-specific data.
    # This avoids treating neutral default values from empty endpoints as real signals.
    score_parts: List[tuple[float, float]] = []
    if news_rows:
        score_parts.append((safe_float(news.get("news_catalyst_score")), 0.30))
    if wiim_rows:
        score_parts.append((safe_float(wiim.get("wiim_score")), 0.14))
    if nq_rows:
        score_parts.append((safe_float(nq.get("news_impact_score")), 0.10))
    if option_rows:
        score_parts.append((safe_float(options.get("options_flow_score")), 0.20))
    if ratings_rows:
        score_parts.append((safe_float(analyst.get("analyst_catalyst_score")), 0.10))
    if guidance_rows:
        score_parts.append((safe_float(guidance.get("guidance_surprise_score")), 0.05))
    if short_rows:
        score_parts.append((safe_float(short_interest.get("short_squeeze_score")), 0.04))
    if block_rows:
        score_parts.append((safe_float(blocks.get("block_trade_score")), 0.03))
    if insider_rows:
        score_parts.append((safe_float(insider.get("insider_activity_score")), 0.04))

    if score_parts:
        weight_sum = sum(w for _, w in score_parts)
        positive_score = sum(score * weight for score, weight in score_parts) / max(weight_sum, 0.01)
    else:
        positive_score = 0.0

    risk_penalty = 0.0
    if offerings.get("offering_flag"):
        risk_penalty += 22
    if halts.get("halt_flag"):
        risk_penalty += 30
    if options.get("flow_direction") == "bearish":
        risk_penalty += 18
    if news.get("news_catalyst_label") in {"negative", "strong_negative"}:
        risk_penalty += 15
    if guidance.get("guidance_direction") == "lowered":
        risk_penalty += 18
    if earnings.get("earnings_risk_flag"):
        risk_penalty += 8
    if fda.get("fda_event_risk"):
        risk_penalty += 10

    final_score = clamp(positive_score - risk_penalty)
    if not score_parts and not risk_flags:
        final_label = "no_benzinga_catalyst"
    elif final_score >= 75:
        final_label = "strong_positive_catalyst"
    elif final_score >= 60:
        final_label = "positive_catalyst"
    elif final_score <= 25:
        final_label = "high_risk_or_negative" if risk_flags else "no_or_weak_catalyst"
    elif final_score <= 40:
        final_label = "negative_or_weak_catalyst"
    else:
        final_label = "neutral_or_mixed"

    return {
        "enabled": bool(BENZINGA_API_KEY),
        "ticker": ticker,
        **news,
        **wiim,
        **nq,
        **options,
        **analyst,
        **earnings,
        **guidance,
        **offerings,
        **fda,
        **mna,
        **halts,
        **short_interest,
        **insider,
        **blocks,
        "final_benzinga_score": round(final_score, 1),
        "final_benzinga_label": final_label,
        "support_factors": support_factors[:8],
        "risk_flags": risk_flags[:10],
        "available_sources": {
            "news": bool(news_rows),
            "wiim": bool(wiim_rows),
            "newsquantified": bool(nq_rows),
            "unusual_options": bool(option_rows),
            "analyst_ratings": bool(ratings_rows),
            "earnings": bool(earnings_rows),
            "guidance": bool(guidance_rows),
            "offerings": bool(offerings_rows),
            "fda": bool(fda_rows),
            "mna": bool(mna_rows),
            "halts": bool(halt_rows),
            "short_interest": bool(short_rows),
            "insider_transactions": bool(insider_rows),
            "block_trades": bool(block_rows),
        },
    }


@app.get("/benzinga_catalyst")
async def benzinga_catalyst(ticker: str = "NVDA") -> Dict[str, Any]:
    timeout = BENZINGA_TIMEOUT
    async with httpx.AsyncClient(timeout=timeout) as client:
        return await fetch_benzinga_catalyst_summary(client, ticker)


class PremarketRecommendationRequest(BaseModel):
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "watchlist"
    horizon: Literal["1d", "1w", "1mo"] = "1d"
    tickers: Optional[List[str]] = Field(default=None, description="Optional explicit ticker list")
    refresh: bool = Field(default=False)
    limit: int = Field(default=10, ge=1, le=25)
    min_price: float = Field(default=0.0, ge=0.0)
    min_volume: float = Field(default=0.0, ge=0.0)
    min_premarket_volume: float = Field(default=0.0, ge=0.0)
    include_benzinga: bool = Field(default=True)
    include_options_flow: bool = Field(default=True)
    include_risk_events: bool = Field(default=True)
    strategy: Literal["momentum", "rebound", "rebound_and_momentum", "conservative"] = "rebound_and_momentum"


def classify_premarket_recommendation(row: Dict[str, Any]) -> str:
    gap = safe_float(row.get("gap_pct"))
    dist = safe_float(row.get("distance_from_premarket_high_pct"))
    pre_setup = str(row.get("premarket_setup") or "")
    benz = row.get("benzinga") or {}
    benz_score = safe_float(benz.get("final_benzinga_score"))
    options_dir = str(benz.get("flow_direction") or "none")
    risk_flags = benz.get("risk_flags") or []

    if any("Offering" in x or "Halt" in x or "Lowered guidance" in x for x in risk_flags):
        return "avoid_event_risk"
    if pre_setup in {"gap_and_fade", "premarket_exhaustion"} or dist < -2.0:
        return "avoid_fake_gap_or_fade"
    if options_dir == "bearish":
        return "avoid_bearish_options_flow"
    if pre_setup == "extended_gap_and_hold":
        return "wait_for_pullback_or_opening_range_break"
    if pre_setup == "gap_and_hold" and benz_score >= 60 and dist >= -1.0:
        if options_dir == "bullish":
            return "options_flow_confirmed_gap_and_hold"
        return "news_driven_gap_and_hold"
    if pre_setup in {"gap_and_hold", "red_to_green_attempt"} and dist >= -1.0:
        return "opening_range_break_candidate"
    if gap < 0 and benz_score >= 55:
        return "rebound_watch"
    if benz_score >= 65:
        return "news_driven_momentum_watch"
    return "watch_only"


def compact_premarket_recommendation(sig: Dict[str, Any]) -> Dict[str, Any]:
    pre = sig.get("premarket", {}) or {}
    trade = sig.get("trade_plan", {}) or {}
    benz = sig.get("benzinga", {}) or {}

    technical_score = safe_float(sig.get("overall_score"))
    premarket_score = safe_float(sig.get("premarket_quality_score"))
    benz_score = safe_float(benz.get("final_benzinga_score"))

    # Penalty for major risk flags.
    risk_flags = list(benz.get("risk_flags") or [])
    penalty = 0.0
    if any("Offering" in f for f in risk_flags):
        penalty += 25
    if any("Halt" in f for f in risk_flags):
        penalty += 30
    if any("Bearish" in f for f in risk_flags):
        penalty += 15
    if safe_float(pre.get("distance_from_premarket_high_pct")) < -2:
        penalty += 12

    final_score = clamp(
        premarket_score * 0.38
        + technical_score * 0.27
        + benz_score * 0.35
        - penalty
    )

    row = {
        "ticker": sig.get("ticker"),
        "premarket_price": pre.get("premarket_price"),
        "gap_pct": pre.get("gap_pct"),
        "premarket_volume": pre.get("premarket_volume"),
        "premarket_high": pre.get("premarket_high"),
        "premarket_low": pre.get("premarket_low"),
        "distance_from_premarket_high_pct": pre.get("distance_from_premarket_high_pct"),
        "premarket_setup": pre.get("premarket_setup"),
        "opening_strategy": pre.get("opening_strategy"),
        "entry": trade.get("entry"),
        "entry_basis": trade.get("entry_basis"),
        "stop": trade.get("stop"),
        "target_1": trade.get("target_1"),
        "target_2": trade.get("target_2"),
        "risk_reward": trade.get("reward_risk_to_target_2"),
        "invalidates_below": trade.get("invalidates_below"),
        "technical_score": round(technical_score, 1),
        "premarket_quality_score": round(premarket_score, 1),
        "benzinga_score": benz.get("final_benzinga_score", 0),
        "benzinga_label": benz.get("final_benzinga_label"),
        "news_catalyst_score": benz.get("news_catalyst_score", 0),
        "news_catalyst_label": benz.get("news_catalyst_label"),
        "options_flow_score": benz.get("options_flow_score", 0),
        "flow_direction": benz.get("flow_direction"),
        "call_put_ratio": benz.get("call_put_ratio"),
        "analyst_catalyst_score": benz.get("analyst_catalyst_score", 0),
        "earnings_risk_flag": benz.get("earnings_risk_flag", False),
        "guidance_direction": benz.get("guidance_direction"),
        "offering_flag": benz.get("offering_flag", False),
        "halt_flag": benz.get("halt_flag", False),
        "fda_event_risk": benz.get("fda_event_risk", False),
        "mna_flag": benz.get("mna_flag", False),
        "short_squeeze_score": benz.get("short_squeeze_score", 0),
        "insider_activity_score": benz.get("insider_activity_score", 0),
        "why_moving": benz.get("why_moving"),
        "top_headline": benz.get("top_headline"),
        "headline_count_1h": benz.get("headline_count_1h", 0),
        "news_recency_minutes": benz.get("news_recency_minutes"),
        "support_factors": benz.get("support_factors") or [],
        "risk_flags": risk_flags,
        "final_recommendation_score": round(final_score, 1),
        "risk_profile": sig.get("risk_profile"),
        "setup": sig.get("setup"),
    }
    row["action"] = classify_premarket_recommendation({**row, "benzinga": benz})
    return row


async def build_premarket_recommendation_rows(req: PremarketRecommendationRequest, universe: str) -> Dict[str, Any]:
    pre_req = PremarketRequest(
        universe=universe,
        horizon=req.horizon,
        tickers=req.tickers,
        refresh=False,
        limit=req.limit,
        min_price=req.min_price,
        min_volume=req.min_volume,
        min_premarket_volume=req.min_premarket_volume,
    )
    pre_req.include_news_catalysts = False
    block = await build_premarket_rows(pre_req, universe)
    signals = block.get("signals", []) or []

    if req.include_benzinga and signals:
        timeout = BENZINGA_TIMEOUT
        semaphore = asyncio.Semaphore(4)
        async with httpx.AsyncClient(timeout=timeout) as client:
            async def one(sig: Dict[str, Any]) -> Dict[str, Any]:
                ticker = str(sig.get("ticker") or "").upper()
                async with semaphore:
                    sig["benzinga"] = await fetch_benzinga_catalyst_summary(
                        client,
                        ticker,
                        include_options_flow=req.include_options_flow,
                        include_risk_events=req.include_risk_events,
                    )
                    await asyncio.sleep(0.05)
                    return sig
            updated = await asyncio.gather(*(one(s) for s in signals), return_exceptions=True)
            signals = [x for x in updated if isinstance(x, dict)]

    compact = [compact_premarket_recommendation(sig) for sig in signals]
    compact.sort(
        key=lambda x: (
            safe_float(x.get("final_recommendation_score")),
            safe_float(x.get("premarket_quality_score")),
            safe_float(x.get("benzinga_score")),
        ),
        reverse=True,
    )

    return {
        "universe": universe,
        "candidate_count": len(compact),
        "returned": min(req.limit, len(compact)),
        "signals": compact[:req.limit],
        "skipped": (block.get("skipped") or [])[:25],
    }


@app.post("/premarket_recommendations")
async def premarket_recommendations(req: PremarketRecommendationRequest) -> Dict[str, Any]:
    if not INDEX_MAP and not req.tickers:
        raise HTTPException(status_code=500, detail="No constituents loaded. Put constituents.json next to main.py or pass explicit tickers.")

    if req.refresh or not cache_is_fresh():
        await refresh_cache()

    universes = ALL_UNIVERSES if req.universe == "all" else [req.universe]
    cache = cache_status()
    stale = compute_stale_data_warning(cache)
    universe_blocks = [await build_premarket_recommendation_rows(req, universe) for universe in universes]

    all_rows: List[Dict[str, Any]] = []
    for block in universe_blocks:
        all_rows.extend(block.get("signals", []) or [])

    all_rows.sort(key=lambda x: safe_float(x.get("final_recommendation_score")), reverse=True)

    return {
        "horizon": req.horizon,
        "strategy": req.strategy,
        "polygon_enabled": bool(POLYGON_API_KEY),
        "benzinga_enabled": bool(BENZINGA_API_KEY),
        "cache": {
            "cached": cache.get("cached"),
            "fresh": cache.get("fresh"),
            "generated_at": cache.get("generated_at"),
            "market_date": cache.get("market_date"),
            "stale_data_warning": stale.get("stale_data_warning"),
            "stale_data_reason": stale.get("stale_data_reason"),
            "market_date_age_calendar_days": stale.get("market_date_age_calendar_days"),
            "errors": (cache.get("errors") or [])[:5],
        },
        "methodology": {
            "version": "premarket_recommendations_benzinga_v2_4",
            "purpose": "Compact premarket recommendations using Polygon technicals plus Benzinga catalysts, options flow, analyst actions, and risk events.",
            "benzinga_sources": [
                "news", "wiim", "newsquantified", "unusual_options", "market_movers",
                "analyst_ratings", "earnings", "guidance", "offerings", "fda",
                "mna", "halts", "short_interest", "insider_transactions", "block_trades",
            ],
            "excluded_sources": ["government_trades"],
        },
        "universes": universe_blocks,
        "summary": {
            "top_recommendations": [
                {
                    "ticker": r.get("ticker"),
                    "action": r.get("action"),
                    "score": r.get("final_recommendation_score"),
                    "gap_pct": r.get("gap_pct"),
                    "benzinga_score": r.get("benzinga_score"),
                    "flow_direction": r.get("flow_direction"),
                    "top_headline": r.get("top_headline"),
                }
                for r in all_rows[:10]
            ],
            "avoid": [
                {"ticker": r.get("ticker"), "action": r.get("action"), "risk_flags": r.get("risk_flags")}
                for r in all_rows if str(r.get("action") or "").startswith("avoid")
            ][:10],
            "options_flow_confirmed": [
                r.get("ticker") for r in all_rows if r.get("action") == "options_flow_confirmed_gap_and_hold"
            ][:10],
        },
    }






class DailyReportRequest(BaseModel):
    include_news_catalysts: bool = Field(default=True, description="Include separate Polygon news catalyst scoring when available")
    universe: Literal["sp500", "dow30", "nasdaq100", "russell2000", "crypto", "watchlist", "all"] = "sp500"
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
            lines.append(f"   Entry basis: {trade.get('entry_basis')} | Previous historical entry: {trade.get('previous_historical_entry')}")
            if sig.get("stale_entry_warning"):
                lines.append(f"   Stale-entry adjustment: {sig.get('stale_entry_diff_pct')}% — live price used.")
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
              <th>Entry</th><th>Basis</th><th>Stop</th><th>T1</th><th>T2</th><th>R/R</th><th>Structure</th>
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
                f"<td>{esc(trade.get('entry_basis'))}</td>"
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
            "version": "signals_v9_news_catalysts",
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

    if getattr(req, "include_news_catalysts", True):
        payload = await enrich_payload_with_news_catalysts(payload)
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
