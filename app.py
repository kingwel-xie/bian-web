#!/usr/bin/env python3
"""Web console for the Binance leaderboard workflow."""

from __future__ import annotations

import json
import math
import os
import re
import socket
import subprocess
import sys
import threading
import time
import uuid
import gzip
import io
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from flask import Flask, Response, jsonify, request, send_from_directory


APP_DIR = Path(__file__).resolve().parent
DATA_ROOT = Path(os.environ.get("DATA_ROOT", APP_DIR.parent)).expanduser().resolve()
STATE_DIR = DATA_ROOT / ".workflow"
JOBS_FILE = STATE_DIR / "jobs.json"
SCHEDULES_FILE = STATE_DIR / "schedules.json"
DISCOVERY_CACHE_FILE = STATE_DIR / ".url_discovery_cache.json"
ACTIVITIES_CACHE_FILE = STATE_DIR / "activities_cache.json"
ACTIVITIES_CACHE_TTL = 600
ACTIVITIES_DB_FILE = STATE_DIR / "activities_db.json"
TEAMS_FILE = STATE_DIR / "teams.json"
KNOWN_SYMBOLS = {
    "bill": "BILLUSDT",
    "aig": "AIGENSYNUSDT",
}
MARK_RANKS = (20, 50, 200)
LIVE_RANKS = tuple(sorted({*range(10, 201, 10), 35}))
BJ = timezone(timedelta(hours=8))
FAPI_KLINES = "https://fapi.binance.com/fapi/v1/klines"
SPOT_KLINES = "https://api.binance.com/api/v3/klines"
FSTREAM_WS = "wss://fstream.binance.com/ws"
LIVE_KLINE_INTERVAL = "1m"
DEFAULT_PROXY_PORTS = (7897, 7890, 7891, 10809, 1080, 8011)
SCRAPE_TOP_DEFAULTS: dict[str, int] = {"um": 400, "spot": 1000, "saving": 1500}
SCRAPE_PAGE_SIZE = 100

_running_processes: dict[str, subprocess.Popen] = {}
_rp_lock = threading.Lock()
SCRAPE_MARKETS = {"um", "spot", "saving"}

app = Flask(__name__, static_folder="web", static_url_path="/_static")


@app.after_request
def compress_response(response: Response) -> Response:
    if response.status_code < 200 or response.status_code >= 300:
        return response
    if response.is_streamed or response.direct_passthrough:
        return response
    if len(response.data) < 512:
        return response
    accept = request.headers.get("Accept-Encoding", "")
    if "gzip" not in accept:
        return response
    response.direct_passthrough = False
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(response.get_data())
    response.set_data(buf.getvalue())
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = len(response.get_data())
    return response


state_lock = threading.RLock()
live_states_lock = threading.Lock()
live_kline_states: dict[tuple[str, int, str], dict[str, Any]] = {}


class ScriptError(RuntimeError):
    pass


def normalize_scrape_symbol(raw_symbol: Any) -> tuple[str, str]:
    symbol = str(raw_symbol or "").upper().strip()
    symbol = re.sub(r"[^A-Z0-9]", "", symbol)
    if not symbol:
        raise ScriptError("缺少 symbol。")
    if symbol.endswith("USDT") and len(symbol) > 4:
        token = symbol[:-4]
    else:
        token = symbol
        symbol = f"{symbol}USDT"
    if not re.fullmatch(r"[A-Z0-9]{1,24}", token):
        raise ScriptError("symbol 格式无效。")
    return token.lower(), symbol


def normalize_scrape_market(raw_market: Any) -> str:
    market = str(raw_market or "").lower().strip()
    if market not in SCRAPE_MARKETS:
        raise ScriptError("market 只能是 um、spot 或 saving。")
    return market




def normalize_scrape_payload(payload: dict[str, Any]) -> dict[str, Any]:
    market = normalize_scrape_market(payload.get("market"))
    token, symbol = normalize_scrape_symbol(payload.get("symbol"))
    url = str(payload.get("url") or "").strip()
    resource_id = str(payload.get("resourceId") or "").strip()
    if not resource_id or not re.fullmatch(r"\d{1,12}", resource_id):
        raise ScriptError("resourceId 必须是数字。")
    raw_top = payload.get("top")
    if raw_top is not None:
        try:
            top = int(raw_top)
        except (TypeError, ValueError):
            raise ScriptError("top 必须是数字。")
        if top < 1 or top > 10000:
            raise ScriptError("top 必须在 1-10000 之间。")
    else:
        top = SCRAPE_TOP_DEFAULTS.get(market, 1000)
    return {
        **payload,
        "mode": "scrape",
        "market": market,
        "token": token,
        "symbol": symbol,
        "name": resource_id,
        "url": url,
        "resourceId": resource_id,
        "top": top,
        "pageSize": SCRAPE_PAGE_SIZE,
    }


def ensure_state() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    for path, default in [(JOBS_FILE, []), (SCHEDULES_FILE, []), (TEAMS_FILE, {"teams": []})]:
        if not path.exists():
            write_json(path, default)


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, payload: Any) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def normalize_discovery_url(raw: str) -> str:
    try:
        url = urlparse(raw)
        path = url.path.rstrip("/")
        return f"{url.scheme}://{url.netloc}{path}"
    except Exception:
        return raw.strip().rstrip("/")


def load_discovery_cache() -> dict[str, Any]:
    return read_json(DISCOVERY_CACHE_FILE, {})


def save_discovery_cache(cache: dict[str, Any]) -> None:
    write_json(DISCOVERY_CACHE_FILE, cache)


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip().replace(",", "")
        if not value:
            return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def decimal_float(value: Any) -> float | None:
    decimal = to_decimal(value)
    return float(decimal) if decimal is not None else None


def decimal_text(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def restored_trading_volume(row: dict[str, Any]) -> Decimal | None:
    volume = to_decimal(row.get("restoredTradingVolume"))
    if volume is None:
        volume = to_decimal(row.get("tradingVolume"))
    if volume is None:
        grade = to_decimal(row.get("grade"))
        if grade is not None:
            volume = grade * grade
    return volume


def snapshot_date(meta: dict[str, Any], fallback: str) -> str:
    updated_time = to_decimal(meta.get("updatedTime"))
    if updated_time is None:
        return fallback
    return datetime.fromtimestamp(
        float(updated_time / Decimal("1000")),
        timezone.utc,
    ).astimezone(BJ).date().isoformat()


def public_file(path: Path) -> str:
    relative = path.resolve().relative_to(DATA_ROOT)
    return f"/files/{relative.as_posix()}"


def safe_child(root: Path, relative: str) -> Path:
    target = (root / relative).resolve()
    target.relative_to(root.resolve())
    return target


def infer_name_from_file(path: Path) -> str | None:
    match = re.match(r"\d{4}-\d{2}-\d{2}(?:T\d{6}|_\d{4})?_([a-z0-9]+)_top\d+\.json$", path.name)
    return match.group(1) if match else None


def load_snapshots(activity_dir: Path, name: str) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for path in sorted(activity_dir.glob(f"*_{name}_top*.json")):
        try:
            data = read_json(path, {})
            meta = data.get("meta") or {}
            snapshots.append(
                {
                    "date": snapshot_date(meta, data.get("date") or path.name[:10]),
                    "path": str(path),
                    "file": path.name,
                    "url": public_file(path),
                    "resourceId": data.get("resourceId"),
                    "sum": data.get("sum"),
                    "sumNumber": decimal_float(data.get("sum")),
                    "count": data.get("count"),
                    "updatedTime": meta.get("updatedTime"),
                    "eligibleTradingVolume": meta.get("eligibleTradingVolume"),
                    "totalUsers": meta.get("total"),
                }
            )
        except Exception:
            continue
    return sorted(snapshots, key=lambda item: (item.get("date") or "", item.get("file") or ""))


def load_ratio_summary(activity_dir: Path) -> dict[str, Any] | None:
    summaries = sorted(activity_dir.glob("*_rank_ratio_summary.json"), key=lambda p: p.stat().st_mtime)
    if not summaries:
        return None
    path = summaries[-1]
    data = read_json(path, {})
    if not isinstance(data, dict):
        return None
    for item in data.get("auto") or []:
        if isinstance(item, dict):
            item.pop("charts", None)
    if isinstance(data.get("fit"), dict):
        data["fit"].pop("chart", None)
    data["file"] = path.name
    data["url"] = public_file(path)
    csv_path = path.with_suffix(".csv")
    if csv_path.exists():
        data["csvUrl"] = public_file(csv_path)
    return data


def load_market_samples(activity_dir: Path) -> list[dict[str, Any]]:
    paths = sorted(activity_dir.glob("*_market_fit_samples.csv"), key=lambda p: p.stat().st_mtime)
    if not paths:
        return []
    import csv

    with paths[-1].open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        row["marketQuoteVolumeNumber"] = decimal_float(row.get("marketQuoteVolume"))
        row["leaderboardDeltaNumber"] = decimal_float(row.get("leaderboardDelta"))
        row["eligibleDeltaNumber"] = decimal_float(row.get("eligibleDelta"))
    return rows


def activity_directory(name: str) -> Path | None:
    safe_name = name.lower().strip()
    if not re.fullmatch(r"[a-z0-9_-]+", safe_name):
        return None
    candidate = DATA_ROOT / safe_name
    if candidate.is_dir():
        return candidate
    for activity_dir in sorted(DATA_ROOT.iterdir() if DATA_ROOT.exists() else []):
        if not activity_dir.is_dir() or activity_dir.name.startswith("."):
            continue
        json_files = sorted(activity_dir.glob("*_top*.json"))
        if json_files and infer_name_from_file(json_files[-1]) == safe_name:
            return activity_dir
    return None


def ms_to_bj(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(BJ)


def load_snapshot_records(activity_dir: Path, name: str) -> list[dict[str, Any]]:
    records = []
    for path in sorted(activity_dir.glob(f"*_{name}_top*.json")):
        data = read_json(path, {})
        if not isinstance(data, dict):
            continue
        meta = data.get("meta") or {}
        updated_time = to_decimal(meta.get("updatedTime"))
        if updated_time is None:
            continue
        updated_ms = int(updated_time)
        records.append(
            {
                "date": snapshot_date(meta, data.get("date") or path.name[:10]),
                "path": path,
                "data": data,
                "updatedMs": updated_ms,
                "updatedAtBj": ms_to_bj(updated_ms),
                "mtime": path.stat().st_mtime,
            }
        )
    return sorted(records, key=lambda item: (item["updatedAtBj"], item["mtime"]))


def values_by_rank(snapshot: dict[str, Any], ranks: tuple[int, ...]) -> dict[int, Decimal]:
    wanted = set(ranks)
    values: dict[int, Decimal] = {}
    for row in snapshot.get("rows") or []:
        try:
            rank = int(row.get("sequence"))
        except (TypeError, ValueError):
            continue
        if rank not in wanted:
            continue
        value = to_decimal(row.get("grade"))
        if value is not None:
            values[rank] = value
    return values


def market_quote_for_date(activity_dir: Path, date: str) -> Decimal | None:
    for row in load_market_samples(activity_dir):
        if row.get("date") == date:
            quote = to_decimal(row.get("marketQuoteVolume"))
            if quote is not None:
                return quote
    summary = load_ratio_summary(activity_dir)
    if summary and summary.get("date") == date:
        return to_decimal(summary.get("marketQuoteVolume"))
    return None


def format_percent(value: Decimal) -> str:
    return f"{value * Decimal('100'):.6f}%"


def build_live_projection(name: str) -> dict[str, Any]:
    activity_dir = activity_directory(name)
    if activity_dir is None:
        raise ScriptError("活动不存在。")
    safe_name = name.lower().strip()
    snapshots = load_snapshot_records(activity_dir, safe_name)
    if len(snapshots) < 2:
        raise ScriptError("至少需要两个日快照才能推算实时增量。")

    previous, current = snapshots[-2], snapshots[-1]
    quote = market_quote_for_date(activity_dir, current["date"])
    if quote is None or quote <= 0:
        raise ScriptError("缺少最新快照对应的市场成交额样本。")

    previous_values = values_by_rank(previous["data"], LIVE_RANKS)
    current_values = values_by_rank(current["data"], LIVE_RANKS)
    ranks = []
    for rank in LIVE_RANKS:
        old_value = previous_values.get(rank)
        base_value = current_values.get(rank)
        if old_value is None or base_value is None:
            continue
        rank_delta = base_value - old_value
        weight = rank_delta / quote
        ranks.append(
            {
                "rank": rank,
                "previousValue": decimal_text(old_value),
                "previousValueNumber": decimal_float(old_value),
                "baseValue": decimal_text(base_value),
                "baseValueNumber": decimal_float(base_value),
                "sampleDelta": decimal_text(rank_delta),
                "sampleDeltaNumber": decimal_float(rank_delta),
                "weight": decimal_text(weight),
                "weightNumber": decimal_float(weight),
                "weightPercent": format_percent(weight),
            }
        )

    summary = load_ratio_summary(activity_dir) or {}
    window_start_ms = current["updatedMs"] + 1
    return {
        "name": safe_name,
        "symbol": KNOWN_SYMBOLS.get(safe_name) or summary.get("symbol"),
        "ranks": ranks,
        "sampleDate": current["date"],
        "previousDate": previous["date"],
        "sampleMarketQuoteVolume": decimal_text(quote),
        "sampleMarketQuoteVolumeNumber": decimal_float(quote),
        "snapshotUpdatedMs": current["updatedMs"],
        "snapshotUpdatedBj": current["updatedAtBj"].strftime("%Y-%m-%d %H:%M:%S"),
        "windowStartMs": window_start_ms,
        "windowStartBj": ms_to_bj(window_start_ms).strftime("%Y-%m-%d %H:%M:%S"),
    }


def port_is_open(host: str, port: int, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def proxy_candidates(proxy_arg: str) -> list[str | None]:
    if proxy_arg == "none":
        return [None]
    if proxy_arg != "auto":
        return [proxy_arg]
    candidates: list[str | None] = []
    for port in DEFAULT_PROXY_PORTS:
        if port_is_open("127.0.0.1", port):
            candidates.append(f"http://127.0.0.1:{port}")
    candidates.append(None)
    deduped: list[str | None] = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def request_proxies(proxy: str | None) -> dict[str, str] | None:
    if proxy is None:
        return None
    return {"http": proxy, "https": proxy}


def choose_binance_proxy(proxy_arg: str, timeout: float = 8) -> str | None:
    import requests

    errors = []
    for proxy in proxy_candidates(proxy_arg):
        try:
            response = requests.get(
                FAPI_KLINES,
                params={"symbol": "BTCUSDT", "interval": LIVE_KLINE_INTERVAL, "limit": 1},
                proxies=request_proxies(proxy),
                timeout=timeout,
            )
            if response.status_code == 200:
                return proxy
            errors.append(f"{proxy or 'direct'} HTTP {response.status_code}")
        except requests.RequestException as exc:
            errors.append(f"{proxy or 'direct'} {exc}")
    raise ScriptError("没有可用的 Binance 连接方式：" + "; ".join(errors[-3:]))


def ws_proxy_options(proxy: str | None) -> dict[str, Any]:
    if proxy is None:
        return {}
    parsed = urlparse(proxy)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or not parsed.port:
        return {}
    return {
        "http_proxy_host": parsed.hostname,
        "http_proxy_port": parsed.port,
        "proxy_type": "http",
    }


def ws_proxy_uri(proxy: str | None) -> str | None:
    if proxy is None:
        return None
    parsed = urlparse(proxy)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or not parsed.port:
        return None
    return proxy


def fetch_kline_quote_volumes(
    symbol: str,
    start_ms: int,
    end_ms: int,
    proxy: str | None,
    timeout: float = 15,
) -> dict[int, Decimal]:
    import requests

    response = requests.get(
        FAPI_KLINES,
        params={
            "symbol": symbol,
            "interval": LIVE_KLINE_INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 1500,
        },
        proxies=request_proxies(proxy),
        timeout=timeout,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise ScriptError(f"K线接口返回非 JSON：HTTP {response.status_code}") from exc
    if response.status_code != 200 or not isinstance(payload, list):
        raise ScriptError(f"K线接口失败：HTTP {response.status_code}, payload={payload}")

    volumes: dict[int, Decimal] = {}
    for item in payload:
        if not isinstance(item, list) or len(item) < 8:
            continue
        try:
            open_ms = int(item[0])
        except (TypeError, ValueError):
            continue
        quote = to_decimal(item[7])
        if quote is not None and open_ms >= start_ms:
            volumes[open_ms] = quote
    return volumes


def kline_stream_payload(
    symbol: str,
    start_ms: int,
    volumes: dict[int, Decimal],
    source: str,
    current_open_ms: int | None = None,
    closed: bool | None = None,
) -> dict[str, Any]:
    total = sum(volumes.values(), Decimal("0"))
    current = volumes.get(current_open_ms, Decimal("0")) if current_open_ms is not None else Decimal("0")
    base = total - current
    payload = {
        "symbol": symbol,
        "interval": LIVE_KLINE_INTERVAL,
        "source": source,
        "startMs": start_ms,
        "windowStartBj": ms_to_bj(start_ms).strftime("%Y-%m-%d %H:%M:%S"),
        "baseQuoteVolume": decimal_text(base),
        "baseQuoteVolumeNumber": decimal_float(base),
        "currentQuoteVolume": decimal_text(current),
        "currentQuoteVolumeNumber": decimal_float(current),
        "quoteVolume": decimal_text(total),
        "quoteVolumeNumber": decimal_float(total),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }
    if current_open_ms is not None:
        payload["klineOpenMs"] = current_open_ms
        payload["klineOpenBj"] = ms_to_bj(current_open_ms).strftime("%Y-%m-%d %H:%M:%S")
    if closed is not None:
        payload["closed"] = closed
    return payload


def live_state_payload(state: dict[str, Any]) -> dict[str, Any]:
    with state["lock"]:
        payload = kline_stream_payload(
            state["symbol"],
            state["startMs"],
            state["volumes"],
            state["source"],
            current_open_ms=state.get("currentOpenMs"),
            closed=state.get("closed"),
        )
        payload["connected"] = bool(state.get("connected"))
        payload["stream"] = state.get("stream")
        payload["error"] = state.get("error")
        payload["lastMessageAt"] = state.get("lastMessageAt")
        payload["proxy"] = state.get("proxy") or "direct"
        return payload


def live_kline_worker(state: dict[str, Any]) -> None:
    import websocket

    symbol = state["symbol"]
    start_ms = state["startMs"]
    while True:
        ws = None
        try:
            proxy = None
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            volumes = fetch_kline_quote_volumes(symbol, start_ms, now_ms, proxy)
            with state["lock"]:
                state["volumes"].update(volumes)
                state["currentOpenMs"] = max(state["volumes"]) if state["volumes"] else None
                state["source"] = "rest"
                state["proxy"] = proxy
                state["error"] = None
                state["lastMessageAt"] = datetime.now(timezone.utc).isoformat()

            stream_name = f"{symbol.lower()}@trade"
            with state["lock"]:
                state["stream"] = stream_name
                state["connected"] = False
            ws = websocket.create_connection(
                f"{FSTREAM_WS}/{stream_name}",
                timeout=30,
            )
            ws.settimeout(60)
            with state["lock"]:
                state["connected"] = True
                state["error"] = None

            while True:
                raw = ws.recv()
                message = json.loads(raw)
                event = message.get("data") if isinstance(message.get("data"), dict) else message
                if event.get("e") != "trade":
                    continue
                trade_ms = int(event.get("T") or event.get("E") or 0)
                if trade_ms < start_ms:
                    continue
                price = to_decimal(event.get("p"))
                qty = to_decimal(event.get("q"))
                if price is None or qty is None:
                    continue
                open_ms = trade_ms - (trade_ms % 60000)
                quote = price * qty
                with state["lock"]:
                    state["volumes"][open_ms] = state["volumes"].get(open_ms, Decimal("0")) + quote
                    state["currentOpenMs"] = open_ms
                    state["closed"] = False
                    state["source"] = "rest+trade-wss"
                    state["connected"] = True
                    state["error"] = None
                    state["lastMessageAt"] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            with state["lock"]:
                state["connected"] = False
                state["error"] = str(exc)
                state["lastMessageAt"] = datetime.now(timezone.utc).isoformat()
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
            time.sleep(2)


def ensure_live_kline_state(symbol: str, start_ms: int, proxy_arg: str) -> dict[str, Any]:
    key = (symbol, start_ms, "direct")
    with live_states_lock:
        state = live_kline_states.get(key)
        if state is not None:
            return state
        state = {
            "symbol": symbol,
            "startMs": start_ms,
            "proxyArg": "none",
            "proxy": None,
            "stream": None,
            "volumes": {},
            "currentOpenMs": None,
            "closed": None,
            "source": "starting",
            "connected": False,
            "error": None,
            "lastMessageAt": None,
            "lock": threading.RLock(),
        }
        thread = threading.Thread(target=live_kline_worker, args=(state,), daemon=True)
        state["thread"] = thread
        live_kline_states[key] = state
        thread.start()
        return state


def discover_activities() -> list[dict[str, Any]]:
    activities = []
    for activity_dir in sorted(DATA_ROOT.iterdir() if DATA_ROOT.exists() else []):
        if not activity_dir.is_dir() or activity_dir.name.startswith("."):
            continue
        json_files = sorted(activity_dir.glob("*_top*.json"))
        if not json_files:
            continue
        name = infer_name_from_file(json_files[-1]) or activity_dir.name
        snapshots = load_snapshots(activity_dir, name)
        ratio_summary = load_ratio_summary(activity_dir)
        activities.append(
            {
                "name": name,
                "symbol": KNOWN_SYMBOLS.get(name),
                "dir": str(activity_dir),
                "snapshots": snapshots,
                "latestSnapshot": snapshots[-1] if snapshots else None,
                "ratioSummary": ratio_summary,
                "marketSamples": load_market_samples(activity_dir),
            }
        )
    return activities


def load_jobs() -> list[dict[str, Any]]:
    with state_lock:
        return read_json(JOBS_FILE, [])


def save_jobs(jobs: list[dict[str, Any]]) -> None:
    write_json(JOBS_FILE, jobs[-200:])


def update_job(job_id: str, **updates: Any) -> None:
    with state_lock:
        jobs = load_jobs()
        for job in jobs:
            if job["id"] == job_id:
                job.update(updates)
                job["updatedAt"] = datetime.now(timezone.utc).isoformat()
                break
        save_jobs(jobs)


def workflow_command(payload: dict[str, Any]) -> list[str]:
    command = [
        sys.executable,
        str(APP_DIR / "workflow.py"),
        payload["url"],
        "--output-root",
        str(DATA_ROOT),
        "--browser-wait-ms",
        str(payload.get("browserWaitMs") or 30000),
    ]
    if payload.get("name"):
        command.extend(["--name", str(payload["name"])])
    if payload.get("symbol"):
        command.extend(["--symbol", str(payload["symbol"]).upper()])
    if payload.get("refresh"):
        command.append("--refresh")
    if payload.get("proxy"):
        command.extend(["--proxy", str(payload["proxy"])])
    return command


def scrape_command(payload: dict[str, Any]) -> list[str]:
    normalized = normalize_scrape_payload(payload)
    url = (normalized.get("url") or "").strip()
    if not url:
        rid = normalized.get("resourceId", "")
        market = normalized.get("market", "um")
        path = {"um": "futures-activity", "spot": "spot-activity", "saving": "saving-activity"}.get(market, "futures-activity")
        url = f"https://www.binance.com/zh-CN/{path}/leaderboard?resourceId={rid}"
    command = [
        sys.executable,
        str(APP_DIR / "auto_leaderboard.py"),
        "--activity",
        f"{normalized['name']}={url}",
        "--top",
        str(normalized["top"]),
        "--page-size",
        str(SCRAPE_PAGE_SIZE),
        "--output-root",
        str(DATA_ROOT),
        "--no-browser",
        "--resource-id",
        f"{normalized['name']}={normalized['resourceId']}",
        "--no-charts",
    ]
    if normalized.get("proxy"):
        command.extend(["--proxy", str(normalized["proxy"])])
    last_ts = normalized.get("lastUpdated")
    if last_ts:
        command.extend(["--last-updated", str(last_ts)])
    return command


def job_command(payload: dict[str, Any]) -> list[str]:
    if payload.get("mode") == "workflow":
        return workflow_command(payload)
    return scrape_command(payload)


def public_file_or_none(path_value: Any) -> str | None:
    if not path_value:
        return None
    try:
        path = Path(str(path_value)).resolve()
        if path.exists():
            return public_file(path)
    except (OSError, ValueError):
        return None
    return None


def public_file_with_mtime_or_none(path_value: Any) -> str | None:
    url = public_file_or_none(path_value)
    if not url:
        return None
    try:
        path = Path(str(path_value)).resolve()
        return f"{url}?v={int(path.stat().st_mtime)}"
    except OSError:
        return url


def nickname_value(row: dict[str, Any]) -> str:
    return str(row.get("nickName") or row.get("nickname") or "").strip()


def find_previous_snapshot(activity_dir: Path, name: str, current_json_path: Path) -> Path | None:
    current_name = current_json_path.name
    older: list[Path] = []
    for path in sorted(activity_dir.glob(f"*_{name}_top*.json")):
        if path.name >= current_name:
            continue
        if path.resolve() == current_json_path.resolve():
            continue
        data = read_json(path, {})
        if not isinstance(data, dict) or not isinstance(data.get("rows"), list):
            continue
        older.append(path)
    if not older:
        return None
    return max(older, key=lambda p: p.name)


def preview_delta_by_nickname(
    rows: list[dict[str, Any]],
    delta_payload: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    if not delta_payload:
        return mapped
    if delta_payload.get("firstSnapshot"):
        for row in rows:
            nickname = nickname_value(row)
            if nickname:
                mapped[nickname] = {
                    "deltaGrade": decimal_text(
                        to_decimal(row.get("grade")) or Decimal("0")
                    ),
                    "prevRank": None,
                }
        return mapped

    previous_path = delta_payload.get("previousSnapshot")
    previous_rows = []
    if previous_path:
        previous_data = read_json(Path(str(previous_path)), {})
        raw_rows = previous_data.get("rows") if isinstance(previous_data, dict) else []
        previous_rows = raw_rows if isinstance(raw_rows, list) else []
    previous = {}
    for row in previous_rows:
        nickname = nickname_value(row)
        if nickname and nickname not in previous:
            previous[nickname] = row
    for row in rows:
        nickname = nickname_value(row)
        if not nickname:
            continue
        current_grade = to_decimal(row.get("grade")) or Decimal("0")
        prev = previous.get(nickname)
        previous_grade = to_decimal(prev.get("grade")) if prev else Decimal("0")
        if previous_grade is None:
            previous_grade = Decimal("0")
        mapped[nickname] = {
            "deltaGrade": decimal_text(current_grade - previous_grade),
            "prevRank": prev.get("sequence") if prev else None,
        }
    return mapped


def compact_leaderboard_rows(
    rows: list[dict[str, Any]],
    limit: int = 1000,
    delta_by_nickname: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    compact = []
    for row in rows[:limit]:
        nickname = row.get("nickName") or row.get("nickname")
        delta_row = (delta_by_nickname or {}).get(nickname_value(row))
        compact.append(
            {
                "rank": row.get("sequence"),
                "nickname": nickname,
                "userId": row.get("userId"),
                "grade": row.get("grade"),
                "restoredTradingVolume": decimal_float(restored_trading_volume(row)),
                "deltaGrade": decimal_float(
                    delta_row.get("deltaGrade") if delta_row else None
                ),
                "prevRank": delta_row.get("prevRank") if delta_row else None,
                "tradingVolume": row.get("tradingVolume"),
                "region": row.get("region"),
            }
        )
    return compact


def ensure_scrape_xlsx(json_path: Path, sheet_name: str | None = None) -> Path | None:
    xlsx_path = json_path.with_suffix(".xlsx")
    try:
        if xlsx_path.exists() and xlsx_path.stat().st_mtime >= json_path.stat().st_mtime:
            return xlsx_path
        subprocess.run(
            [
                sys.executable,
                str(APP_DIR / "export_leaderboards_xlsx.py"),
                "--output",
                str(xlsx_path),
                "--sheet",
                f"{sheet_name or json_path.parent.name}={json_path}",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return xlsx_path if xlsx_path.exists() else None
    except Exception:
        return xlsx_path if xlsx_path.exists() else None


def detect_teams(
    snapshot_paths: list[Path],
    max_rank_gap: int = 3,
    top_n: int = 500,
    delta_err: int = 1000,
    min_delta: int = 500,
) -> list[dict[str, Any]]:
    if len(snapshot_paths) < 2:
        return []

    paths = snapshot_paths[-2:]
    grades1: dict[str, float] = {}
    ranks1: dict[str, int] = {}
    for path in paths:
        data = read_json(path, {})
        rows = data.get("rows", []) if isinstance(data, dict) else []
        if not isinstance(rows, list):
            return []
        for row in rows:
            nick = nickname_value(row)
            if nick:
                grades1[nick] = float(to_decimal(row.get("grade")) or 0)
                ranks1[nick] = int(row["sequence"]) if row.get("sequence") is not None else 0
        break  # only first (older) snapshot

    path2 = paths[-1]
    data2 = read_json(path2, {})
    rows2 = data2.get("rows", []) if isinstance(data2, dict) else []
    if not isinstance(rows2, list) or len(rows2) < 6:
        return []

    users: list[dict[str, Any]] = []
    for row in rows2:
        nick = nickname_value(row)
        if not nick:
            continue
        rank = int(row["sequence"]) if row.get("sequence") is not None else 0
        if rank > top_n:
            continue
        grade2 = float(to_decimal(row.get("grade")) or 0)
        g1 = grades1.get(nick)
        r1 = ranks1.get(nick)
        if g1 is None or r1 is None:
            continue
        delta = grade2 - g1
        if abs(delta) <= min_delta:
            continue
        users.append({
            "nickname": nick,
            "userId": row.get("userId") or "",
            "rank": rank,
            "grade": grade2,
            "delta": delta,
            "history": [
                {"rank": r1, "grade": g1, "delta": 0},
                {"rank": rank, "grade": grade2, "delta": delta},
            ],
        })

    if len(users) < 6:
        return []

    users.sort(key=lambda u: u["rank"])

    candidate_teams: list[set[int]] = []
    seen: set[frozenset[int]] = set()
    for i in range(len(users)):
        team: set[int] = {i}
        for j in range(i + 1, len(users)):
            if users[j]["rank"] - users[i]["rank"] > max_rank_gap:
                break
            if all(abs(users[j]["delta"] - users[k]["delta"]) <= delta_err for k in team):
                team.add(j)
        if len(team) >= 2:
            fs = frozenset(team)
            if fs not in seen:
                seen.add(fs)
                candidate_teams.append(team)

    candidate_teams.sort(key=len, reverse=True)
    kept: list[set[int]] = []
    for team in candidate_teams:
        if not any(team.issubset(other) for other in kept):
            kept.append(team)

    kept.sort(key=lambda t: (sum(users[i]["rank"] for i in t) / len(t), -len(t)))
    assigned: set[int] = set()
    teams: list[set[int]] = []
    for team in kept:
        if not any(i in assigned for i in team):
            teams.append(team)
            assigned.update(team)

    team_results: list[dict[str, Any]] = []
    for team in teams:
        members = [{
            "nickname": users[i]["nickname"],
            "userId": users[i]["userId"],
            "rank": users[i]["rank"],
            "delta": users[i]["delta"],
            "history": users[i]["history"],
        } for i in team]
        members.sort(key=lambda m: m["rank"])
        team_results.append({
            "size": len(team),
            "avgRank": round(sum(m["rank"] for m in members) / len(members), 1),
            "members": members,
        })
    team_results.sort(key=lambda t: (t["avgRank"], -t["size"]))
    for i, t in enumerate(team_results):
        t["id"] = i
    return team_results


_no_job_context = object()

def scrape_preview_from_json(
    json_path: Path,
    limit: int = 1000,
    previous_json_path: Path | None | object = _no_job_context,
) -> dict[str, Any]:
    data = read_json(json_path, {})
    rows = data.get("rows") if isinstance(data, dict) else []
    if not isinstance(rows, list):
        rows = []
    csv_path = json_path.with_suffix(".csv")
    xlsx_path = ensure_scrape_xlsx(json_path, str(data.get("name") or json_path.parent.name))
    discovery_candidates = sorted(json_path.parent.glob("*_discovery.json"), key=lambda p: p.stat().st_mtime)
    discovery_path = discovery_candidates[-1] if discovery_candidates else None

    # Determine previous snapshot path
    prev_path: Path | None = None
    if previous_json_path is not _no_job_context:
        if previous_json_path and previous_json_path.exists():
            prev_path = previous_json_path
    else:
        name = str(data.get("name") or "")
        if name:
            prev_path = find_previous_snapshot(json_path.parent, name, json_path)

    delta_payload = (
        {"previousSnapshot": str(prev_path), "firstSnapshot": False}
        if prev_path
        else {"firstSnapshot": True}
    )
    delta_by_nickname = preview_delta_by_nickname(rows, delta_payload)
    return {
        "name": data.get("name"),
        "url": data.get("url"),
        "date": data.get("date"),
        "resourceId": data.get("resourceId"),
        "top": data.get("top"),
        "count": data.get("count"),
        "sum": data.get("sum"),
        "restoredTradingVolumeSum": data.get("restoredTradingVolumeSum"),
        "meta": data.get("meta") or {},
        "jsonUrl": public_file(json_path),
        "csvUrl": public_file(csv_path) if csv_path.exists() else None,
        "xlsxUrl": public_file(xlsx_path) if xlsx_path and xlsx_path.exists() else None,
        "discoveryUrl": public_file(discovery_path) if discovery_path else None,
        "rows": compact_leaderboard_rows(rows, limit, delta_by_nickname),
    }


def latest_scrape_preview(name: str, limit: int | None = None) -> dict[str, Any] | None:
    safe_name = str(name or "").lower().strip()
    if not re.fullmatch(r"[a-z0-9_-]+", safe_name):
        return None
    activity_dir = DATA_ROOT / safe_name
    if not activity_dir.is_dir():
        return None
    candidates = sorted(activity_dir.glob(f"*_{safe_name}_top*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        candidates = sorted(activity_dir.glob("*_top*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        return None
    if limit is None:
        data = read_json(candidates[-1], {})
        limit = data.get("top") if isinstance(data, dict) else 1000
    return scrape_preview_from_json(candidates[-1], limit=limit)


def attach_scrape_preview(result: Any) -> Any:
    if not isinstance(result, list):
        return result
    enriched = []
    for item in result:
        if not isinstance(item, dict):
            enriched.append(item)
            continue
        current = dict(item)
        json_path = current.get("json")
        if json_path:
            current["jsonUrl"] = public_file_or_none(json_path)
            current["csvUrl"] = public_file_or_none(current.get("csv"))
            xlsx_path = ensure_scrape_xlsx(Path(str(json_path)), str(current.get("name") or "leaderboard"))
            current["xlsxUrl"] = public_file_or_none(xlsx_path)
            try:
                json_path_obj = Path(str(json_path))
                preview_data = read_json(json_path_obj, {})
                preview_top = preview_data.get("top") if isinstance(preview_data, dict) else None
                current["preview"] = scrape_preview_from_json(json_path_obj, limit=preview_top or 1000)
            except Exception as exc:
                current["previewError"] = str(exc)
        enriched.append(current)
    return enriched


def progress_from_stderr(stderr_text: str, status: str = "running", top: int = 1000) -> dict[str, Any]:
    progress: dict[str, Any] = {
        "stage": "queued" if status == "queued" else "starting",
        "label": "等待开始" if status == "queued" else "启动抓取脚本",
        "percent": 0,
    }
    if "使用连接方式" in stderr_text:
        progress.update({"stage": "connectivity", "label": "检测 Binance 连接", "percent": 8})
    if "使用 Node Playwright" in stderr_text or "打开活动页面并监听" in stderr_text:
        progress.update({"stage": "discovery", "label": "打开活动页发现 resourceId", "percent": 18})
    if "候选 resourceId" in stderr_text:
        progress.update({"stage": "discovery", "label": "已发现候选 resourceId", "percent": 30})
        id_match = re.search(r"候选 resourceId=\[([^\]]*)\]", stderr_text)
        if id_match:
            ids = [x.strip() for x in id_match.group(1).split(",") if x.strip()]
            if ids:
                progress["candidateResourceIds"] = ids
    if "测试 resourceId" in stderr_text:
        progress.update({"stage": "validate", "label": "测试排行榜 resourceId", "percent": 38})

    matches = list(re.finditer(r"resourceId=(\d+)\s+第\s+(\d+)/(\d+)\s+页：(\d+)\s+条", stderr_text))
    if matches:
        match = matches[-1]
        resource_id = int(match.group(1))
        page = int(match.group(2))
        total_pages = int(match.group(3))
        page_rows = int(match.group(4))
        percent = 40 + round((page / max(total_pages, 1)) * 55)
        progress.update(
            {
                "stage": "fetching",
                "label": f"抓取排行榜第 {page}/{total_pages} 页",
                "percent": min(percent, 95),
                "resourceId": resource_id,
                "currentPage": page,
                "totalPages": total_pages,
                "pageRows": page_rows,
                "rowsFetched": min(page * SCRAPE_PAGE_SIZE, top),
            }
        )

    if "无变化，跳过抓取" in stderr_text:
        progress.update({"stage": "skipped", "label": "无更新，跳过", "percent": 100})
    elif status == "completed":
        progress.update({"stage": "completed", "label": "抓取完成", "percent": 100})
    elif status == "failed":
        progress.update({"stage": "failed", "label": "抓取失败", "percent": progress.get("percent", 0)})
    return progress


def run_job(job_id: str, payload: dict[str, Any]) -> None:
    top = payload.get("top") or 1000
    update_job(
        job_id,
        status="running",
        progress=progress_from_stderr("", "running", top=top),
        startedAt=datetime.now(timezone.utc).isoformat(),
    )
    command = job_command(payload)
    process = subprocess.Popen(
        command,
        cwd=APP_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    with _rp_lock:
        _running_processes[job_id] = process

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []

    def read_stream(stream, chunks, is_stderr=False):
        for raw in iter(lambda: stream.read(4096), b""):
            text = raw.decode("utf-8", errors="replace")
            chunks.append(text)
            if is_stderr:
                stderr_tail = "".join(chunks)[-12000:]
                update_job(
                    job_id,
                    stderr=stderr_tail,
                    progress=progress_from_stderr(stderr_tail, "running", top=top),
                )

    threads = []
    if process.stdout:
        t = threading.Thread(target=read_stream, args=(process.stdout, stdout_chunks, False), daemon=True)
        t.start()
        threads.append(t)
    if process.stderr:
        t = threading.Thread(target=read_stream, args=(process.stderr, stderr_chunks, True), daemon=True)
        t.start()
        threads.append(t)

    return_code = process.wait()
    with _rp_lock:
        _running_processes.pop(job_id, None)
    for t in threads:
        t.join(timeout=5)
    stdout = "".join(stdout_chunks)
    stderr_text = "".join(stderr_chunks)
    result: Any = None
    if stdout.strip():
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError:
            result = {"raw": stdout}
    if return_code == 0:
        result = attach_scrape_preview(result)
    status = "completed" if return_code == 0 else "failed"
    combined_stderr = stderr_text[-12000:]
    update_job(
        job_id,
        status=status,
        command=command,
        returnCode=return_code,
        stdout=stdout[-12000:],
        stderr=combined_stderr,
        progress=progress_from_stderr(combined_stderr, status, top=top),
        result=result,
        finishedAt=datetime.now(timezone.utc).isoformat(),
    )

    if return_code == 0 and isinstance(result, list):
        for item in result:
            json_path = item.get("json") if isinstance(item, dict) else None
            if json_path:
                json_name = Path(str(json_path)).stem
                ts_match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{6})_", json_name)
                snapshot_ts = ts_match.group(1) if ts_match else json_name
                snapshot = {
                    "timestamp": snapshot_ts,
                    "json": str(json_path),
                    "csv": str(item.get("csv", "")),
                    "rows": item.get("rows", 0),
                    "sum": item.get("sum"),
                    "restoredTradingVolumeSum": item.get("restoredTradingVolumeSum"),
                }
                with state_lock:
                    jobs = load_jobs()
                    for job in jobs:
                        if job.get("id") == job_id:
                            snapshots = job.setdefault("snapshots", [])
                            if not snapshots or snapshots[-1].get("timestamp") != snapshot_ts:
                                snapshots.append(snapshot)
                            save_jobs(jobs)
                            break

    if return_code == 0 and isinstance(result, list):
        with state_lock:
            jobs = load_jobs()
            for j in jobs:
                if j.get("id") == job_id and j.get("snapshots"):
                    j.pop("result", None)
                    save_jobs(jobs)
                    break


def create_job(payload: dict[str, Any], source: str = "manual") -> dict[str, Any]:
    if payload.get("mode") == "workflow":
        if not payload.get("url"):
            raise ScriptError("缺少 url。")
    else:
        payload = normalize_scrape_payload(payload)

    rid = payload.get("resourceId", "").strip()
    with state_lock:
        jobs = load_jobs()
        existing = next(
            (j for j in reversed(jobs) if str(j.get("payload", {}).get("resourceId") or "").strip() == rid and rid),
            None,
        )
        if rid:
            keep_ids = {existing["id"]} if existing else set()
            def job_rid(j):
                val = j.get("payload", {}).get("resourceId")
                return str(val).strip() if val is not None else ""
            jobs[:] = [j for j in jobs if job_rid(j) != rid or j["id"] in keep_ids or not job_rid(j)]
            existing = next(
                (j for j in jobs if j.get("id") in keep_ids),
                None,
            )
        if existing:
            if existing.get("status") == "running":
                raise ScriptError("该活动正在抓取中，请等待完成。")
            existing["status"] = "queued"
            existing["progress"] = progress_from_stderr("", "queued")
            existing["stderr"] = ""
            existing["result"] = None
            existing["updatedAt"] = datetime.now(timezone.utc).isoformat()
            existing["startedAt"] = None
            existing["finishedAt"] = None
            job = existing
        else:
            market = str(payload.get("market") or "").upper()
            token = str(payload.get("token") or payload.get("symbol") or "").upper()
            default_name = f"{market} {token}".strip()
            job = {
                "id": uuid.uuid4().hex[:12],
                "source": source,
                "status": "queued",
                "name": default_name or payload.get("name"),
                "payload": payload,
                "progress": progress_from_stderr("", "queued"),
                "createdAt": datetime.now(timezone.utc).isoformat(),
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "snapshots": [],
            }
            jobs.append(job)
        save_jobs(jobs)

    snapshots = job.get("snapshots") or []
    if snapshots:
        payload["lastUpdated"] = snapshots[-1]["timestamp"]

    thread = threading.Thread(target=run_job, args=(job["id"], payload), daemon=True)
    thread.start()
    return job


def load_schedules() -> list[dict[str, Any]]:
    return read_json(SCHEDULES_FILE, [])


def save_schedules(schedules: list[dict[str, Any]]) -> None:
    write_json(SCHEDULES_FILE, schedules)


def load_teams_db() -> dict[str, Any]:
    return read_json(TEAMS_FILE, {"teams": []})


def save_teams_db(db: dict[str, Any]) -> None:
    write_json(TEAMS_FILE, db)


ACTIVITY_KEYWORDS_FILE = STATE_DIR / "activity_keywords.json"
_keywords_cache: dict[str, Any] | None = None
_keywords_cache_at: float = 0
_KEYWORDS_CACHE_TTL = 300

def _get_keywords() -> dict[str, Any]:
    global _keywords_cache, _keywords_cache_at
    now = time.time()
    if _keywords_cache is None or now - _keywords_cache_at > _KEYWORDS_CACHE_TTL:
        _keywords_cache = read_json(ACTIVITY_KEYWORDS_FILE, {})
        _keywords_cache_at = now
    return _keywords_cache


def _extract_activity_tags(title: str) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {"types": [], "tokens": []}
    seen: set[str] = set()
    kw = _get_keywords()
    token_set = set(kw.get("tokens", []))
    type_keywords = kw.get("typeKeywords", [])
    for m in re.finditer(r"[（(]([A-Z][A-Z0-9]{1,10})[）)]", title):
        t = m.group(1)
        if t in token_set and t not in seen:
            result["tokens"].append(t)
            seen.add(t)
    for m in re.finditer(r"(?<![A-Za-z])[A-Z]{2,10}(?![A-Za-z])", title):
        t = m.group(0)
        if t in token_set and t not in seen:
            result["tokens"].append(t)
            seen.add(t)
    for tkw in type_keywords:
        if tkw in title and tkw not in seen:
            result["types"].append(tkw)
            seen.add(tkw)
    return result


def sync_activities() -> dict[str, Any]:
    """Fetch latest activities from Binance API, compare with stored DB,
    detect new/removed items, keep only latest 50, and save."""
    import json as _j
    import urllib.request
    url = ACTIVITIES_BINANCE_URL + "?type=1&pageNo=1&pageSize=50&catalogId=93"
    req = urllib.request.Request(url)
    req.add_header("accept-language", "zh-CN")
    req.add_header("lang", "zh-CN")
    req.add_header("referer", "https://www.binance.com/zh-CN/messages/v2/group/announcement")
    req.add_header("bnc-time-zone", "Asia/Shanghai")
    req.add_header("user-agent", "Mozilla/5.0")
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode("utf-8")
    data = _j.loads(raw)
    cats = data.get("data", {}).get("catalogs") or [{}]
    fresh = cats[0].get("articles", [])
    fresh.sort(key=lambda a: a.get("releaseDate", 0), reverse=True)
    fresh = fresh[:50]
    for a in fresh:
        a["tags"] = _extract_activity_tags(a.get("title", ""))
    fresh_ids = {a["id"] for a in fresh}

    db = read_json(ACTIVITIES_DB_FILE, {"articles": [], "history": []})
    old = db.get("articles", [])
    old_ids = {a["id"] for a in old}
    now_ts = time.time()
    history = db.get("history", [])

    new_ids = fresh_ids - old_ids
    removed_ids = old_ids - fresh_ids

    for a in fresh:
        if a["id"] in new_ids:
            a["firstSeenAt"] = now_ts
        else:
            existing = next((o for o in old if o["id"] == a["id"]), None)
            a["firstSeenAt"] = existing.get("firstSeenAt", now_ts) if existing else now_ts

    for a in old:
        if a["id"] in removed_ids:
            history.append({"type": "removed", "article": a, "detectedAt": now_ts})
    for a in fresh:
        if a["id"] in new_ids:
            history.append({"type": "new", "article": a, "detectedAt": now_ts})

    if len(history) > 100:
        history = history[-100:]

    db = {"syncedAt": now_ts, "articles": fresh, "history": history}

    try:
        ACTIVITIES_CACHE_FILE.write_text(
            _j.dumps({"fetchedAt": now_ts, "data": {"articles": fresh, "total": len(fresh)}}, ensure_ascii=False), encoding="utf-8"
        )
    except OSError:
        pass
    write_json(ACTIVITIES_DB_FILE, db)
    return db


def _sync_interval() -> int:
    h = datetime.now(BJ).hour
    return 3600 if 8 <= h < 20 else 14400


def sync_activities_loop() -> None:
    time.sleep(5)
    try:
        sync_activities()
    except Exception as exc:
        print(f"initial activities sync error: {exc}", file=sys.stderr)
    while True:
        time.sleep(_sync_interval())
        try:
            sync_activities()
        except Exception as exc:
            print(f"activities sync error: {exc}", file=sys.stderr)


def scheduler_loop() -> None:
    while True:
        try:
            now = datetime.now()
            today_key = now.date().isoformat()
            changed = False
            schedules = load_schedules()
            for schedule in schedules:
                if not schedule.get("enabled", True):
                    continue
                if schedule.get("lastRunDate") == today_key:
                    continue
                hour = int(schedule.get("hour", 8))
                minute = int(schedule.get("minute", 15))
                if now.hour > hour or (now.hour == hour and now.minute >= minute):
                    payload = {
                        "mode": "workflow",
                        "url": schedule["url"],
                        "name": schedule.get("name"),
                        "symbol": schedule.get("symbol"),
                        "proxy": schedule.get("proxy"),
                        "refresh": bool(schedule.get("refresh", False)),
                        "browserWaitMs": int(schedule.get("browserWaitMs", 30000)),
                    }
                    create_job(payload, source=f"schedule:{schedule['id']}")
                    schedule["lastRunDate"] = today_key
                    schedule["lastRunAt"] = datetime.now(timezone.utc).isoformat()
                    changed = True
            if changed:
                save_schedules(schedules)
        except Exception as exc:
            print(f"scheduler error: {exc}", file=sys.stderr)
        time.sleep(30)


@app.get("/")
@app.get("/css888")
@app.get("/css888/")
def index() -> Response:
    response = send_from_directory(app.static_folder, "index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/css888/<path:filename>")
def css888_static(filename: str) -> Response:
    response = send_from_directory(app.static_folder, filename)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/preview.html")
def preview_html() -> Response:
    response = send_from_directory(app.static_folder, "preview.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/team.html")
def team_html() -> Response:
    response = send_from_directory(app.static_folder, "team.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/analysis.html")
def analysis_html() -> Response:
    response = send_from_directory(app.static_folder, "analysis.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response

@app.get("/activity.html")
def activity_html() -> Response:
    response = send_from_directory(app.static_folder, "activity.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/growth.html")
def growth_html() -> Response:
    response = send_from_directory(app.static_folder, "growth.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/api/overview")
def api_overview() -> Response:
    return jsonify({"dataRoot": str(DATA_ROOT), "activities": discover_activities()})


@app.get("/api/scrape/latest")
def api_scrape_latest() -> Response:
    try:
        payload = normalize_scrape_payload(
            {
                "market": request.args.get("market"),
                "symbol": request.args.get("symbol"),
                "label": request.args.get("label"),
                "url": request.args.get("url"),
                "resourceId": request.args.get("resourceId"),
            }
        )
        preview = latest_scrape_preview(payload["name"])
        return jsonify({"query": payload, "result": preview})
    except ScriptError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/discover/cache")
def api_discover_cache() -> Response:
    cache = load_discovery_cache()
    entries = []
    for key, entry in cache.items():
        entries.append({
            "key": key,
            "url": entry.get("url"),
            "title": entry.get("title"),
            "candidates": entry.get("candidates", []),
            "activityStart": entry.get("activityStart"),
            "activityEnd": entry.get("activityEnd"),
            "cachedAt": entry.get("cachedAt"),
        })
    entries.sort(key=lambda e: e.get("cachedAt") or "", reverse=True)
    return jsonify({"entries": entries})


@app.delete("/api/discover/cache/<path:key>")
def api_delete_discover_cache(key: str) -> Response:
    cache = load_discovery_cache()
    if key not in cache:
        return jsonify({"error": "条目不存在"}), 404
    del cache[key]
    save_discovery_cache(cache)
    return jsonify({"ok": True})


@app.post("/api/discover")
def api_discover() -> Response:
    try:
        payload = request.get_json(force=True) or {}
        url = str(payload.get("url") or "").strip()
        if not url:
            return jsonify({"error": "缺少 url"}), 400

        cache_key = normalize_discovery_url(url)
        force = payload.get("force", False)
        cache = load_discovery_cache()
        cached_entry = cache.get(cache_key)

        if cached_entry and not force:
            result = dict(cached_entry)
            result["cached"] = True
        else:
            tmp_name = f"_d{uuid.uuid4().hex[:6]}"
            command = [
                sys.executable,
                str(APP_DIR / "auto_leaderboard.py"),
                "--discover-only",
                "--quiet",
                "--activity",
                f"{tmp_name}={url}",
            ]
            proxy = str(payload.get("proxy", "auto"))
            if proxy:
                command.extend(["--proxy", proxy])
            browser_wait_ms = int(payload.get("browserWaitMs", 30000))
            command.extend(["--browser-wait-ms", str(browser_wait_ms)])
            completed = subprocess.run(
                command,
                cwd=APP_DIR,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=max(120, math.ceil(browser_wait_ms / 1000) + 60),
            )
            if completed.returncode != 0:
                stderr_text = completed.stderr.decode("utf-8", errors="replace")[-2000:]
                return jsonify({"error": stderr_text}), 500
            result = json.loads(completed.stdout)
            cache[cache_key] = {
                "url": result.get("url"),
                "title": result.get("title"),
                "candidates": result.get("candidates", []),
                "activityStart": result.get("activityStart"),
                "activityEnd": result.get("activityEnd"),
                "cachedAt": datetime.now(timezone.utc).isoformat(),
            }
            save_discovery_cache(cache)
            result["cached"] = False

        existing_ids = set()
        for job in load_jobs():
            rid = job.get("payload", {}).get("resourceId")
            if rid:
                existing_ids.add(str(rid).strip())

        raw_candidates = result.get("candidates", [])
        enriched = []
        for c in raw_candidates:
            rid = str(c) if not isinstance(c, dict) else str(c.get("resourceId", c))
            enriched.append({
                "resourceId": rid,
                "hasJob": rid in existing_ids,
            })
        result["candidates"] = enriched
        return jsonify(result)
    except ScriptError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.post("/api/scrape/jobs")
def api_create_scrape_job() -> Response:
    try:
        payload = request.get_json(force=True) or {}
        payload["mode"] = "scrape"
        job = create_job(payload)
        return jsonify({"job": job}), 202
    except ScriptError as exc:
        return jsonify({"error": str(exc)}), 400


@app.get("/api/activities/<name>/projection")
def api_activity_projection(name: str) -> Response:
    try:
        return jsonify(build_live_projection(name))
    except ScriptError as exc:
        return jsonify({"error": str(exc)}), 400


ACTIVITIES_BINANCE_URL = (
    "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
)

@app.get("/api/activities")
def api_activities() -> Response:
    """Return activities from background-synced DB. Pass ?refresh=1 to force immediate sync."""
    if request.args.get("refresh"):
        try:
            sync_activities()
        except Exception as exc:
            return jsonify({"error": str(exc)}), 502
    db = read_json(ACTIVITIES_DB_FILE, {})
    articles = db.get("articles", [])
    return jsonify({"articles": articles, "total": len(articles), "syncedAt": db.get("syncedAt")})


@app.get("/api/activities/changes")
def api_activities_changes() -> Response:
    """Return activity changes (new/removed) since a given timestamp."""
    since = request.args.get("since")
    since_ts = float(since) if since else 0
    db = read_json(ACTIVITIES_DB_FILE, {})
    history = db.get("history", [])
    recent = [h for h in history if h.get("detectedAt", 0) > since_ts]
    has_new = any(h.get("type") == "new" for h in recent)
    return jsonify({"changes": recent, "hasNew": has_new})


@app.get("/api/binance/kline/<symbol>")
def api_kline_snapshot(symbol: str) -> Response:
    safe_symbol = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z0-9]{3,30}", safe_symbol):
        return jsonify({"error": "invalid symbol"}), 400
    try:
        start_ms = int(request.args.get("startMs") or 0)
    except ValueError:
        return jsonify({"error": "invalid startMs"}), 400
    if start_ms <= 0:
        start_ms = int((datetime.now(timezone.utc) - timedelta(days=1)).timestamp() * 1000)
    proxy_arg = request.args.get("proxy", "auto")
    state = ensure_live_kline_state(safe_symbol, start_ms, proxy_arg)
    return jsonify(live_state_payload(state))


@app.get("/api/ticker/<symbol>")
def api_ticker(symbol: str) -> Response:
    import requests
    safe = symbol.upper().strip()
    if not re.fullmatch(r"[A-Z0-9]{2,30}", safe):
        return jsonify({"error": "invalid symbol"}), 400

    endpoints = [
        ("spot", f"https://api.binance.com/api/v3/ticker/price?symbol={safe}"),
        ("futures", f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={safe}"),
    ]
    errors = []
    for market, url in endpoints:
        try:
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if "price" in data:
                return jsonify({"symbol": data["symbol"], "price": float(data["price"]), "market": market})
            errors.append(f"{market}: {data.get('msg', 'unknown error')}")
        except requests.RequestException as e:
            errors.append(f"{market}: {e}")
    return jsonify({"error": "; ".join(errors)}), 400


@app.get("/api/jobs")
def api_jobs() -> Response:
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    per_page = max(1, min(per_page, 100))
    page = max(1, page)

    all_jobs = load_jobs()
    filter_market = (request.args.get("market") or "").strip().lower()
    filter_search = (request.args.get("search") or "").strip()
    filter_active = request.args.get("active", "").strip().lower()

    if filter_market in SCRAPE_MARKETS:
        all_jobs = [j for j in all_jobs if (j.get("payload") or {}).get("market") == filter_market]

    if filter_search:
        q = filter_search.lower()
        all_jobs = [
            j for j in all_jobs
            if q in ((j.get("name") or (j.get("payload") or {}).get("name") or "")).lower()
        ]

    if filter_active == "true":
        now_bj = datetime.now(timezone.utc).astimezone(BJ)
        cutoff = now_bj - timedelta(days=1)
        def is_active(job):
            end_str = (job.get("payload") or {}).get("activityEnd", "") or ""
            if not end_str:
                return True
            try:
                return datetime.strptime(end_str, "%Y-%m-%d %H:%M").replace(tzinfo=BJ) > cutoff
            except ValueError:
                return True
        all_jobs = [j for j in all_jobs if is_active(j)]

    total = len(all_jobs)
    total_pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = min(page, total_pages) if total else 1

    def sort_key(job):
        end_str = (job.get("payload") or {}).get("activityEnd", "") or ""
        if not end_str:
            return (1, "", "")
        try:
            end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
            ts = end_dt.timestamp()
            now_bj = datetime.now(timezone.utc).astimezone(BJ)
            if end_dt + timedelta(hours=24) < now_bj:
                return (2, -ts, "")
            return (0, ts, "")
        except ValueError:
            return (1, end_str, "")

    sorted_jobs = sorted(all_jobs, key=sort_key)
    start = (page - 1) * per_page
    end = start + per_page
    page_jobs = sorted_jobs[start:end]

    jobs = []
    for job in page_jobs:
        payload = job.get("payload") or {}
        current = {
            "id": job.get("id"),
            "name": job.get("name") or payload.get("name"),
            "status": job.get("status"),
            "progress": {
                "stage": job.get("progress", {}).get("stage"),
                "label": job.get("progress", {}).get("label"),
                "percent": job.get("progress", {}).get("percent", 0),
                "rowsFetched": job.get("progress", {}).get("rowsFetched"),
                "currentPage": job.get("progress", {}).get("currentPage"),
                "totalPages": job.get("progress", {}).get("totalPages"),
            },
            "createdAt": job.get("createdAt"),
            "startedAt": job.get("startedAt"),
            "finishedAt": job.get("finishedAt"),
            "updatedAt": job.get("updatedAt"),
            "payload": {
                "market": payload.get("market"),
                "token": payload.get("token"),
                "symbol": payload.get("symbol"),
                "resourceId": payload.get("resourceId"),
                "url": payload.get("url"),
                "rewardToken": payload.get("rewardToken"),
                "rewardAmount": payload.get("rewardAmount"),
                "rewardTiers": payload.get("rewardTiers"),
                "rewardMode": payload.get("rewardMode"),
                "totalReward": payload.get("totalReward"),
                "eligibleUsers": payload.get("eligibleUsers"),
                "activityEnd": payload.get("activityEnd"),
                "activityStart": payload.get("activityStart"),
                "top": payload.get("top"),
            },
            "snapshotCount": len(job.get("snapshots") or []),
            "latestSnapshot": (job.get("snapshots") or [{}])[-1].get("timestamp") if job.get("snapshots") else None,
        }
        stderr_text = job.get("stderr") or ""
        if stderr_text:
            current["stderr"] = stderr_text[-900:]
        jobs.append(current)

    return jsonify({
        "jobs": jobs,
        "pagination": {
            "page": page,
            "perPage": per_page,
            "total": total,
            "totalPages": total_pages,
        },
    })


_ANALYSIS_RANGES = [(1, 5), (6, 20), (21, 50), (51, 200), (201, 1000)]


def _extend_ranges(max_rank: int) -> list[tuple[int, int]]:
    ranges = list(_ANALYSIS_RANGES)
    if max_rank > 1000:
        start = 1001
        while start <= max_rank:
            end = min(start + 999, max_rank)
            ranges.append((start, end))
            start = end + 1
    return ranges


def get_token_price(reward_token: str, activity_end: str) -> float | None:
    if not reward_token or not activity_end:
        return None
    token = reward_token.strip().upper()
    if token in ("USDT", "USDC"):
        return 1.0
    try:
        dt = datetime.strptime(activity_end, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
    except (ValueError, OSError):
        return None
    import requests as req
    now_bj = datetime.now(timezone.utc).astimezone(BJ)
    # Activity not yet ended → use live ticker price
    if dt > now_bj:
        safe = token + "USDT"
        for ticker_url in (
            f"https://api.binance.com/api/v3/ticker/price?symbol={safe}",
            f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={safe}",
        ):
            try:
                resp = req.get(ticker_url, timeout=10)
                data = resp.json()
                if "price" in data:
                    return float(data["price"])
            except Exception:
                continue
        return None
    # Activity ended → use historical kline close at end time
    end_ms = int(dt.astimezone(timezone.utc).timestamp() * 1000)
    symbol = token + "USDT"
    for url in (SPOT_KLINES, FAPI_KLINES):
        try:
            resp = req.get(
                url,
                params={"symbol": symbol, "interval": "1h", "endTime": end_ms, "limit": 1},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and len(data) > 0:
                    close = float(data[0][4])
                    if close > 0:
                        return close
        except Exception:
            continue
    return None


@app.get("/api/analysis")
def api_analysis() -> Response:
    market = (request.args.get("market") or "").strip().lower()
    if market not in ("spot", "um"):
        return jsonify({"error": "market must be spot or um"}), 400

    all_jobs = load_jobs()
    candidates = [
        j for j in all_jobs
        if j.get("status") == "completed"
        and (j.get("payload") or {}).get("market") == market
    ]

    job_data: list[dict[str, Any]] = []
    max_rank = 0

    for job in candidates:
        payload = job.get("payload", {})
        snapshots = job.get("snapshots") or []
        if not snapshots:
            continue
        json_path_str = snapshots[-1].get("json")
        if not json_path_str:
            continue
        json_path = Path(str(json_path_str))
        if not json_path.exists():
            continue
        data = read_json(json_path, {})
        rows = data.get("rows") if isinstance(data, dict) else []
        if not isinstance(rows, list):
            continue
        limit = payload.get("top") or 1000
        limited = rows[:limit]
        if not limited:
            continue

        reward_mode = payload.get("rewardMode") or "rank"
        for r in limited:
            if reward_mode != "rank":
                continue
            seq = int(r.get("sequence") or 0)
            if seq > max_rank:
                max_rank = seq

        job_data.append({
            "job": job,
            "payload": payload,
            "rows": limited,
        })

    ranges = _extend_ranges(max_rank)

    results: list[dict[str, Any]] = []
    needs_save = False
    for entry in job_data:
        job = entry["job"]
        payload = entry["payload"]
        rows = entry["rows"]

        range_stats: list[dict[str, Any]] = []
        total_volume = 0
        for rmin, rmax in ranges:
            items = [float(r.get("grade", 0)) for r in rows if rmin <= int(r.get("sequence") or 0) <= rmax]
            s = sum(items)
            n = len(items)
            avg = s / n if n else 0
            sorted_items = sorted(items)
            med = sorted_items[n // 2] if n else 0
            q1 = sorted_items[min(int(n * 0.25), n - 1)] if n else 0
            last = items[-1] if items else 0
            range_stats.append({"total": s, "avg": avg, "med": med, "q1": q1, "last": last})
            total_volume += s

        reward_tiers = payload.get("rewardTiers") or []
        reward_mode = payload.get("rewardMode") or "rank"
        total_reward = float(payload.get("totalReward") or 0)
        eligible_users = int(payload.get("eligibleUsers") or 0)

        reward_token = (payload.get("rewardToken") or "").strip().upper()
        activity_end = payload.get("activityEnd")
        # Determine if activity is still active (matches sort_key: 24h grace)
        is_active = False
        if activity_end:
            try:
                end_dt = datetime.strptime(activity_end, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
                is_active = end_dt + timedelta(hours=24) > datetime.now(timezone.utc).astimezone(BJ)
            except (ValueError, OSError):
                pass
        if is_active:
            # Active jobs: always fetch live price
            price = get_token_price(reward_token, activity_end)
            job["rewardPriceUsd"] = price
            needs_save = True
        else:
            # Ended jobs: use cached price or fetch once
            price = job.get("rewardPriceUsd")
            if price is None and reward_token and activity_end:
                price = get_token_price(reward_token, activity_end)
                if price is not None:
                    job["rewardPriceUsd"] = price
                    needs_save = True

        ranges_out: list[dict[str, Any]] = []
        for i, (rmin, rmax) in enumerate(ranges):
            rs = range_stats[i]
            pct = round(rs["total"] / total_volume * 100, 1) if total_volume else 0
            ranges_out.append({
                "label": f"{rmin}~{rmax}",
                "total": rs["total"],
                "pct": pct,
                "avg": rs["avg"],
                "med": rs["med"],
                "q1": rs["q1"],
                "last": rs["last"],
            })

        results.append({
            "id": job.get("id"),
            "name": job.get("name"),
            "resourceId": payload.get("resourceId"),
            "token": payload.get("token"),
            "activityEnd": payload.get("activityEnd"),
            "rowCount": len(rows),
            "totalVolume": total_volume,
            "rewardToken": reward_token,
            "rewardPriceUsd": price,
            "rewardPriceIsLive": is_active,
            "activityStart": payload.get("activityStart"),
            "activityEnd": payload.get("activityEnd"),
            "rewardMode": reward_mode,
            "rewardTiers": [
                {"rankMin": t["rankMin"], "rankMax": t["rankMax"], "amount": float(t.get("amount", 0))}
                for t in reward_tiers
            ],
            "totalReward": total_reward,
            "eligibleUsers": eligible_users,
            "ranges": ranges_out,
        })

    if needs_save:
        with state_lock:
            save_jobs(all_jobs)

    # sort: active asc → no end → expired desc
    def sort_key(j):
        end_str = (j.get("activityEnd") or "").strip()
        if not end_str:
            return (1, "", "")
        try:
            end_dt = datetime.strptime(end_str, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
            ts = end_dt.timestamp()
            now_bj = datetime.now(timezone.utc).astimezone(BJ)
            if end_dt + timedelta(hours=24) < now_bj:
                return (2, -ts, "")
            return (0, ts, "")
        except ValueError:
            return (1, end_str, "")

    results.sort(key=sort_key)

    return jsonify({
        "market": market,
        "maxRank": max_rank,
        "ranges": [{"label": f"{r[0]}~{r[1]}", "min": r[0], "max": r[1]} for r in ranges],
        "jobs": results,
    })


@app.get("/api/jobs/completed-growth")
def api_completed_growth() -> Response:
    market = (request.args.get("market") or "").strip().lower()
    if market not in ("spot", "um"):
        return jsonify({"error": "market must be spot or um"}), 400

    all_jobs = load_jobs()
    candidates = [
        j for j in all_jobs
        if j.get("status") == "completed"
        and (j.get("payload") or {}).get("market") == market
    ]

    entries: list[dict[str, Any]] = []
    max_rank = 0

    for job in candidates:
        payload = job.get("payload", {})
        snapshots = job.get("snapshots") or []
        if len(snapshots) < 2:
            continue

        sorted_ss = sorted(snapshots, key=lambda s: s.get("timestamp", ""))

        if market == "spot":
            # SPOT: use activityEnd to find T and T-2h snapshots
            activity_end_str = (payload.get("activityEnd") or "").strip()
            if not activity_end_str:
                continue
            try:
                end_dt = datetime.strptime(activity_end_str, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
            except ValueError:
                continue

            t_minus_2h = end_dt - timedelta(hours=2)

            def _snap_dt(s):
                try:
                    return datetime.strptime(s.get("timestamp", ""), "%Y-%m-%dT%H%M%S").replace(tzinfo=BJ)
                except (ValueError, TypeError):
                    return None

            closest_end: dict | None = None
            closest_m2: dict | None = None
            dist_end: float | None = None
            dist_m2: float | None = None

            for s in snapshots:
                dt = _snap_dt(s)
                if dt is None:
                    continue
                de = abs((dt - end_dt).total_seconds())
                dm = abs((dt - t_minus_2h).total_seconds())
                if dist_end is None or de < dist_end:
                    dist_end = de
                    closest_end = s
                if dist_m2 is None or dm < dist_m2:
                    dist_m2 = dm
                    closest_m2 = s

            if not closest_end or not closest_m2 or closest_end is closest_m2:
                continue
            if (dist_end is None or dist_end > 1800) or (dist_m2 is None or dist_m2 > 1800):
                continue

            cur_ss = closest_end
            prev_ss = closest_m2
        else:
            # UM: compare last two daily snapshots (T vs T-1)
            cur_ss = sorted_ss[-1]
            prev_ss = sorted_ss[-2]

        prev_path = Path(str(prev_ss.get("json", "")))
        cur_path = Path(str(cur_ss.get("json", "")))
        if not prev_path.exists() or not cur_path.exists():
            continue

        prev_data = read_json(prev_path, {})
        cur_data = read_json(cur_path, {})
        prev_rows = prev_data.get("rows") if isinstance(prev_data, dict) else []
        cur_rows = cur_data.get("rows") if isinstance(cur_data, dict) else []
        if not isinstance(prev_rows, list) or not isinstance(cur_rows, list):
            continue

        limit = payload.get("top") or 1000
        prev_limited = prev_rows[:limit]
        cur_limited = cur_rows[:limit]
        if not prev_limited or not cur_limited:
            continue

        reward_mode = payload.get("rewardMode") or "rank"
        for r in cur_limited:
            if reward_mode != "rank":
                continue
            seq = int(r.get("sequence") or 0)
            if seq > max_rank:
                max_rank = seq

        entries.append({
            "job": job,
            "prevRows": prev_limited,
            "curRows": cur_limited,
            "prevTs": prev_ss.get("timestamp"),
            "curTs": cur_ss.get("timestamp"),
        })

    ranges = _extend_ranges(max_rank)

    output: list[dict[str, Any]] = []
    for entry in entries:
        job = entry["job"]
        prev_rows = entry["prevRows"]
        cur_rows = entry["curRows"]

        prev_total = sum(float(r.get("grade", 0)) for r in prev_rows)
        cur_total = sum(float(r.get("grade", 0)) for r in cur_rows)

        tiers: list[dict[str, Any]] = []
        for rmin, rmax in ranges:
            prev_items = [float(r.get("grade", 0)) for r in prev_rows if rmin <= int(r.get("sequence") or 0) <= rmax]
            cur_items = [float(r.get("grade", 0)) for r in cur_rows if rmin <= int(r.get("sequence") or 0) <= rmax]
            prev_s = sum(prev_items)
            cur_s = sum(cur_items)
            prev_n = len(prev_items)
            cur_n = len(cur_items)
            prev_avg = round(prev_s / prev_n, 2) if prev_n else 0
            cur_avg = round(cur_s / cur_n, 2) if cur_n else 0
            avg_growth = round(cur_avg - prev_avg, 2)
            prev_pct = round(prev_s / prev_total * 100, 2) if prev_total else 0
            cur_pct = round(cur_s / cur_total * 100, 2) if cur_total else 0
            pct_growth = round(cur_pct - prev_pct, 2)
            tiers.append({
                "label": f"{rmin}~{rmax}",
                "prevPct": prev_pct,
                "curPct": cur_pct,
                "pctGrowth": pct_growth,
                "prevAvg": prev_avg,
                "curAvg": cur_avg,
                "avgGrowth": avg_growth,
                "prevTotal": round(prev_s, 2),
                "curTotal": round(cur_s, 2),
            })

        output.append({
            "id": job.get("id"),
            "name": job.get("name"),
            "market": market,
            "totalPrev": round(prev_total, 2),
            "totalCur": round(cur_total, 2),
            "prevTs": entry["prevTs"],
            "curTs": entry["curTs"],
            "tiers": tiers,
        })

    output.sort(key=lambda j: j["name"] or "")

    return jsonify({
        "jobs": output,
        "rangeLabels": [f"{r[0]}~{r[1]}" for r in ranges],
    })


@app.post("/api/jobs")
def api_create_job() -> Response:
    try:
        payload = request.get_json(force=True) or {}
        job = create_job(payload)
        return jsonify({"job": job}), 202
    except ScriptError as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def api_delete_job(job_id: str) -> Response:
    jobs = load_jobs()
    new_jobs = [job for job in jobs if job.get("id") != job_id]
    if len(new_jobs) == len(jobs):
        return jsonify({"error": "任务不存在"}), 404
    save_jobs(new_jobs)
    return jsonify({"ok": True})


@app.route("/api/jobs/<job_id>", methods=["PATCH"])
def api_update_job(job_id: str) -> Response:
    payload = request.get_json(force=True) or {}
    name = str(payload.get("name") or "").strip()
    if not name:
        return jsonify({"error": "缺少 name"}), 400
    jobs = load_jobs()
    for job in jobs:
        if job.get("id") == job_id:
            job["name"] = name
            job["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save_jobs(jobs)
            return jsonify({"job": job})
    return jsonify({"error": "任务不存在"}), 404


@app.put("/api/jobs/<job_id>/params")
def api_update_job_params(job_id: str) -> Response:
    body = request.get_json(force=True) or {}
    market = str(body.get("market") or "").strip().lower()
    token = str(body.get("token") or "").strip().upper()
    symbol = str(body.get("symbol") or "").strip().upper()

    if market not in SCRAPE_MARKETS:
        return jsonify({"error": "market 必须为 um、spot 或 saving"}), 400
    if not token or not re.match(r"^[A-Z0-9]{1,24}$", token):
        return jsonify({"error": "token 格式无效"}), 400
    if not symbol or not re.match(r"^[A-Z0-9]{2,30}$", symbol):
        return jsonify({"error": "symbol 格式无效"}), 400

    with state_lock:
        jobs = load_jobs()
        for job in jobs:
            if job.get("id") == job_id:
                p = job.setdefault("payload", {})
                p["market"] = market
                p["token"] = token
                p["symbol"] = symbol
                name = str(body.get("name") or "").strip()
                job["name"] = name if name else f"{market.upper()} {token}"
                reward_token = str(body.get("rewardToken") or "").strip().upper()
                if reward_token:
                    p["rewardToken"] = reward_token
                else:
                    p.pop("rewardToken", None)
                p.pop("rewardAmount", None)
                reward_mode = body.get("rewardMode")
                if reward_mode in ("rank", "total"):
                    p["rewardMode"] = reward_mode
                else:
                    p.pop("rewardMode", None)
                reward_tiers = body.get("rewardTiers")
                if isinstance(reward_tiers, list) and reward_tiers:
                    cleaned = []
                    for t in reward_tiers:
                        rmin = t.get("rankMin")
                        rmax = t.get("rankMax")
                        amt = t.get("amount")
                        if isinstance(rmin, int) and isinstance(rmax, int) and rmin >= 1 and rmax >= rmin:
                            cleaned.append({"rankMin": rmin, "rankMax": rmax, "amount": str(int(amt) if isinstance(amt, (int, float)) else amt or "0")})
                    if cleaned:
                        p["rewardTiers"] = cleaned
                    else:
                        p.pop("rewardTiers", None)
                else:
                    p.pop("rewardTiers", None)
                total_reward = body.get("totalReward")
                if total_reward:
                    p["totalReward"] = str(total_reward)
                else:
                    p.pop("totalReward", None)
                eligible_users = body.get("eligibleUsers")
                if eligible_users is not None:
                    try:
                        p["eligibleUsers"] = int(eligible_users)
                    except (TypeError, ValueError):
                        pass
                else:
                    p.pop("eligibleUsers", None)
                for k in ("activityStart", "activityEnd"):
                    v = body.get(k)
                    if v:
                        p[k] = str(v)
                    else:
                        p.pop(k, None)
                top = body.get("top")
                if top is not None:
                    try:
                        p["top"] = int(top)
                    except (TypeError, ValueError):
                        pass
                else:
                    p.pop("top", None)
                job["updatedAt"] = datetime.now(timezone.utc).isoformat()
                save_jobs(jobs)
                return jsonify({"job": job})
        return jsonify({"error": "任务不存在"}), 404


@app.get("/api/jobs/<job_id>/snapshots")
def api_job_snapshots(job_id: str) -> Response:
    jobs = load_jobs()
    job = next((j for j in jobs if j.get("id") == job_id), None)
    if not job:
        return jsonify({"error": "任务不存在"}), 404
    snapshots = job.get("snapshots", [])
    enriched = []
    for snap in snapshots:
        json_path = snap.get("json")
        entry = dict(snap)
        if json_path:
            entry["jsonUrl"] = public_file_or_none(json_path)
            entry["csvUrl"] = public_file_or_none(snap.get("csv"))
        enriched.append(entry)
    return jsonify({"snapshots": enriched})


@app.delete("/api/jobs/<job_id>/snapshots/<snapshot_timestamp>")
def api_delete_snapshot(job_id: str, snapshot_timestamp: str) -> Response:
    with state_lock:
        jobs = load_jobs()
        job = next((j for j in jobs if j.get("id") == job_id), None)
        if not job:
            return jsonify({"error": "任务不存在"}), 404

        snapshots = job.get("snapshots") or []
        if len(snapshots) < 2:
            return jsonify({"error": "至少保留一个快照"}), 400

        idx = next(
            (i for i, s in enumerate(snapshots) if s.get("timestamp") == snapshot_timestamp),
            None,
        )
        if idx is None:
            return jsonify({"error": "快照不存在"}), 404

        removed = snapshots.pop(idx)
        deleted_files: list[str] = []
        for key in ("json", "csv"):
            fp = removed.get(key)
            if fp:
                try:
                    Path(str(fp)).unlink(missing_ok=True)
                    deleted_files.append(str(fp))
                except OSError:
                    pass

        # clear result if it references a deleted file
        removed_json = removed.get("json")
        if removed_json:
            result = job.get("result")
            if isinstance(result, list):
                job["result"] = [r for r in result if r.get("json") != removed_json]
            elif isinstance(result, dict) and result.get("json") == removed_json:
                job.pop("result", None)

        job["snapshots"] = snapshots
        job["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save_jobs(jobs)

    return jsonify({"success": True, "deleted": deleted_files})


@app.post("/api/jobs/<job_id>/kill")
def api_kill_job(job_id: str) -> Response:
    with _rp_lock:
        proc = _running_processes.get(job_id)
    if proc:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            return jsonify({"error": "终止进程失败"}), 500

        with _rp_lock:
            _running_processes.pop(job_id, None)

    with state_lock:
        jobs = load_jobs()
        found = False
        for j in jobs:
            if j.get("id") == job_id and j.get("status") in ("running", "queued"):
                j["status"] = "failed"
                j["finishedAt"] = datetime.now(timezone.utc).isoformat()
                j["updatedAt"] = datetime.now(timezone.utc).isoformat()
                j["returnCode"] = -1
                j["stderr"] = (j.get("stderr") or "") + "\nterminated by user"
                save_jobs(jobs)
                found = True
                break

    if not found:
        return jsonify({"error": "任务不在运行中"}), 404

    return jsonify({"success": True})


@app.get("/api/jobs/<job_id>/preview")
def api_job_preview(job_id: str) -> Response:
    jobs = load_jobs()
    job = next((j for j in jobs if j.get("id") == job_id), None)
    if not job:
        return jsonify({"error": "任务不存在"}), 404
    payload = job.get("payload") or {}

    snapshots = job.get("snapshots") or []
    snapshot_ts = request.args.get("snapshot", "").strip()

    if snapshot_ts:
        entry = next((s for s in snapshots if s.get("timestamp") == snapshot_ts), None)
        if not entry:
            return jsonify({"error": "快照不存在"}), 404
        json_path_str = entry.get("json")
        if not json_path_str:
            return jsonify({"error": "快照文件路径缺失"}), 400
        json_path = Path(str(json_path_str))
    else:
        if job.get("status") != "completed":
            return jsonify({"error": "任务未完成"}), 400
        result = job.get("result")
        json_path_str = None
        if isinstance(result, list) and result:
            candidate = result[0].get("json")
            if candidate and Path(str(candidate)).exists():
                json_path_str = candidate
        if not json_path_str and snapshots:
            json_path_str = snapshots[-1].get("json")
        if not json_path_str:
            return jsonify({"error": "没有预览数据"}), 400
        json_path = Path(str(json_path_str))

    try:
        prev_json_path = None
        compare_ts = request.args.get("compare")
        if compare_ts and snapshots:
            compare_entry = next(
                (s for s in snapshots if s.get("timestamp") == compare_ts),
                None,
            )
            if compare_entry:
                p = compare_entry.get("json")
                if p and Path(str(p)).exists():
                    prev_json_path = Path(str(p))
        if prev_json_path is None and snapshots:
            json_path_str = str(json_path)
            current_idx = next(
                (i for i, s in enumerate(snapshots) if s.get("json") == json_path_str),
                None,
            )
            if current_idx is not None and current_idx > 0:
                prev_entry = snapshots[current_idx - 1]
                prev_path_str = prev_entry.get("json")
                if prev_path_str:
                    prev_json_path = Path(str(prev_path_str))

        preview = scrape_preview_from_json(json_path, limit=payload.get("top") or 1000, previous_json_path=prev_json_path)
        preview["market"] = payload.get("market", "").upper()
        preview["symbol"] = payload.get("symbol", "")
        preview["token"] = payload.get("token", "").upper()
        preview["rewardToken"] = payload.get("rewardToken", "")
        preview["rewardAmount"] = payload.get("rewardAmount", "")
        preview["rewardTiers"] = payload.get("rewardTiers")
        preview["rewardMode"] = payload.get("rewardMode")
        preview["totalReward"] = payload.get("totalReward")
        preview["eligibleUsers"] = payload.get("eligibleUsers")
        if preview.get("rewardTiers"):
            preview["totalRewardAmount"] = sum(
                int(t.get("amount", 0)) for t in preview["rewardTiers"]
            )
        job_name = job.get("name") or payload.get("name") or payload.get("resourceId") or job.get("id", "")
        rid = str(payload.get("resourceId") or "").strip()
        preview["taskName"] = f"{job_name} [{rid}]" if rid else job_name
        preview["activityStart"] = payload.get("activityStart")
        preview["activityEnd"] = payload.get("activityEnd")
        preview["snapshots"] = [
            {"timestamp": s["timestamp"], "rows": s.get("rows"), "sum": s.get("sum")}
            for s in snapshots
        ]
        # Previous snapshot stats for环比计算
        prev_stats = None
        if prev_json_path and prev_json_path.exists():
            prev_data = read_json(prev_json_path, {})
            prev_rows = prev_data.get("rows") if isinstance(prev_data, dict) else []
            if isinstance(prev_rows, list):
                r_mode = payload.get("rewardMode") or "rank"
                r_tiers = payload.get("rewardTiers") or []
                r_eligible = int(payload.get("eligibleUsers") or 0)
                if r_mode == "rank" and r_tiers:
                    sorted_tiers = sorted(r_tiers, key=lambda t: t.get("rankMin", 0))
                    ranges = []
                    cursor = 1
                    for t in sorted_tiers:
                        rmin = t.get("rankMin", 0)
                        rmax = t.get("rankMax", 0)
                        if rmin > cursor:
                            ranges.append((f"{cursor}~{rmin-1}", cursor, rmin-1))
                        ranges.append((f"{rmin}~{rmax}", rmin, rmax))
                        cursor = rmax + 1
                elif r_mode == "total" and r_eligible > 0:
                    ranges = [(f"1~{r_eligible}", 1, r_eligible)]
                else:
                    ranges = [("1~5",1,5),("6~20",6,20),("21~50",21,50),("51~200",51,200),("201~1000",201,1000)]
                stats_list = []
                for label, rmin, rmax in ranges:
                    items = [r for r in prev_rows if r.get("sequence") and rmin <= int(r["sequence"]) <= rmax]
                    n = len(items)
                    if n == 0:
                        stats_list.append({"label": label, "total": 0, "avg": 0, "med": 0, "q1": 0, "last": 0})
                        continue
                    total = sum(float(r.get("grade", 0) or 0) for r in items)
                    by_grade = sorted(items, key=lambda r: float(r.get("grade", 0) or 0))
                    grades = [float(r.get("grade", 0) or 0) for r in by_grade]
                    avg = total / n
                    med = grades[n // 2] if n % 2 else (grades[n // 2 - 1] + grades[n // 2]) / 2
                    q1_index = int(n * 0.25)
                    q1 = grades[min(q1_index, n - 1)]
                    last = float(items[-1].get("grade", 0) or 0)
                    stats_list.append({"label": label, "total": total, "avg": avg, "med": med, "q1": q1, "last": last})
                prev_stats = stats_list
        preview["prevStats"] = prev_stats
        team_db = load_teams_db()
        team_lookup: dict[str, str] = {}
        team_sizes: dict[str, int] = {}
        for team in team_db.get("teams") or []:
            team_name = team.get("name", "")
            team_sizes[team_name] = len(team.get("members") or [])
            for m in team.get("members") or []:
                key = (m.get("nickname") or "").strip()
                if key and key not in team_lookup:
                    team_lookup[key] = team_name
        team_map: dict[str, str] = {}
        for row in preview.get("rows") or []:
            nick = row.get("nickname") or ""
            team_name = team_lookup.get(nickname_value({"nickName": nick}))
            if team_name:
                team_map[nick] = team_name
        preview["teamMap"] = team_map
        preview["teamSizes"] = team_sizes
        p_reward_token = (payload.get("rewardToken", "") or "").strip().upper()
        p_activity_end = payload.get("activityEnd", "")
        preview["rewardPriceUsd"] = get_token_price(p_reward_token, p_activity_end)
        preview["rewardPriceIsLive"] = False
        if p_activity_end:
            try:
                p_end_dt = datetime.strptime(p_activity_end, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
                preview["rewardPriceIsLive"] = p_end_dt + timedelta(hours=24) > datetime.now(timezone.utc).astimezone(BJ)
            except (ValueError, OSError):
                pass
        return jsonify(preview)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/jobs/<job_id>/trend")
def api_job_trend(job_id: str) -> Response:
    """Return per-tier statistics across all snapshots for a job."""
    jobs = load_jobs()
    job = next((j for j in jobs if j.get("id") == job_id), None)
    if not job:
        return jsonify({"error": "任务不存在"}), 404
    payload = job.get("payload") or {}
    snapshots = job.get("snapshots") or []
    if not snapshots:
        return jsonify({"error": "没有快照"}), 400
    sorted_snaps = sorted(snapshots, key=lambda s: s.get("timestamp", ""))
    # Build ranges (same logic as preview endpoint)
    r_mode = payload.get("rewardMode") or "rank"
    r_tiers = payload.get("rewardTiers") or []
    r_eligible = int(payload.get("eligibleUsers") or 0)
    if r_mode == "rank" and r_tiers:
        sorted_tiers = sorted(r_tiers, key=lambda t: t.get("rankMin", 0))
        ranges: list[tuple[str, int, int]] = []
        cursor = 1
        for t in sorted_tiers:
            rmin = t.get("rankMin", 0)
            rmax = t.get("rankMax", 0)
            if rmin > cursor:
                ranges.append((f"{cursor}~{rmin-1}", cursor, rmin - 1))
            ranges.append((f"{rmin}~{rmax}", rmin, rmax))
            cursor = rmax + 1
    elif r_mode == "total" and r_eligible > 0:
        ranges = [(f"1~{r_eligible}", 1, r_eligible)]
    else:
        ranges = [("1~5", 1, 5), ("6~20", 6, 20), ("21~50", 21, 50), ("51~200", 51, 200), ("201~1000", 201, 1000)]
    top = payload.get("top") or 1000
    result_snapshots = []
    prev_grade_map: dict[int, float] = {}
    for snap in sorted_snaps:
        json_path_str = snap.get("json")
        if not json_path_str:
            continue
        json_path = Path(str(json_path_str))
        if not json_path.exists():
            continue
        data = read_json(json_path, {})
        rows = data.get("rows") if isinstance(data, dict) else []
        if not isinstance(rows, list):
            continue
        limited = rows[:top]
        if not limited:
            continue
        cur_map: dict[int, float] = {}
        for r in limited:
            seq = int(r.get("sequence") or 0)
            cur_map[seq] = float(r.get("grade", 0) or 0)
        range_out = []
        for label, rmin, rmax in ranges:
            items = [r for r in limited if r.get("sequence") and rmin <= int(r["sequence"]) <= rmax]
            n = len(items)
            if n == 0:
                range_out.append({"label": label, "total": 0, "avg": 0, "med": 0, "q1": 0, "last": 0, "pct": 0, "sumDelta": 0, "dratio": 0})
                continue
            total = sum(float(r.get("grade", 0) or 0) for r in items)
            grades = sorted(float(r.get("grade", 0) or 0) for r in items)
            avg = total / n
            med = grades[n // 2] if n % 2 else (grades[n // 2 - 1] + grades[n // 2]) / 2
            q1_index = int(n * 0.25)
            q1 = grades[min(q1_index, n - 1)]
            last = float(items[-1].get("grade", 0) or 0)
            sumDelta = 0.0
            for r in items:
                seq = int(r.get("sequence") or 0)
                cur_grade = float(r.get("grade", 0) or 0)
                prev_grade = prev_grade_map.get(seq)
                if prev_grade is not None:
                    sumDelta += cur_grade - prev_grade
            range_out.append({"label": label, "total": total, "avg": avg, "med": med, "q1": q1, "last": last, "sumDelta": sumDelta})
        total_vol = sum(rs["total"] for rs in range_out)
        total_delta = sum(rs["sumDelta"] for rs in range_out)
        for rs in range_out:
            rs["pct"] = round(rs["total"] / total_vol * 100, 2) if total_vol else 0
            rs["dratio"] = round(rs["sumDelta"] / total_delta * 100, 2) if total_delta else 0
        result_snapshots.append({"timestamp": snap["timestamp"], "rows": len(limited), "totalVol": total_vol, "ranges": range_out})
        prev_grade_map = cur_map
    return jsonify({"snapshots": result_snapshots, "rangeLabels": [r[0] for r in ranges]})


@app.get("/api/jobs/<job_id>/team-analysis")
def api_team_analysis(job_id: str) -> Response:
    jobs = load_jobs()
    job = next((j for j in jobs if j.get("id") == job_id), None)
    if not job:
        return jsonify({"error": "任务不存在"}), 404

    snapshots = job.get("snapshots") or []
    if len(snapshots) < 2:
        return jsonify({"error": "快照不足（需要至少 2 个）"}), 400

    max_rank_gap = request.args.get("max_rank_gap", 20, type=int)
    top_n = request.args.get("top_n", 500, type=int)
    delta_err = request.args.get("delta_err", 1000, type=int)
    min_delta = request.args.get("min_delta", 500, type=int)

    ts1 = request.args.get("snapshot1")
    ts2 = request.args.get("snapshot2")

    if ts1 and ts2:
        s1 = next((s for s in snapshots if s.get("timestamp") == ts1), None)
        s2 = next((s for s in snapshots if s.get("timestamp") == ts2), None)
        if not s1 or not s2:
            return jsonify({"error": "指定快照不存在"}), 400
        if ts1 > ts2:
            s1, s2 = s2, s1
        selected = [s1, s2]
    else:
        selected = snapshots[-2:]

    paths: list[Path] = []
    for s in selected:
        p = s.get("json")
        if p:
            path = Path(str(p))
            if path.exists():
                paths.append(path)
    if len(paths) < 2:
        return jsonify({"error": "快照文件不足"}), 400

    try:
        teams = detect_teams(
            paths,
            max_rank_gap=max_rank_gap,
            top_n=top_n,
            delta_err=delta_err,
            min_delta=min_delta,
        )
        return jsonify({
            "teams": teams,
            "params": {
                "snapshot1": selected[0]["timestamp"],
                "snapshot2": selected[1]["timestamp"],
                "maxRankGap": max_rank_gap,
                "topN": top_n,
                "deltaErr": delta_err,
                "minDelta": min_delta,
            },
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/jobs/team-analysis-cross")
def api_team_analysis_cross() -> Response:
    all_jobs = load_jobs()
    market = (request.args.get("market") or "").strip().lower()
    job_ids_str = (request.args.get("job_ids") or "").strip()
    max_rank_gap = request.args.get("max_rank_gap", 20, type=int)
    grade_err = request.args.get("grade_err", 0.5, type=float)
    min_shared = request.args.get("min_shared_jobs", 2, type=int)
    top_n = request.args.get("top_n", 200, type=int)
    skip_top = request.args.get("skip_top", 50, type=int)

    # only include jobs where the activity has ended (activityEnd is in the past)
    now_bj = datetime.now(BJ)
    candidates = [
        j for j in all_jobs
        if j.get("status") == "completed"
        and not (market and (j.get("payload") or {}).get("market") != market)
    ]
    candidates2 = []
    for j in candidates:
        end_str = (j.get("payload") or {}).get("activityEnd", "")
        if not end_str:
            continue
        try:
            clean = end_str.strip().replace("T", " ")
            if clean.count(":") == 1:
                clean += ":00"
            end_dt = datetime.strptime(clean[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ)
            if end_dt >= now_bj:
                continue
        except Exception:
            continue
        candidates2.append(j)
    candidates = candidates2
    if job_ids_str:
        wanted = set(job_ids_str.split(","))
        candidates = [j for j in candidates if j.get("id") in wanted]
    if not candidates:
        return jsonify({"error": "没有符合条件的已完成任务"}), 400

    # Build per-user rank data across jobs
    user_jobs: dict[str, list[dict]] = {}  # nickname → [{jobId, jobName, rank, grade, userId}]
    for job in candidates:
        job_id = job.get("id", "")
        payload = job.get("payload", {})
        job_name = (payload.get("token") or payload.get("symbol") or job.get("name") or job_id)
        snapshots = job.get("snapshots") or []
        if not snapshots:
            continue
        sorted_ss = sorted(snapshots, key=lambda s: s.get("timestamp", ""))
        last_ss = sorted_ss[-1]
        ss_path = last_ss.get("json")
        if not ss_path:
            continue
        path = Path(str(ss_path))
        if not path.exists():
            continue
        data = read_json(path, {})
        if not isinstance(data, dict):
            continue
        rows = data.get("rows")
        if not isinstance(rows, list):
            continue
        for row in rows:
            nick = nickname_value(row)
            if not nick:
                continue
            rank = row.get("sequence")
            if not isinstance(rank, int):
                continue
            if rank <= skip_top:
                continue
            if rank > skip_top + top_n:
                continue
            grade = row.get("grade") or 0
            uid = row.get("userId") or ""
            if nick not in user_jobs:
                user_jobs[nick] = []
            user_jobs[nick].append({
                "jobId": job_id,
                "jobName": str(job_name),
                "rank": rank,
                "grade": float(grade),
                "userId": str(uid),
            })

    nicks = list(user_jobs.keys())
    if len(nicks) < 2:
        return jsonify({"teams": [], "params": {"totalUsers": len(nicks)}})

    # Precompute per-job entries for fast lookup
    user_entries: dict[str, dict[str, dict]] = {}  # nickname → {jobId → entry}
    for nick, entries in user_jobs.items():
        user_entries[nick] = {e["jobId"]: e for e in entries}

    # Greedy team formation with range-based connectivity
    # Each team maintains per-job [min_rank, max_rank] and [min_grade, max_grade];
    # new member connects if within max_rank_gap and grade_err of either boundary.
    def avg_rank_of_user(nick):
        entries = user_entries[nick]
        vals = [e["rank"] for e in entries.values()]
        return sum(vals) / len(vals) if vals else 999999

    sorted_users = sorted(nicks, key=avg_rank_of_user)
    candidate_teams: list[list[str]] = []

    for i, nick_a in enumerate(sorted_users):
        entries_a = user_entries[nick_a]
        team_range: dict[str, dict[str, int | float]] = {}
        for jid, ea in entries_a.items():
            team_range[jid] = {"rMin": ea["rank"], "rMax": ea["rank"], "gMin": ea["grade"], "gMax": ea["grade"]}
        team = {nick_a}

        for j in range(i + 1, len(sorted_users)):
            nick_b = sorted_users[j]
            entries_b = user_entries[nick_b]
            ok = True
            shared = 0
            for jid, eb in entries_b.items():
                tr = team_range.get(jid)
                if tr is None:
                    continue
                shared += 1
                r_ok = abs(eb["rank"] - tr["rMin"]) <= max_rank_gap or abs(eb["rank"] - tr["rMax"]) <= max_rank_gap
                g_base = max(abs(tr["gMin"]), abs(tr["gMax"]), 1)
                g_ok = (abs(eb["grade"] - tr["gMin"]) / g_base <= grade_err or
                        abs(eb["grade"] - tr["gMax"]) / g_base <= grade_err)
                if not (r_ok and g_ok):
                    ok = False
                    break
            if ok and shared >= min_shared:
                team.add(nick_b)
                for jid, eb in entries_b.items():
                    tr = team_range.get(jid)
                    if tr is not None:
                        if eb["rank"] < tr["rMin"]: tr["rMin"] = eb["rank"]
                        if eb["rank"] > tr["rMax"]: tr["rMax"] = eb["rank"]
                        if eb["grade"] < tr["gMin"]: tr["gMin"] = eb["grade"]
                        if eb["grade"] > tr["gMax"]: tr["gMax"] = eb["grade"]
        if len(team) >= 2:
            candidate_teams.append(list(team))

    # Deduplicate — keep non-overlapping teams sorted by avg rank
    used = set()
    kept = []
    candidate_teams.sort(key=lambda t: (avg_rank_of_user(t[0]), -len(t)))
    for team in candidate_teams:
        if any(n in used for n in team):
            continue
        used.update(team)
        kept.append(team)

    result = []
    for tidx, members in enumerate(kept):
        member_list = []
        for n in members:
            jobs_info = sorted(user_entries[n].values(), key=lambda x: x.get("jobName", ""))
            member_list.append({
                "nickname": n,
                "userId": jobs_info[0]["userId"] if jobs_info else "",
                "jobs": jobs_info,
            })
        avg_r = sum(avg_rank_of_user(n) for n in members) / len(members) if members else 0
        result.append({
            "id": tidx,
            "size": len(members),
            "avgRank": round(avg_r, 1),
            "members": member_list,
        })

    return jsonify({
        "teams": result,
        "params": {
            "totalUsers": len(nicks),
            "totalJobs": len(candidates),
            "maxRankGap": max_rank_gap,
            "gradeErr": grade_err,
            "minSharedJobs": min_shared,
            "topN": top_n,
            "skipTop": skip_top,
        },
    })


@app.get("/api/schedules")
def api_schedules() -> Response:
    return jsonify({"schedules": load_schedules()})


@app.post("/api/schedules")
def api_create_schedule() -> Response:
    payload = request.get_json(force=True) or {}
    if not payload.get("url"):
        return jsonify({"error": "缺少 url"}), 400
    schedule = {
        "id": uuid.uuid4().hex[:10],
        "url": payload["url"],
        "name": payload.get("name"),
        "symbol": payload.get("symbol"),
        "hour": int(payload.get("hour", 8)),
        "minute": int(payload.get("minute", 15)),
        "proxy": payload.get("proxy"),
        "refresh": bool(payload.get("refresh", False)),
        "browserWaitMs": int(payload.get("browserWaitMs", 30000)),
        "enabled": bool(payload.get("enabled", True)),
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }
    schedules = load_schedules()
    schedules.append(schedule)
    save_schedules(schedules)
    return jsonify({"schedule": schedule}), 201


@app.delete("/api/schedules/<schedule_id>")
def api_delete_schedule(schedule_id: str) -> Response:
    schedules = [item for item in load_schedules() if item.get("id") != schedule_id]
    save_schedules(schedules)
    return jsonify({"ok": True})


@app.post("/api/schedules/<schedule_id>/toggle")
def api_toggle_schedule(schedule_id: str) -> Response:
    schedules = load_schedules()
    for schedule in schedules:
        if schedule.get("id") == schedule_id:
            schedule["enabled"] = not schedule.get("enabled", True)
            save_schedules(schedules)
            return jsonify({"schedule": schedule})
    return jsonify({"error": "not found"}), 404


def _team_member_key(m: dict) -> str:
    return (m.get("nickname") or "").strip().lower()


def _team_item(member: dict, weight: int = 1) -> dict:
    return {
        "nickname": str(member.get("nickname") or ""),
        "userId": str(member.get("userId") or ""),
        "weight": weight,
    }


@app.get("/api/teams")
def api_get_teams() -> Response:
    db = load_teams_db()
    return jsonify({"db": db})


@app.post("/api/teams")
def api_create_teams() -> Response:
    db = load_teams_db()
    if db.get("teams"):
        return jsonify({"error": "数据库已存在，请使用 PUT 追加"}), 400
    body = request.get_json(force=True)
    raw_teams = body.get("teams") or []
    teams_out = []
    for team in raw_teams:
        name = str(team.get("name") or "").strip() or f"团队 {len(teams_out) + 1}"
        members_in = team.get("members") or []
        members_out = [_team_item(m, weight=1) for m in members_in]
        teams_out.append({"name": name, "members": members_out})
    now = datetime.now(timezone.utc).isoformat()
    db = {
        "teams": teams_out,
        "createdAt": now,
        "updatedAt": now,
    }
    save_teams_db(db)
    return jsonify({"db": db}), 201


@app.put("/api/teams")
def api_append_teams() -> Response:
    db = load_teams_db()
    if not db.get("teams"):
        db = {"teams": [], "createdAt": datetime.now(timezone.utc).isoformat(), "updatedAt": datetime.now(timezone.utc).isoformat()}
    body = request.get_json(force=True)
    raw_teams = body.get("teams") or []
    existing_teams = db.get("teams") or []

    def _team_index_by_name(name: str) -> int | None:
        for i, t in enumerate(existing_teams):
            if t.get("name") == name:
                return i
        return None

    for incoming in raw_teams:
        name = str(incoming.get("name") or "").strip() or "未命名"
        idx = _team_index_by_name(name)
        if idx is not None:
            target = existing_teams[idx]
            existing_members = target.get("members") or []
            existing_map: dict[str, dict] = {}
            for em in existing_members:
                existing_map[_team_member_key(em)] = em
            for m in incoming.get("members") or []:
                key = _team_member_key(m)
                if key in existing_map:
                    existing_map[key]["weight"] = existing_map[key].get("weight", 1) + 1
                else:
                    existing_members.append(_team_item(m, weight=1))
            target["members"] = existing_members
        else:
            members_out = [_team_item(m, weight=1) for m in (incoming.get("members") or [])]
            existing_teams.append({"name": name, "members": members_out})

    db["teams"] = existing_teams
    db["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save_teams_db(db)
    return jsonify({"db": db})


@app.delete("/api/teams")
def api_delete_teams() -> Response:
    db = load_teams_db()
    if not db.get("teams"):
        return jsonify({"error": "数据库不存在"}), 404
    TEAMS_FILE.unlink(missing_ok=True)
    return jsonify({"ok": True})


@app.put("/api/teams/team")
def api_rename_team() -> Response:
    db = load_teams_db()
    body = request.get_json(force=True)
    team_idx = int(body.get("teamIndex", 0))
    new_name = str(body.get("name") or "").strip()
    if not new_name:
        return jsonify({"error": "名称不能为空"}), 400
    teams = db.get("teams") or []
    if team_idx < 0 or team_idx >= len(teams):
        return jsonify({"error": "团队索引无效"}), 400
    teams[team_idx]["name"] = new_name
    db["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save_teams_db(db)
    return jsonify({"db": db})


@app.delete("/api/teams/team")
def api_delete_team() -> Response:
    db = load_teams_db()
    body = request.get_json(force=True)
    team_idx = int(body.get("teamIndex", 0))
    teams = db.get("teams") or []
    if team_idx < 0 or team_idx >= len(teams):
        return jsonify({"error": "团队索引无效"}), 400
    del teams[team_idx]
    db["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save_teams_db(db)
    return jsonify({"db": db})


@app.delete("/api/teams/member")
def api_delete_member() -> Response:
    db = load_teams_db()
    body = request.get_json(force=True)
    team_idx = body.get("teamIndex")
    nickname = str(body.get("nickname") or "").strip().lower()
    teams = db.get("teams") or []
    if team_idx is not None and 0 <= team_idx < len(teams):
        teams[team_idx]["members"] = [
            m for m in (teams[team_idx].get("members") or [])
            if _team_member_key(m) != nickname
        ]
    else:
        for team in teams:
            team["members"] = [
                m for m in (team.get("members") or [])
                if _team_member_key(m) != nickname
            ]
    db["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save_teams_db(db)
    return jsonify({"db": db})


@app.post("/api/teams/member")
def api_add_member() -> Response:
    db = load_teams_db()
    body = request.get_json(force=True)
    team_idx = int(body.get("teamIndex", 0))
    teams = db.get("teams") or []
    if team_idx < 0 or team_idx >= len(teams):
        return jsonify({"error": "团队索引无效"}), 400
    member = _team_item(body, weight=int(body.get("weight", 1)))
    teams[team_idx].setdefault("members", []).append(member)
    db["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save_teams_db(db)
    return jsonify({"db": db})


@app.put("/api/teams/member/weight")
def api_update_member_weight() -> Response:
    db = load_teams_db()
    body = request.get_json(force=True)
    team_idx = int(body.get("teamIndex", 0))
    nickname = str(body.get("nickname") or "").strip().lower()
    new_weight = int(body.get("weight", 1))
    teams = db.get("teams") or []
    if team_idx < 0 or team_idx >= len(teams):
        return jsonify({"error": "团队索引无效"}), 400
    for m in (teams[team_idx].get("members") or []):
        if _team_member_key(m) == nickname:
            m["weight"] = new_weight
            db["updatedAt"] = datetime.now(timezone.utc).isoformat()
            save_teams_db(db)
            return jsonify({"db": db})
    return jsonify({"error": "成员不存在"}), 404


@app.post("/api/teams/merge")
def api_merge_teams() -> Response:
    db = load_teams_db()
    body = request.get_json(force=True)
    source_idx = int(body.get("sourceIndex", -1))
    target_idx = int(body.get("targetIndex", -1))
    teams = db.get("teams") or []
    if source_idx < 0 or source_idx >= len(teams):
        return jsonify({"error": "源团队索引无效"}), 400
    if target_idx < 0 or target_idx >= len(teams):
        return jsonify({"error": "目标团队索引无效"}), 400
    if source_idx == target_idx:
        return jsonify({"error": "不能合并到自身"}), 400

    source = teams[source_idx]
    target = teams[target_idx]
    target_members = target.get("members") or []
    target_map = {_team_member_key(m): m for m in target_members}

    for m in (source.get("members") or []):
        key = _team_member_key(m)
        if key in target_map:
            target_map[key]["weight"] = max(
                target_map[key].get("weight", 1), m.get("weight", 1)
            )
        else:
            target_members.append(dict(m))

    target["members"] = target_members
    del teams[source_idx]
    db["teams"] = teams
    db["updatedAt"] = datetime.now(timezone.utc).isoformat()
    save_teams_db(db)
    return jsonify({"db": db})


@app.get("/teams.html")
def teams_page() -> Response:
    resp = send_from_directory(str(APP_DIR / "web"), "teams.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


@app.get("/files/<path:relative>")
def files(relative: str) -> Response:
    try:
        target = safe_child(DATA_ROOT, relative)
    except ValueError:
        return jsonify({"error": "invalid path"}), 400
    parts = Path(relative).parts
    blocked_suffixes = {".env", ".har", ".pem", ".key", ".p12", ".crt", ".sqlite", ".db"}
    if any(part.startswith(".") for part in parts) or target.suffix.lower() in blocked_suffixes:
        return jsonify({"error": "not found"}), 404
    if not target.exists() or not target.is_file():
        return jsonify({"error": "not found"}), 404
    return send_from_directory(target.parent, target.name)


@app.get("/api/jobs/<job_id>/delta-analysis")
def api_job_delta_analysis(job_id: str) -> Response:
    import requests

    jobs = load_jobs()
    job = next((j for j in jobs if j.get("id") == job_id), None)
    if not job:
        return jsonify({"error": "任务不存在"}), 404
    payload = job.get("payload") or {}
    token = (request.args.get("token") or payload.get("token") or "").upper()
    symbol = (request.args.get("symbol") or payload.get("symbol") or "").upper()
    if not symbol:
        if token:
            symbol = token + "USDT"
        else:
            return jsonify({"error": "缺少交易对"}), 400
    market_type = str(payload.get("market") or "").lower()
    klines_url = SPOT_KLINES if market_type == "spot" else FAPI_KLINES

    snapshots = job.get("snapshots") or []
    if len(snapshots) < 2:
        return jsonify({"error": "至少需要 2 个快照"}), 400

    try:
        proxy = choose_binance_proxy("auto", timeout=8)
    except ScriptError:
        proxy = _no_job_context  # sentinel: no connection at all
    no_connection = proxy is _no_job_context

    snapshot_data = []
    for s in snapshots:
        json_path = s.get("json")
        if not json_path:
            continue
        path = Path(str(json_path))
        if not path.exists():
            continue
        data = read_json(path, {})
        if not isinstance(data, dict):
            continue
        meta = data.get("meta") or {}
        updated_at_ms = to_decimal(meta.get("updatedTime"))
        if updated_at_ms is None:
            continue
        snapshot_data.append({
            "timestamp": s["timestamp"],
            "sum": to_decimal(data.get("sum")),
            "eligibleVolume": to_decimal(meta.get("eligibleTradingVolume")),
            "totalUsers": meta.get("total"),
            "updatedTimeMs": updated_at_ms,
            "updatedAtBj": datetime.fromtimestamp(
                float(updated_at_ms / Decimal("1000")), timezone.utc
            ).astimezone(BJ),
        })

    if len(snapshot_data) < 2:
        return jsonify({"error": "可读的快照不足 2 个"}), 400

    pairs = []
    for i in range(1, len(snapshot_data)):
        prev = snapshot_data[i - 1]
        cur = snapshot_data[i]
        leaderboard_delta = None
        if prev["sum"] is not None and cur["sum"] is not None:
            leaderboard_delta = cur["sum"] - prev["sum"]
        eligible_delta = None
        if prev["eligibleVolume"] is not None and cur["eligibleVolume"] is not None:
            eligible_delta = cur["eligibleVolume"] - prev["eligibleVolume"]
        end_bj = cur["updatedAtBj"].replace(microsecond=0)
        start_bj = prev["updatedAtBj"].replace(microsecond=0)
        pair = {
            "prevTimestamp": prev["timestamp"],
            "curTimestamp": cur["timestamp"],
            "prev": {
                "sum": decimal_text(prev["sum"]),
                "eligibleVolume": decimal_text(prev["eligibleVolume"]),
                "totalUsers": prev["totalUsers"],
                "updatedAt": prev["updatedAtBj"].strftime("%Y-%m-%d %H:%M:%S"),
            },
            "cur": {
                "sum": decimal_text(cur["sum"]),
                "eligibleVolume": decimal_text(cur["eligibleVolume"]),
                "totalUsers": cur["totalUsers"],
                "updatedAt": cur["updatedAtBj"].strftime("%Y-%m-%d %H:%M:%S"),
            },
            "leaderboardDelta": decimal_text(leaderboard_delta),
            "eligibleDelta": decimal_text(eligible_delta),
            "windowStart": start_bj.strftime("%Y-%m-%d %H:%M:%S"),
            "windowEnd": end_bj.strftime("%Y-%m-%d %H:%M:%S"),
            "marketBaseVolume": None,
            "marketQuoteVolume": None,
            "leaderboardDeltaRatio": None,
            "klines": 0,
            "error": None,
        }
        if no_connection:
            pair["error"] = "无可用连接方式"
        else:
            try:
                start_ms = int(start_bj.astimezone(timezone.utc).timestamp() * 1000)
                end_ms = int(end_bj.astimezone(timezone.utc).timestamp() * 1000)
                resp = requests.get(
                    klines_url,
                    params={
                        "symbol": symbol,
                        "interval": "1h",
                        "startTime": start_ms,
                        "endTime": end_ms,
                        "limit": 1500,
                    },
                    proxies=request_proxies(proxy),
                    timeout=15,
                )
                payload_k = resp.json()
                if resp.status_code == 200 and isinstance(payload_k, list):
                    base_v = Decimal("0")
                    quote_v = Decimal("0")
                    for k in payload_k:
                        if isinstance(k, list) and len(k) >= 8:
                            base_v += to_decimal(k[5]) or Decimal("0")
                            quote_v += to_decimal(k[7]) or Decimal("0")
                    pair["marketBaseVolume"] = decimal_text(base_v)
                    pair["marketQuoteVolume"] = decimal_text(quote_v)
                    pair["klines"] = len(payload_k)
                    if quote_v and leaderboard_delta is not None:
                        pair["leaderboardDeltaRatio"] = decimal_text(leaderboard_delta / quote_v)
                else:
                    pair["error"] = f"Binance API HTTP {resp.status_code}"
            except Exception as exc:
                pair["error"] = str(exc)
        pairs.append(pair)

    # hourly market / prediction rows
    now_bj = datetime.now(BJ)
    ratio = None
    running_sum = None
    # parse activity end time (used as boundary for both market and prediction rows)
    act_end_dt = None
    act_end_str = payload.get("activityEnd")
    if act_end_str:
        try:
            act_end_clean = act_end_str.strip().replace("T", " ")
            if act_end_clean.count(":") == 1:
                act_end_clean += ":00"
            act_end_dt = datetime.strptime(act_end_clean[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ)
        except Exception:
            pass
    end_boundary = min(now_bj, act_end_dt) if act_end_dt else now_bj
    if snapshot_data and pairs:
        last_snap = snapshot_data[-1]
        last_time = last_snap["updatedAtBj"]
        last_pair = pairs[-1]
        ratio_raw = last_pair.get("leaderboardDeltaRatio")
        if ratio_raw is not None:
            try:
                ratio = Decimal(str(ratio_raw))
            except Exception:
                ratio = None
        running_sum = to_decimal(last_snap.get("sum"))
        if running_sum is None:
            cur_sum_str = last_pair.get("cur", {}).get("sum")
            if cur_sum_str is not None:
                running_sum = to_decimal(cur_sum_str)

    # market rows (real klines from last snapshot to end_boundary)
    if not no_connection and snapshot_data and pairs and end_boundary > last_time:
        gap_start_ms = int(last_time.astimezone(timezone.utc).timestamp() * 1000)
        gap_end_ms = int(end_boundary.astimezone(timezone.utc).timestamp() * 1000)
        try:
            resp_gap = requests.get(
                klines_url,
                params={
                    "symbol": symbol,
                    "interval": "1h",
                    "startTime": gap_start_ms,
                    "endTime": gap_end_ms,
                    "limit": 1500,
                },
                proxies=request_proxies(proxy),
                timeout=15,
            )
            payload_k = resp_gap.json()
            if resp_gap.status_code == 200 and isinstance(payload_k, list):
                for k in payload_k:
                    if not isinstance(k, list) or len(k) < 8:
                        continue
                    open_ms = k[0]
                    close_ms = k[6]
                    base_v = to_decimal(k[5]) or Decimal("0")
                    quote_v = to_decimal(k[7]) or Decimal("0")
                    open_dt = datetime.fromtimestamp(open_ms / 1000, BJ)
                    close_dt = datetime.fromtimestamp(close_ms / 1000, BJ)
                    if open_dt >= end_boundary:
                        continue
                    is_partial = close_dt > end_boundary
                    scale = Decimal("1")
                    if is_partial:
                        elapsed = max((end_boundary - open_dt).total_seconds(), 1)
                        scale = Decimal("3600") / Decimal(str(elapsed))
                    scaled_quote = quote_v * scale if quote_v else None
                    scaled_base = base_v * scale if base_v else None
                    est_delta = (scaled_quote * ratio if ratio is not None and scaled_quote else None)
                    prev_sum = running_sum
                    cur_sum = (prev_sum + est_delta if est_delta is not None and prev_sum is not None else None)
                    if cur_sum is not None:
                        running_sum = cur_sum
                    pairs.append({
                        "type": "market",
                        "windowStart": open_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "windowEnd": close_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "marketBaseVolume": decimal_text(scaled_base) if scaled_base is not None else None,
                        "marketQuoteVolume": decimal_text(scaled_quote) if scaled_quote is not None else None,
                        "rawQuoteVolume": decimal_text(quote_v) if quote_v else None,
                        "leaderboardDelta": decimal_text(est_delta) if est_delta is not None else None,
                        "leaderboardDeltaRatio": None,
                        "prevSum": decimal_text(prev_sum) if prev_sum is not None else None,
                        "curSum": decimal_text(cur_sum) if cur_sum is not None else None,
                        "klines": 1,
                        "partialHour": is_partial,
                        "error": None,
                        "symbol": symbol,
                    })
        except Exception as exc:
            pass  # silently skip on error

    # prediction rows from now to activityEnd
    if pairs and ratio is not None and act_end_dt and now_bj < act_end_dt <= now_bj + timedelta(hours=6):
        # baseline volume from last row (market or snapshot)
        last_row = pairs[-1]
        base_vol = to_decimal(last_row.get("marketQuoteVolume"))
        if base_vol is None:
            base_vol = to_decimal(last_row.get("marketBaseVolume"))
        pred_hour = now_bj.replace(minute=0, second=0, microsecond=0)
        while pred_hour < act_end_dt:
            hour_end = pred_hour + timedelta(hours=1)
            partial = hour_end > act_end_dt
            if partial and base_vol is not None:
                portion = max((act_end_dt - pred_hour).total_seconds(), 1) / 3600
                hour_vol = base_vol * Decimal(str(portion))
            elif pred_hour < now_bj and base_vol is not None:
                # current hour: use remaining time from now to hour_end/act_end
                remain_end = hour_end if hour_end <= act_end_dt else act_end_dt
                remain = max((remain_end - now_bj).total_seconds(), 1) / 3600
                hour_vol = base_vol * Decimal(str(remain))
            else:
                hour_vol = base_vol
            est_delta = (hour_vol * ratio if hour_vol is not None else None)
            prev_sum = running_sum
            cur_sum = (prev_sum + est_delta if est_delta is not None and prev_sum is not None else None)
            if cur_sum is not None:
                running_sum = cur_sum
            pairs.append({
                "type": "prediction",
                "windowStart": pred_hour.strftime("%Y-%m-%d %H:%M:%S"),
                "windowEnd": hour_end.strftime("%Y-%m-%d %H:%M:%S"),
                "marketQuoteVolume": decimal_text(hour_vol) if hour_vol is not None else None,
                "leaderboardDelta": decimal_text(est_delta) if est_delta is not None else None,
                "leaderboardDeltaRatio": decimal_text(ratio) if ratio is not None else None,
                "prevSum": decimal_text(prev_sum) if prev_sum is not None else None,
                "curSum": decimal_text(cur_sum) if cur_sum is not None else None,
                "partialHour": partial,
                "predicted": True,
                "klines": 0,
                "error": None,
                "symbol": symbol,
            })
            pred_hour = hour_end

    proxy_label = "无可用连接" if no_connection else ("直连" if proxy is None else str(proxy))
    return jsonify({"pairs": pairs, "symbol": symbol, "proxyStatus": proxy_label, "klinesUrl": klines_url})


def main() -> None:
    ensure_state()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=sync_activities_loop, daemon=True).start()
    host = os.environ.get("WEB_HOST", "0.0.0.0")
    port = int(os.environ.get("WEB_PORT", "48234"))
    app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
