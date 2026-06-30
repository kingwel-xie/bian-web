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
KNOWN_SYMBOLS = {
    "bill": "BILLUSDT",
    "aig": "AIGENSYNUSDT",
}
MARK_RANKS = (20, 50, 200)
LIVE_RANKS = tuple(sorted({*range(10, 201, 10), 35}))
BJ = timezone(timedelta(hours=8))
FAPI_KLINES = "https://fapi.binance.com/fapi/v1/klines"
FSTREAM_WS = "wss://fstream.binance.com/ws"
LIVE_KLINE_INTERVAL = "1m"
DEFAULT_PROXY_PORTS = (7897, 7890, 7891, 10809, 1080, 8011)
SCRAPE_TOP = 1000
SCRAPE_PAGE_SIZE = 100
SCRAPE_MARKETS = {"um", "spot"}

app = Flask(__name__, static_folder="web", static_url_path="/_static")
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
    if not re.fullmatch(r"[A-Z0-9]{2,24}", token):
        raise ScriptError("symbol 格式无效。")
    return token.lower(), symbol


def normalize_scrape_market(raw_market: Any) -> str:
    market = str(raw_market or "").lower().strip()
    if market not in SCRAPE_MARKETS:
        raise ScriptError("market 只能是 um 或 spot。")
    return market




def normalize_scrape_payload(payload: dict[str, Any]) -> dict[str, Any]:
    market = normalize_scrape_market(payload.get("market"))
    token, symbol = normalize_scrape_symbol(payload.get("symbol"))
    url = str(payload.get("url") or "").strip()
    resource_id = str(payload.get("resourceId") or "").strip()
    if not resource_id or not re.fullmatch(r"\d{1,12}", resource_id):
        raise ScriptError("resourceId 必须是数字。")
    return {
        **payload,
        "mode": "scrape",
        "market": market,
        "token": token,
        "symbol": symbol,
        "name": resource_id,
        "url": url,
        "resourceId": resource_id,
        "top": SCRAPE_TOP,
        "pageSize": SCRAPE_PAGE_SIZE,
    }


def ensure_state() -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    for path, default in [(JOBS_FILE, []), (SCHEDULES_FILE, [])]:
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
    for path in sorted(activity_dir.glob(f"*_{name}_top500.json")):
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
        json_files = sorted(activity_dir.glob("*_top500.json"))
        if json_files and infer_name_from_file(json_files[-1]) == safe_name:
            return activity_dir
    return None


def ms_to_bj(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(BJ)


def load_snapshot_records(activity_dir: Path, name: str) -> list[dict[str, Any]]:
    records = []
    for path in sorted(activity_dir.glob(f"*_{name}_top500.json")):
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
        json_files = sorted(activity_dir.glob("*_top500.json"))
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
    command = [
        sys.executable,
        str(APP_DIR / "auto_leaderboard.py"),
        "--activity",
        f"{normalized['name']}={normalized['url']}",
        "--top",
        str(SCRAPE_TOP),
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


def latest_delta_payload(activity_dir: Path, name: str, json_path: Path) -> dict[str, Any] | None:
    prefix = json_path.name.rsplit("_top", 1)[0]
    path = activity_dir / f"{prefix}_delta_by_nickname.json"
    data = read_json(path, None)
    if not isinstance(data, dict):
        return None
    for item in data.get("ranges") or []:
        if not isinstance(item, dict):
            continue
        for key in ("csv",):
            url = public_file_or_none(item.get(key))
            if url:
                item[f"{key}Url"] = url
    json_url = public_file_or_none(path)
    if json_url:
        data["jsonUrl"] = json_url
    combined_chart_url = public_file_with_mtime_or_none(data.get("combinedChart"))
    if combined_chart_url:
        data["combinedChartUrl"] = combined_chart_url
    return data


def load_delta_by_nickname(activity_dir: Path, delta_payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    mapped = {}
    if not delta_payload:
        return mapped
    for item in delta_payload.get("ranges") or []:
        if not isinstance(item, dict):
            continue
        csv_path = item.get("csv")
        if not csv_path:
            continue
        path = Path(str(csv_path))
        if not path.is_absolute():
            path = activity_dir / str(csv_path)
        try:
            import csv

            with path.open(encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle):
                    nickname = str(row.get("nickName") or "").strip()
                    if nickname:
                        mapped[nickname] = row
        except OSError:
            continue
    return mapped


def nickname_value(row: dict[str, Any]) -> str:
    return str(row.get("nickName") or row.get("nickname") or "").strip()


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
                    )
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
        previous_grade = to_decimal(previous[nickname].get("grade")) if nickname in previous else Decimal("0")
        if previous_grade is None:
            previous_grade = Decimal("0")
        mapped[nickname] = {"deltaGrade": decimal_text(current_grade - previous_grade)}
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
    delta_payload = latest_delta_payload(json_path.parent, str(data.get("name") or ""), json_path)
    if previous_json_path is not _no_job_context:
        if previous_json_path and previous_json_path.exists():
            if delta_payload:
                delta_payload["previousSnapshot"] = str(previous_json_path)
                delta_payload["firstSnapshot"] = False
            else:
                delta_payload = {
                    "name": data.get("name") or "",
                    "previousSnapshot": str(previous_json_path),
                    "firstSnapshot": False,
                }
        else:
            delta_payload = None
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
        "delta": delta_payload,
        "rows": compact_leaderboard_rows(rows, limit, delta_by_nickname),
    }


def latest_scrape_preview(name: str, limit: int = 1000) -> dict[str, Any] | None:
    safe_name = str(name or "").lower().strip()
    if not re.fullmatch(r"[a-z0-9_-]+", safe_name):
        return None
    activity_dir = DATA_ROOT / safe_name
    if not activity_dir.is_dir():
        return None
    candidates = sorted(activity_dir.glob(f"*_{safe_name}_top{SCRAPE_TOP}.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        candidates = sorted(activity_dir.glob("*_top*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        return None
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
                current["preview"] = scrape_preview_from_json(Path(str(json_path)), limit=1000)
            except Exception as exc:
                current["previewError"] = str(exc)
        enriched.append(current)
    return enriched


def progress_from_stderr(stderr_text: str, status: str = "running") -> dict[str, Any]:
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
                "rowsFetched": min(page * SCRAPE_PAGE_SIZE, SCRAPE_TOP),
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
    update_job(
        job_id,
        status="running",
        progress=progress_from_stderr("", "running"),
        startedAt=datetime.now(timezone.utc).isoformat(),
    )
    command = job_command(payload)
    process = subprocess.Popen(
        command,
        cwd=APP_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

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
                    progress=progress_from_stderr(stderr_tail, "running"),
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
        progress=progress_from_stderr(combined_stderr, status),
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
            job = {
                "id": uuid.uuid4().hex[:12],
                "source": source,
                "status": "queued",
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
            "cachedAt": entry.get("cachedAt"),
        })
    entries.sort(key=lambda e: e.get("cachedAt") or "", reverse=True)
    return jsonify({"entries": entries})


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


@app.get("/api/jobs")
def api_jobs() -> Response:
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    per_page = max(1, min(per_page, 100))
    page = max(1, page)

    all_jobs = load_jobs()
    total = len(all_jobs)
    total_pages = max(1, (total + per_page - 1) // per_page) if total else 1
    page = min(page, total_pages) if total else 1

    sorted_jobs = sorted(
        all_jobs,
        key=lambda j: j.get("updatedAt") or j.get("startedAt") or j.get("createdAt") or "",
        reverse=True,
    )
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
                "symbol": payload.get("symbol"),
                "resourceId": payload.get("resourceId"),
                "url": payload.get("url"),
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
            json_path_str = result[0].get("json")
        if not json_path_str and snapshots:
            json_path_str = snapshots[-1].get("json")
        if not json_path_str:
            return jsonify({"error": "没有预览数据"}), 400
        json_path = Path(str(json_path_str))

    try:
        prev_json_path = None
        if snapshots:
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

        preview = scrape_preview_from_json(json_path, limit=1000, previous_json_path=prev_json_path)
        preview["market"] = payload.get("market", "").upper()
        preview["symbol"] = payload.get("symbol", "")
        job_name = job.get("name") or payload.get("name") or payload.get("resourceId") or job.get("id", "")
        rid = str(payload.get("resourceId") or "").strip()
        preview["taskName"] = f"{job_name} [{rid}]" if rid else job_name
        preview["snapshots"] = [
            {"timestamp": s["timestamp"], "rows": s.get("rows"), "sum": s.get("sum")}
            for s in snapshots
        ]
        return jsonify(preview)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


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


def main() -> None:
    ensure_state()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    host = os.environ.get("WEB_HOST", "0.0.0.0")
    port = int(os.environ.get("WEB_PORT", "48234"))
    app.run(host=host, port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
