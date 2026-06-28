#!/usr/bin/env python3
"""
Fetch leaderboard rows from a HAR-recorded Binance request and sum a numeric field.

Default usage:
    python3 sum_har_leaderboard.py trafi.har
    python3 sum_har_leaderboard.py trafi.har xau.har alt.har um.har

The script finds the recorded leaderboard API request in the HAR, reuses its
headers and JSON payload, paginates until it has the requested top N rows, and
sums the requested field with Decimal precision.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import socket
import sys
import time
import unicodedata
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError as exc:  # pragma: no cover - environment guard
    raise SystemExit("缺少依赖 requests，请先安装：python3 -m pip install requests") from exc


DEFAULT_ENDPOINT_SUBSTRING = (
    "/bapi/growth/v1/friendly/growth-paas/resource/summary/list"
)
DEFAULT_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE_CANDIDATES = (500, 200, 100, 50, 20, 10)
DEFAULT_PROXY_PORTS = (7897, 7890, 7891, 10809, 1080, 8011)
SKIP_REQUEST_HEADERS = {
    "accept-encoding",
    "content-length",
    "host",
}
DEFAULT_PRICE_SYMBOLS = {
    "BNB": "BNBUSDT",
    "PUMP": "PUMPUSDT",
    "BANK": "BANKUSDT",
}
BINANCE_PRICE_ENDPOINT = "https://api.binance.com/api/v3/ticker/price"
DEFAULT_CACHE_DIR = Path(".leaderboard_cache")
DEFAULT_CACHE_TTL = 0
DEFAULT_PRICE_CACHE_TTL = 30


class ScriptError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从 HAR 中复用排行榜接口请求，抓取前 N 条并汇总指定字段。"
    )
    parser.add_argument(
        "har_files",
        nargs="+",
        help="一个或多个 HAR 文件路径，例如 trafi.har xau.har alt.har um.har",
    )
    parser.add_argument(
        "-n",
        "--top",
        type=int,
        default=500,
        help="抓取前 N 名，默认 500",
    )
    parser.add_argument(
        "--field",
        default="grade",
        help="要求和的字段，默认 grade；也可用 tradingVolume",
    )
    parser.add_argument(
        "--page-size",
        default=str(DEFAULT_PAGE_SIZE),
        help="每页抓取数量，默认 100；传 auto 可自动探测接口允许值",
    )
    parser.add_argument(
        "-u",
        "--user",
        "--target-name",
        dest="target_name",
        default="mm周",
        help="要查找的昵称，默认 mm周；例如 --user wudi523",
    )
    parser.add_argument(
        "--no-rewards",
        action="store_true",
        help="不按内置奖池计算目标用户奖励",
    )
    parser.add_argument(
        "--no-prices",
        action="store_true",
        help="不抓取 BNB/PUMP/BANK 价格，也不折算 USDT 总额",
    )
    parser.add_argument(
        "--compare-rank",
        type=int,
        default=50,
        help="用于对比的名次，默认第 50 名",
    )
    parser.add_argument(
        "--target-search-limit",
        type=int,
        default=0,
        help=(
            "查找昵称时最多抓到第几名；默认 0 表示只在 --top 范围内查找。"
        ),
    )
    parser.add_argument(
        "--endpoint-substring",
        default=DEFAULT_ENDPOINT_SUBSTRING,
        help="用于定位接口请求的 URL 片段",
    )
    parser.add_argument(
        "--proxy",
        default="auto",
        help=(
            "代理设置：auto 自动尝试直连和常见本地代理；none 只直连；"
            "或传入 http://127.0.0.1:7897"
        ),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="分页请求间隔秒数，默认 0.35",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="单次请求超时秒数，默认 30",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=DEFAULT_CACHE_TTL,
        help="排行榜缓存有效秒数，默认 0 表示不过期；缓存 key 包含 HAR MD5",
    )
    parser.add_argument(
        "--price-cache-ttl",
        type=int,
        default=DEFAULT_PRICE_CACHE_TTL,
        help="价格缓存有效秒数，默认 30；设为 0 表示不过期",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="忽略缓存，强制重新抓取排行榜和价格",
    )
    parser.add_argument(
        "--cache-dir",
        default=str(DEFAULT_CACHE_DIR),
        help=f"缓存目录，默认 {DEFAULT_CACHE_DIR}",
    )
    parser.add_argument(
        "--save-json",
        help="可选：把抓到的行和汇总信息保存为 JSON",
    )
    parser.add_argument(
        "--save-csv",
        help="可选：把抓到的行保存为 CSV",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="不输出分页进度，只输出最终结果",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="输出机器可读 JSON，而不是中文摘要",
    )
    return parser.parse_args()


def log(message: str, quiet: bool) -> None:
    if not quiet:
        print(message, file=sys.stderr)


def load_har(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ScriptError(f"找不到 HAR 文件：{path}") from exc
    except json.JSONDecodeError as exc:
        raise ScriptError(f"HAR 不是合法 JSON：{path}: {exc}") from exc


def file_md5(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def cache_dir(args: argparse.Namespace) -> Path:
    path = Path(args.cache_dir).expanduser()
    path.mkdir(parents=True, exist_ok=True)
    return path


def cache_key(parts: dict[str, Any]) -> str:
    return hashlib.md5(stable_json(parts).encode("utf-8")).hexdigest()


def cache_path(args: argparse.Namespace, kind: str, key: str) -> Path:
    return cache_dir(args) / f"{kind}_{key}.json"


def cache_is_fresh(payload: dict[str, Any], ttl: int) -> bool:
    if ttl == 0:
        return True
    created_at = payload.get("createdAt")
    return isinstance(created_at, (int, float)) and (time.time() - created_at) <= ttl


def read_cache(path: Path, ttl: int, refresh: bool) -> dict[str, Any] | None:
    if refresh or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not cache_is_fresh(payload, ttl):
        return None
    return payload


def write_cache(path: Path, payload: dict[str, Any]) -> None:
    payload = dict(payload)
    payload["createdAt"] = time.time()
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_json_text(text: str | None) -> Any | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def resource_summary_list(api_payload: Any) -> dict[str, Any] | None:
    if not isinstance(api_payload, dict):
        return None
    data = api_payload.get("data")
    if not isinstance(data, dict):
        return None
    summary = data.get("resourceSummaryList")
    if isinstance(summary, dict) and isinstance(summary.get("data"), list):
        return summary
    return None


def find_leaderboard_entry(
    har: dict[str, Any], endpoint_substring: str
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    entries = har.get("log", {}).get("entries", [])
    candidates: list[tuple[int, int, int, dict[str, Any], dict[str, Any]]] = []

    for index, entry in enumerate(entries):
        request = entry.get("request", {})
        response = entry.get("response", {})
        url = request.get("url", "")
        method = request.get("method", "")
        if method.upper() != "POST" or endpoint_substring not in url:
            continue

        post_text = request.get("postData", {}).get("text")
        post_payload = parse_json_text(post_text)
        if not isinstance(post_payload, dict):
            continue

        response_text = response.get("content", {}).get("text")
        response_payload = parse_json_text(response_text)
        summary = resource_summary_list(response_payload)
        row_count = len(summary.get("data", [])) if summary else 0
        page_size = int(post_payload.get("pageSize") or row_count or 0)
        candidates.append((row_count, page_size, index, entry, post_payload))

    if not candidates:
        raise ScriptError(
            "没有在 HAR 中找到排行榜接口请求；可用 --endpoint-substring 指定 URL 片段。"
        )

    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    _, _, index, entry, post_payload = candidates[0]
    return index, entry, post_payload


def headers_from_har(entry: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for header in entry.get("request", {}).get("headers", []):
        name = header.get("name")
        value = header.get("value")
        if not name or value is None:
            continue
        if name.startswith(":") or name.lower() in SKIP_REQUEST_HEADERS:
            continue
        headers[name] = str(value)

    headers["accept"] = "application/json, text/plain, */*"
    headers["content-type"] = "application/json"
    return headers


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


def proxy_label(proxy: str | None) -> str:
    return "direct" if proxy is None else proxy


def request_proxies(proxy: str | None) -> dict[str, str] | None:
    if proxy is None:
        return None
    return {"http": proxy, "https": proxy}


def api_success(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("success") is True
        and payload.get("code") == "000000"
        and resource_summary_list(payload) is not None
    )


def fetch_page(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    base_payload: dict[str, Any],
    page_index: int,
    page_size: int,
    proxy: str | None,
    timeout: float,
) -> dict[str, Any]:
    payload = dict(base_payload)
    payload["pageIndex"] = page_index
    payload["pageSize"] = page_size

    response = session.post(
        url,
        headers=headers,
        json=payload,
        proxies=request_proxies(proxy),
        timeout=timeout,
    )

    try:
        data = response.json()
    except ValueError as exc:
        head = response.text[:200].replace("\n", " ")
        raise ScriptError(
            f"接口返回非 JSON，HTTP {response.status_code}，片段：{head}"
        ) from exc

    if response.status_code != 200 or not api_success(data):
        code = data.get("code") if isinstance(data, dict) else None
        message = data.get("message") if isinstance(data, dict) else None
        raise ScriptError(
            f"接口请求失败：HTTP {response.status_code}, code={code}, message={message}"
        )

    return data


def try_fetch_page(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    base_payload: dict[str, Any],
    page_index: int,
    page_size: int,
    proxy: str | None,
    timeout: float,
) -> tuple[dict[str, Any] | None, str | None]:
    try:
        return (
            fetch_page(
                session,
                url,
                headers,
                base_payload,
                page_index,
                page_size,
                proxy,
                timeout,
            ),
            None,
        )
    except (requests.RequestException, ScriptError) as exc:
        return None, str(exc)


def choose_proxy(
    url: str,
    headers: dict[str, str],
    base_payload: dict[str, Any],
    candidates: list[str | None],
    timeout: float,
    quiet: bool,
) -> str | None:
    session = requests.Session()
    errors: list[str] = []

    for proxy in candidates:
        log(f"测试连接：{proxy_label(proxy)}", quiet)
        _, error = try_fetch_page(
            session,
            url,
            headers,
            base_payload,
            page_index=1,
            page_size=10,
            proxy=proxy,
            timeout=timeout,
        )
        if error is None:
            return proxy
        errors.append(f"{proxy_label(proxy)} => {error}")

    raise ScriptError("所有连接方式都失败：\n" + "\n".join(errors))


def detect_page_size(
    session: requests.Session,
    url: str,
    headers: dict[str, str],
    base_payload: dict[str, Any],
    top_n: int,
    proxy: str | None,
    timeout: float,
    quiet: bool,
) -> tuple[int, dict[str, Any]]:
    candidates = []
    for size in DEFAULT_PAGE_SIZE_CANDIDATES:
        if size not in candidates:
            candidates.append(size)
    if top_n not in candidates:
        candidates.insert(0, top_n)

    for page_size in candidates:
        if page_size <= 0:
            continue
        log(f"测试 pageSize={page_size}", quiet)
        data, error = try_fetch_page(
            session,
            url,
            headers,
            base_payload,
            page_index=1,
            page_size=page_size,
            proxy=proxy,
            timeout=timeout,
        )
        if data is not None:
            return page_size, data
        if "参数非法" not in (error or ""):
            log(f"pageSize={page_size} 失败：{error}", quiet)

    raise ScriptError("没有找到可用 pageSize。")


def rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    summary = resource_summary_list(payload)
    if not summary:
        return []
    return summary.get("data", [])


def meta_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    summary = data.get("resourceSummaryList", {}) if isinstance(data, dict) else {}
    return {
        "eligibleUserCount": data.get("eligibleUserCount"),
        "eligibleTradingVolume": data.get("eligibleTradingVolume"),
        "updatedTime": data.get("updatedTime"),
        "total": summary.get("total"),
    }


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


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def truncate_2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def format_fixed_2(value: Decimal | None) -> str:
    if value is None:
        return "无"
    return f"{truncate_2(value):,.2f}"


def format_trimmed_decimal(value: Decimal | None) -> str:
    if value is None:
        return "无"
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_amount(value: Decimal | None) -> str:
    if value is None:
        return "无"

    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= Decimal("100000000"):
        scaled = value / Decimal("100000000")
        return f"{sign}{truncate_2(scaled)}亿"
    if value >= Decimal("10000"):
        scaled = value / Decimal("10000")
        return f"{sign}{truncate_2(scaled)}万"
    return f"{sign}{truncate_2(value)}"


def field_label(field: str) -> str:
    if field == "grade":
        return "成交量(grade)"
    if field == "tradingVolume":
        return "成交量(tradingVolume)"
    return field


def competition_from_filename(har_path: Path) -> dict[str, Any]:
    name = har_path.name.lower()
    if "trafi" in name:
        return {"name": "TradFi", "pools": [{"unit": "USDT", "amount": "120000"}]}
    if "xau" in name:
        return {"name": "XAU", "pools": [{"unit": "USDT", "amount": "60000"}]}
    if "alt" in name:
        return {
            "name": "Alt",
            "pools": [
                {"unit": "PUMP", "amount": "50000000"},
                {"unit": "BANK", "amount": "2500000"},
            ],
        }
    if "um" in name:
        return {"name": "UM", "pools": [{"unit": "BNB", "amount": "330"}]}
    raise ScriptError(f"无法从文件名识别比赛类型：{har_path.name}，文件名需包含 trafi/xau/alt/um")


def competition_name_from_result(result: dict[str, Any]) -> str:
    return (result.get("competition") or {}).get("name") or "未知"


def format_updated_time(ms: Any) -> str:
    value = to_decimal(ms)
    if value is None:
        return "无"

    timestamp = float(value / Decimal("1000"))
    tz = timezone(timedelta(hours=8))
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).astimezone(tz).strftime(
        "%Y-%m-%d %H:%M:%S 北京时间"
    )


def row_by_rank(rows: list[dict[str, Any]], rank: int) -> dict[str, Any] | None:
    for row in rows:
        if row.get("sequence") == rank:
            return row
    index = rank - 1
    if 0 <= index < len(rows):
        return rows[index]
    return None


def find_row_by_nick(rows: list[dict[str, Any]], target_name: str) -> dict[str, Any] | None:
    target = target_name.strip().lower()
    if not target:
        return None

    for row in rows:
        nick = str(row.get("nickName") or "").strip()
        if nick.lower() == target:
            return row

    for row in rows:
        nick = str(row.get("nickName") or "").strip()
        if target in nick.lower():
            return row

    return None


def sequence_status(rows: list[dict[str, Any]]) -> tuple[Any, Any, bool | None]:
    if not rows:
        return None, None, None
    sequences = [row.get("sequence") for row in rows]
    if not all(isinstance(seq, int) for seq in sequences):
        return sequences[0], sequences[-1], None
    expected = list(range(sequences[0], sequences[0] + len(sequences)))
    return sequences[0], sequences[-1], sequences == expected


def calculate_rewards(
    target_value: Decimal | None,
    total_value: Decimal,
    no_rewards: bool,
    pools: list[dict[str, str]],
) -> list[dict[str, str]]:
    if no_rewards or target_value is None or total_value <= 0:
        return []

    rewards = []
    for pool in pools:
        pool_amount = to_decimal(pool.get("amount"))
        if pool_amount is None:
            continue
        reward = truncate_2(target_value / total_value * pool_amount)
        rewards.append(
            {
                "unit": pool["unit"],
                "pool": decimal_text(pool_amount),
                "reward": decimal_text(reward),
            }
        )
    return rewards


def calculate_rewards_for_value(
    value: Decimal,
    total_value: Decimal,
    no_rewards: bool,
    pools: list[dict[str, str]],
) -> list[dict[str, str]]:
    return calculate_rewards(value, total_value, no_rewards, pools)


def format_rewards(rewards: list[dict[str, str]]) -> str:
    parts = []
    for reward in rewards:
        amount = to_decimal(reward.get("reward"))
        unit = reward.get("unit", "")
        text = f"{format_amount(amount)} {unit}".strip()
        usdt_value = to_decimal(reward.get("usdtValue"))
        if unit != "USDT" and usdt_value is not None:
            text += f"≈{format_fixed_2(usdt_value)}U"
        parts.append(text)
    return " + ".join(parts) if parts else "未配置奖池"


def format_pools(rewards: list[dict[str, str]]) -> str:
    parts = []
    for reward in rewards:
        pool = to_decimal(reward.get("pool"))
        unit = reward.get("unit", "")
        parts.append(f"{format_amount(pool)} {unit}".strip())
    return " + ".join(parts) if parts else "未配置奖池"


def format_share(value: Decimal | None) -> str:
    if value is None:
        return "无"
    return f"{truncate_2(value * Decimal('100'))}%"


def combined_rewards(results: list[dict[str, Any]]) -> dict[str, Decimal]:
    combined: dict[str, Decimal] = {}
    for result in results:
        target = result.get("target") or {}
        for reward in target.get("rewards") or []:
            unit = reward.get("unit")
            amount = to_decimal(reward.get("reward"))
            if not unit or amount is None:
                continue
            combined[unit] = combined.get(unit, Decimal("0")) + amount
    return combined


def format_combined_rewards(rewards: dict[str, Decimal]) -> str:
    if not rewards:
        return "无"
    return " + ".join(
        f"{format_amount(amount)} {unit}" for unit, amount in rewards.items()
    )


def reward_units(results: list[dict[str, Any]]) -> list[str]:
    units: list[str] = []
    for result in results:
        target = result.get("target") or {}
        for reward in target.get("rewards") or []:
            unit = reward.get("unit")
            if unit and unit != "USDT" and unit not in units:
                units.append(unit)
    return units


def price_symbols_for_units(units: list[str]) -> list[str]:
    symbols = []
    for unit in units:
        symbol = DEFAULT_PRICE_SYMBOLS.get(unit)
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def fetch_prices(units: list[str], args: argparse.Namespace) -> dict[str, dict[str, str]]:
    symbols = price_symbols_for_units(units)
    if not symbols:
        return {}

    key = cache_key({"kind": "prices", "symbols": symbols})
    path = cache_path(args, "prices", key)
    cached = read_cache(path, args.price_cache_ttl, args.refresh)
    if cached and cached.get("symbols") == symbols and isinstance(cached.get("prices"), dict):
        log("价格缓存命中", args.quiet)
        return cached["prices"]

    errors: list[str] = []
    for proxy in proxy_candidates(args.proxy):
        try:
            response = requests.get(
                BINANCE_PRICE_ENDPOINT,
                params={"symbols": json.dumps(symbols, separators=(",", ":"))},
                proxies=request_proxies(proxy),
                timeout=args.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            errors.append(f"{proxy_label(proxy)} => {exc}")
            continue

        if not isinstance(payload, list):
            errors.append(f"{proxy_label(proxy)} => unexpected payload")
            continue

        by_symbol = {item.get("symbol"): item.get("price") for item in payload}
        prices: dict[str, dict[str, str]] = {}
        for unit in units:
            symbol = DEFAULT_PRICE_SYMBOLS.get(unit)
            price = by_symbol.get(symbol)
            if symbol and price is not None:
                prices[unit] = {"symbol": symbol, "price": str(price)}
        missing = [unit for unit in units if unit not in prices]
        if missing:
            errors.append(f"{proxy_label(proxy)} => missing prices: {', '.join(missing)}")
            continue
        write_cache(path, {"symbols": symbols, "prices": prices})
        return prices

    raise ScriptError("价格抓取失败：\n" + "\n".join(errors))


def apply_usdt_values(
    results: list[dict[str, Any]],
    prices: dict[str, dict[str, str]],
) -> Decimal:
    total_usdt = Decimal("0")
    for result in results:
        target = result.get("target") or {}
        rewards = target.get("rewards") or []
        target_usdt = Decimal("0")
        for reward in rewards:
            usdt_value = apply_reward_usdt_value(reward, prices)
            if usdt_value is None:
                continue
            target_usdt += usdt_value
        target["usdtTotal"] = decimal_text(truncate_2(target_usdt))
        per_10k_usdt = calculate_value_usdt_reward(
            Decimal("10000"),
            to_decimal(result.get("sum")) or Decimal("0"),
            prices,
            (result.get("competition") or {}).get("pools"),
        )
        result["per10kUsdtTotal"] = decimal_text(truncate_2(per_10k_usdt))
        total_usdt += target_usdt
    return truncate_2(total_usdt)


def apply_reward_usdt_value(
    reward: dict[str, str],
    prices: dict[str, dict[str, str]],
) -> Decimal | None:
    unit = reward.get("unit")
    reward_amount = to_decimal(reward.get("reward"))
    if not unit or reward_amount is None:
        return None
    if unit == "USDT":
        usdt_value = reward_amount
        price = Decimal("1")
        symbol = "USDT"
    else:
        price_info = prices.get(unit)
        price = to_decimal(price_info.get("price")) if price_info else None
        symbol = price_info.get("symbol") if price_info else None
        if price is None:
            return None
        usdt_value = truncate_2(reward_amount * price)
    reward["usdtPrice"] = decimal_text(price)
    reward["priceSymbol"] = symbol
    reward["usdtValue"] = decimal_text(usdt_value)
    return usdt_value


def price_for_unit(unit: str, prices: dict[str, dict[str, str]]) -> Decimal | None:
    if unit == "USDT":
        return Decimal("1")
    price_info = prices.get(unit)
    return to_decimal(price_info.get("price")) if price_info else None


def calculate_value_usdt_reward(
    value: Decimal,
    total_value: Decimal,
    prices: dict[str, dict[str, str]],
    pools: list[dict[str, str]],
) -> Decimal:
    if total_value <= 0:
        return Decimal("0")

    total_usdt = Decimal("0")
    for pool in pools:
        pool_amount = to_decimal(pool.get("amount"))
        unit = pool.get("unit")
        if pool_amount is None or not unit:
            continue
        price = price_for_unit(unit, prices)
        if price is None:
            continue
        native_reward = value / total_value * pool_amount
        total_usdt += native_reward * price
    return truncate_2(total_usdt)


def format_prices(prices: dict[str, dict[str, str]]) -> str:
    if not prices:
        return "无"
    parts = []
    for unit, info in prices.items():
        symbol = info.get("symbol")
        parts.append(f"{symbol}={format_trimmed_decimal(to_decimal(info.get('price')))}")
    return "，".join(parts)


def display_width(value: Any) -> int:
    text = "" if value is None else str(value)
    width = 0
    for char in text:
        if unicodedata.combining(char):
            continue
        width += 2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1
    return width


def pad_display(value: Any, width: int, align: str = "left") -> str:
    text = "" if value is None else str(value)
    padding = max(0, width - display_width(text))
    if align == "right":
        return " " * padding + text
    return text + " " * padding


def print_terminal_table(
    title: str,
    headers: list[str],
    rows: list[list[Any]],
    right_align: set[int] | None = None,
) -> None:
    right_align = right_align or set()
    widths = [
        max(display_width(header), *(display_width(row[index]) for row in rows))
        for index, header in enumerate(headers)
    ]

    def render_row(row: list[Any]) -> str:
        cells = []
        for index, value in enumerate(row):
            align = "right" if index in right_align else "left"
            cells.append(pad_display(value, widths[index], align))
        return "  ".join(cells)

    print(title)
    print(render_row(headers))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print(render_row(row))


def main_table_row(
    result: dict[str, Any],
    rows: list[dict[str, Any]],
    compare_rank: int,
) -> list[Any]:
    target = result.get("target") or {}
    rank_row = row_by_rank(rows, compare_rank)
    rank_value = to_decimal(rank_row.get(result["field"])) if rank_row else None
    target_value = to_decimal(target.get("fieldValue"))
    share = to_decimal(target.get("share"))
    diff = target_value - rank_value if target_value is not None and rank_value is not None else None
    diff_text = "无"
    if diff is not None:
        relation = "+" if diff >= 0 else "-"
        diff_text = f"{relation}{format_amount(abs(diff))}"

    return [
        competition_name_from_result(result),
        target.get("sequence") or "未找到",
        format_amount(target_value),
        format_amount(Decimal(result["sum"])),
        format_amount(rank_value),
        diff_text,
        (
            f"{format_fixed_2(to_decimal(result.get('per10kUsdtTotal')))}U"
            if to_decimal(result.get("per10kUsdtTotal")) is not None
            else "无"
        ),
        (
            f"{format_fixed_2(to_decimal(target.get('usdtTotal')))}U"
            if to_decimal(target.get("usdtTotal")) is not None
            else "无"
        ),
    ]


def reward_table_row(result: dict[str, Any]) -> list[Any]:
    target = result.get("target") or {}
    return [
        competition_name_from_result(result),
        format_pools(target.get("rewards") or []),
        format_rewards(target.get("rewards") or []),
    ]


def print_results_tables(
    processed: list[tuple[Path, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]],
    args: argparse.Namespace,
    prices: dict[str, dict[str, str]],
    total_usdt: Decimal | None,
) -> None:
    main_rows = [
        main_table_row(result, rows, args.compare_rank)
        for _, result, rows, _ in processed
    ]
    if total_usdt is not None and not args.no_prices:
        main_rows.append(
            [
                "合计",
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
                f"{format_fixed_2(total_usdt)}U",
            ]
        )

    print_terminal_table(
        f"{args.target_name} 成交量 / 收益",
        [
            "榜",
            "排名",
            "刷量",
            f"前{args.top}",
            f"第{args.compare_rank}",
            "差距",
            "1万≈U",
            "折U",
        ],
        main_rows,
        right_align={1, 2, 3, 4, 5, 6, 7},
    )

    print()
    print_terminal_table(
        "奖池 / 奖励明细",
        ["榜", "奖池", "预计奖励"],
        [reward_table_row(result) for _, result, _, _ in processed],
    )

    if args.target_name and not args.no_rewards:
        print()
        summary_rows = [["币种奖励合计", format_combined_rewards(combined_rewards([item[1] for item in processed]))]]
        if prices:
            summary_rows.append(["使用价格", format_prices(prices)])
        if total_usdt is not None and not args.no_prices:
            summary_rows.append(["折U总计", f"{format_fixed_2(total_usdt)}U"])
        print_terminal_table("奖励汇总", ["项目", "数值"], summary_rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    preferred = [
        "sequence",
        "userId",
        "nickName",
        "grade",
        "tradingVolume",
        "hitRisk",
        "avatarUrl",
        "optInId",
        "resourceId",
        "type",
        "mine",
        "updatedTime",
        "region",
        "rewardCount",
        "bigCardCount",
        "smallCardCount",
        "metaInfo",
        "reasonCodeList",
    ]
    seen = set(preferred)
    extras = sorted({key for row in rows for key in row.keys()} - seen)
    fieldnames = [name for name in preferred if any(name in row for row in rows)] + extras

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            cleaned = {}
            for key in fieldnames:
                value = row.get(key)
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, ensure_ascii=False)
                cleaned[key] = value
            writer.writerow(cleaned)


def parse_page_size_arg(value: str) -> int | None:
    text = str(value).strip().lower()
    if text == "auto":
        return None
    try:
        page_size = int(text)
    except ValueError as exc:
        raise ScriptError("--page-size 必须是正整数，或 auto") from exc
    if page_size <= 0:
        raise ScriptError("--page-size 必须大于 0")
    return page_size


def output_path_for(base_path: str, har_path: Path, multiple: bool) -> Path:
    path = Path(base_path).expanduser()
    if not multiple:
        return path
    return path.with_name(f"{path.stem}_{har_path.stem}{path.suffix}")


def process_har(
    har_path: Path, args: argparse.Namespace
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    har_md5 = file_md5(har_path)
    har = load_har(har_path)
    entry_index, entry, post_payload = find_leaderboard_entry(
        har, args.endpoint_substring
    )

    url = entry.get("request", {}).get("url")
    if not url:
        raise ScriptError("HAR 接口请求缺少 URL。")

    headers = headers_from_har(entry)
    base_payload = {
        key: value
        for key, value in post_payload.items()
        if key not in {"pageIndex", "pageSize"}
    }
    if not base_payload:
        raise ScriptError("HAR 接口请求缺少可复用的 JSON 参数。")
    competition = competition_from_filename(har_path)
    pools = competition.get("pools") or []

    requested_page_size = parse_page_size_arg(args.page_size)
    fetch_limit = max(args.top, args.compare_rank)
    if args.target_name and args.target_search_limit > 0:
        fetch_limit = max(fetch_limit, args.target_search_limit)

    leaderboard_cache_key = cache_key(
        {
            "kind": "leaderboard",
            "harMd5": har_md5,
            "url": url,
            "basePayload": base_payload,
            "pageSize": requested_page_size if requested_page_size is not None else "auto",
            "fetchLimit": fetch_limit,
        }
    )
    leaderboard_cache_path = cache_path(args, "leaderboard", leaderboard_cache_key)
    cached = read_cache(leaderboard_cache_path, args.cache_ttl, args.refresh)

    if (
        cached
        and cached.get("harMd5") == har_md5
        and cached.get("url") == url
        and cached.get("basePayload") == base_payload
        and isinstance(cached.get("rows"), list)
        and isinstance(cached.get("meta"), dict)
    ):
        page_size = int(cached.get("pageSize") or requested_page_size or DEFAULT_PAGE_SIZE)
        rows = cached["rows"][:fetch_limit]
        meta = cached["meta"]
        proxy_used = cached.get("proxy", "cache")
        log(f"排行榜缓存命中：{har_path.name} md5={har_md5}", args.quiet)
    else:
        log(f"使用 HAR entry #{entry_index}: {url}", args.quiet)
        proxy = choose_proxy(
            url,
            headers,
            base_payload,
            proxy_candidates(args.proxy),
            args.timeout,
            args.quiet,
        )
        proxy_used = proxy_label(proxy)
        log(f"使用连接方式：{proxy_used}", args.quiet)

        session = requests.Session()
        if requested_page_size is None:
            page_size, first_page_payload = detect_page_size(
                session,
                url,
                headers,
                base_payload,
                args.top,
                proxy,
                args.timeout,
                args.quiet,
            )
        else:
            page_size = requested_page_size
            first_page_payload = fetch_page(
                session,
                url,
                headers,
                base_payload,
                page_index=1,
                page_size=page_size,
                proxy=proxy,
                timeout=args.timeout,
            )
        log(f"使用 pageSize={page_size}", args.quiet)

        pages_needed = math.ceil(fetch_limit / page_size)
        rows: list[dict[str, Any]] = []
        meta = meta_from_payload(first_page_payload)

        for page_index in range(1, pages_needed + 1):
            if page_index == 1:
                payload = first_page_payload
            else:
                if args.delay > 0:
                    time.sleep(args.delay)
                payload = fetch_page(
                    session,
                    url,
                    headers,
                    base_payload,
                    page_index=page_index,
                    page_size=page_size,
                    proxy=proxy,
                    timeout=args.timeout,
                )
            page_rows = rows_from_payload(payload)
            rows.extend(page_rows)
            first_seq = page_rows[0].get("sequence") if page_rows else None
            last_seq = page_rows[-1].get("sequence") if page_rows else None
            log(
                f"第 {page_index}/{pages_needed} 页：{len(page_rows)} 条，sequence {first_seq}-{last_seq}",
                args.quiet,
            )
        rows = rows[:fetch_limit]
        write_cache(
            leaderboard_cache_path,
            {
                "harMd5": har_md5,
                "url": url,
                "basePayload": base_payload,
                "fetchLimit": fetch_limit,
                "pageSize": page_size,
                "proxy": proxy_used,
                "rows": rows,
                "meta": meta,
            },
        )

    rows = rows[:fetch_limit]
    top_rows = rows[: args.top]
    values = [to_decimal(row.get(args.field)) for row in top_rows]
    numeric_values = [value for value in values if value is not None]
    field_sum = sum(numeric_values, Decimal("0"))
    first_seq, last_seq, contiguous = sequence_status(top_rows)
    target_row = find_row_by_nick(rows, args.target_name) if args.target_name else None
    target_value = to_decimal(target_row.get(args.field)) if target_row else None
    target_share = target_value / field_sum if target_value is not None and field_sum > 0 else None
    rewards = calculate_rewards(
        target_value,
        field_sum,
        args.no_rewards,
        pools,
    )
    per_10k_rewards = calculate_rewards_for_value(
        Decimal("10000"),
        field_sum,
        args.no_rewards,
        pools,
    )

    result = {
        "source": str(har_path),
        "harMd5": har_md5,
        "endpoint": url,
        "requestEntryIndex": entry_index,
        "basePayload": base_payload,
        "competition": competition,
        "topN": args.top,
        "field": args.field,
        "count": len(top_rows),
        "numericCount": len(numeric_values),
        "sum": decimal_text(field_sum),
        "compareRank": args.compare_rank,
        "targetName": args.target_name,
        "fetchedCount": len(rows),
        "fetchLimit": fetch_limit,
        "target": {
            "found": target_row is not None,
            "sequence": target_row.get("sequence") if target_row else None,
            "nickName": target_row.get("nickName") if target_row else None,
            "fieldValue": decimal_text(target_value) if target_value is not None else None,
            "share": decimal_text(target_share) if target_share is not None else None,
            "rewards": rewards,
        },
        "per10kRewards": per_10k_rewards,
        "pageSize": page_size,
        "proxy": proxy_used,
        "sequenceFirst": first_seq,
        "sequenceLast": last_seq,
        "sequenceContiguous": contiguous,
        "meta": meta,
    }

    if len(top_rows) < args.top:
        log(f"警告：只抓到 {len(top_rows)} 条，少于请求的 {args.top} 条。", args.quiet)
    if len(numeric_values) < len(top_rows):
        log(
            f"警告：字段 {args.field} 有 {len(top_rows) - len(numeric_values)} 条不是数值。",
            args.quiet,
        )

    return result, rows, top_rows


def main() -> int:
    args = parse_args()
    if args.top <= 0:
        raise ScriptError("--top 必须大于 0")
    if args.compare_rank <= 0:
        raise ScriptError("--compare-rank 必须大于 0")

    har_paths = [Path(path).expanduser().resolve() for path in args.har_files]
    multiple = len(har_paths) > 1
    results: list[dict[str, Any]] = []
    processed: list[tuple[Path, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]] = []

    for index, har_path in enumerate(har_paths):
        if multiple:
            log(f"处理文件 {index + 1}/{len(har_paths)}：{har_path}", args.quiet)

        try:
            result, rows, top_rows = process_har(har_path, args)
        except ScriptError as exc:
            raise ScriptError(f"{har_path}: {exc}") from exc

        results.append(result)
        processed.append((har_path, result, rows, top_rows))

    prices: dict[str, dict[str, str]] = {}
    total_usdt: Decimal | None = None
    if not args.no_rewards:
        units = reward_units(results)
        if units and not args.no_prices:
            prices = fetch_prices(units, args)
            log(f"价格：{format_prices(prices)}", args.quiet)
        total_usdt = apply_usdt_values(results, prices)

    for index, (har_path, result, rows, top_rows) in enumerate(processed):
        result["prices"] = prices
        if total_usdt is not None:
            result["combinedUsdtTotal"] = decimal_text(total_usdt)

        if args.save_json:
            output = dict(result)
            output["rows"] = top_rows
            output["fetchedRows"] = rows
            json_path = output_path_for(args.save_json, har_path, multiple)
            write_json(json_path, output)
            log(f"已保存 JSON：{json_path}", args.quiet)

        if args.save_csv:
            csv_path = output_path_for(args.save_csv, har_path, multiple)
            write_csv(csv_path, top_rows)
            log(f"已保存 CSV：{csv_path}", args.quiet)

    if args.json:
        payload: dict[str, Any] | list[dict[str, Any]]
        payload = results if multiple else results[0]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print_results_tables(processed, args, prices, total_usdt)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ScriptError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1)
