#!/usr/bin/env python3
"""
Build daily samples between Binance activity leaderboard deltas and market volume.

Window rule:
    Leaderboard snapshot dated D has updatedTime at D 07:59:59 Beijing time.
    Its market-volume window is D-1 08:00:00 through D 07:59:59 Beijing time.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import socket
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError as exc:
    raise SystemExit("缺少依赖 requests，请先安装：python3 -m pip install requests") from exc


BJ = timezone(timedelta(hours=8))
DEFAULT_PROXY_PORTS = (7897, 7890, 7891, 10809, 1080, 8011)
FAPI_KLINES = "https://fapi.binance.com/fapi/v1/klines"


class ScriptError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="拟合日市场成交量与排行榜增量关系，窗口固定为北京时间 08:00 到次日 07:59:59。"
    )
    parser.add_argument("--name", default="bill", help="目录/榜单名，默认 bill")
    parser.add_argument("--symbol", default="BILLUSDT", help="合约交易对，默认 BILLUSDT")
    parser.add_argument("--root", default="..", help="数据根目录，默认 ..")
    parser.add_argument("--top", type=int, default=500, help="排行榜 topN 文件，默认 500")
    parser.add_argument("--proxy", default="auto", help="代理：auto/none/http://127.0.0.1:7890")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def log(message: str, quiet: bool) -> None:
    if not quiet:
        print(message, file=sys.stderr)


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


def choose_proxy(proxy_arg: str, timeout: float, quiet: bool) -> str | None:
    for proxy in proxy_candidates(proxy_arg):
        try:
            response = requests.get(
                FAPI_KLINES,
                params={"symbol": "BTCUSDT", "interval": "1h", "limit": 1},
                proxies=request_proxies(proxy),
                timeout=timeout,
            )
            if response.status_code == 200:
                log(f"使用连接方式：{'direct' if proxy is None else proxy}", quiet)
                return proxy
        except requests.RequestException as exc:
            log(f"连接测试失败：{'direct' if proxy is None else proxy} => {exc}", quiet)
    raise ScriptError("没有可用连接方式。")


def updated_time_bj(meta: dict[str, Any]) -> datetime:
    value = to_decimal(meta.get("updatedTime"))
    if value is None:
        raise ScriptError("快照缺少 meta.updatedTime。")
    return datetime.fromtimestamp(float(value / Decimal("1000")), timezone.utc).astimezone(BJ)


def load_snapshots(directory: Path, name: str, top: int) -> list[dict[str, Any]]:
    snapshots = []
    for path in sorted(directory.glob(f"????-??-??_{name}_top{top}.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        meta = data.get("meta") or {}
        updated_at = updated_time_bj(meta)
        total = to_decimal(data.get("sum"))
        eligible = to_decimal(meta.get("eligibleTradingVolume"))
        if total is None:
            continue
        snapshots.append(
            {
                "path": path,
                "date": updated_at.date().isoformat(),
                "updatedAtBj": updated_at,
                "leaderboardSum": total,
                "eligibleTradingVolume": eligible,
                "resourceId": data.get("resourceId"),
                "totalUsers": meta.get("total"),
            }
        )
    snapshots.sort(key=lambda item: item["updatedAtBj"])
    return snapshots


def market_window_for_snapshot(updated_at: datetime) -> tuple[datetime, datetime]:
    # updatedAt is normally D 07:59:59 BJ, so the market window starts at D-1 08:00:00.
    end = updated_at.replace(microsecond=0)
    start = (end + timedelta(seconds=1)) - timedelta(days=1)
    return start, end


def fetch_klines(
    symbol: str,
    start: datetime,
    end: datetime,
    proxy: str | None,
    timeout: float,
) -> list[list[Any]]:
    start_ms = int(start.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = int(end.astimezone(timezone.utc).timestamp() * 1000)
    response = requests.get(
        FAPI_KLINES,
        params={
            "symbol": symbol,
            "interval": "1h",
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
    return payload


def kline_volume(klines: list[list[Any]]) -> tuple[Decimal, Decimal]:
    base = Decimal("0")
    quote = Decimal("0")
    for kline in klines:
        base += to_decimal(kline[5]) or Decimal("0")
        quote += to_decimal(kline[7]) or Decimal("0")
    return base, quote


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "date",
        "windowStartBj",
        "windowEndBj",
        "resourceId",
        "leaderboardSum",
        "leaderboardDelta",
        "eligibleTradingVolume",
        "eligibleDelta",
        "marketBaseVolume",
        "marketQuoteVolume",
        "leaderboardDeltaPerQuote",
        "eligibleDeltaPerQuote",
        "klines",
        "totalUsers",
        "source",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_relation_chart(path: Path, rows: list[dict[str, Any]], name: str, symbol: str) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise ScriptError("缺少 matplotlib，无法出图：python3 -m pip install matplotlib") from exc

    valid = [
        row
        for row in rows
        if to_decimal(row.get("marketQuoteVolume")) is not None
        and to_decimal(row.get("leaderboardDelta")) is not None
    ]
    fig, ax = plt.subplots(figsize=(12, 7), dpi=160)
    fig.patch.set_facecolor("#f6f1e8")
    ax.set_facecolor("#fffaf0")
    if valid:
        x = [float(to_decimal(row["marketQuoteVolume"]) or 0) for row in valid]
        y = [float(to_decimal(row["leaderboardDelta"]) or 0) for row in valid]
        ax.scatter(x, y, s=90, color="#8f3f18")
        for row, xi, yi in zip(valid, x, y):
            ax.annotate(row["date"], (xi, yi), xytext=(8, 8), textcoords="offset points")
        if len(valid) >= 2:
            n = len(valid)
            sx = sum(x)
            sy = sum(y)
            sxx = sum(value * value for value in x)
            sxy = sum(xi * yi for xi, yi in zip(x, y))
            denominator = n * sxx - sx * sx
            if denominator:
                slope = (n * sxy - sx * sy) / denominator
                intercept = (sy - slope * sx) / n
                x_min, x_max = min(x), max(x)
                ax.plot(
                    [x_min, x_max],
                    [slope * x_min + intercept, slope * x_max + intercept],
                    color="#1d4ed8",
                    linewidth=2,
                    label=f"fit: y={slope:.4f}x+{intercept:.2f}",
                )
                ax.legend()
    ax.set_title(f"{name.upper()} Leaderboard Delta vs {symbol} 24h Quote Volume")
    ax.set_xlabel(f"{symbol} quote volume, BJ 08:00-07:59")
    ax.set_ylabel("Top500 leaderboard sum delta")
    ax.grid(True, color="#d9cfc0", alpha=0.7)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def inherit_owner(path: Path, owner_source: Path) -> None:
    if not hasattr(Path, "stat"):
        return
    try:
        import os

        if os.geteuid() != 0:
            return
        stat = owner_source.stat()
        os.chown(path, stat.st_uid, stat.st_gid)
    except OSError:
        return


def main() -> int:
    args = parse_args()
    root = Path(args.root).expanduser().resolve()
    directory = root / args.name
    snapshots = load_snapshots(directory, args.name, args.top)
    if len(snapshots) < 2:
        raise ScriptError(f"{directory} 下至少需要 2 个快照才能计算增量。")

    proxy = choose_proxy(args.proxy, args.timeout, args.quiet)
    rows: list[dict[str, Any]] = []
    previous = None
    for snapshot in snapshots:
        if previous is None:
            previous = snapshot
            continue
        start, end = market_window_for_snapshot(snapshot["updatedAtBj"])
        klines = fetch_klines(args.symbol, start, end, proxy, args.timeout)
        base, quote = kline_volume(klines)
        leaderboard_delta = snapshot["leaderboardSum"] - previous["leaderboardSum"]
        eligible_delta = None
        if snapshot["eligibleTradingVolume"] is not None and previous["eligibleTradingVolume"] is not None:
            eligible_delta = snapshot["eligibleTradingVolume"] - previous["eligibleTradingVolume"]
        rows.append(
            {
                "date": snapshot["date"],
                "windowStartBj": start.strftime("%Y-%m-%d %H:%M:%S"),
                "windowEndBj": end.strftime("%Y-%m-%d %H:%M:%S"),
                "resourceId": snapshot["resourceId"],
                "leaderboardSum": str(snapshot["leaderboardSum"]),
                "leaderboardDelta": str(leaderboard_delta),
                "eligibleTradingVolume": (
                    str(snapshot["eligibleTradingVolume"])
                    if snapshot["eligibleTradingVolume"] is not None
                    else ""
                ),
                "eligibleDelta": str(eligible_delta) if eligible_delta is not None else "",
                "marketBaseVolume": str(base),
                "marketQuoteVolume": str(quote),
                "leaderboardDeltaPerQuote": str(leaderboard_delta / quote) if quote else "",
                "eligibleDeltaPerQuote": (
                    str(eligible_delta / quote) if eligible_delta is not None and quote else ""
                ),
                "klines": len(klines),
                "totalUsers": snapshot["totalUsers"],
                "source": str(snapshot["path"]),
            }
        )
        previous = snapshot

    csv_path = directory / f"{args.name}_{args.symbol.lower()}_market_fit_samples.csv"
    chart_path = directory / f"{args.name}_{args.symbol.lower()}_market_fit.png"
    write_csv(csv_path, rows)
    make_relation_chart(chart_path, rows, args.name, args.symbol)
    inherit_owner(csv_path, directory)
    inherit_owner(chart_path, directory)

    print(json.dumps({"samples": rows, "csv": str(csv_path), "chart": str(chart_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ScriptError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1)
