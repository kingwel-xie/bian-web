#!/usr/bin/env python3
"""
One-command Binance competition workflow.

Input:
    python3 workflow.py URL

Flow:
    1. infer name/symbol from URL slug unless manually overridden
    2. discover resourceId from the activity page
    3. fetch top 500 leaderboard and charts
    4. if at least two daily snapshots exist, calculate market-volume ratios
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


BJ = timezone(timedelta(hours=8))
MARK_RANKS = (20, 50, 200)
KNOWN_SLUGS = {
    "futures-bill-challenge": ("bill", "BILLUSDT"),
    "futures-aigensyn-challenge": ("aig", "AIGENSYNUSDT"),
}


class ScriptError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="输入活动 URL，自动抓榜、出图、计算市场成交量比例。")
    parser.add_argument("url", help="Binance 活动页面 URL")
    parser.add_argument("--name", help="手动指定输出目录/榜单名，例如 bill 或 aig")
    parser.add_argument("--symbol", help="手动指定合约交易对，例如 BILLUSDT")
    parser.add_argument("--top", type=int, default=500, help="抓取前 N 名，默认 500")
    parser.add_argument("--page-size", type=int, default=100, help="分页大小，默认 100")
    parser.add_argument("--output-root", default="..", help="输出根目录，默认 ..")
    parser.add_argument("--proxy", default="auto", help="代理：auto/none/http://127.0.0.1:7890")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--browser-wait-ms", type=int, default=30000)
    parser.add_argument("--refresh", action="store_true", help="允许覆盖同日期快照")
    parser.add_argument(
        "--snapshot-label",
        help="另存一个带时间标签的快照，例如 1305；一般不需要",
    )
    parser.add_argument("--no-charts", action="store_true", help="不生成分布图")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def log(message: str, quiet: bool) -> None:
    if not quiet:
        print(message, file=sys.stderr)


def infer_from_url(url: str) -> tuple[str, str, str]:
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1].lower()
    if slug in KNOWN_SLUGS:
        name, symbol = KNOWN_SLUGS[slug]
        return name, symbol, slug

    token = slug
    if token.startswith("futures-"):
        token = token[len("futures-") :]
    if token.endswith("-challenge"):
        token = token[: -len("-challenge")]
    token = re.sub(r"[^a-z0-9]+", "", token)
    if not token:
        raise ScriptError("无法从 URL 推断 name/symbol，请手动传 --name 和 --symbol。")
    return token, f"{token.upper()}USDT", slug


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


def updated_time_bj(meta: dict[str, Any]) -> datetime:
    value = to_decimal(meta.get("updatedTime"))
    if value is None:
        raise ScriptError("快照缺少 meta.updatedTime。")
    return datetime.fromtimestamp(float(value / Decimal("1000")), timezone.utc).astimezone(BJ)


def run_json(command: list[str], quiet: bool) -> Any:
    completed = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    stdout = completed.stdout.decode("utf-8", errors="replace") if isinstance(completed.stdout, bytes) else completed.stdout or ""
    stderr = completed.stderr.decode("utf-8", errors="replace") if isinstance(completed.stderr, bytes) else completed.stderr or ""
    if stderr and not quiet:
        print(stderr, file=sys.stderr, end="" if stderr.endswith("\n") else "\n")
    if completed.returncode != 0:
        raise ScriptError(
            f"命令失败：{' '.join(command)}\n"
            + (stderr.strip() or stdout.strip())
        )
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ScriptError(f"命令输出不是 JSON：{stdout[:500]}") from exc


def snapshot_candidates(directory: Path, name: str, top: int) -> list[dict[str, Any]]:
    pattern = re.compile(
        rf"^\d{{4}}-\d{{2}}-\d{{2}}(?:_\d{{4}})?_{re.escape(name)}_top{top}\.json$"
    )
    by_date: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob(f"*_{name}_top{top}.json")):
        if not pattern.match(path.name):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        updated_at = updated_time_bj(data.get("meta") or {})
        date = updated_at.date().isoformat()
        item = {
            "path": path,
            "date": date,
            "updatedAtBj": updated_at,
            "mtime": path.stat().st_mtime,
            "data": data,
        }
        current = by_date.get(date)
        if current is None or item["mtime"] > current["mtime"]:
            by_date[date] = item
    return sorted(by_date.values(), key=lambda item: item["updatedAtBj"])


def rows_by_rank(snapshot: dict[str, Any]) -> dict[int, Decimal]:
    values: dict[int, Decimal] = {}
    for row in snapshot["data"].get("rows") or []:
        try:
            rank = int(row.get("sequence"))
        except (TypeError, ValueError):
            continue
        value = to_decimal(row.get("grade"))
        if value is not None:
            values[rank] = value
    return values


def read_fit_sample(csv_path: Path, date: str) -> dict[str, str]:
    with csv_path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("date") == date:
                return row
    raise ScriptError(f"没有找到 {date} 的市场成交量样本：{csv_path}")


def decimal_text(value: Decimal) -> str:
    return format(value, "f")


def format_percent(value: Decimal) -> str:
    return f"{value * Decimal('100'):.6f}%"


def write_ratio_outputs(
    directory: Path,
    name: str,
    symbol: str,
    summary: dict[str, Any],
) -> tuple[Path, Path]:
    json_path = directory / f"{name}_{symbol.lower()}_rank_ratio_summary.json"
    csv_path = directory / f"{name}_{symbol.lower()}_rank_ratio_summary.csv"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "date",
            "rank",
            "oldValue",
            "newValue",
            "rankDelta",
            "marketQuoteVolume",
            "rankDeltaPerQuote",
            "rankDeltaPerQuotePercent",
            "quotePerRankDelta",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in summary["rankRatios"]:
            writer.writerow(
                {
                    "date": summary["date"],
                    "rank": item["rank"],
                    "oldValue": item["oldValue"],
                    "newValue": item["newValue"],
                    "rankDelta": item["rankDelta"],
                    "marketQuoteVolume": summary["marketQuoteVolume"],
                    "rankDeltaPerQuote": item["rankDeltaPerQuote"],
                    "rankDeltaPerQuotePercent": item["rankDeltaPerQuotePercent"],
                    "quotePerRankDelta": item["quotePerRankDelta"],
                }
            )
    inherit_owner(json_path, directory)
    inherit_owner(csv_path, directory)
    return json_path, csv_path


def inherit_owner(path: Path, owner_source: Path) -> None:
    if not hasattr(os, "geteuid") or os.geteuid() != 0:
        return
    try:
        stat = owner_source.stat()
        os.chown(path, stat.st_uid, stat.st_gid)
    except OSError:
        return


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    root = Path(args.output_root).expanduser().resolve()
    inferred_name, inferred_symbol, slug = infer_from_url(args.url)
    name = (args.name or inferred_name).lower()
    symbol = (args.symbol or inferred_symbol).upper()
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)

    log(f"活动：{slug} => name={name}, symbol={symbol}", args.quiet)
    auto_cmd = [
        sys.executable,
        str(script_dir / "auto_leaderboard.py"),
        "--activity",
        f"{name}={args.url}",
        "--top",
        str(args.top),
        "--page-size",
        str(args.page_size),
        "--output-root",
        str(root),
        "--proxy",
        args.proxy,
        "--timeout",
        str(args.timeout),
        "--browser-wait-ms",
        str(args.browser_wait_ms),
        "--quiet",
    ]
    if args.refresh:
        auto_cmd.append("--refresh")
    if args.snapshot_label:
        auto_cmd.extend(["--snapshot-label", args.snapshot_label])
    if args.no_charts:
        auto_cmd.append("--no-charts")

    auto_result = run_json(auto_cmd, args.quiet)
    snapshots = snapshot_candidates(directory, name, args.top)
    if len(snapshots) < 2:
        output = {
            "status": "initialized",
            "message": "当前只有 1 个日快照，已完成初始化；下一次日快照后才能计算增量比例。",
            "name": name,
            "symbol": symbol,
            "url": args.url,
            "auto": auto_result,
            "snapshots": [
                {"date": item["date"], "path": str(item["path"])} for item in snapshots
            ],
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0

    fit_cmd = [
        sys.executable,
        str(script_dir / "fit_market_volume.py"),
        "--name",
        name,
        "--symbol",
        symbol,
        "--root",
        str(root),
        "--top",
        str(args.top),
        "--proxy",
        args.proxy,
        "--timeout",
        str(args.timeout),
        "--quiet",
    ]
    fit_result = run_json(fit_cmd, args.quiet)

    previous, current = snapshots[-2], snapshots[-1]
    previous_values = rows_by_rank(previous)
    current_values = rows_by_rank(current)
    sample_csv = Path(fit_result["csv"])
    sample = read_fit_sample(sample_csv, current["date"])
    quote = to_decimal(sample.get("marketQuoteVolume"))
    if quote is None or quote <= 0:
        raise ScriptError("市场成交额 marketQuoteVolume 无效。")

    ratios = []
    for rank in MARK_RANKS:
        old_value = previous_values.get(rank)
        new_value = current_values.get(rank)
        if old_value is None or new_value is None:
            continue
        delta = new_value - old_value
        ratio = delta / quote
        quote_per_delta = quote / delta if delta else None
        ratios.append(
            {
                "rank": rank,
                "oldValue": decimal_text(old_value),
                "newValue": decimal_text(new_value),
                "rankDelta": decimal_text(delta),
                "rankDeltaPerQuote": decimal_text(ratio),
                "rankDeltaPerQuotePercent": format_percent(ratio),
                "quotePerRankDelta": decimal_text(quote_per_delta) if quote_per_delta else None,
            }
        )

    summary = {
        "status": "updated",
        "name": name,
        "symbol": symbol,
        "url": args.url,
        "date": current["date"],
        "previousDate": previous["date"],
        "windowStartBj": sample["windowStartBj"],
        "windowEndBj": sample["windowEndBj"],
        "marketQuoteVolume": sample["marketQuoteVolume"],
        "rankRatios": ratios,
        "auto": auto_result,
        "fit": fit_result,
    }
    json_path, csv_path = write_ratio_outputs(directory, name, symbol, summary)
    summary["ratioSummaryJson"] = str(json_path)
    summary["ratioSummaryCsv"] = str(csv_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ScriptError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1)
