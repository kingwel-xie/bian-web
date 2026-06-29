#!/usr/bin/env python3
"""
Discover Binance activity leaderboard resource IDs from the activity page, then
fetch top leaderboard rows and save dated CSV/JSON/charts.

Example:
    python3 auto_leaderboard.py
    python3 auto_leaderboard.py --activity bill=https://www.binance.com/zh-CN/activity/trading-competition/futures-bill-challenge
    python3 auto_leaderboard.py --activity aig=https://www.binance.com/zh-CN/activity/trading-competition/...
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError as exc:  # pragma: no cover - environment guard
    raise SystemExit("缺少依赖 requests，请先安装：python3 -m pip install requests") from exc

if sys.platform == "win32":
    for stream in (sys.stdout, sys.stderr):
        if isinstance(stream, io.TextIOWrapper):
            stream.reconfigure(encoding="utf-8", errors="replace")


SUMMARY_LIST_ENDPOINT = (
    "https://www.binance.com/bapi/growth/v1/friendly/"
    "growth-paas/resource/summary/list"
)
DEFAULT_BILL_URL = (
    "https://www.binance.com/zh-CN/activity/trading-competition/"
    "futures-bill-challenge"
)
DEFAULT_PAGE_SIZE = 100
DEFAULT_TOP = 500
DEFAULT_PROXY_PORTS = (7897, 7890, 7891, 10809, 1080, 8011)
DEFAULT_MARK_RANKS = (20, 50, 200)
DELTA_RANGES = ((10, 25), (26, 50), (180, 200))



class ScriptError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="自动打开 Binance 活动页，发现 resourceId，抓取排行榜并出图。"
    )
    parser.add_argument(
        "--activity",
        action="append",
        default=[],
        metavar="NAME=URL",
        help=(
            "活动配置，可重复传入。例如 "
            "--activity bill=https://www.binance.com/... "
            "--activity aig=https://www.binance.com/..."
        ),
    )
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help="抓取前 N 名，默认 500")
    parser.add_argument(
        "--page-size", type=int, default=DEFAULT_PAGE_SIZE, help="分页大小，默认 100"
    )
    parser.add_argument(
        "--output-root",
        default="..",
        help="输出根目录，默认 ..，会写入 ../bill 或 ../aig 这种单层目录",
    )
    parser.add_argument(
        "--date",
        help="手动指定文件时间戳前缀；默认使用接口 updatedTime 对应的北京时间时间戳",
    )
    parser.add_argument(
        "--snapshot-label",
        help="可选快照时间标签，例如 1305；不传则文件名只用数据日期",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="允许覆盖同名日期快照；默认同一天数据已存在时不覆盖",
    )
    parser.add_argument(
        "--proxy",
        default="auto",
        help="代理：auto 自动选择；none 直连；或 http://127.0.0.1:7890",
    )
    parser.add_argument("--timeout", type=float, default=30, help="接口请求超时秒数")
    parser.add_argument(
        "--browser-wait-ms",
        type=int,
        default=30000,
        help="页面打开后等待网络请求的毫秒数，默认 30000",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="跳过浏览器发现，只使用 --resource-id；用于临时兜底",
    )
    parser.add_argument(
        "--resource-id",
        action="append",
        default=[],
        metavar="NAME=ID",
        help="手动指定资源 ID 兜底，例如 --resource-id bill=54211",
    )
    parser.add_argument("--no-charts", action="store_true", help="只导 CSV/JSON，不出图")
    parser.add_argument("--discover-only", action="store_true", help="只发现 resourceId，不抓取排行榜，输出 JSON 到 stdout")
    parser.add_argument("--quiet", action="store_true", help="减少进度输出")
    parser.add_argument(
        "--last-updated",
        help="上次抓取的 updatedTime 时间戳 (YYYY-MM-DDTHHmmss)，匹配则跳过",
    )
    return parser.parse_args()


def log(message: str, quiet: bool = False) -> None:
    if not quiet:
        print(message, file=sys.stderr, flush=True)


def now_bj() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%dT%H%M%S")


def current_time_label_bj() -> str:
    return datetime.now(timezone(timedelta(hours=8))).strftime("%H%M")


def timestamp_from_updated_time(ms: Any) -> str | None:
    value = to_decimal(ms)
    if value is None:
        return None
    return datetime.fromtimestamp(
        float(value / Decimal("1000")),
        tz=timezone.utc,
    ).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%dT%H%M%S")


def parse_name_value(values: list[str], label: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ScriptError(f"{label} 必须是 NAME=VALUE 格式：{value}")
        name, raw = value.split("=", 1)
        name = name.strip().lower()
        raw = raw.strip()
        if not name or not raw:
            raise ScriptError(f"{label} 必须是 NAME=VALUE 格式：{value}")
        parsed[name] = raw
    return parsed


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


def choose_working_proxy(timeout: float, proxy_arg: str, quiet: bool) -> str | None:
    url = "https://www.binance.com/bapi/accounts/v1/public/authcenter/auth"
    headers = default_headers("https://www.binance.com/")
    for proxy in proxy_candidates(proxy_arg):
        try:
            response = requests.post(
                url,
                headers=headers,
                json={},
                proxies=request_proxies(proxy),
                timeout=timeout,
            )
            if response.status_code < 500:
                log(f"使用连接方式：{proxy_label(proxy)}", quiet)
                return proxy
        except requests.RequestException as exc:
            log(f"连接测试失败：{proxy_label(proxy)} => {exc}", quiet)
    raise ScriptError("没有可用连接方式，请检查代理或网络。")


def default_headers(referer: str) -> dict[str, str]:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "clienttype": "web",
        "content-type": "application/json",
        "lang": "zh-CN",
        "origin": "https://www.binance.com",
        "referer": referer,
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
    }


DISCOVERY_JS = r"""
const { chromium } = require('playwright');

const activities = JSON.parse(process.env.ACTIVITIES_JSON || '[]');
const proxy = process.env.PLAYWRIGHT_PROXY || '';
const waitMs = Number(process.env.BROWSER_WAIT_MS || '30000');

function parseJson(text) {
  if (!text) return null;
  try { return JSON.parse(text); } catch { return null; }
}

function addCandidate(result, id, source) {
  const value = Number(id);
  if (!Number.isFinite(value) || value <= 0) return;
  if (!result.candidates.some((item) => item.resourceId === value)) {
    result.candidates.push({ resourceId: value, source });
  }
}

(async () => {
  const launchOptions = { headless: true };
  if (proxy) launchOptions.proxy = { server: proxy };
  const browser = await chromium.launch(launchOptions);
  const context = await browser.newContext({
    locale: 'zh-CN',
    timezoneId: 'Asia/Shanghai',
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    viewport: { width: 1440, height: 1100 },
  });

  const results = [];
  for (const activity of activities) {
    const result = { name: activity.name, url: activity.url, title: null, candidates: [], events: [], errors: [] };
    const page = await context.newPage();

    page.on('response', async (response) => {
      const url = response.url();
      if (!url.includes('/growth-paas/')) return;
      const request = response.request();
      const reqBody = parseJson(request.postData() || '');
      const event = { url, method: request.method(), status: response.status(), request: reqBody };

      if (url.includes('/resource/summary/participant/list')) {
        try {
          const payload = parseJson(await response.text());
          event.response = payload;
          if (payload && Array.isArray(payload.data)) {
            for (const item of payload.data) addCandidate(result, item.resourceId, 'participant/list response');
          }
          if (reqBody && Array.isArray(reqBody.resourceIdList)) {
            for (const id of reqBody.resourceIdList) addCandidate(result, id, 'participant/list request');
          }
        } catch (error) {
          result.errors.push(String(error));
        }
      } else if (url.includes('/resource/summary/list')) {
        if (reqBody && reqBody.resourceId) addCandidate(result, reqBody.resourceId, 'summary/list request');
        try {
          const payload = parseJson(await response.text());
          event.response = payload;
        } catch {}
      } else if (url.includes('/user/user-group-eligibility')) {
        if (reqBody && reqBody.resourceId) addCandidate(result, reqBody.resourceId, 'user-group-eligibility request');
      }

      result.events.push(event);
    });

    try {
      await page.goto(activity.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
      await page.waitForTimeout(waitMs);
      await page.evaluate(() => window.scrollTo(0, Math.floor(document.body.scrollHeight * 0.55))).catch(() => {});
      await page.waitForTimeout(Math.max(2000, Math.floor(waitMs / 3)));
      result.title = await page.title().catch(() => null);
    } catch (error) {
      result.errors.push(String(error));
    } finally {
      await page.close().catch(() => {});
    }
    results.push(result);
  }

  await browser.close();
  console.log(JSON.stringify(results));
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
"""


def has_candidates(results: dict[str, dict[str, Any]]) -> bool:
    return any(
        item.get("candidates") for item in results.values() if isinstance(item, dict)
    )


def discover_with_playwright(
    activities: dict[str, str],
    proxy: str | None,
    wait_ms: int,
    quiet: bool,
) -> dict[str, dict[str, Any]]:
    """Discover resource IDs: Node Playwright → playwright-cli."""
    if not activities:
        return {}
    mode = os.environ.get("LEADERBOARD_DISCOVERY", "auto").strip().lower()
    if mode in {"auto", "node", "playwright"} and node_playwright_available():
        try:
            node_result = discover_with_node_playwright(activities, proxy, wait_ms, quiet)
            if has_candidates(node_result):
                return node_result
            log("Node Playwright 未发现候选，回退 playwright-cli", quiet)
        except ScriptError as exc:
            if mode in {"node", "playwright"}:
                raise
            log(f"Node Playwright 发现失败，回退 playwright-cli：{exc}", quiet)
    pwcli_result = discover_with_pwcli(activities, proxy, wait_ms, quiet)
    if has_candidates(pwcli_result):
        return pwcli_result
    raise ScriptError("Node Playwright 和 playwright-cli 均未能发现 resourceId，请手动指定 --resource-id 或提供 HAR 文件")


def discover_with_node_playwright(
    activities: dict[str, str],
    proxy: str | None,
    wait_ms: int,
    quiet: bool,
) -> dict[str, dict[str, Any]]:
    script_dir = Path(__file__).resolve().parent
    with tempfile.NamedTemporaryFile(
        "w", suffix=".cjs", dir=script_dir, delete=False, encoding="utf-8"
    ) as handle:
        handle.write(DISCOVERY_JS)
        script_path = Path(handle.name)

    env = os.environ.copy()
    env["ACTIVITIES_JSON"] = json.dumps(
        [{"name": name, "url": url} for name, url in activities.items()],
        ensure_ascii=False,
    )
    env["BROWSER_WAIT_MS"] = str(wait_ms)
    if proxy:
        env["PLAYWRIGHT_PROXY"] = proxy

    try:
        log("使用 Node Playwright 打开活动页面并监听 resourceId...", quiet)
        completed = subprocess.run(
            ["node", str(script_path)],
            cwd=script_dir,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(90, math.ceil(wait_ms / 1000) * len(activities) + 90),
            check=False,
        )
    finally:
        try:
            script_path.unlink()
        except OSError:
            pass

    stdout = completed.stdout.decode("utf-8", errors="replace") if isinstance(completed.stdout, bytes) else completed.stdout or ""
    stderr = completed.stderr.decode("utf-8", errors="replace") if isinstance(completed.stderr, bytes) else completed.stderr or ""

    if completed.returncode != 0:
        raise ScriptError(stderr.strip() or stdout.strip())
    try:
        items = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ScriptError(f"Node Playwright 输出不是 JSON：{stdout[:500]}") from exc

    results: dict[str, dict[str, Any]] = {}
    for item in items:
        name = str(item.get("name") or "").lower()
        results[name] = item
        candidates = [candidate.get("resourceId") for candidate in item.get("candidates", [])]
        log(f"{name}: 页面标题={item.get('title')!r}, 候选 resourceId={candidates}", quiet)
    return results


def node_playwright_available() -> bool:
    if not shutil_which("node"):
        return False
    script_dir = Path(__file__).resolve().parent
    completed = subprocess.run(
        ["node", "-e", "require.resolve('playwright')"],
        cwd=script_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        check=False,
    )
    return completed.returncode == 0


def discover_with_pwcli(
    activities: dict[str, str],
    proxy: str | None,
    wait_ms: int,
    quiet: bool,
) -> dict[str, dict[str, Any]]:
    """Use the verified local playwright-cli wrapper to capture page requests."""
    pwcli = find_pwcli()
    results: dict[str, dict[str, Any]] = {}
    for name, url in activities.items():
        name = name.lower()
        result = {"name": name, "url": url, "title": None, "candidates": [], "events": []}
        log(f"{name}: 打开活动页面并监听网络请求", quiet)
        run_pwcli(pwcli, ["close-all"], proxy, timeout=20, check=False)
        run_pwcli(pwcli, ["open", url], proxy, timeout=90, check=True)
        time.sleep(max(2, wait_ms / 1000))

        snapshot = run_pwcli(pwcli, ["snapshot"], proxy, timeout=30, check=False)
        title_match = re.search(r"Page Title:\s*(.+)", snapshot.stdout)
        if title_match:
            result["title"] = title_match.group(1).strip()

        requests_output = run_pwcli(pwcli, ["requests"], proxy, timeout=30, check=True).stdout
        request_indexes = parse_growth_request_indexes(requests_output)
        for index, request_url in request_indexes:
            request_body = cli_json(
                run_pwcli(pwcli, ["request-body", str(index)], proxy, timeout=30, check=False).stdout
            )
            response_body = cli_json(
                run_pwcli(pwcli, ["response-body", str(index)], proxy, timeout=30, check=False).stdout
            )
            event = {
                "index": index,
                "url": request_url,
                "request": request_body,
                "response": response_body,
            }
            result["events"].append(event)
            collect_candidates(result, request_url, request_body, response_body)

        run_pwcli(pwcli, ["close"], proxy, timeout=20, check=False)
        candidates = [candidate.get("resourceId") for candidate in result["candidates"]]
        log(f"{name}: 页面标题={result.get('title')!r}, 候选 resourceId={candidates}", quiet)
        results[name] = result
    return results


def find_pwcli() -> str:
    env_path = os.environ.get("PWCLI")
    candidates = [
        env_path,
        str(Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills/playwright/scripts/playwright_cli.sh"),
        "/root/.codex/skills/playwright/scripts/playwright_cli.sh",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists() and os.access(candidate, os.X_OK):
            return candidate
    raise ScriptError("找不到 playwright-cli 包装器，无法自动打开页面。")


def run_pwcli(
    pwcli: str,
    args: list[str],
    proxy: str | None,
    timeout: float,
    check: bool,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if proxy:
        env["HTTP_PROXY"] = proxy
        env["HTTPS_PROXY"] = proxy
        env["http_proxy"] = proxy
        env["https_proxy"] = proxy
    try:
        completed = subprocess.run(
            [pwcli, *args],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        if check:
            raise ScriptError(f"playwright-cli {' '.join(args)} 超时：{timeout}s") from exc
        return subprocess.CompletedProcess([pwcli, *args], returncode=124, stdout="", stderr=str(exc))
    stdout = completed.stdout.decode("utf-8", errors="replace") if isinstance(completed.stdout, bytes) else completed.stdout or ""
    stderr = completed.stderr.decode("utf-8", errors="replace") if isinstance(completed.stderr, bytes) else completed.stderr or ""
    result = subprocess.CompletedProcess(completed.args, completed.returncode, stdout, stderr)
    if check and result.returncode != 0:
        raise ScriptError(
            f"playwright-cli {' '.join(args)} 失败：\n"
            + (result.stderr.strip() or result.stdout.strip())
        )
    return result


def parse_growth_request_indexes(output: str) -> list[tuple[int, str]]:
    indexes: list[tuple[int, str]] = []
    pattern = re.compile(r"^\s*(\d+)\.\s+\[[A-Z]+\]\s+(\S*?/growth-paas/\S*)\s+=>", re.MULTILINE)
    for match in pattern.finditer(output):
        indexes.append((int(match.group(1)), match.group(2)))
    return indexes


def cli_json(output: str) -> Any | None:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    for line in reversed(lines):
        if not line.startswith(("{", "[")):
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def add_candidate(result: dict[str, Any], resource_id: Any, source: str) -> None:
    try:
        value = int(resource_id)
    except (TypeError, ValueError):
        return
    if value <= 0:
        return
    candidates = result.setdefault("candidates", [])
    if not any(candidate.get("resourceId") == value for candidate in candidates):
        candidates.append({"resourceId": value, "source": source})


def collect_candidates(
    result: dict[str, Any],
    request_url: str,
    request_body: Any,
    response_body: Any,
) -> None:
    if "resource/summary/participant/list" in request_url:
        if isinstance(request_body, dict) and isinstance(request_body.get("resourceIdList"), list):
            for resource_id in request_body["resourceIdList"]:
                add_candidate(result, resource_id, "participant/list request")
        if isinstance(response_body, dict) and isinstance(response_body.get("data"), list):
            for item in response_body["data"]:
                if isinstance(item, dict):
                    add_candidate(result, item.get("resourceId"), "participant/list response")
    elif "resource/summary/list" in request_url:
        if isinstance(request_body, dict):
            add_candidate(result, request_body.get("resourceId"), "summary/list request")
    elif "user/user-group-eligibility" in request_url:
        if isinstance(request_body, dict):
            add_candidate(result, request_body.get("resourceId"), "user-group-eligibility request")


def shutil_which(command: str) -> str | None:
    if os.name == "nt":
        extensions = ("", ".exe", ".cmd", ".bat")
    else:
        extensions = ("",)
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        for ext in extensions:
            path = Path(directory) / (command + ext)
            if path.exists() and os.access(path, os.X_OK):
                return str(path)
    return None



def api_success(payload: Any) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("success") is True
        and payload.get("code") == "000000"
        and isinstance(payload.get("data"), dict)
    )


def rows_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    summary = data.get("resourceSummaryList") if isinstance(data, dict) else {}
    rows = summary.get("data") if isinstance(summary, dict) else []
    return rows if isinstance(rows, list) else []


def meta_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload, dict) else {}
    summary = data.get("resourceSummaryList") if isinstance(data, dict) else {}
    return {
        "eligibleUserCount": data.get("eligibleUserCount") if isinstance(data, dict) else None,
        "eligibleTradingVolume": data.get("eligibleTradingVolume") if isinstance(data, dict) else None,
        "updatedTime": data.get("updatedTime") if isinstance(data, dict) else None,
        "total": summary.get("total") if isinstance(summary, dict) else None,
    }


def fetch_page(
    session: requests.Session,
    resource_id: int,
    page_index: int,
    page_size: int,
    referer: str,
    proxy: str | None,
    timeout: float,
) -> dict[str, Any]:
    payload = {
        "resourceId": resource_id,
        "leaderboardType": "USER",
        "pageIndex": page_index,
        "pageSize": page_size,
    }
    response = session.post(
        SUMMARY_LIST_ENDPOINT,
        headers=default_headers(referer),
        json=payload,
        proxies=request_proxies(proxy),
        timeout=timeout,
    )
    try:
        data = response.json()
    except ValueError as exc:
        raise ScriptError(
            f"排行榜接口返回非 JSON：HTTP {response.status_code}, {response.text[:200]}"
        ) from exc
    if response.status_code != 200 or not api_success(data):
        code = data.get("code") if isinstance(data, dict) else None
        message = data.get("message") if isinstance(data, dict) else None
        raise ScriptError(
            f"排行榜接口失败：resourceId={resource_id}, HTTP {response.status_code}, "
            f"code={code}, message={message}"
        )
    return data


def choose_leaderboard_resource_id(
    candidates: list[int],
    referer: str,
    proxy: str | None,
    timeout: float,
    quiet: bool,
) -> tuple[int, dict[str, Any], list[dict[str, Any]]]:
    if not candidates:
        raise ScriptError("没有发现候选 resourceId。")
    session = requests.Session()
    tested: list[dict[str, Any]] = []
    best: tuple[int, dict[str, Any], list[dict[str, Any]]] | None = None
    for resource_id in candidates:
        payload = fetch_page(
            session,
            resource_id,
            page_index=1,
            page_size=min(10, DEFAULT_PAGE_SIZE),
            referer=referer,
            proxy=proxy,
            timeout=timeout,
        )
        rows = rows_from_payload(payload)
        meta = meta_from_payload(payload)
        tested.append({"resourceId": resource_id, "rowCount": len(rows), "meta": meta})
        log(f"测试 resourceId={resource_id}: 首页 {len(rows)} 条", quiet)
        if rows and (best is None or len(rows) > len(best[2])):
            best = (resource_id, payload, rows)

    if best is None:
        raise ScriptError(f"候选 resourceId 都没有排行榜数据：{tested}")
    return best


def fetch_top_rows(
    resource_id: int,
    referer: str,
    top: int,
    page_size: int,
    proxy: str | None,
    timeout: float,
    quiet: bool,
    skip_if_updated: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str | None]:
    session = requests.Session()
    rows: list[dict[str, Any]] = []
    pages = math.ceil(top / page_size)
    meta: dict[str, Any] = {}
    for page_index in range(1, pages + 1):
        payload = fetch_page(
            session,
            resource_id,
            page_index=page_index,
            page_size=page_size,
            referer=referer,
            proxy=proxy,
            timeout=timeout,
        )
        if page_index == 1:
            meta = meta_from_payload(payload)
            if skip_if_updated:
                actual_ts = timestamp_from_updated_time(meta.get("updatedTime"))
                if actual_ts and actual_ts == skip_if_updated:
                    log(f"updatedTime 无变化，跳过抓取", quiet)
                    return [], meta, "no_update"
        page_rows = rows_from_payload(payload)
        rows.extend(page_rows)
        first_seq = page_rows[0].get("sequence") if page_rows else None
        last_seq = page_rows[-1].get("sequence") if page_rows else None
        log(
            f"resourceId={resource_id} 第 {page_index}/{pages} 页："
            f"{len(page_rows)} 条，sequence {first_seq}-{last_seq}",
            quiet,
        )
        if len(page_rows) < page_size:
            break
        time.sleep(0.1)
    return rows[:top], meta, None


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


def restored_trading_volume(row: dict[str, Any]) -> Decimal | None:
    volume = to_decimal(row.get("tradingVolume"))
    if volume is not None:
        return volume
    grade = to_decimal(row.get("grade"))
    if grade is None:
        return None
    return grade * grade


def enrich_restored_trading_volume(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for row in rows:
        current = dict(row)
        volume = restored_trading_volume(current)
        if volume is not None:
            current["restoredTradingVolume"] = float(volume)
        enriched.append(current)
    return enriched


def row_rank(row: dict[str, Any]) -> int | None:
    try:
        return int(row.get("sequence"))
    except (TypeError, ValueError):
        return None


def row_nickname(row: dict[str, Any]) -> str:
    return str(row.get("nickName") or row.get("nickname") or "").strip()


def find_previous_snapshot(out_dir: Path, name: str, current_json_path: Path) -> Path | None:
    candidates = []
    for path in sorted(out_dir.glob(f"*_{name}_top*.json")):
        if path.resolve() == current_json_path.resolve():
            continue
        data = read_json(path, {})
        if not isinstance(data, dict) or not isinstance(data.get("rows"), list):
            continue
        candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.name, path.stat().st_mtime))


def load_snapshot_rows(path: Path | None) -> list[dict[str, Any]] | None:
    if path is None:
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    rows = data.get("rows") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return None
    return enrich_restored_trading_volume(rows)


def previous_by_nickname(rows: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    if rows is None:
        return {}
    mapped: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: row_rank(item) or 10**9):
        nickname = row_nickname(row)
        if nickname and nickname not in mapped:
            mapped[nickname] = row
    return mapped


def build_delta_rows(
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]] | None,
    start_rank: int,
    end_rank: int,
) -> list[dict[str, Any]]:
    previous = previous_by_nickname(previous_rows)
    has_previous = previous_rows is not None
    delta_rows = []
    for row in sorted(current_rows, key=lambda item: row_rank(item) or 10**9):
        rank = row_rank(row)
        if rank is None or rank < start_rank or rank > end_rank:
            continue
        current_volume = restored_trading_volume(row) or Decimal("0")
        nickname = row_nickname(row)
        previous_row = previous.get(nickname)
        previous_volume = (
            restored_trading_volume(previous_row)
            if previous_row is not None
            else Decimal("0")
        )
        if previous_volume is None:
            previous_volume = Decimal("0")
        delta = current_volume - previous_volume
        delta_rows.append(
            {
                "rank": rank,
                "nickName": nickname,
                "userId": row.get("userId"),
                "grade": row.get("grade"),
                "restoredTradingVolume": float(current_volume),
                "previousRank": row_rank(previous_row) if previous_row is not None else None,
                "previousRestoredTradingVolume": float(previous_volume) if has_previous else 0,
                "deltaRestoredTradingVolume": float(delta),
                "matchedBy": "nickName" if previous_row is not None else None,
            }
        )
    return delta_rows


def write_delta_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "rank",
        "nickName",
        "userId",
        "grade",
        "restoredTradingVolume",
        "previousRank",
        "previousRestoredTradingVolume",
        "deltaRestoredTradingVolume",
        "matchedBy",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    preferred = [
        "sequence",
        "userId",
        "nickName",
        "grade",
        "restoredTradingVolume",
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


def inherit_owner(path: Path, owner_source: Path) -> None:
    """When run as root, keep generated files writable by the directory owner."""
    if not hasattr(os, "chown") or os.geteuid() != 0:
        return
    try:
        stat = owner_source.stat()
        os.chown(path, stat.st_uid, stat.st_gid)
    except OSError:
        return


def fmt_volume(value: Decimal) -> str:
    numeric = float(value)
    if numeric >= 100_000_000:
        return f"{numeric / 100_000_000:.2f}B"
    return f"{numeric / 10000:.2f}w"


def make_distribution_chart(
    rows: list[dict[str, Any]],
    name: str,
    output_path: Path,
    limit: int,
    log_scale: bool,
    mark_ranks: tuple[int, ...] = DEFAULT_MARK_RANKS,
) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter, LogLocator
    except ImportError as exc:
        raise ScriptError("缺少 matplotlib，无法出图：python3 -m pip install matplotlib") from exc
    plt.rcParams["font.sans-serif"] = ["Noto Sans CJK SC", "Noto Sans CJK JP", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    selected = []
    for row in rows:
        rank = int(row.get("sequence") or 0)
        volume = to_decimal(row.get("grade"))
        if 1 <= rank <= limit and volume is not None:
            selected.append((rank, volume))
    selected.sort(key=lambda item: item[0])
    if len(selected) < limit:
        raise ScriptError(f"{name} 只有 {len(selected)} 条可画图数据，少于 {limit}。")

    by_rank = {rank: volume for rank, volume in selected}
    ranks = [rank for rank, _ in selected]
    volumes = [float(volume) for _, volume in selected]
    total = sum((volume for _, volume in selected), Decimal("0"))

    def fmt_axis(value: float, _pos: object = None) -> str:
        if value >= 100_000_000:
            return f"{value / 100_000_000:.1f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.0f}M"
        if value >= 10_000:
            return f"{value / 10000:.0f}w"
        return f"{value:.0f}"

    fig, ax = plt.subplots(figsize=(15, 8), dpi=160)
    fig.patch.set_facecolor("#f6f1e8")
    ax.set_facecolor("#fffaf0")
    ax.bar(ranks, volumes, width=1.0, color="#d89a55", alpha=0.34, edgecolor="none")
    ax.plot(ranks, volumes, color="#8f3f18", linewidth=2.5)
    if log_scale:
        ax.set_yscale("log")
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=8))

    colors = {20: "#0f766e", 50: "#b45309", 200: "#1d4ed8"}
    for rank in mark_ranks:
        if rank not in by_rank:
            continue
        volume = by_rank[rank]
        y = float(volume)
        color = colors.get(rank, "#1d4ed8")
        ax.axvline(rank, color=color, linewidth=1.4, alpha=0.62, linestyle="--")
        ax.scatter([rank], [y], color=color, s=70, zorder=6)
        ax.annotate(
            f"#{rank}\n{fmt_volume(volume)}",
            xy=(rank, y),
            xytext=(14, 18),
            textcoords="offset points",
            fontsize=10,
            weight="bold",
            color="#111827",
            bbox=dict(boxstyle="round,pad=0.35", fc="#fffaf0", ec=color, alpha=0.96),
            arrowprops=dict(arrowstyle="->", color=color, lw=1.1),
        )

    suffix = " - Log Scale" if log_scale else ""
    ax.set_title(
        f"{name.upper()} Top {limit} Trading Volume Distribution{suffix}",
        fontsize=20,
        weight="bold",
        color="#1f2933",
        pad=18,
    )
    mark_text = "   ".join(
        f"#{rank}: {fmt_volume(by_rank[rank])}" for rank in mark_ranks if rank in by_rank
    )
    ax.text(
        0.01,
        0.98,
        f"Total top{limit}: {float(total) / 100000000:.2f}B   #1: {fmt_volume(by_rank[1])}   {mark_text}",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=11,
        color="#52616b",
    )
    ax.set_xlabel("Rank", fontsize=12, color="#334e68")
    ax.set_ylabel(
        "Trading volume (grade, log scale)" if log_scale else "Trading volume (grade)",
        fontsize=12,
        color="#334e68",
    )
    ax.set_xlim(1, limit)
    ticks = [1, 20, 50, 75, 100, 125, 150, 175, 200]
    if limit > 200:
        ticks.extend([250, 300, 350, 400, 450, 500])
    ax.set_xticks([tick for tick in ticks if tick <= limit])
    ax.yaxis.set_major_formatter(FuncFormatter(fmt_axis))
    ax.grid(True, axis="y", which="major", color="#d9cfc0", alpha=0.8, linewidth=0.8)
    ax.grid(True, axis="y", which="minor", color="#eadfce", alpha=0.35, linewidth=0.4)
    ax.grid(True, axis="x", color="#eadfce", alpha=0.35, linewidth=0.6)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#b7aa98")
    ax.spines["bottom"].set_color("#b7aa98")
    ax.tick_params(colors="#334e68")
    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def make_combined_delta_chart(
    range_rows: list[tuple[int, int, list[dict[str, Any]]]],
    name: str,
    output_path: Path,
    has_previous: bool,
) -> None:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except ImportError as exc:
        raise ScriptError("缺少 matplotlib，无法出图：python3 -m pip install matplotlib") from exc
    plt.rcParams["font.sans-serif"] = ["Noto Sans CJK SC", "Noto Sans CJK JP", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    def fmt_axis(value: float, _pos: object = None) -> str:
        abs_value = abs(value)
        sign = "-" if value < 0 else ""
        if abs_value >= 10_000:
            return f"{sign}{abs_value / 10000:.0f}W"
        return f"{value:.0f}"

    fig, axes = plt.subplots(len(range_rows), 1, figsize=(16, 18), dpi=160)
    if len(range_rows) == 1:
        axes = [axes]
    fig.patch.set_facecolor("#f6f1e8")
    note = "first snapshot, delta = current volume" if not has_previous else "matched by nickName"
    fig.suptitle(f"{name.upper()} Daily Delta by Nickname ({note})", fontsize=22, weight="bold", color="#1f2933")

    for ax, (start_rank, end_rank, rows) in zip(axes, range_rows):
        ranks = [int(row["rank"]) for row in rows]
        deltas = [float(row.get("deltaRestoredTradingVolume") or 0) for row in rows]
        labels = [str(row.get("nickName") or "")[:12] for row in rows]
        colors = ["#0e747a" if value >= 0 else "#c45145" for value in deltas]
        ax.set_facecolor("#fffaf0")
        ax.bar(ranks, deltas, width=0.72, color=colors, alpha=0.82)
        ax.axhline(0, color="#5f554b", linewidth=1.1)
        ax.set_title(f"Rank {start_rank}-{end_rank}   Total: {fmt_axis(sum(deltas))}", fontsize=15, weight="bold", color="#1f2933")
        ax.set_ylabel("Delta", fontsize=11, color="#334e68")
        ax.set_xticks(ranks)
        ax.set_xticklabels([f"#{rank}\n{label}" for rank, label in zip(ranks, labels)], rotation=45, ha="right", fontsize=8)
        ax.yaxis.set_major_formatter(FuncFormatter(fmt_axis))
        ax.grid(True, axis="y", color="#d9cfc0", alpha=0.8, linewidth=0.8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#b7aa98")
        ax.spines["bottom"].set_color("#b7aa98")
        ax.tick_params(colors="#334e68")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def build_delta_outputs(
    name: str,
    out_dir: Path,
    file_prefix: str,
    json_path: Path,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    previous_path = find_previous_snapshot(out_dir, name, json_path)
    previous_rows = load_snapshot_rows(previous_path)
    has_previous = previous_rows is not None
    ranges = []
    combined_rows = []
    for start_rank, end_rank in DELTA_RANGES:
        delta_rows = build_delta_rows(rows, previous_rows, start_rank, end_rank)
        combined_rows.append((start_rank, end_rank, delta_rows))
        range_key = f"{start_rank}_{end_rank}"
        csv_path = out_dir / f"{file_prefix}_rank{range_key}_delta_by_nickname.csv"
        write_delta_csv(csv_path, delta_rows)
        inherit_owner(csv_path, out_dir)
        ranges.append(
            {
                "range": f"{start_rank}-{end_rank}",
                "rows": len(delta_rows),
                "sumDeltaRestoredTradingVolume": decimal_text(
                    sum(
                        (to_decimal(row.get("deltaRestoredTradingVolume")) or Decimal("0"))
                        for row in delta_rows
                    )
                ),
                "csv": str(csv_path),
            }
        )
    combined_chart_path = out_dir / f"{file_prefix}_delta_by_nickname_combined.png"
    make_combined_delta_chart(combined_rows, name, combined_chart_path, has_previous)
    inherit_owner(combined_chart_path, out_dir)
    delta_json_path = out_dir / f"{file_prefix}_delta_by_nickname.json"
    payload = {
        "name": name,
        "matchBy": "nickName",
        "previousSnapshot": str(previous_path) if previous_path else None,
        "firstSnapshot": not has_previous,
        "combinedChart": str(combined_chart_path),
        "ranges": ranges,
    }
    write_json(delta_json_path, payload)
    inherit_owner(delta_json_path, out_dir)
    return {
        "json": str(delta_json_path),
        "previousSnapshot": str(previous_path) if previous_path else None,
        "firstSnapshot": not has_previous,
        "combinedChart": str(combined_chart_path),
        "ranges": ranges,
        "charts": [str(combined_chart_path)],
    }


def normalize_resource_ids(discovery: dict[str, Any], manual_id: str | None) -> list[int]:
    ids: list[int] = []
    if manual_id is not None:
        ids.append(int(manual_id))
    for candidate in discovery.get("candidates") or []:
        try:
            resource_id = int(candidate.get("resourceId"))
        except (TypeError, ValueError):
            continue
        if resource_id not in ids:
            ids.append(resource_id)
    return ids


def main() -> int:
    args = parse_args()
    if args.top <= 0:
        raise ScriptError("--top 必须大于 0")
    if args.page_size <= 0:
        raise ScriptError("--page-size 必须大于 0")

    activities = parse_name_value(args.activity, "--activity")
    if not activities:
        activities = {"bill": DEFAULT_BILL_URL}
    manual_ids = parse_name_value(args.resource_id, "--resource-id")
    cli_ts_prefix = args.date
    snapshot_label = args.snapshot_label
    output_root = Path(args.output_root).expanduser().resolve()

    proxy = choose_working_proxy(args.timeout, args.proxy, args.quiet)
    discovery: dict[str, dict[str, Any]] = {}
    if not args.no_browser:
        discovery = discover_with_playwright(
            activities,
            proxy=proxy,
            wait_ms=args.browser_wait_ms,
            quiet=args.quiet,
        )

    if args.discover_only:
        for name, url in activities.items():
            discovered = discovery.get(name, {})
            result = {
                "url": url,
                "title": discovered.get("title"),
                "candidates": sorted(set(
                    c.get("resourceId") for c in discovered.get("candidates", [])
                    if c.get("resourceId")
                )),
            }
            print(json.dumps(result, ensure_ascii=False))
        return 0

    summaries = []
    for name, url in activities.items():
        name = name.lower()
        out_dir = output_root / name
        out_dir.mkdir(parents=True, exist_ok=True)
        inherit_owner(out_dir, output_root)

        candidates = normalize_resource_ids(discovery.get(name, {}), manual_ids.get(name))
        resource_id, _, _ = choose_leaderboard_resource_id(
            candidates,
            referer=url,
            proxy=proxy,
            timeout=args.timeout,
            quiet=args.quiet,
        )

        rows, meta, skip_reason = fetch_top_rows(
            resource_id,
            referer=url,
            top=args.top,
            page_size=args.page_size,
            proxy=proxy,
            timeout=args.timeout,
            quiet=args.quiet,
            skip_if_updated=args.last_updated,
        )
        if skip_reason:
            summaries.append(
                {
                    "name": name,
                    "resourceId": resource_id,
                    "rows": 0,
                    "sum": "0",
                    "restoredTradingVolumeSum": "0",
                    "skipped": True,
                    "reason": skip_reason,
                }
            )
            continue
        rows = enrich_restored_trading_volume(rows)
        values = [to_decimal(row.get("grade")) for row in rows]
        total = sum((value for value in values if value is not None), Decimal("0"))
        restored_values = [restored_trading_volume(row) for row in rows]
        restored_total = sum(
            (value for value in restored_values if value is not None),
            Decimal("0"),
        )

        ts_prefix = cli_ts_prefix or timestamp_from_updated_time(meta.get("updatedTime")) or now_bj()
        file_prefix = (
            f"{ts_prefix}_{snapshot_label}_{name}"
            if snapshot_label
            else f"{ts_prefix}_{name}"
        )
        base = f"{file_prefix}_top{args.top}"
        csv_path = out_dir / f"{base}.csv"
        json_path = out_dir / f"{base}.json"
        discovery_path = out_dir / f"{file_prefix}_discovery.json"
        chart_paths: list[Path] = []
        if not args.no_charts and args.top >= 200:
            chart_path = out_dir / f"{file_prefix}_top200_volume_distribution.png"
            chart_log_path = out_dir / f"{file_prefix}_top200_volume_distribution_log.png"
            chart_paths.extend([chart_path, chart_log_path])

        existing_outputs = [path for path in (csv_path, json_path) if path.exists()]
        if existing_outputs and not args.refresh and not snapshot_label:
            summaries.append(
                {
                    "name": name,
                    "resourceId": resource_id,
                    "rows": len(rows),
                    "sum": decimal_text(total),
                    "restoredTradingVolumeSum": decimal_text(restored_total),
                    "skipped": True,
                    "reason": "snapshot_exists",
                    "csv": str(csv_path),
                    "json": str(json_path),
                    "discovery": str(discovery_path),
                    "charts": [str(path) for path in chart_paths],
                }
            )
            continue

        write_csv(csv_path, rows)
        write_json(
            json_path,
            {
                "name": name,
                "url": url,
                "date": ts_prefix,
                "snapshotLabel": snapshot_label,
                "resourceId": resource_id,
                "top": args.top,
                "field": "grade",
                "restoredField": "restoredTradingVolume",
                "count": len(rows),
                "sum": decimal_text(total),
                "restoredTradingVolumeSum": decimal_text(restored_total),
                "meta": meta,
                "rows": rows,
            },
        )
        write_json(
            discovery_path,
            {
                "name": name,
                "url": url,
                "selectedResourceId": resource_id,
                "candidateResourceIds": candidates,
                "discovery": discovery.get(name, {}),
            },
        )
        for path in (csv_path, json_path, discovery_path):
            inherit_owner(path, out_dir)

        delta = build_delta_outputs(name, out_dir, file_prefix, json_path, rows)

        if not args.no_charts and args.top >= 200:
            make_distribution_chart(rows, name, chart_path, limit=200, log_scale=False)
            make_distribution_chart(rows, name, chart_log_path, limit=200, log_scale=True)
            inherit_owner(chart_path, out_dir)
            inherit_owner(chart_log_path, out_dir)

        summaries.append(
            {
                "name": name,
                "resourceId": resource_id,
                "rows": len(rows),
                "sum": decimal_text(total),
                "restoredTradingVolumeSum": decimal_text(restored_total),
                "csv": str(csv_path),
                "json": str(json_path),
                "discovery": str(discovery_path),
                "delta": delta,
                "charts": [str(path) for path in chart_paths],
            }
        )

    print(json.dumps(summaries, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ScriptError as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1)
