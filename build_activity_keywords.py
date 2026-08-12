#!/usr/bin/env python3
"""Fetch multiple pages of Binance activities and build a keyword dictionary."""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
DATA_ROOT = Path(os.environ.get("DATA_ROOT", str(APP_DIR / "data"))).expanduser().resolve()
STATE_DIR = DATA_ROOT / ".workflow"
OUTPUT = STATE_DIR / "activity_keywords.json"
BINANCE_URL = (
    "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
)
HEADERS = {
    "accept-language": "zh-CN",
    "lang": "zh-CN",
    "referer": "https://www.binance.com/zh-CN/messages/v2/group/announcement",
    "bnc-time-zone": "Asia/Shanghai",
    "user-agent": "Mozilla/5.0",
}
CATALOG_IDS = [93, 83]
PAGES = 5
PAGE_SIZE = 50
TOKEN_PATTERNS = [
    re.compile(r"[（(]([A-Z][A-Z0-9]{1,10})[）)]"),
    re.compile(r"(?<![A-Za-z])[A-Z]{2,10}(?![A-Za-z])"),
]

COMMON_WORDS = {
    "TO", "THE", "FOR", "AND", "NOT", "YOU", "YOUR", "NEW", "GET",
    "USD", "USDT", "BTC", "ETH", "BNB",
}

TYPE_SEGMENTS = [
    r"交易竞赛", r"交易量锦标赛", r"邀请赛", r"交易者联盟",
    r"嘉年华", r"理财", r"Alpha", r"DeFi", r"币安学院",
    r"交易大赛", r"体验金", r"学习", r"测验",
    r"新用户专享", r"币安广场",
]

# 无论当前批次频次高低，始终纳入 typeKeywords（保证筛选选项存在）
FORCED_TYPES = ["新用户专享", "币安广场"]


def fetch_page(catalog_id: int, page: int) -> list[dict]:
    url = f"{BINANCE_URL}?type=1&pageNo={page}&pageSize={PAGE_SIZE}&catalogId={catalog_id}"
    req = urllib.request.Request(url)
    for k, v in HEADERS.items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode("utf-8")
    data = json.loads(raw)
    cats = data.get("data", {}).get("catalogs") or [{}]
    return cats[0].get("articles", [])


def extract_tokens(title: str, freq: Counter) -> None:
    seen: set[str] = set()
    for pat in TOKEN_PATTERNS:
        for m in pat.finditer(title):
            t = m.group(1) if pat.groups else m.group(0)
            if t in COMMON_WORDS or t in seen:
                continue
            seen.add(t)
            freq[t] += 1


def extract_type_keywords(title: str, freq: Counter) -> None:
    for kw in TYPE_SEGMENTS:
        if kw in title:
            freq[kw] += 1


def main() -> int:
    state_dir = STATE_DIR
    state_dir.mkdir(parents=True, exist_ok=True)

    all_articles: list[dict] = []
    seen_ids: set[int] = set()

    for cid in CATALOG_IDS:
        for page in range(1, PAGES + 1):
            print(f"fetching catalog={cid} page={page}...", file=sys.stderr)
            try:
                articles = fetch_page(cid, page)
            except Exception as exc:
                print(f"  error: {exc}", file=sys.stderr)
                continue
            if not articles:
                break
            for a in articles:
                if a["id"] not in seen_ids:
                    seen_ids.add(a["id"])
                    all_articles.append(a)
            print(f"  got {len(articles)} articles, total unique: {len(all_articles)}", file=sys.stderr)
            time.sleep(0.5)

    if not all_articles:
        print("no articles fetched", file=sys.stderr)
        return 1

    token_freq: Counter = Counter()
    type_freq: Counter = Counter()

    for a in all_articles:
        title = a.get("title", "")
        extract_tokens(title, token_freq)
        extract_type_keywords(title, type_freq)

    tokens = sorted(t for t, c in token_freq.items() if c >= 1)
    type_keywords = sorted(t for t, c in type_freq.items() if c >= 2 and t != "活动")
    for _t in FORCED_TYPES:
        if _t not in type_keywords:
            type_keywords.append(_t)
    type_keywords.sort()

    result = {
        "generatedAt": time.time(),
        "totalArticles": len(all_articles),
        "tokens": tokens,
        "tokenFreq": dict(token_freq.most_common(50)),
        "typeKeywords": type_keywords,
        "typeFreq": dict(type_freq.most_common(30)),
    }

    tmp = OUTPUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(OUTPUT)
    print(f"\nwritten {OUTPUT}", file=sys.stderr)
    print(f"  articles: {len(all_articles)}", file=sys.stderr)
    print(f"  tokens: {len(tokens)}", file=sys.stderr)
    print(f"  typeKeywords: {len(type_keywords)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
