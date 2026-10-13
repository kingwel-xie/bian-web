#!/usr/bin/env python3
"""扫描全部 SPOT 快照，建立"确定不同用户"证据库。

规则：同一榜单的同一份快照内，同一脱敏 userId 出现在多行 => 这些行必为不同真实用户
（一个账号在同一榜单只占一行）。据此，把每个 (userId, 榜单, 快照) 里的多行昵称记录为一组
"两两确定不同"的证据。

范围：market=spot 的全部任务、全部快照（SPRINT 为独立 market，天然排除）。

输出 user_identity_db.json：
  {
    "generatedAt": ...,
    "scope": {"market":"spot","snapshots":"all"},
    "boards": 61, "snapshotsScanned": 700, "conflictGroups": N,
    "users": {
      "<maskedUserId>": {
        "nicknames": [...],            # 该 mask 出现过的所有昵称
        "confirmedDistinct": [...],    # 参与过"同榜多行"的昵称并集（彼此确定不同）
        "groups": [
          {"board":..., "timestamp":..., "members":[{"nickname":..., "avatarUrl":...}, ...]}
        ]
      }, ...
    }
  }

用法：
  python3 build_user_identity_db.py
  python3 build_user_identity_db.py --market spot --out user_identity_db.json
  DATA_ROOT=/path/to/data python3 build_user_identity_db.py
"""
import argparse
import json
import os
from collections import defaultdict
from datetime import datetime, timezone

DEFAULT_DATA_ROOT = "/home/xjw/bian-web/data"


def to_host(data_root, p):
    return os.path.join(data_root, p[len("/data/"):]) if p.startswith("/data/") else p


def main():
    ap = argparse.ArgumentParser(description="建立确定不同用户证据库（SPOT）")
    ap.add_argument("--market", default="spot")
    ap.add_argument("--snapshots", choices=["all", "last"], default="last",
                    help="all=全部快照；last=每榜最后一份快照（默认）")
    ap.add_argument("--top", type=int, default=200, help="每榜只统计前多少名（默认200）")
    ap.add_argument("--out", default="user_identity_db.json")
    ap.add_argument("--data-root", default=os.environ.get("DATA_ROOT", DEFAULT_DATA_ROOT))
    args = ap.parse_args()

    jobs = json.load(open(os.path.join(args.data_root, ".workflow", "jobs.json")))
    jobs = [o for o in jobs if (o.get("payload") or {}).get("market", "").lower() == args.market]

    users: dict[str, dict] = {}
    boards = 0
    snaps_scanned = 0
    conflict_groups = 0

    for o in jobs:
        name = o.get("name") or ""
        snaps = o.get("snapshots") or []
        if not snaps:
            continue
        boards += 1
        if args.snapshots == "last":
            snaps = snaps[-1:]
        for snap in snaps:
            path = snap.get("json")
            if not path:
                continue
            try:
                d = json.load(open(to_host(args.data_root, path)))
            except (OSError, json.JSONDecodeError):
                continue
            rows = d.get("rows") if isinstance(d, dict) else None
            if not isinstance(rows, list):
                continue
            snaps_scanned += 1
            ts = snap.get("timestamp")
            by_uid: dict[str, list] = {}
            for r in rows:
                if not isinstance(r, dict):
                    continue
                seq = r.get("sequence")
                if args.top and isinstance(seq, int) and seq > args.top:
                    continue
                uid = r.get("userId") or ""
                if uid:
                    by_uid.setdefault(uid, []).append(r)
            for uid, grp in by_uid.items():
                if len(grp) < 2:
                    continue
                conflict_groups += 1
                members = [
                    {"nickname": r.get("nickName") or r.get("nickname") or "", "avatarUrl": r.get("avatarUrl")}
                    for r in grp
                ]
                e = users.setdefault(uid, {"nicknames": set(), "confirmedDistinct": set(), "groups": []})
                for m in members:
                    if m["nickname"]:
                        e["nicknames"].add(m["nickname"])
                        e["confirmedDistinct"].add(m["nickname"])
                e["groups"].append({"board": name, "timestamp": ts, "members": members})

    out = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "scope": {"market": args.market, "snapshots": args.snapshots, "top": args.top},
        "boards": boards,
        "snapshotsScanned": snaps_scanned,
        "conflictGroups": conflict_groups,
        "users": {
            uid: {
                "nicknames": sorted(e["nicknames"]),
                "confirmedDistinct": sorted(e["confirmedDistinct"]),
                "groupCount": len(e["groups"]),
                "groups": e["groups"],
            }
            for uid, e in sorted(users.items())
        },
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    multi = sum(1 for e in users.values() if len(e["groups"]) > 0)
    print(f"扫描: {boards} 榜单 / {snaps_scanned} 快照")
    print(f"确定存在多用户的 mask 数: {multi}  冲突组总数: {conflict_groups}")
    print(f"已写入 {args.out}")


if __name__ == "__main__":
    main()
