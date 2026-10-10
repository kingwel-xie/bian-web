#!/usr/bin/env python3
"""统计 SPOT（不含 SPRINT）任务榜单成交量。

默认口径：结束日期在 2026-09-18 18:00 之后、market=spot、非 SPRINT 子任务，
取每个榜单最后一次快照的前 100 名，统计成交量（tradingVolume）。

输出：控制台表格 + 两份 CSV（按榜单、按用户合计排名）。

用法示例（均可选，默认值即上述口径）:
  python3 stats_spot_top100_volume.py
  python3 stats_spot_top100_volume.py --after 2026-09-18 --top 100 --outdir .
  DATA_ROOT=/path/to/data python3 stats_spot_top100_volume.py --market spot
"""
import argparse
import csv
import json
import os
from datetime import datetime

DEFAULT_DATA_ROOT = "/home/xjw/bian-web/data"


def to_host(data_root, p):
    return os.path.join(data_root, p[len("/data/") :]) if p.startswith("/data/") else p


def load_qualified(jobs, market, end_after):
    """返回符合条件的任务：最后一次快照的行 + 元数据。"""
    out = []
    for o in jobs:
        p = o.get("payload") or {}
        if (p.get("market") or "").lower() != market:
            continue
        if p.get("parentJobId") or p.get("sprintIndex"):
            continue  # 排除 SPRINT 子任务
        end = p.get("activityEnd") or ""
        try:
            end_dt = datetime.fromisoformat(end.replace(" ", "T"))
        except ValueError:
            continue
        if end_after and end_dt <= end_after:
            continue
        snaps = o.get("snapshots") or []
        if not snaps:
            continue
        last = snaps[-1]
        if not last.get("json"):
            continue
        out.append(
            {
                "resourceId": p.get("resourceId"),
                "name": o.get("name", ""),
                "end": end,
                "snapshot": last.get("timestamp"),
                "json": last["json"],
            }
        )
    out.sort(key=lambda r: int(r["resourceId"]) if str(r["resourceId"]).isdigit() else 0)
    return out


def volume_of(r):
    return r.get("tradingVolume") or r.get("grade") or 0


def main():
    ap = argparse.ArgumentParser(description="SPOT 榜单前 N 名成交量统计")
    ap.add_argument("--after", default="2026-09-18", help="结束日期下限(含当天 18:00 之后), 空串=不限")
    ap.add_argument("--top", type=int, default=100, help="每榜统计前多少名")
    ap.add_argument("--market", default="spot", help="market 过滤, 默认 spot")
    ap.add_argument("--outdir", default=".", help="CSV 输出目录")
    ap.add_argument("--data-root", default=os.environ.get("DATA_ROOT", DEFAULT_DATA_ROOT))
    args = ap.parse_args()

    end_after = None
    if args.after:
        end_after = datetime.fromisoformat(args.after.replace(" ", "T"))
        if end_after.hour == 0 and end_after.minute == 0:
            end_after = end_after.replace(hour=18)  # 只给日期时按当天 18:00 计

    jobs = json.load(open(os.path.join(args.data_root, ".workflow", "jobs.json")))
    qual = load_qualified(jobs, args.market, end_after)

    task_rows = []
    user_vol = {}
    for q in qual:
        d = json.load(open(to_host(args.data_root, q["json"])))
        rows = [r for r in d.get("rows", []) if 1 <= (r.get("sequence") or 0) <= args.top]
        task_rows.append((q, len(rows), sum(volume_of(r) for r in rows)))
        for r in rows:
            uid = r.get("userId") or ""
            v = volume_of(r)
            cur = user_vol.setdefault(uid, {"vol": 0.0, "nick": "", "peak": -1.0})
            cur["vol"] += v
            if v > cur["peak"]:
                cur["peak"] = v
                cur["nick"] = r.get("nickName") or cur["nick"]

    total = sum(t[2] for t in task_rows)
    ranking = sorted(user_vol.items(), key=lambda kv: -kv[1]["vol"])

    print(f"{'资源ID':<12}{'任务':<22}{'结束时间':<20}{'快照时间':<18}{'人数':>7}{'成交量':>18}")
    print("-" * 98)
    for q, n, vol in task_rows:
        print(f"{q['resourceId']:<12}{q['name'][:20]:<22}{q['end']:<20}{str(q['snapshot']):<18}{n:>7}{vol:>18,.0f}")
    print("-" * 98)
    print(f"{'合计':<12}{len(task_rows):<3}个榜单{'':<37}{total:>18,.0f}")

    print()
    print(f"=== 按用户合计排名（共 {len(ranking)} 个用户）===")
    print(f"{'排名':>5}  {'用户ID':<16}{'昵称':<24}{'总成交量':>18}{'占比':>8}")
    print("-" * 76)
    for i, (uid, cu) in enumerate(ranking, 1):
        print(f"{i:>5}  {uid:<16}{cu['nick'][:22]:<24}{cu['vol']:>18,.0f}{cu['vol'] / total * 100:>7.2f}%")

    os.makedirs(args.outdir, exist_ok=True)
    task_csv = os.path.join(args.outdir, "stats_spot_top100_tasks.csv")
    with open(task_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["资源ID", "任务", "结束时间", "快照时间", "榜单人数", "成交量"])
        for q, n, vol in task_rows:
            w.writerow([q["resourceId"], q["name"], q["end"], q["snapshot"], n, round(vol, 2)])
        w.writerow(["合计", f"{len(task_rows)}个榜单", "", "", "", round(total, 2)])

    user_csv = os.path.join(args.outdir, "stats_spot_top100_user_volume.csv")
    with open(user_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["排名", "用户ID", "昵称", "总成交量", "占比"])
        for i, (uid, cu) in enumerate(ranking, 1):
            w.writerow([i, uid, cu["nick"], round(cu["vol"], 2), f"{cu['vol'] / total * 100:.2f}%"])

    print(f"\n按榜单已写入 {task_csv}\n按用户排名已写入 {user_csv}")


if __name__ == "__main__":
    main()
