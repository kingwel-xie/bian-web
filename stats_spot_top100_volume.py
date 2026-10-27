#!/usr/bin/env python3
"""统计 SPOT（不含 SPRINT）任务成交量（9/18-10/17 期间）。

规则：
- 活动结束时间 activityEnd >= 2026-09-18 18:00 且 activityEnd <= 2026-10-17 18:00（9/18-10/17期间比赛）
- 10/17之前结束的（activityEnd <= 2026-10-17 18:00）：用最后一次快照
- 10/17之后结束的（activityEnd > 2026-10-17 18:00）：用10/17当天的快照数据
"""
import argparse
import csv
import json
import os
from datetime import datetime

DEFAULT_DATA_ROOT = "/home/xjw/bian-web/data"
WINDOW_START = datetime(2026, 9, 18, 18, 0)
CUTOFF_1017 = datetime(2026, 10, 17, 18, 0)
SNAP_DATE_1017 = "2026-10-17"


def to_host(data_root, p):
    return os.path.join(data_root, p[len("/data/") :]) if p.startswith("/data/") else p


def pick_snapshot(snaps, end_dt):
    """根据结束时间选取快照：
    - 10/17之前结束（end_dt <= 10/17 18:00）：用最后一次快照
    - 10/17之后结束（end_dt > 10/17 18:00）：用10/17当天的快照
    """
    if not snaps:
        return None
    if end_dt <= CUTOFF_1017:
        return snaps[-1]
    # 10/17之后结束的，用10/17当天的快照（优先选最靠近当天的，通常取最后一份当天快照）
    candidates = []
    for s in snaps:
        ts = s.get("timestamp") or ""
        if SNAP_DATE_1017 in ts:
            candidates.append(s)
    if candidates:
        return candidates[-1]
    # 如果当天没有快照，回退到最后一次快照
    return snaps[-1]


def load_qualified(jobs, market):
    """返回符合条件的任务：选定的快照 + 元数据。"""
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
        if end_dt < WINDOW_START:
            continue
        if end_dt > CUTOFF_1017:
            continue
        snaps = o.get("snapshots") or []
        if not snaps:
            continue
        picked = pick_snapshot(snaps, end_dt)
        if not picked or not picked.get("json"):
            continue
        out.append(
            {
                "resourceId": p.get("resourceId"),
                "name": o.get("name", ""),
                "end": end,
                "snapshot": picked.get("timestamp"),
                "json": picked["json"],
                "end_dt": end_dt,
            }
        )
    out.sort(key=lambda r: int(r["resourceId"]) if str(r["resourceId"]).isdigit() else 0)
    return out


def volume_of(r):
    return r.get("tradingVolume") or r.get("grade") or 0


def load_user_map(path):
    """读取 stats_spot_users.csv，返回 {(userId, 昵称): (userId, 用户序号, 全部昵称列表)}。

    用于把脱敏 userId（掩码）按真实用户拆分：同一掩码下不同用户，靠昵称区分。
    """
    if not path or not os.path.exists(path):
        return {}
    m = {}
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            uid = row.get("用户ID(掩码)") or ""
            gi = row.get("用户序号") or ""
            nicks = [n for n in (row.get("昵称列表(按首次出现)") or "").split(" | ") if n]
            for n in nicks:
                m[(uid, n)] = (uid, gi, nicks)
    return m


def main():
    ap = argparse.ArgumentParser(description="SPOT 榜单成交量统计（9/18-10/17期间）")
    ap.add_argument("--top", type=int, default=100, help="每榜统计前多少名")
    ap.add_argument("--market", default="spot", help="market 过滤, 默认 spot")
    ap.add_argument("--users-csv", default="stats_spot_users.csv", help="用户拆分表(默认 stats_spot_users.csv)")
    ap.add_argument("--outdir", default=".", help="CSV 输出目录")
    ap.add_argument("--data-root", default=os.environ.get("DATA_ROOT", DEFAULT_DATA_ROOT))
    args = ap.parse_args()

    users_csv = args.users_csv if os.path.isabs(args.users_csv) else os.path.join(args.outdir, args.users_csv)
    user_map = load_user_map(users_csv)
    print(f"载入用户拆分表: {users_csv}  ({len(user_map)} 个(用户ID,昵称)映射)")

    jobs = json.load(open(os.path.join(args.data_root, ".workflow", "jobs.json")))
    qual = load_qualified(jobs, args.market)

    task_rows = []
    user_vol = {}
    for q in qual:
        d = json.load(open(to_host(args.data_root, q["json"])))
        rows = [r for r in d.get("rows", []) if 1 <= (r.get("sequence") or 0) <= args.top]
        task_rows.append((q, len(rows), sum(volume_of(r) for r in rows)))
        for r in rows:
            uid = r.get("userId") or ""
            nick = r.get("nickName") or ""
            v = volume_of(r)
            ent = user_map.get((uid, nick))
            if ent:
                key_uid, key_gi = ent[0], ent[1]
                ukey = f"{key_uid}#{key_gi}"
            else:
                key_uid, key_gi, ukey = uid, "0", uid
            cur = user_vol.setdefault(
                ukey,
                {"uid": key_uid, "gi": key_gi, "vol": 0.0, "boards": set(), "nicks": set(),
                 "all_nicks": set(), "peak": -1.0, "nick": ""},
            )
            cur["vol"] += v
            cur["boards"].add(q["name"])
            if nick:
                cur["nicks"].add(nick)
            if ent:
                cur["all_nicks"].update(ent[2])
            if v > cur["peak"]:
                cur["peak"] = v
                cur["nick"] = nick or cur["nick"]

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
    print(f"{'排名':>5}  {'用户ID':<14}{'用户#':>5}  {'昵称':<24}{'总成交量':>18}{'占比':>8}")
    print("-" * 80)
    for i, (ukey, cu) in enumerate(ranking, 1):
        print(f"{i:>5}  {cu['uid']:<14}{cu['gi']:>5}  {cu['nick'][:22]:<24}{cu['vol']:>18,.0f}{cu['vol'] / total * 100:>7.2f}%")

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
        w.writerow(["排名", "用户ID", "用户序号", "昵称", "全部昵称", "本期昵称", "总成交量", "榜单数", "占比"])
        for i, (ukey, cu) in enumerate(ranking, 1):
            w.writerow([
                i, cu["uid"], cu["gi"], cu["nick"],
                " | ".join(sorted(cu["all_nicks"])) or cu["nick"],
                " | ".join(sorted(cu["nicks"])),
                round(cu["vol"], 2), len(cu["boards"]),
                f"{cu['vol'] / total * 100:.2f}%",
            ])

    print(f"\n按榜单已写入 {task_csv}\n按用户排名已写入 {user_csv}")


if __name__ == "__main__":
    main()
