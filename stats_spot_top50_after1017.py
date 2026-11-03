#!/usr/bin/env python3
"""统计「2026-10-17 之后开始、且已结束」的 SPOT 任务，各榜前 N 名及其成交量。

规则：
- market == spot，且不是 sprint 子任务
- activityStart > 2026-10-17 23:59:59（即 10/18 起开始）
- activityEnd < 当前时间（已结束）
- 取最后一次快照，按 sequence 排序取前 N 名
- 成交量优先取 tradingVolume，缺失时回退 grade

输出：
- 控制台：任务清单 + 各榜前 N 名 + 跨榜合计前 N 名
- CSV：stats_spot_after1017_tasks.csv
       stats_spot_after1017_top50_by_task.csv
       stats_spot_after1017_top50_aggregate.csv
"""
import argparse
import csv
import json
import os
from datetime import datetime

DEFAULT_DATA_ROOT = "/home/xjw/bian-web/data"
CUTOFF_START = datetime(2026, 10, 17, 23, 59, 59)


def to_host(data_root, p):
    return os.path.join(data_root, p[len("/data/"):]) if p.startswith("/data/") else p


def parse_dt(s):
    try:
        return datetime.fromisoformat(str(s or "").replace(" ", "T"))
    except ValueError:
        return None


def load_user_map(path):
    """{(userId(掩码), 昵称): (userId, 用户序号)}，用于区分同一掩码下的不同用户。"""
    if not path or not os.path.exists(path):
        return {}
    m = {}
    with open(path, encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            uid = row.get("用户ID(掩码)") or ""
            gi = row.get("用户序号") or "0"
            for n in (row.get("昵称列表(按首次出现)") or "").split(" | "):
                if n:
                    m[(uid, n)] = (uid, gi)
    return m


def volume_of(r):
    v = r.get("tradingVolume")
    if v is None:
        v = r.get("grade")
    return float(v or 0)


def load_qualified(jobs, market, cutoff_start, as_of):
    out = []
    for job in jobs:
        p = job.get("payload") or {}
        if (p.get("market") or "").lower() != market:
            continue
        if p.get("parentJobId") or p.get("sprintIndex"):
            continue
        start_dt = parse_dt(p.get("activityStart"))
        end_dt = parse_dt(p.get("activityEnd"))
        if not start_dt or not end_dt:
            continue
        if start_dt <= cutoff_start:
            continue
        if end_dt >= as_of:
            continue
        snaps = sorted(job.get("snapshots") or [], key=lambda s: s.get("timestamp") or "")
        if not snaps or not snaps[-1].get("json"):
            continue
        out.append({
            "jobId": job.get("id"),
            "resourceId": p.get("resourceId"),
            "name": job.get("name") or "",
            "start": p.get("activityStart"),
            "end": p.get("activityEnd"),
            "snapshot": snaps[-1].get("timestamp"),
            "json": snaps[-1]["json"],
            "start_dt": start_dt,
            "end_dt": end_dt,
        })
    out.sort(key=lambda r: r["start_dt"])
    return out


def top_rows(data, top):
    rows = [r for r in (data.get("rows") or []) if r.get("sequence") is not None]
    rows.sort(key=lambda r: int(r["sequence"]))
    return rows[:top]


def main():
    ap = argparse.ArgumentParser(description="10/17 后开始且已结束的 SPOT 任务前 N 名成交量统计")
    ap.add_argument("--top", type=int, default=50, help="每榜取前多少名（默认 50）")
    ap.add_argument("--market", default="spot")
    ap.add_argument("--start-cutoff", default=CUTOFF_START.strftime("%Y-%m-%d %H:%M:%S"),
                    help="开始时间下限（不含），默认 2026-10-17 23:59:59")
    ap.add_argument("--as-of", default="", help="截止时间，默认当前时间")
    ap.add_argument("--outdir", default=".")
    ap.add_argument("--data-root", default=os.environ.get("DATA_ROOT", DEFAULT_DATA_ROOT))
    ap.add_argument("--users-csv", default="stats_spot_users.csv")
    args = ap.parse_args()

    cutoff_start = parse_dt(args.start_cutoff)
    as_of = parse_dt(args.as_of) if args.as_of else datetime.now()
    users_csv = args.users_csv if os.path.isabs(args.users_csv) else os.path.join(args.outdir, args.users_csv)
    user_map = load_user_map(users_csv)

    jobs_path = os.path.join(args.data_root, ".workflow", "jobs.json")
    jobs = json.load(open(jobs_path, encoding="utf-8"))
    jobs = jobs if isinstance(jobs, list) else jobs.get("jobs", jobs)

    qual = load_qualified(jobs, args.market, cutoff_start, as_of)
    print(f"截止时间: {as_of:%Y-%m-%d %H:%M}   开始时间 > {cutoff_start:%Y-%m-%d %H:%M}")
    print(f"符合条件的 {args.market.upper()} 任务: {len(qual)} 个\n")
    if not qual:
        return

    task_rows = []
    per_task = {}
    agg = {}
    for q in qual:
        data = json.load(open(to_host(args.data_root, q["json"]), encoding="utf-8"))
        rows = top_rows(data, args.top)
        board_sum = sum(volume_of(r) for r in rows)
        task_rows.append((q, len(rows), board_sum, float(data.get("sum") or 0)))
        per_task[q["jobId"]] = []
        for r in rows:
            uid = r.get("userId") or ""
            nick = r.get("nickName") or ""
            v = volume_of(r)
            seq = int(r["sequence"])
            per_task[q["jobId"]].append((seq, nick, uid, v))
            key_uid, key_gi = user_map.get((uid, nick), (uid, "0"))
            ukey = f"{key_uid}#{key_gi}"
            cur = agg.setdefault(ukey, {"uid": key_uid, "gi": key_gi, "nicks": set(), "vol": 0.0,
                                        "boards": set(), "by_job": {}})
            cur["vol"] += v
            cur["boards"].add(q["name"])
            cur["nicks"].add(nick)
            cur["by_job"][q["jobId"]] = cur["by_job"].get(q["jobId"], 0.0) + v

    # 任务清单
    print(f"{'资源ID':<11}{'任务':<34}{'开始时间':<18}{'结束时间':<18}{'快照时间':<18}{'人数':>5}{'前N成交量和':>18}{'全榜总和':>18}")
    print("-" * 148)
    for q, n, s, total in task_rows:
        print(f"{str(q['resourceId']):<11}{q['name'][:32]:<34}{q['start']:<18}{q['end']:<18}"
              f"{str(q['snapshot']):<18}{n:>5}{s:>18,.0f}{total:>18,.0f}")
    print("-" * 148)

    # 各榜前 N 名
    for q, n, s, total in task_rows:
        print(f"\n=== {q['name']} · 前 {n} 名（快照 {q['snapshot']}）===")
        print(f"{'排名':>4}  {'昵称':<22}{'用户ID':<12}{'成交量':>18}{'占前N':>9}{'占全榜':>9}")
        print("-" * 76)
        for seq, nick, uid, v in per_task[q["jobId"]]:
            print(f"{seq:>4}  {nick[:20]:<22}{uid:<12}{v:>18,.0f}"
                  f"{(v / s * 100 if s else 0):>8.2f}%{(v / total * 100 if total else 0):>8.2f}%")

    # 跨榜合计
    ranking = sorted(agg.items(), key=lambda kv: -kv[1]["vol"])
    agg_total = sum(cu["vol"] for _, cu in ranking)
    print(f"\n=== 跨 {len(qual)} 个榜合计排名（前 {min(args.top, len(ranking))} / 共 {len(ranking)} 人）===")
    print(f"{'排名':>4}  {'昵称':<22}{'用户ID':<12}{'用户#':>5}{'总成交量':>18}{'榜数':>5}{'占比':>8}")
    print("-" * 82)
    for i, (_, cu) in enumerate(ranking[:args.top], 1):
        nick = max(cu["nicks"], key=len) if cu["nicks"] else ""
        print(f"{i:>4}  {nick[:20]:<22}{cu['uid']:<12}{cu['gi']:>5}{cu['vol']:>18,.0f}"
              f"{len(cu['boards']):>5}{(cu['vol'] / agg_total * 100 if agg_total else 0):>7.2f}%")

    # CSV
    os.makedirs(args.outdir, exist_ok=True)
    task_csv = os.path.join(args.outdir, "stats_spot_after1017_tasks.csv")
    with open(task_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["资源ID", "任务", "开始时间", "结束时间", "快照时间", "榜单人数", f"前{args.top}名成交量和", "全榜总和"])
        for q, n, s, total in task_rows:
            w.writerow([q["resourceId"], q["name"], q["start"], q["end"], q["snapshot"], n, round(s, 2), round(total, 2)])

    per_csv = os.path.join(args.outdir, f"stats_spot_after1017_top{args.top}_by_task.csv")
    with open(per_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["任务", "资源ID", "快照时间", "排名", "昵称", "用户ID(掩码)", "成交量", "占前N合计", "占全榜"])
        for q, n, s, total in task_rows:
            for seq, nick, uid, v in per_task[q["jobId"]]:
                w.writerow([q["name"], q["resourceId"], q["snapshot"], seq, nick, uid, round(v, 2),
                            f"{v / s * 100:.2f}%" if s else "0.00%",
                            f"{v / total * 100:.2f}%" if total else "0.00%"])

    agg_csv = os.path.join(args.outdir, f"stats_spot_after1017_top{args.top}_aggregate.csv")
    with open(agg_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["排名", "昵称", "用户ID(掩码)", "用户序号", "总成交量", "榜数", "占比"])
        for i, (_, cu) in enumerate(ranking[:args.top], 1):
            nick = max(cu["nicks"], key=len) if cu["nicks"] else ""
            w.writerow([i, nick, cu["uid"], cu["gi"], round(cu["vol"], 2), len(cu["boards"]),
                        f"{cu['vol'] / agg_total * 100:.2f}%" if agg_total else "0.00%"])

    print(f"\n已写入:\n  {task_csv}\n  {per_csv}\n  {agg_csv}")


if __name__ == "__main__":
    main()
