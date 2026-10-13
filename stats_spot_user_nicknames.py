#!/usr/bin/env python3
"""统计 SPOT 榜单前 N 名各脱敏 userId 使用过的昵称，并把每个 mask 拆成若干"用户"。

思路（简化版，按"活动/榜单"是否重复做排除）：
  - 仅 market=spot 且非 SPRINT 子任务；取每榜最后一次快照的前 N 名（默认 200）。
  - 每个昵称记下它出现过的"活动列表"（这里以榜单名 = 一个活动）。
  - 对同一 mask：按"活动数从多到少"依次处理各昵称；
      * 若某昵称的活动与某个已有用户组的活动集合**有交集** => 判为另一个用户（排除）；
      * 若与已有组**都不交集** => 并入活动集合最大的那个组（视为同一人）。
    这样同一用户组内的昵称两两不共享任何活动。
  - 昵称在组内按首次出现时间排序，即"改名轨迹"（时间线可再人工核对）。
  - 报告同榜同快照多行（user_identity_db.json）的"确定多用户"，用于校验。

输出：控制台 + CSV：
  - stats_spot_users.csv           : 每个 (userId, 用户) 一行 —— **最终结果：一个用户用过的所有昵称**
  - stats_spot_user_nicknames.csv  : 每个 userId 一行（昵称数、用户数、榜单数、确定多用户、昵称列表）
  - stats_spot_user_identities.csv : 每个 (userId, 昵称) 一行（含活动列表与时间范围）
  - stats_spot_user_timeline.csv   : 每个 (userId, 昵称, 榜单) 观测一行，按 activityEnd 排序

用法示例:
  python3 stats_spot_user_nicknames.py
  python3 stats_spot_user_nicknames.py --top 200 --show 5 --outdir .
  DATA_ROOT=/path/to/data python3 stats_spot_user_nicknames.py
"""
import argparse
import csv
import json
import os

DEFAULT_DATA_ROOT = "/home/xjw/bian-web/data"


def to_host(data_root, p):
    return os.path.join(data_root, p[len("/data/"):]) if p.startswith("/data/") else p


def load_identity_db(path):
    """返回 {maskedUserId: set(确定不同的昵称)}；文件不存在则返回 {}。"""
    if not path or not os.path.exists(path):
        return {}
    try:
        d = json.load(open(path, encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {uid: set(e.get("confirmedDistinct") or []) for uid, e in (d.get("users") or {}).items()}


def group_users(nick_acts, nick_first):
    """nick_acts: {昵称: set(活动)}；nick_first: {昵称: 首次时间}。

    贪心：昵称按活动数降序；与已有组活动有交集 => 新用户，否则并入活动最多的可并组。
    返回用户组列表，每组 {'acts':set, 'nicks':[按首次时间排序的昵称]}，按活动数降序。
    """
    items = sorted(nick_acts.items(), key=lambda kv: (-len(kv[1]), nick_first.get(kv[0], ""), kv[0]))
    groups: list[dict] = []
    for nick, acts in items:
        best = None
        for g in groups:
            if g["acts"].isdisjoint(acts) and (best is None or len(g["acts"]) > len(best["acts"])):
                best = g
        if best is None:
            groups.append({"acts": set(acts), "nicks": [nick]})
        else:
            best["acts"] |= acts
            best["nicks"].append(nick)
    for g in groups:
        g["nicks"].sort(key=lambda n: (nick_first.get(n, ""), n))
    groups.sort(key=lambda g: (-len(g["acts"]), -len(g["nicks"]), g["nicks"][0]))
    return groups


def main():
    ap = argparse.ArgumentParser(description="SPOT 榜单前 N 名 userId 昵称 -> 用户拆分（活动排除法）")
    ap.add_argument("--top", type=int, default=200, help="每榜统计前多少名")
    ap.add_argument("--market", default="spot", help="market 过滤, 默认 spot")
    ap.add_argument("--db", default="user_identity_db.json", help="身份证据库路径")
    ap.add_argument("--outdir", default=".", help="CSV 输出目录")
    ap.add_argument("--show", type=int, default=0, help="控制台打印前 N 个多昵称 mask 的用户拆分")
    ap.add_argument("--data-root", default=os.environ.get("DATA_ROOT", DEFAULT_DATA_ROOT))
    args = ap.parse_args()

    db_path = args.db if os.path.isabs(args.db) else os.path.join(args.outdir, args.db)
    idb = load_identity_db(db_path)

    jobs = json.load(open(os.path.join(args.data_root, ".workflow", "jobs.json")))

    users: dict[str, dict] = {}
    boards = 0
    for o in jobs:
        p = o.get("payload") or {}
        if (p.get("market") or "").lower() != args.market:
            continue
        if p.get("parentJobId") or p.get("sprintIndex"):
            continue  # 排除 SPRINT 子任务
        snaps = o.get("snapshots") or []
        if not snaps or not snaps[-1].get("json"):
            continue
        d = json.load(open(to_host(args.data_root, snaps[-1]["json"])))
        rows = [r for r in d.get("rows", []) if 1 <= (r.get("sequence") or 0) <= args.top]
        if not rows:
            continue
        boards += 1
        name = o.get("name") or ""
        start = p.get("activityStart") or ""
        end = p.get("activityEnd") or ""
        for r in rows:
            uid = r.get("userId") or ""
            nick = r.get("nickName") or ""
            if not uid:
                continue
            e = users.setdefault(uid, {"nicks": set(), "tasks": set(), "obs": []})
            e["tasks"].add(name)
            if nick:
                e["nicks"].add(nick)
            e["obs"].append((nick, name, start, end))

    ranking = []
    for uid, e in users.items():
        confirmed = idb.get(uid)
        e["confirmed"] = confirmed is not None
        e["confirmed_nicks"] = sorted((confirmed or set()) & e["nicks"])
        nick_acts: dict[str, set] = {}
        nick_first: dict[str, str] = {}
        nick_ends: dict[str, list] = {}
        for nick, name, start, end in e["obs"]:
            if not nick:
                continue
            nick_acts.setdefault(nick, set()).add(name)
            nick_ends.setdefault(nick, []).append(end)
            cur = nick_first.get(nick)
            if cur is None or (start and start < cur):
                nick_first[nick] = start
        e["nick_acts"] = nick_acts
        e["nick_ends"] = nick_ends
        e["groups"] = group_users(nick_acts, nick_first)
        ranking.append((uid, e))

    ranking.sort(key=lambda kv: (not kv[1]["confirmed"], -len(kv[1]["nicks"]), kv[0]))
    total_nicks = sum(len(e["nicks"]) for _, e in ranking)
    total_users = sum(len(e["groups"]) for _, e in ranking)
    n_confirmed = sum(1 for _, e in ranking if e["confirmed"])

    print(f"榜单数: {boards}  用户条目(mask): {len(ranking)}  昵称数: {total_nicks}  拆出用户数: {total_users}  确定多用户: {n_confirmed}")
    print()
    print(f"{'用户ID':<14}{'昵称数':>6}{'用户数':>6}{'榜单数':>7}  {'确定多用户':<10}昵称列表")
    print("-" * 130)
    for uid, e in ranking:
        flag = "是" if e["confirmed"] else ""
        nicks = " | ".join(sorted(e["nicks"]))
        print(f"{uid:<14}{len(e['nicks']):>6}{len(e['groups']):>6}{len(e['tasks']):>7}  {flag:<10}{nicks}")

    out_users = os.path.join(args.outdir, "stats_spot_users.csv")
    with open(out_users, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["用户ID(掩码)", "用户序号", "昵称数", "活动数", "确定多用户", "昵称列表(按首次出现)"])
        for uid, e in ranking:
            for gi, g in enumerate(e["groups"], 1):
                w.writerow([
                    uid, gi, len(g["nicks"]), len(g["acts"]),
                    "是" if e["confirmed"] else "",
                    " | ".join(g["nicks"]),
                ])

    out_csv = os.path.join(args.outdir, "stats_spot_user_nicknames.csv")
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["用户ID", "昵称数量", "用户数", "榜单数", "确定多用户", "确定不同昵称", "昵称列表"])
        for uid, e in ranking:
            w.writerow([
                uid, len(e["nicks"]), len(e["groups"]), len(e["tasks"]),
                "是" if e["confirmed"] else "",
                " | ".join(e["confirmed_nicks"]),
                " | ".join(sorted(e["nicks"])),
            ])

    out_ident = os.path.join(args.outdir, "stats_spot_user_identities.csv")
    with open(out_ident, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["用户ID", "昵称", "榜单数", "活动结束(最早)", "活动结束(最晚)", "确定多用户", "榜单列表"])
        for uid, e in ranking:
            for nick in sorted(e["nick_acts"], key=lambda n: (e["nick_ends"][n] and min(e["nick_ends"][n]) or "", n)):
                ends = sorted(x for x in e["nick_ends"][nick] if x)
                acts = sorted(e["nick_acts"][nick])
                w.writerow([
                    uid, nick, len(acts),
                    ends[0] if ends else "", ends[-1] if ends else "",
                    "是" if e["confirmed"] else "",
                    " | ".join(acts),
                ])

    out_tl = os.path.join(args.outdir, "stats_spot_user_timeline.csv")
    with open(out_tl, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["用户ID", "活动结束", "活动开始", "昵称", "榜单", "确定多用户"])
        for uid, e in ranking:
            for nick, name, start, end in sorted(e["obs"], key=lambda x: (x[3], x[1])):
                w.writerow([uid, end, start, nick, name, "是" if e["confirmed"] else ""])

    print(f"\n已写入 {out_users}")
    print(f"已写入 {out_csv}")
    print(f"已写入 {out_ident}")
    print(f"已写入 {out_tl}")

    if args.show > 0:
        top = sorted(ranking, key=lambda kv: -len(kv[1]["nicks"]))[:args.show]
        for uid, e in top:
            tag = "确定多用户" if e["confirmed"] else ""
            print(f"\n=== {uid}  ({tag}, {len(e['tasks'])} 榜, {len(e['nicks'])} 昵称, {len(e['groups'])} 用户) ===")
            for gi, g in enumerate(e["groups"], 1):
                print(f"    用户{gi} ({len(g['acts'])}活动): {' → '.join(g['nicks'])}")


if __name__ == "__main__":
    main()
