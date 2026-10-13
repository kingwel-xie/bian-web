#!/usr/bin/env python3
"""统计 SPOT（不含 SPRINT）任务最后一次快照中，各 userId 使用过的昵称，并做身份拆分与冲突标记。

口径：market=spot 且非 SPRINT 子任务，取每个榜单最后一次快照的前 N 名（默认 100），
按 userId 汇总其出现过的所有 nickName，并按 optInId 拆分身份。

背景：
  - userId 是脱敏值（仅保留前2位+后2位），不同真实用户可能落入同一掩码；
  - optInId 是"用户×活动"级标识：同一活动（可跨多个榜单，如联赛各轮）内同一用户 optInId 恒定，
    且同一榜单内 optInId 唯一。因此：
      * 同一 userId 在**同一榜单**出现多个不同 optInId => 确定是不同真实用户（决定性）；
      * 同一 userId 跨活动出现**不同 optInId 且不同昵称** => 判为不同身份（本脚本按此拆分输出，
        注意：若同一人换名进入多个活动也可能被拆分，属该规则的已知边界）。

输出：控制台表格 + 两份 CSV：
  - stats_spot_user_nicknames.csv     : 每个 userId 一行（含身份数、冲突标记、昵称列表）
  - stats_spot_user_identities.csv    : 每个 (userId, optInId) 身份一行（按 optInId 拆行）

用法示例（均可选，默认值即上述口径）:
  python3 stats_spot_user_nicknames.py
  python3 stats_spot_user_nicknames.py --top 100 --conflict-hours 24 --outdir .
  DATA_ROOT=/path/to/data python3 stats_spot_user_nicknames.py
"""
import argparse
import csv
import json
import os
import re
from datetime import datetime

DEFAULT_DATA_ROOT = "/home/xjw/bian-web/data"
AUTO_NICK_RE = re.compile(r"^User-[0-9a-z]{3,}$", re.I)


def to_host(data_root, p):
    return os.path.join(data_root, p[len("/data/"):]) if p.startswith("/data/") else p


def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%dT%H%M%S")
    except ValueError:
        return None


def detect_conflict(entries, conflict_hours, dup_boards=None):
    """entries: list of (nick, task, ts, optInId)；dup_boards: 同一快照内该 userId 出现多次的榜单集合。

    返回 (原因列表, 确定不同人bool)。空列表 = 未发现冲突。
    """
    nicks = {n for n, _, _, _ in entries if n}
    optids = {o for _, _, _, o in entries if o}
    reasons = []
    definitive = False
    if dup_boards:
        reasons.append(f"同榜重复出现({len(dup_boards)})")
        definitive = True
    autos = {n for n in nicks if AUTO_NICK_RE.match(n)}
    if len(autos) > 1:
        reasons.append(f"多个系统昵称({len(autos)})")
    if len(optids) >= 2 and len(nicks) >= 2:
        reasons.append(f"optInId不同且昵称不同({len(optids)}活动)")
    window = conflict_hours * 3600
    for i in range(len(entries)):
        n1, _, t1, _ = entries[i]
        if not t1:
            continue
        for j in range(i + 1, len(entries)):
            n2, _, t2, _ = entries[j]
            if not n2 or not t2 or n1 == n2:
                continue
            if abs((t1 - t2).total_seconds()) <= window:
                reasons.append("同期不同昵称")
                break
        if "同期不同昵称" in reasons:
            break
    return reasons, definitive


def main():
    ap = argparse.ArgumentParser(description="SPOT 榜单前 N 名 userId 昵称汇总 + 身份拆分 + 冲突标记")
    ap.add_argument("--top", type=int, default=100, help="每榜统计前多少名")
    ap.add_argument("--market", default="spot", help="market 过滤, 默认 spot")
    ap.add_argument("--conflict-hours", type=float, default=72, help="同期不同昵称的时间窗(小时)")
    ap.add_argument("--outdir", default=".", help="CSV 输出目录")
    ap.add_argument("--data-root", default=os.environ.get("DATA_ROOT", DEFAULT_DATA_ROOT))
    args = ap.parse_args()

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
        ts = parse_ts(snaps[-1].get("timestamp"))
        board_uid_opts: dict[str, set] = {}
        for r in rows:
            uid = r.get("userId") or ""
            nick = r.get("nickName") or ""
            opt = str(r.get("optInId") or "").strip()
            if not uid:
                continue
            e = users.setdefault(uid, {"entries": [], "tasks": set(), "dup_boards": set()})
            e["tasks"].add(name)
            if nick or opt:
                e["entries"].append((nick, name, ts, opt))
            board_uid_opts.setdefault(uid, set()).add(opt)
        for uid, opts in board_uid_opts.items():
            if len(opts) > 1:
                users[uid]["dup_boards"].add(name)

    ranking = []
    for uid, e in users.items():
        nicks = {n for n, _, _, _ in e["entries"] if n}
        optids = {o for _, _, _, o in e["entries"] if o}
        groups: dict[str, set] = {}
        gboards: dict[str, set] = {}
        for n, b, _, optid in e["entries"]:
            key = optid or "(无optInId)"
            groups.setdefault(key, set()).add(n)
            gboards.setdefault(key, set()).add(b)
        e["nicks"] = nicks
        e["optids"] = optids
        e["groups"] = groups
        e["gboards"] = gboards
        e["conflict"], e["definitive"] = detect_conflict(e["entries"], args.conflict_hours, e["dup_boards"])
        e["opt_split"] = len(optids) >= 2 and len(nicks) >= 2
        e["same_board_dup"] = bool(e["dup_boards"])
        e["sev"] = (100 if e["definitive"] else 0) + (50 if e["opt_split"] else 0) + sum(
            100 if r.startswith("同榜重复") else 10 if r.startswith("多个系统") else 5 if r.startswith("optInId") else 1
            for r in e["conflict"]
        )
        ranking.append((uid, e))

    ranking.sort(key=lambda kv: (-kv[1]["sev"], -len(kv[1]["nicks"]), kv[0]))
    renamed = sum(1 for _, e in ranking if len(e["nicks"]) > 1)
    conflicts = [(u, e) for u, e in ranking if e["conflict"]]
    definitive = [(u, e) for u, e in ranking if e["definitive"]]
    n_opt_split = sum(1 for _, e in ranking if e["opt_split"])

    print(f"榜单数: {boards}  用户数: {len(ranking)}  改过名: {renamed}  疑似冲突: {len(conflicts)}")
    print(f"  同榜重复出现(决定性): {len(definitive)}  optInId不同且昵称不同: {n_opt_split}")
    print()
    print(f"{'用户ID':<14}{'昵称数':>6}{'榜单数':>7}{'身份数':>7}  {'冲突':<4} 昵称列表")
    print("-" * 120)
    for uid, e in ranking:
        tags = []
        if e["same_board_dup"]:
            tags.append("同榜重复")
        if e["opt_split"]:
            tags.append("多身份")
        flag = ("⚠ " + "、".join(tags)) if tags else ""
        nicks = " | ".join(sorted(e["nicks"]))
        print(f"{uid:<14}{len(e['nicks']):>6}{len(e['tasks']):>7}{len(e['optids']):>7}  {flag:<4} {nicks}")

    out_csv = os.path.join(args.outdir, "stats_spot_user_nicknames.csv")
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["用户ID", "昵称数量", "榜单数", "身份数", "确定不同人", "同榜重复", "冲突原因", "昵称列表"])
        for uid, e in ranking:
            w.writerow([
                uid,
                len(e["nicks"]),
                len(e["tasks"]),
                len(e["optids"]),
                "是" if e["opt_split"] else "",
                "是" if e["same_board_dup"] else "",
                "、".join(e["conflict"]),
                " | ".join(sorted(e["nicks"])),
            ])

    out_split = os.path.join(args.outdir, "stats_spot_user_identities.csv")
    with open(out_split, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["用户ID", "optInId", "昵称数量", "昵称列表", "榜单数", "榜单列表"])
        for uid, e in ranking:
            for optid in sorted(e["groups"], key=lambda k: -len(e["gboards"][k])):
                gnicks = sorted(n for n in e["groups"][optid] if n)
                gboards = sorted(e["gboards"][optid])
                w.writerow([uid, optid, len(gnicks), " | ".join(gnicks), len(gboards), " | ".join(gboards)])

    print(f"\n已写入 {out_csv}")
    print(f"已写入 {out_split}")


if __name__ == "__main__":
    main()
