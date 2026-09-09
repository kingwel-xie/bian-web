import os

from app import load_jobs, load_rewards_db, save_rewards_db

DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"


def _last_tier_index(tiers):
    best = -1
    best_max = -1
    for i, t in enumerate(tiers):
        try:
            rmax = int(t.get("rankMax", 0) or 0)
        except (TypeError, ValueError):
            continue
        if rmax > best_max:
            best_max = rmax
            best = i
    return best


def _convert(records):
    jobs = {j.get("id"): j for j in load_jobs()}
    changed = 0
    skipped = 0
    for r in records:
        job = jobs.get(r.get("jobId"))
        if not job:
            skipped += 1
            continue
        payload = job.get("payload") or {}
        mode = payload.get("rewardMode")
        if mode not in ("rank", "rank_last_volume"):
            continue
        tiers = payload.get("rewardTiers") or []
        ti = r.get("tierIndex")
        if ti is None or not isinstance(ti, int) or ti < 0 or ti >= len(tiers):
            skipped += 1
            continue
        if mode == "rank_last_volume":
            if r.get("rewardMode") == "rank_last_volume" or ti == _last_tier_index(tiers):
                continue
        t = tiers[ti]
        try:
            pool = float(t.get("amount", 0) or 0)
            rmin = int(t.get("rankMin", 0) or 0)
            rmax = int(t.get("rankMax", 0) or 0)
        except (TypeError, ValueError):
            skipped += 1
            continue
        tier_count = rmax - rmin + 1
        if tier_count <= 0 or pool <= 0:
            skipped += 1
            continue
        cur = float(r.get("amount", 0) or 0)
        if abs(cur - pool) > 1e-9 * max(1.0, abs(pool)):
            skipped += 1
            continue
        r["amount"] = str(round(cur / tier_count, 4))
        changed += 1
    return changed, skipped


def main() -> None:
    db = load_rewards_db()
    records = db.get("records", [])
    print(f"总记录数 {len(records)}")
    changed, skipped = _convert(records)
    print(f"待转换为人均 {changed}，跳过 {skipped}")
    if DRY_RUN:
        print("DRY_RUN=1 未写入；设置 DRY_RUN=0 应用")
    else:
        save_rewards_db(db)
        print("已写入 rewards.json")


if __name__ == "__main__":
    main()