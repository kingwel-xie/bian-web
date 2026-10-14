"""一次性迁移：把奖励记录里过时的位置型 tierIndex 校正为当前 rewardTiers 下标。

背景：奖励记录存的是创建时刻 rewardTiers 的数组下标。当后来在数组前部插入
新档位（例如给锦标赛补 1~5 名奖励）时，所有存量记录的下标会整体偏移，导致
奖励记录的档位/金额错配。本脚本按记录自带的 rankMin/rankMax 重新解析档位下标。

运行（容器内，容器已挂载 /app/app.py 与 /data）：
    DRY_RUN=1 python3 /tmp/fix_reward_tier_index.py   # 预览
    DRY_RUN=0 python3 /tmp/fix_reward_tier_index.py   # 写入
"""
import os

from app import _resolve_tier_index, load_jobs, load_rewards_db, save_rewards_db

DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"


def main() -> None:
    db = load_rewards_db()
    records = db.get("records", [])
    jobs = {j.get("id"): j for j in load_jobs()}
    changed = 0
    skipped = 0
    for r in records:
        job = jobs.get(r.get("jobId"))
        tiers = ((job or {}).get("payload") or {}).get("rewardTiers") or []
        if not tiers:
            skipped += 1
            continue
        old = r.get("tierIndex")
        new = _resolve_tier_index(tiers, r)
        if new != old:
            print(
                f"{r.get('jobName')} team={r.get('team')} id={r.get('id')} "
                f"rank {r.get('rankMin')}~{r.get('rankMax')}: tierIndex {old} -> {new}"
            )
            r["tierIndex"] = new
            changed += 1
    print(f"总记录 {len(records)}，修正 {changed}，跳过 {skipped}")
    if DRY_RUN:
        print("DRY_RUN=1 未写入；设置 DRY_RUN=0 应用")
    else:
        save_rewards_db(db)
        print("已写入 rewards.json")


if __name__ == "__main__":
    main()
