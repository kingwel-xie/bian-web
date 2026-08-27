import os
import app
from app import load_jobs, load_teams_db, save_teams_db, read_json, _team_member_key
from pathlib import Path
from datetime import datetime
import datetime as _dt

DRY_RUN = os.environ.get("DRY_RUN", "1") != "0"
TOP_N = 75
RECENT_N = 15
TEAM_NAME = "t75"
THR = [(0.33, 1), (0.50, 2), (0.67, 3)]


def _recency(j):
    fa = j.get("finishedAt") or ""
    if fa:
        return fa
    snaps = j.get("snapshots") or []
    return snaps[-1].get("timestamp", "") if snaps else (j.get("createdAt", "") or "")


def build_bands(reward_tiers):
    if not reward_tiers:
        return None
    st = sorted(reward_tiers, key=lambda t: int(t.get("rankMin") or 0))
    bands = []
    cursor = 1
    for t in st:
        rmin = int(t.get("rankMin") or 0)
        rmax = int(t.get("rankMax") or 0)
        if rmin > cursor:
            bands.append((cursor, rmin - 1))
        bands.append((rmin, rmax))
        cursor = rmax + 1
    return bands


def band_for(bands, rank):
    for (a, b) in bands:
        if a <= rank <= b:
            return (a, b)
    return None


def job_date(payload):
    s = (payload.get("activityEnd") or "").strip()
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date().isoformat()
    except Exception:
        return None


def snap_with_prefix(snaps, prefix):
    for s in snaps:
        t = s.get("timestamp", "") or ""
        if t.startswith(prefix):
            return s
    return None


def load_rows(snap):
    if not snap:
        return []
    jp = snap.get("json")
    if not jp:
        return []
    p = Path(str(jp))
    if not p.exists():
        return []
    data = read_json(p, {})
    rows = data.get("rows") if isinstance(data, dict) else []
    if not isinstance(rows, list):
        return []
    def seq(r):
        try:
            return int(r.get("sequence") or 999999)
        except Exception:
            return 999999
    return sorted(rows, key=seq)


def grade(r):
    try:
        return float(r.get("grade") or 0)
    except Exception:
        return 0.0


def nick(r):
    return (r.get("nickName") or "").strip().lower()


jobs = load_jobs()
spot = [j for j in jobs if j.get("status") == "completed"
         and (j.get("payload") or {}).get("market") == "spot"
         and (j.get("payload") or {}).get("rewardMode") in ("rank", "rank_last_volume")]
spot.sort(key=_recency, reverse=True)
spot = spot[:RECENT_N]
print(f"completed spot rank-tiered jobs (recent {RECENT_N}): {len(spot)}")

nick_tier = {}
jobs_used = []
tier_counts = {1: 0, 2: 0, 3: 0}

for j in spot:
    p = j.get("payload") or {}
    d = job_date(p)
    if not d:
        continue
    bands = build_bands(p.get("rewardTiers") or [])
    if not bands:
        continue
    snaps = j.get("snapshots") or []
    s17 = snap_with_prefix(snaps, f"{d}T1759")
    s15 = snap_with_prefix(snaps, f"{d}T1559")
    if not s17 or not s15:
        continue
    r17 = load_rows(s17)
    r15 = load_rows(s15)
    if not r17 or not r15:
        continue
    g15 = {}
    for r in r15:
        k = nick(r)
        if k:
            g15[k] = grade(r)
    band_mean = {}
    for (a, b) in bands:
        xs = [grade(r) for r in r17 if a <= int(r.get("sequence") or 0) <= b]
        band_mean[(a, b)] = (sum(xs) / len(xs)) if xs else 0.0
    top75 = r17[:TOP_N]
    job_hits = {1: 0, 2: 0, 3: 0}
    for r in top75:
        k = nick(r)
        if not k:
            continue
        rk = int(r.get("sequence") or 0)
        bd = band_for(bands, rk)
        if not bd:
            continue
        M = band_mean[bd]
        if M <= 0:
            continue
        delta = grade(r) - g15.get(k, 0)
        t = 3 if delta > M * 0.67 else 2 if delta > M * 0.50 else 1 if delta > M * 0.33 else 0
        if t > 0:
            job_hits[t] += 1
            if t > nick_tier.get(k, 0):
                nick_tier[k] = t
    for t in (1, 2, 3):
        tier_counts[t] += job_hits[t]
    jobs_used.append({
        "id": j.get("id"), "rid": p.get("resourceId"), "date": d,
        "bands": bands, "band_mean": {f"{a}-{b}": round(v) for (a, b), v in band_mean.items()},
        "hits": job_hits, "top75": len(top75),
    })

print(f"\njobs with both snapshots: {len(jobs_used)}")
for ju in jobs_used:
    print(f"  {ju['id']} rid={ju['rid']} {ju['date']} bands={ju['bands']}")
    print(f"      band_means={ju['band_mean']} per-job hits={ju['hits']}")

print(f"\nunique nicknames with tier>0: {len(nick_tier)}")
for t in (1, 2, 3):
    c = sum(1 for v in nick_tier.values() if v == t)
    print(f"  tier {t}: {c} nicknames (cumulative job-hits {tier_counts[t]})")

db = load_teams_db()
teams = db.get("teams") or []
t75 = next((t for t in teams if t.get("name") == TEAM_NAME), None)
if not t75:
    print(f"\nNo team '{TEAM_NAME}' found; abort.")
else:
    members = t75.get("members") or []
    dist = {0: 0, 1: 0, 2: 0, 3: 0}
    for m in members:
        key = _team_member_key(m)
        t = nick_tier.get(key, 0)
        m["madDog"] = t
        dist[t] = dist.get(t, 0) + 1
    print(f"\nt75 members: {len(members)} | madDog distribution: {dist}")
    print("tier-3 members:")
    cnt = 0
    for m in members:
        if m.get("madDog") == 3:
            print(f"  {m.get('nickname')} ({m.get('userId')}) w={m.get('weight')}")
            cnt += 1
            if cnt >= 15:
                break
    if DRY_RUN:
        print("\n[DRY RUN] not saving. Re-run with DRY_RUN=0 to persist.")
    else:
        db["updatedAt"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
        save_teams_db(db)
        print("\nSAVED.")
print("DONE")
