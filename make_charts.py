#!/usr/bin/env python3
"""
CENG 465 - Report figure generator.
Reads the REAL logged data from the database tables that experiments.py and
lag_demo.py populated (replication_lag_samples, read_log, consistency_observations)
and produces the report figures as PNG files. All labels in English.

Run on the laptop:   python3 make_charts.py
Requires:            pip install matplotlib psycopg2-binary --break-system-packages
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psycopg2
from pathlib import Path

from config import LEADER as DB   # all tables live on the leader; follower is a read-only copy

# Figures are written into docs/ so the README can reference them directly.
DOCS = Path(__file__).with_name("docs")
DOCS.mkdir(exist_ok=True)


def fetch(sql, params=None):
    c = psycopg2.connect(**DB)
    with c.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
    c.close()
    return rows


# ---------- FIGURE 1: Replication lag over time ----------
def fig_replication_lag():
    rows = fetch("""SELECT sampled_at, lag_bytes, EXTRACT(EPOCH FROM replay_lag)
                    FROM replication_lag_samples
                    WHERE state = 'streaming'
                    ORDER BY sampled_at""")
    if not rows:
        print("[!] replication_lag_samples bos - lag_demo.py'yi calistirdin mi?")
        return

    # En uzun kesintisiz olcum blogunu sec (lag_demo kosumu). 3sn'den buyuk
    # bosluklar farkli kosumlari ayirir.
    segments, cur = [], [rows[0]]
    for prev, r in zip(rows, rows[1:]):
        if (r[0] - prev[0]).total_seconds() > 3:
            segments.append(cur); cur = [r]
        else:
            cur.append(r)
    segments.append(cur)
    seg = max(segments, key=len)

    t0 = seg[0][0]
    t  = [(s[0] - t0).total_seconds() for s in seg]
    mb = [(s[1] or 0) / 1024 / 1024 for s in seg]
    rl = [float(s[2]) if s[2] is not None else 0.0 for s in seg]

    # Yuk bolgesi (yaklasik): lag > 0.5 MB kaldigi sure
    load_end = max([t[i] for i in range(len(t)) if mb[i] > 0.5], default=0)
    baseline = min([v for v in rl if v > 0], default=0)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6.5), sharex=True)

    ax1.plot(t, mb, marker="o", ms=3, color="#1f5fbf", lw=1.5)
    if load_end > 0:
        ax1.axvspan(0, load_end, color="#ffd9d9", alpha=0.6, label="Heavy write load (approx.)")
        ax1.legend(loc="upper right", fontsize=9)
    ax1.set_ylabel("Byte lag (MB)\nsent_lsn − replay_lsn")
    ax1.set_title("Leader–Follower Replication Lag (leader: Asia, follower: Europe)")
    ax1.grid(alpha=0.3)

    ax2.plot(t, rl, marker="o", ms=3, color="#c0392b", lw=1.5)
    if load_end > 0:
        ax2.axvspan(0, load_end, color="#ffd9d9", alpha=0.6)
    if baseline > 0:
        ax2.axhline(baseline, color="gray", ls="--", lw=1,
                    label=f"Baseline network delay ≈ {baseline:.3f}s")
        ax2.legend(loc="upper right", fontsize=9)
    ax2.set_ylabel("replay_lag (seconds)")
    ax2.set_xlabel("Time (s)")
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(DOCS / "fig1_replication_lag.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[ok] fig1_replication_lag.png  (peak lag {max(mb):.2f} MB, "
          f"max replay_lag {max(rl):.2f}s, baseline {baseline:.3f}s)")


# ---------- FIGURE 2: Monotonic reads ----------
def fig_monotonic():
    run = fetch("""SELECT run_id FROM experiment_runs
                   WHERE experiment_name='monotonic' ORDER BY run_id DESC LIMIT 1""")
    if not run:
        print("[!] monotonic kosumu yok - experiments.py'yi calistirdin mi?")
        return
    run_id = run[0][0]
    rows = fetch("""SELECT read_at, version_seen FROM read_log
                    WHERE run_id=%s AND read_from='follower' AND version_seen IS NOT NULL
                    ORDER BY read_at""", (run_id,))
    if not rows:
        print("[!] monotonic read_log bos")
        return
    t0 = rows[0][0]
    t  = [(r[0] - t0).total_seconds() for r in rows]
    v  = [r[1] for r in rows]
    violations = sum(1 for i in range(1, len(v)) if v[i] < v[i-1])

    plt.figure(figsize=(9, 4.5))
    plt.step(t, v, where="post", marker="o", color="#2e8b57", lw=1.8)
    plt.title(f"Monotonic Reads: Version Observed on Follower (violations = {violations})")
    plt.xlabel("Time (s)")
    plt.ylabel("Version observed on follower")
    plt.yticks(range(min(v), max(v) + 1))
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(DOCS / "fig2_monotonic_reads.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"[ok] fig2_monotonic_reads.png  (sequence {v}, violations {violations})")


# ---------- FIGURE 3: Consistency latency summary ----------
def fig_latency_summary():
    rows = fetch("""SELECT metric, AVG(value_num)
                    FROM consistency_observations
                    WHERE metric IN ('convergence_seconds','follower_visible_seconds')
                    GROUP BY metric""")
    base = fetch("""SELECT MIN(EXTRACT(EPOCH FROM replay_lag))
                    FROM replication_lag_samples WHERE replay_lag > interval '0'""")
    d = {m: float(v) for m, v in rows}
    labels, vals = [], []
    if base and base[0][0]:
        labels.append("Baseline\nnetwork RTT"); vals.append(float(base[0][0]))
    if "convergence_seconds" in d:
        labels.append("Eventual:\nconvergence"); vals.append(d["convergence_seconds"])
    if "follower_visible_seconds" in d:
        labels.append("Read-after-write:\nfollower delay"); vals.append(d["follower_visible_seconds"])
    if not vals:
        print("[!] consistency_observations bos"); return

    plt.figure(figsize=(7, 4.5))
    bars = plt.bar(labels, vals, color=["#888", "#1f5fbf", "#c0392b"][:len(vals)])
    for b, val in zip(bars, vals):
        plt.text(b.get_x() + b.get_width()/2, val, f"{val:.3f}s",
                 ha="center", va="bottom", fontsize=9)
    plt.title("Consistency Latency Summary")
    plt.ylabel("Seconds")
    plt.grid(alpha=0.3, axis="y")
    plt.tight_layout()
    plt.savefig(DOCS / "fig3_latency_summary.png", dpi=150, bbox_inches="tight")
    plt.close()
    print("[ok] fig3_latency_summary.png")


if __name__ == "__main__":
    fig_replication_lag()
    fig_monotonic()
    fig_latency_summary()
    print("\nDone. Figures written to docs/")