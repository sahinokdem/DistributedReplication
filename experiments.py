#!/usr/bin/env python3
"""
CENG 465 - Consistency experiments on single-leader PostgreSQL replication.
Runs ON THE LEADER VM. Writes go to the leader; reads come from the follower.
All results are logged into experiment_runs / read_log /
consistency_observations / replication_lag_samples.
"""
import time, json, psycopg2

from config import LEADER, FOLLOWER   # settings come from .env / environment

def connect(cfg, name):
    c = psycopg2.connect(**cfg)
    c.autocommit = True
    print(f"[OK] {name} baglantisi kuruldu ({cfg['host']})")
    return c


# ---------------- log helpers ----------------
def start_run(leader, name, params):
    with leader.cursor() as cur:
        cur.execute("INSERT INTO experiment_runs(experiment_name, parameters) "
                    "VALUES (%s, %s::jsonb) RETURNING run_id", (name, json.dumps(params)))
        return cur.fetchone()[0]

def end_run(leader, run):
    with leader.cursor() as cur:
        cur.execute("UPDATE experiment_runs SET ended_at = clock_timestamp() WHERE run_id = %s", (run,))

def log_read(leader, run, pid, version, src):
    with leader.cursor() as cur:
        cur.execute("INSERT INTO read_log(run_id, product_id, version_seen, read_from, client_id) "
                    "VALUES (%s, %s, %s, %s, 'exp-client')", (run, pid, version, src))

def record_obs(leader, run, pid, metric, value):
    with leader.cursor() as cur:
        cur.execute("INSERT INTO consistency_observations(run_id, product_id, metric, value_num) "
                    "VALUES (%s, %s, %s, %s)", (run, pid, metric, value))

def sample_lag(leader, note):
    with leader.cursor() as cur:
        cur.execute("""SELECT client_addr::text, state, sent_lsn, replay_lsn,
                              pg_wal_lsn_diff(sent_lsn, replay_lsn),
                              write_lag, flush_lag, replay_lag
                       FROM pg_stat_replication""")
        rows = cur.fetchall()
        for r in rows:
            cur.execute("""INSERT INTO replication_lag_samples
                           (replica_addr, state, sent_lsn, replay_lsn, lag_bytes,
                            write_lag, flush_lag, replay_lag)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", r)
    if rows:
        print(f"   [lag {note}] lag_bytes={rows[0][4]}  replay_lag={rows[0][7]}")


# ---------------- DENEY 1: Eventual Consistency ----------------
def exp_eventual(leader, follower):
    print("\n=== DENEY 1: Eventual Consistency ===")
    run = start_run(leader, 'eventual', {"poll_interval_s": 1, "timeout_s": 60})
    sku = f"EVT-{int(time.time())}"
    with leader.cursor() as cur:
        cur.execute("INSERT INTO products(sku,name,stock_qty,price) "
                    "VALUES (%s,'Eventual Test',5,9.99) RETURNING product_id", (sku,))
        pid = cur.fetchone()[0]
    t0 = time.monotonic()
    print(f"Leader'a yeni kayit yazildi: product_id={pid}  (t=0.00s)")
    converged = None
    for _ in range(60):
        with follower.cursor() as fc:
            fc.execute("SELECT version FROM products WHERE product_id=%s", (pid,))
            row = fc.fetchone()
        el = time.monotonic() - t0
        log_read(leader, run, pid, row[0] if row else None, 'follower')
        print(f"   t={el:5.2f}s  follower goruyor mu? {'EVET' if row else 'hayir'}")
        if row:
            converged = el
            break
        time.sleep(1)
    if converged is not None:
        record_obs(leader, run, pid, 'convergence_seconds', converged)
        print(f">> Follower {converged:.2f} saniye sonra yakinsadi.")
    end_run(leader, run)


# ---------------- DENEY 2: Monotonic Reads ----------------
def exp_monotonic(leader, follower):
    print("\n=== DENEY 2: Monotonic Reads ===")
    run = start_run(leader, 'monotonic', {"updates": 5})
    sku = f"MON-{int(time.time())}"
    with leader.cursor() as cur:
        cur.execute("INSERT INTO products(sku,name,stock_qty,price,version) "
                    "VALUES (%s,'Monotonic Test',0,1.0,1) RETURNING product_id", (sku,))
        pid = cur.fetchone()[0]
    print(f"Kayit olusturuldu product_id={pid}, version=1")
    last_seen, violations, seq = -1, 0, []

    def read_follower():
        nonlocal last_seen, violations
        with follower.cursor() as fc:
            fc.execute("SELECT version FROM products WHERE product_id=%s", (pid,))
            r = fc.fetchone()
        v = r[0] if r else None
        log_read(leader, run, pid, v, 'follower')
        if v is not None:
            if v < last_seen:
                violations += 1
                print(f"   !! GERIYE OKUMA (monotonic ihlali): {last_seen} -> {v}")
            last_seen = max(last_seen, v)
        seq.append(v)
        return v

    for target in range(2, 6):
        with leader.cursor() as cur:
            cur.execute("UPDATE products SET version=%s, stock_qty=stock_qty+10, "
                        "last_updated=clock_timestamp() WHERE product_id=%s", (target, pid))
        print(f"Leader version={target} yapildi")
        for _ in range(3):
            print(f"   follower version_seen={read_follower()}")
            time.sleep(0.3)
    for _ in range(40):                       # follower 5'e ulasana kadar yakala
        v = read_follower()
        if v == 5:
            break
        time.sleep(0.5)
    record_obs(leader, run, pid, 'monotonic_violations', violations)
    print(f">> Geriye-okuma sayisi (monotonic ihlali): {violations}")
    print(f">> Gozlemlenen version dizisi: {seq}")
    end_run(leader, run)


# ---------------- DENEY 3: Read-After-Write ----------------
def exp_raw(leader, follower):
    print("\n=== DENEY 3: Read-After-Write ===")
    run = start_run(leader, 'raw', {})
    sku = f"RAW-{int(time.time())}"
    with leader.cursor() as cur:
        cur.execute("INSERT INTO products(sku,name,stock_qty,price) "
                    "VALUES (%s,'RAW Test',7,3.5) RETURNING product_id, version", (sku,))
        pid, written = cur.fetchone()
    with leader.cursor() as cur:                # hemen leader'dan geri oku
        cur.execute("SELECT version FROM products WHERE product_id=%s", (pid,))
        got = cur.fetchone()[0]
    log_read(leader, run, pid, got, 'leader')
    ok = (got == written)
    record_obs(leader, run, pid, 'raw_success', 1 if ok else 0)
    print(f"Leader'a yazildi version={written}; hemen leader'dan okundu version={got} "
          f"-> RAW {'BASARILI' if ok else 'BASARISIZ'}")
    t0 = time.monotonic()                        # follower ne zaman yansitiyor
    for _ in range(60):
        with follower.cursor() as fc:
            fc.execute("SELECT version FROM products WHERE product_id=%s", (pid,))
            r = fc.fetchone()
        log_read(leader, run, pid, r[0] if r else None, 'follower')
        if r:
            el = time.monotonic() - t0
            record_obs(leader, run, pid, 'follower_visible_seconds', el)
            print(f">> Ayni yazimi follower {el:.2f}s sonra gosterdi.")
            break
        time.sleep(0.5)
    end_run(leader, run)


# ---------------- DENEY 4: Concurrent / Rapid Writes ----------------
def exp_concurrent(leader, follower):
    print("\n=== DENEY 4: Concurrent / Rapid Writes ===")
    run = start_run(leader, 'concurrent', {"n": 10})
    base = f"CON-{int(time.time())}"
    sample_lag(leader, 'burst-oncesi')
    ids = []
    for i in range(10):
        with leader.cursor() as cur:
            cur.execute("INSERT INTO products(sku,name,stock_qty,price) "
                        "VALUES (%s,%s,%s,%s) RETURNING product_id",
                        (f"{base}-{i:02d}", f"Concurrent {i}", i, float(i)))
            ids.append(cur.fetchone()[0])
    print(f"Leader'a 10 kayit hizla yazildi: {ids}")
    sample_lag(leader, 'burst-sonrasi')          # lag zirvesini yakala
    with leader.cursor() as cur:
        cur.execute("SELECT product_id FROM write_log WHERE product_id = ANY(%s) ORDER BY log_id", (ids,))
        leader_order = [r[0] for r in cur.fetchall()]
    for _ in range(60):                          # follower hepsini alana kadar bekle
        with follower.cursor() as fc:
            fc.execute("SELECT count(*) FROM products WHERE product_id = ANY(%s)", (ids,))
            if fc.fetchone()[0] == len(ids):
                break
        time.sleep(0.5)
    with follower.cursor() as fc:
        fc.execute("SELECT product_id FROM write_log WHERE product_id = ANY(%s) ORDER BY log_id", (ids,))
        follower_order = [r[0] for r in fc.fetchall()]
    same = (leader_order == follower_order)
    record_obs(leader, run, None, 'order_preserved', 1 if same else 0)
    print(f"Leader yazim sirasi    : {leader_order}")
    print(f"Follower goruldugu sira: {follower_order}")
    print(f">> Siralama korundu mu? {'EVET' if same else 'HAYIR'}")
    end_run(leader, run)


def main():
    leader = connect(LEADER, "LEADER")
    follower = connect(FOLLOWER, "FOLLOWER")
    exp_eventual(leader, follower)
    exp_monotonic(leader, follower)
    exp_raw(leader, follower)
    exp_concurrent(leader, follower)
    print("\nTUM DENEYLER BITTI. Sonuclar log tablolarinda kayitli.")


if __name__ == "__main__":
    main()