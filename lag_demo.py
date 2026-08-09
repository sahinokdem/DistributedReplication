#!/usr/bin/env python3
"""
CENG 465 - Replication lag demonstrasyonu.
Leader'a agir yazim yuku bindirirken follower'in lag'ini surekli ornekler.
Yuk bitince lag'in dususunu de kaydeder -> yukselip inen bir lag egrisi.
Sonuclar replication_lag_samples tablosuna yazilir.
Laptop'tan calistir (leader public IP).
"""
import time, threading, psycopg2

from config import LEADER   # settings come from .env / environment

LOAD_SECONDS = 25       # kac saniye yuk binecek
DRAIN_SECONDS= 15       # yuk bittikten sonra lag'in dususunu izleme suresi
SAMPLE_EVERY = 0.3      # ornekleme araligi (s)
ROWS_PER_BATCH = 50000  # her yazim turunda eklenen satir (WAL uretir)


def sampler(total_seconds):
    c = psycopg2.connect(**LEADER); c.autocommit = True
    t0 = time.monotonic()
    while time.monotonic() - t0 < total_seconds:
        with c.cursor() as cur:
            cur.execute("""SELECT client_addr::text, state, sent_lsn, replay_lsn,
                                  pg_wal_lsn_diff(sent_lsn, replay_lsn),
                                  write_lag, flush_lag, replay_lag
                           FROM pg_stat_replication""")
            for r in cur.fetchall():
                cur.execute("""INSERT INTO replication_lag_samples
                    (replica_addr, state, sent_lsn, replay_lsn, lag_bytes,
                     write_lag, flush_lag, replay_lag)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", r)
                mb = (r[4] or 0) / 1024 / 1024
                print(f"t={time.monotonic()-t0:5.1f}s  lag={mb:7.2f} MB  replay_lag={r[7]}")
        time.sleep(SAMPLE_EVERY)
    c.close()


def loader(seconds):
    c = psycopg2.connect(**LEADER); c.autocommit = True
    with c.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS lag_load(id bigserial PRIMARY KEY, blob text)")
    t0 = time.monotonic()
    while time.monotonic() - t0 < seconds:
        with c.cursor() as cur:
            cur.execute("INSERT INTO lag_load(blob) "
                        "SELECT repeat('x',100) FROM generate_series(1,%s)", (ROWS_PER_BATCH,))
    c.close()


if __name__ == "__main__":
    print("Lag demosu: leader'a agir yazim + surekli lag ornekleme...\n")
    st = threading.Thread(target=sampler, args=(LOAD_SECONDS + DRAIN_SECONDS,))
    st.start()
    loader(LOAD_SECONDS)
    print("\n>> Yuk durdu, lag'in dususu izleniyor...\n")
    st.join()

    # Disk temizligi (8GB disk kucuk) - load tablosunu sil, follower'a da yansir.
    c = psycopg2.connect(**LEADER); c.autocommit = True
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS lag_load")
    c.close()
    print("\nBitti. Zaman serisi replication_lag_samples tablosunda. lag_load tablosu silindi.")