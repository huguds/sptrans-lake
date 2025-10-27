# -*- coding: utf-8 -*-
from __future__ import annotations

import os, time
from datetime import datetime, timedelta, date
from typing import Any, Dict, Iterable, List, Optional

import boto3
import duckdb
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
import logging

# ----------------- Helpers -----------------
def get_var(name: str, default: str | None = None) -> str:
    try:
        from airflow.models import Variable
        return Variable.get(name, default_var=os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)

def duckdb_config(con: duckdb.DuckDBPyConnection, *, endpoint_url: str, access_key: str, secret_key: str, use_ssl: bool):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    host = endpoint_url.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{host}'")
    con.execute(f"SET s3_access_key_id='{access_key}'")
    con.execute(f"SET s3_secret_access_key='{secret_key}'")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
    con.execute("SET s3_url_style='path'")  # MinIO path-style

def reverse_geocode(lat: float, lon: float, key: str, session: requests.Session) -> Dict[str, Any]:
    if not key:
        return {}
    r = session.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={"latlng": f"{lat},{lon}", "key": key, "result_type": "street_address|route", "language": "pt-BR"},
        timeout=10,
    )
    r.raise_for_status()
    js = r.json()
    if js.get("status") != "OK" or not js.get("results"):
        return {}
    res = js["results"][0]
    comp = {t: c["long_name"] for c in res["address_components"] for t in c["types"]}
    return {
        "formatted_address": res.get("formatted_address"),
        "street": comp.get("route"),
        "number": comp.get("street_number"),
        "neighborhood": comp.get("sublocality") or comp.get("sublocality_level_1"),
        "city": comp.get("locality") or comp.get("administrative_area_level_2"),
        "state": comp.get("administrative_area_level_1"),
        "postal_code": comp.get("postal_code"),
    }

def _fmt_secs(s: float) -> str:
    s = max(0.0, float(s))
    h = int(s // 3600); s -= 3600*h
    m = int(s // 60);   s -= 60*m
    return f"{h:d}h{m:02d}m{s:04.1f}s" if h else (f"{m:d}m{s:04.1f}s" if m else f"{s:0.1f}s")

def log_progress(done: int, total: int, start_ts: float, prefix: str = "[GEOCODE]"):
    if total <= 0:
        return
    elapsed = time.time() - start_ts
    rate = (done / elapsed) if elapsed > 0 else 0.0
    remaining = max(0, total - done)
    eta = remaining / rate if rate > 0 else 0.0
    pct = 100.0 * done / total
    logging.getLogger("airflow.task").info(
        f"{prefix} {done}/{total} ({pct:5.1f}%) | ~{rate:0.2f}/s | elapsed {_fmt_secs(elapsed)} | ETA {_fmt_secs(eta)}"
    )

def get_watermark(conn, part_date: date) -> Optional[datetime]:
    row = conn.execute(
        text("SELECT last_event_ts FROM public.ingestion_state WHERE partition_date=:d"),
        {"d": part_date}
    ).fetchone()
    return row[0] if row and row[0] else None

def upsert_watermark(conn, part_date: date, last_ts: Optional[datetime], rows: int):
    conn.execute(text("""
        INSERT INTO public.ingestion_state(partition_date,last_event_ts,last_rowcount,updated_at)
        VALUES (:d,:ts,:n, now())
        ON CONFLICT (partition_date) DO UPDATE SET
          last_event_ts = GREATEST(COALESCE(public.ingestion_state.last_event_ts,'epoch'::timestamptz),
                                   COALESCE(EXCLUDED.last_event_ts,'epoch'::timestamptz)),
          last_rowcount = EXCLUDED.last_rowcount,
          updated_at    = now()
    """), {"d": part_date, "ts": last_ts, "n": rows})

# ----------------- Task principal -----------------
def trusted_to_postgres_geocode(**_):
    # MinIO / Trusted
    endpoint   = get_var("MINIO_ENDPOINT", "http://minio:9000")
    access_key = get_var("MINIO_ACCESS_KEY", "admin")
    secret_key = get_var("MINIO_SECRET_KEY", "minioadmin")

    bucket = get_var("TRUSTED_BUCKET", "trusted")
    prefix = get_var("TRUSTED_PREFIX", "sptrans/positions/")  # termina com '/'

    # Partição alvo = hoje (ajuste se quiser ontem)
    part_date = datetime.utcnow().date()
    # Aceita evt_date=YYYY-MM-DD/ e evt_date=YYYY-MM-DD 00:00:00+00/
    glob_pattern = f"s3://{bucket}/{prefix}evt_date={part_date.isoformat()}*/**.parquet"

    # Postgres
    pg_url = get_var("POSTGRES_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/refined_sptrans")
    eng = create_engine(pg_url)

    ddl_positions = """
    CREATE TABLE IF NOT EXISTS public.rf_positions (
      route_id INT NOT NULL,
      route_code TEXT,
      direction SMALLINT,
      dir_from TEXT,
      dir_to TEXT,
      vehicle_id INT NOT NULL,
      in_service BOOLEAN,
      event_ts TIMESTAMPTZ NOT NULL,
      lat DOUBLE PRECISION,
      lon DOUBLE PRECISION,
      speed DOUBLE PRECISION,
      stop_id TEXT,
      ingestion_ts TIMESTAMPTZ DEFAULT now(),
      formatted_address TEXT,
      street TEXT,
      number TEXT,
      neighborhood TEXT,
      city TEXT,
      state TEXT,
      postal_code TEXT,
      PRIMARY KEY (route_id, vehicle_id, event_ts)
    );
    """
    ddl_state = """
    CREATE TABLE IF NOT EXISTS public.ingestion_state(
      partition_date date PRIMARY KEY,
      last_event_ts timestamptz,
      last_rowcount bigint,
      updated_at timestamptz default now()
    );
    """
    ddl_cache = """
    CREATE TABLE IF NOT EXISTS public.geocode_cache(
      lat_r DOUBLE PRECISION NOT NULL,
      lon_r DOUBLE PRECISION NOT NULL,
      formatted_address TEXT,
      street TEXT,
      number TEXT,
      neighborhood TEXT,
      city TEXT,
      state TEXT,
      postal_code TEXT,
      updated_at timestamptz DEFAULT now(),
      PRIMARY KEY (lat_r, lon_r)
    );
    """

    with eng.begin() as conn:
        conn.execute(text(ddl_positions))
        conn.execute(text(ddl_state))
        conn.execute(text(ddl_cache))
        watermark = get_watermark(conn, part_date)

    # DuckDB: ler parquet(s) do dia
    con = duckdb.connect()
    duckdb_config(con, endpoint_url=endpoint, access_key=access_key, secret_key=secret_key, use_ssl=endpoint.startswith("https"))

    base_sql = f"SELECT * FROM read_parquet('{glob_pattern}', union_by_name=true)"

    where_clause = f"WHERE event_ts > TIMESTAMP '{watermark.isoformat()}'" if watermark else ""

    # Tipagem + filtro incremental
    con.execute(f"""
        WITH src AS (
          {base_sql}
        ),
        casted AS (
          SELECT
            CAST(route_id AS INTEGER)                  AS route_id,
            route_code::VARCHAR                        AS route_code,
            CAST(direction AS SMALLINT)                AS direction,
            dir_from::VARCHAR                          AS dir_from,
            dir_to::VARCHAR                            AS dir_to,
            CAST(vehicle_id AS INTEGER)                AS vehicle_id,
            CAST(in_service AS BOOLEAN)                AS in_service,
            CAST(event_ts AS TIMESTAMP WITH TIME ZONE) AS event_ts,
            CAST(lat AS DOUBLE)                        AS lat,
            CAST(lon AS DOUBLE)                        AS lon,
            CAST(speed AS DOUBLE)                      AS speed,
            NULLIF(CAST(stop_id AS VARCHAR), '')       AS stop_id,
            CAST(ingestion_ts AS TIMESTAMP WITH TIME ZONE) AS ingestion_ts
          FROM src
          WHERE route_id IS NOT NULL
            AND vehicle_id IS NOT NULL
            AND event_ts IS NOT NULL
        ),
        -- Mantém apenas o ÚLTIMO registro de cada veículo (reduz carga do geocode)
        latest_per_vehicle AS (
          SELECT *
          FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY event_ts DESC, ingestion_ts DESC) AS rn
            FROM casted
            {where_clause}
          )
          WHERE rn = 1
        )
        SELECT *
        FROM latest_per_vehicle
        LIMIT 1000;
    """)
    df = con.fetch_df()

    if df.empty:
        raise AirflowSkipException("Nada novo para processar (incremental / latest por veículo).")

    # Arredonda coord. para cache (reduz cardinalidade)
    df["lat_r"] = (df["lat"].astype(float).round(4))
    df["lon_r"] = (df["lon"].astype(float).round(4))

    # ---- Consulta cache já existente para estas coords
    keys_df = df[["lat_r","lon_r"]].dropna().drop_duplicates()
    with eng.begin() as conn:
        keys_df.to_sql("gc_keys_tmp", conn, if_exists="replace", index=False)
        cache_df = pd.read_sql("""
            SELECT k.lat_r, k.lon_r,
                   c.formatted_address, c.street, c.number, c.neighborhood, c.city, c.state, c.postal_code
            FROM gc_keys_tmp k
            LEFT JOIN geocode_cache c USING(lat_r, lon_r)
        """, conn)

    # Merge cache -> df
    df = df.merge(cache_df, on=["lat_r","lon_r"], how="left")

    # ---- Geocode apenas coords faltantes
    api_key = get_var("GOOGLE_MAPS_KEY", "")
    rate = float(get_var("GEOCODE_RATE_PER_SEC", "5"))
    min_interval = 1.0 / max(rate, 0.1)

    need_cols = ["formatted_address","street","number","neighborhood","city","state","postal_code"]
    missing = df[df[need_cols].isna().all(axis=1) & df["lat_r"].notna() & df["lon_r"].notna()][["lat_r","lon_r"]].drop_duplicates()

    if api_key and not missing.empty:
        logging.getLogger("airflow.task").info(f"[GEOCODE] {len(missing)} coordenadas novas para geocodificar")
        s = requests.Session()
        to_upsert = []
        total = len(missing)
        start_ts = time.time()
        last_call = 0.0

        for i, row in enumerate(missing.itertuples(index=False), 1):
            lat_r = float(row.lat_r); lon_r = float(row.lon_r)
            elapsed = time.time() - last_call
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            try:
                addr = reverse_geocode(lat_r, lon_r, api_key, s) or {}
            except Exception as e:
                logging.getLogger("airflow.task").warning(f"[GEOCODE] erro {lat_r},{lon_r}: {e}")
                addr = {}
            last_call = time.time()
            to_upsert.append({
                "lat_r": lat_r, "lon_r": lon_r,
                "formatted_address": addr.get("formatted_address"),
                "street": addr.get("street"),
                "number": addr.get("number"),
                "neighborhood": addr.get("neighborhood"),
                "city": addr.get("city"),
                "state": addr.get("state"),
                "postal_code": addr.get("postal_code"),
            })
            if i % 25 == 0 or i == total:
                log_progress(i, total, start_ts, prefix="[GEOCODE]")

        # upsert cache e recarrega para aplicar
        if to_upsert:
            up_df = pd.DataFrame(to_upsert)
            with eng.begin() as conn:
                up_df.to_sql("gc_upsert_tmp", conn, if_exists="replace", index=False)
                conn.execute(text("""
                    INSERT INTO geocode_cache AS g
                    SELECT lat_r, lon_r, formatted_address, street, number, neighborhood, city, state, postal_code
                    FROM gc_upsert_tmp
                    ON CONFLICT (lat_r, lon_r) DO UPDATE SET
                      formatted_address = COALESCE(EXCLUDED.formatted_address, g.formatted_address),
                      street            = COALESCE(EXCLUDED.street,            g.street),
                      number            = COALESCE(EXCLUDED.number,            g.number),
                      neighborhood      = COALESCE(EXCLUDED.neighborhood,      g.neighborhood),
                      city              = COALESCE(EXCLUDED.city,              g.city),
                      state             = COALESCE(EXCLUDED.state,             g.state),
                      postal_code       = COALESCE(EXCLUDED.postal_code,       g.postal_code),
                      updated_at        = now();
                    DROP TABLE gc_upsert_tmp;
                """))
                cache_df = pd.read_sql("""
                    SELECT k.lat_r, k.lon_r,
                           c.formatted_address, c.street, c.number, c.neighborhood, c.city, c.state, c.postal_code
                    FROM gc_keys_tmp k
                    LEFT JOIN geocode_cache c USING(lat_r, lon_r)
                """, conn)
            df = df.drop(columns=need_cols).merge(cache_df, on=["lat_r","lon_r"], how="left")

    # ---- UPSERT em rf_positions
    new_max_ts = pd.to_datetime(df["event_ts"]).max().to_pydatetime()

    with eng.begin() as conn:
        tmp = "rf_positions_stg"
        df[[
            "route_id","route_code","direction","dir_from","dir_to",
            "vehicle_id","in_service","event_ts","lat","lon","speed","stop_id","ingestion_ts",
            "formatted_address","street","number","neighborhood","city","state","postal_code"
        ]].to_sql(tmp, conn, if_exists="replace", index=False, method="multi", chunksize=1000)

        conn.execute(text("""
            INSERT INTO public.rf_positions AS p
            SELECT
              route_id, route_code, direction, dir_from, dir_to,
              vehicle_id, in_service, event_ts, lat, lon, speed, stop_id, ingestion_ts,
              formatted_address, street, number, neighborhood, city, state, postal_code
            FROM rf_positions_stg
            ON CONFLICT (route_id, vehicle_id, event_ts) DO UPDATE SET
              route_code = EXCLUDED.route_code,
              direction  = EXCLUDED.direction,
              dir_from   = EXCLUDED.dir_from,
              dir_to     = EXCLUDED.dir_to,
              in_service = EXCLUDED.in_service,
              lat        = EXCLUDED.lat,
              lon        = EXCLUDED.lon,
              speed      = EXCLUDED.speed,
              stop_id    = EXCLUDED.stop_id,
              ingestion_ts   = EXCLUDED.ingestion_ts,
              formatted_address = COALESCE(EXCLUDED.formatted_address, p.formatted_address),
              street            = COALESCE(EXCLUDED.street,            p.street),
              number            = COALESCE(EXCLUDED.number,            p.number),
              neighborhood      = COALESCE(EXCLUDED.neighborhood,      p.neighborhood),
              city              = COALESCE(EXCLUDED.city,              p.city),
              state             = COALESCE(EXCLUDED.state,             p.state),
              postal_code       = COALESCE(EXCLUDED.postal_code,       p.postal_code);
            DROP TABLE rf_positions_stg;
        """))
        upsert_watermark(conn, part_date, new_max_ts, int(len(df)))

    logging.getLogger("airflow.task").info(
        f"[OK] Incremental latest/vehicle: {len(df)} linhas -> rf_positions | watermark={new_max_ts.isoformat()}"
    )

# ----------------- DAG -----------------
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}


with DAG(
    dag_id="dag-sptrans-latest-per-vehicle-append",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    
    PythonOperator(task_id="trusted-to-postgres-geocode", python_callable=trusted_to_postgres_geocode)
