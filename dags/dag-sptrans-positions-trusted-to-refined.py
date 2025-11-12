# -*- coding: utf-8 -*-
from __future__ import annotations

import os, logging
from datetime import datetime, timedelta, date
from typing import Optional

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

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

# ----------------- Task principal (SEM GEOCODE) -----------------
def positions_trusted_to_refined(**_):
    # MinIO / Trusted
    endpoint   = get_var("MINIO_ENDPOINT", "http://minio:9000")
    access_key = get_var("MINIO_ACCESS_KEY", "admin")
    secret_key = get_var("MINIO_SECRET_KEY", "minioadmin")

    bucket = get_var("TRUSTED_BUCKET", "trusted")
    prefix = get_var("TRUSTED_PREFIX", "sptrans/positions/")  # termina com '/'

    # Partição alvo = hoje (aceita evt_date=YYYY-MM-DD/ ou evt_date=YYYY-MM-DD 00:00:00+00/)
    part_date = datetime.utcnow().date()
    glob_pattern = f"s3://{bucket}/{prefix}evt_date={part_date.isoformat()}*/**.parquet"

    # Postgres
    pg_url = get_var("POSTGRES_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/refined_sptrans")
    eng = create_engine(pg_url)

    # Tabelas-alvo
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
      -- colunas de endereço (sem geocode => ficam NULL para novos registros)
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

    with eng.begin() as conn:
        conn.execute(text(ddl_positions))
        conn.execute(text(ddl_state))
        watermark = get_watermark(conn, part_date)

    # DuckDB: ler parquet(s) do dia
    con = duckdb.connect()
    duckdb_config(
        con,
        endpoint_url=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        use_ssl=endpoint.startswith("https"),
    )

    where_clause = f"WHERE event_ts > TIMESTAMP '{watermark.isoformat()}'" if watermark else ""
    con.execute(f"""
        WITH src AS (
          SELECT * FROM read_parquet('{glob_pattern}', union_by_name=true)
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
        ranked AS (
          SELECT * ,
                 ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY event_ts DESC, ingestion_ts DESC) AS rn
          FROM casted
          {where_clause}
        ),
        latest_per_vehicle AS (
          SELECT *
          FROM ranked
          WHERE rn = 1
        )
        SELECT * FROM latest_per_vehicle LIMIT 2000;
    """)
    df = con.fetch_df()

    if df.empty:
        raise AirflowSkipException("Nada novo para processar (incremental / latest por veículo).")

    # Garante colunas de endereço no DF (ficam None)
    addr_cols = ["formatted_address","street","number","neighborhood","city","state","postal_code"]
    for c in addr_cols:
        if c not in df.columns:
            df[c] = None

    # UPSERT em rf_positions
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
              -- mantém endereço antigo quando o novo é NULL (sem geocode)
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
        f"[OK] Latest/vehicle sem geocode: {len(df)} linhas -> rf_positions | watermark={new_max_ts.isoformat()}"
    )

# ----------------- DAG -----------------
default_args = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dag-sptrans-positions-trusted-to-refined",
    description="Trusted (Parquet/MinIO) -> Latest por veículo -> UPSERT Postgres (sem geocode)",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # ou "*/5 * * * *"
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="positions-trusted-to-refined",
        python_callable=positions_trusted_to_refined,
    )
