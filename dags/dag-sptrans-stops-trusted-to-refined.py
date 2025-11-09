# -*- coding: utf-8 -*-
from __future__ import annotations
import os
from datetime import datetime, timedelta
from typing import List

import duckdb
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator


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
    con.execute("SET s3_url_style='path'")


def stops_trusted_to_refined(**_):
    # MinIO
    endpoint   = get_var("MINIO_ENDPOINT", "http://minio:9000")
    access_key = get_var("MINIO_ACCESS_KEY", "admin")
    secret_key = get_var("MINIO_SECRET_KEY", "minioadmin")

    trusted_bucket = get_var("TRUSTED_BUCKET", "trusted")
    trusted_prefix = get_var("STOPS_TRUSTED_PREFIX", "sptrans/stops/")

    # partição do dia
    date_now = datetime.now() - timedelta(hours=3)
    reference_date = date_now.strftime("%Y-%m-%d")
    pattern = f"s3://{trusted_bucket}/{trusted_prefix}evt_date={reference_date}/*/*.parquet"  # evt_date + route_id

    con = duckdb.connect()
    duckdb_config(con, endpoint_url=endpoint, access_key=access_key, secret_key=secret_key, use_ssl=endpoint.startswith("https"))

    # lê parquet
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW src AS
        SELECT * FROM read_parquet('{pattern}', union_by_name=true);
    """)
    total = con.execute("SELECT COUNT(*) FROM src").fetchone()[0]
    if total == 0:
        raise AirflowSkipException(f"Nenhum Parquet encontrado em {pattern}")

    # tipagem mínima (por segurança)
    df = con.execute("""
        SELECT
          CAST(route_id AS INTEGER)              AS route_id,
          CAST(stop_id  AS INTEGER)              AS stop_id,
          stop_name::VARCHAR                     AS stop_name,
          address::VARCHAR                       AS address,
          CAST(lat AS DOUBLE)                    AS lat,
          CAST(lon AS DOUBLE)                    AS lon,
          CAST(ingestion_ts AS TIMESTAMPTZ)      AS ingestion_ts
        FROM src
        WHERE stop_id IS NOT NULL
    """).df()

    if df.empty:
        raise AirflowSkipException("Sem registros válidos para inserir.")

    # Postgres + DDL + UPSERT
    pg_url = get_var("POSTGRES_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/refined_sptrans")
    eng = create_engine(pg_url)

    ddl = """
    CREATE TABLE IF NOT EXISTS public.rf_stops (
      route_id INT,
      stop_id INT NOT NULL,
      stop_name TEXT,
      address   TEXT,
      lat DOUBLE PRECISION,
      lon DOUBLE PRECISION,
      ingestion_ts TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (stop_id)       -- se preferir por rota: PRIMARY KEY(route_id, stop_id)
    );
    """
    with eng.begin() as conn:
        conn.execute(text(ddl))
        # índices auxiliares (idempotentes)
        conn.execute(text("""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relname='idx_rf_stops_route' AND n.nspname='public'
          ) THEN
            CREATE INDEX idx_rf_stops_route ON public.rf_stops(route_id);
          END IF;
        END$$;
        """))

        tmp = "rf_stops_stg"
        df["stop_id"] = df["stop_id"].astype("int64")
        df["route_id"] = df["route_id"].astype("int32")
        df = df.dropna(subset=["stop_id", "route_id"]).drop_duplicates(["stop_id", "route_id"])
        df.to_sql(tmp, conn, if_exists="replace", index=False)

        conn.execute(text(f"""
            INSERT INTO public.rf_stops AS t
            (stop_id, route_id, stop_name, address, lat, lon)
            SELECT stop_id, route_id, stop_name, address, lat, lon
            FROM {tmp}
            ON CONFLICT (stop_id, route_id) DO UPDATE SET
            stop_name   = EXCLUDED.stop_name,
            address     = EXCLUDED.address,
            lat         = EXCLUDED.lat,
            lon         = EXCLUDED.lon,
            updated_at  = now();
            DROP TABLE {tmp};
        """))

    print(f"[OK] TRUSTED -> REFINED (rf_stops): {len(df)} linhas")

default_args = {"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)}

with DAG(
    dag_id="dag-sptrans-stops-trusted-to-refined",
    description="Stops TRUSTED (Parquet/MinIO) -> UPSERT Postgres (rf_stops)",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    PythonOperator(task_id="stops-trusted-to-refined", python_callable=stops_trusted_to_refined)
