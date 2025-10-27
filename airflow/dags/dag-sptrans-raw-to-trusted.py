from datetime import datetime, timedelta
import os, duckdb, pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow.operators.empty import EmptyOperator


MINIO = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    "key": os.getenv("MINIO_ACCESS_KEY", "admin"),
    "secret": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
}
PG_URL = os.getenv("POSTGRES_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/refined_sptrans")

RAW_PATH = "s3://raw/olhovivo/posicao"
SLV = "s3://trusted/olhovivo/posicao/"

REFERENCE_DATE_STR = datetime.now().strftime('%Y-%m-%d')

def _duckdb_config(con):
    con.execute("install httpfs; load httpfs;")
    con.execute(f"SET s3_endpoint='{MINIO['endpoint'].replace('http://','').replace('https://','')}'")
    con.execute(f"SET s3_access_key_id='{MINIO['key']}'")
    con.execute(f"SET s3_secret_access_key='{MINIO['secret']}'")
    con.execute("SET s3_use_ssl=false; SET s3_url_style='path';")

def raw_to_trusted(**ctx):
    con = duckdb.connect()
    _duckdb_config(con)

    # Caminho-base (MinIO/S3) — ajuste se necessário
    src_glob = f"{RAW_PATH}/{REFERENCE_DATE_STR}/*.json"        # ex.: s3://raw/olhovivo/posicao/data-de-referencia/*.json

    # 1) Tenta ler como NDJSON (um objeto por linha)
    # 2) Se der erro, tenta o parser automático do DuckDB
    # Obs: DuckDB precisa de try/except no Python, não no SQL.
    def create_raw_view_ndjson():
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW raw_positions AS
            SELECT * FROM read_json(
              '{src_glob}',
              format='newline_delimited',     -- NDJSON
              maximum_object_size=16777216    -- 16 MB (ajuste se seus objetos forem maiores)
            );
        """)

    def create_raw_view_auto():
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW raw_positions AS
            SELECT * FROM read_json_auto(
              '{src_glob}',
              maximum_object_size=16777216
            );
        """)

    try:
        create_raw_view_ndjson()
    except Exception:
        create_raw_view_auto()

    con.execute("""
        CREATE OR REPLACE TEMP VIEW t AS
        SELECT
          CAST(route_id AS INTEGER)                        AS route_id,
          CAST(route_code AS VARCHAR)                      AS route_code,
          CAST(direction AS SMALLINT)                      AS direction,
          CAST(dir_from AS VARCHAR)                        AS dir_from,
          CAST(dir_to AS VARCHAR)                          AS dir_to,
          CAST(vehicle_id AS INTEGER)                      AS vehicle_id,
          COALESCE(CAST(in_service AS BOOLEAN), TRUE)      AS in_service,
          /* event_ts ISO 8601 '...Z' -> TIMESTAMP (UTC) */
          CAST(strptime(event_ts, '%Y-%m-%dT%H:%M:%SZ') AS TIMESTAMP) AS event_ts,
          CAST(lat AS DOUBLE)                              AS lat,
          CAST(lon AS DOUBLE)                              AS lon,
          COALESCE(CAST(speed AS DOUBLE), 0.0)             AS speed,
          NULLIF(CAST(stop_id AS VARCHAR), '')             AS stop_id,
          now()::TIMESTAMP                                 AS ingestion_ts
        FROM raw_positions;
    """)

    con.execute("""
        CREATE OR REPLACE TEMP VIEW t_dedup AS
        SELECT * EXCLUDE (rn) FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY route_id, vehicle_id, event_ts
              ORDER BY ingestion_ts DESC
            ) AS rn
          FROM t
        )
        WHERE rn = 1;
    """)

    con.execute(f"""
        COPY (
          SELECT
            *,
            date_trunc('day', event_ts) AS evt_date
          FROM t_dedup
        )
        TO '{SLV}'
        (FORMAT PARQUET, PARTITION_BY (evt_date), OVERWRITE_OR_IGNORE TRUE);
    """)


def silver_to_postgres(**ctx):
    con = duckdb.connect()
    _duckdb_config(con)
    df = con.execute(f"SELECT * FROM read_parquet('{SLV}**/*.parquet')").df()
    # upsert simples no Postgres
    if df.empty: return
    from sqlalchemy import text, create_engine
    eng = create_engine(PG_URL)
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.positions (
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
              PRIMARY KEY (route_id, vehicle_id, event_ts)
            );
        """))
        # escreve em staging e faz MERGE
        tmp = "positions_stg"
        df.to_sql(tmp, conn, if_exists="replace", index=False)
        conn.execute(text(f"""
          INSERT INTO public.positions AS p
          SELECT route_id, route_code, direction, dir_from, dir_to,
                 vehicle_id, in_service, event_ts, lat, lon, speed,
                 stop_id, ingestion_ts
          FROM {tmp}
          ON CONFLICT (route_id, vehicle_id, event_ts) DO UPDATE SET
            route_code=EXCLUDED.route_code,
            direction=EXCLUDED.direction,
            dir_from=EXCLUDED.dir_from,
            dir_to=EXCLUDED.dir_to,
            in_service=EXCLUDED.in_service,
            lat=EXCLUDED.lat, lon=EXCLUDED.lon, speed=EXCLUDED.speed,
            stop_id=EXCLUDED.stop_id, ingestion_ts=EXCLUDED.ingestion_ts;
          DROP TABLE {tmp};
        """))

with DAG(
    "dag-sptrans-raw-to-trusted",
    start_date=datetime(2025,1,1),
    schedule_interval=None,  # "*/5 * * * *"
    catchup=False,
    default_args={"owner":"airflow", "retries":1, "retry_delay":timedelta(minutes=2)},
) as dag:

    start = EmptyOperator(task_id="start", dag=dag)
    
    t1 = PythonOperator(task_id="bronze-to-silver", python_callable=raw_to_trusted)
    
    end = EmptyOperator(task_id="end", dag=dag)
    
    # t2 = PythonOperator(task_id="silver_to_postgres", python_callable=silver_to_postgres)
    # t1 >> t2
    start >> t1 >> end
