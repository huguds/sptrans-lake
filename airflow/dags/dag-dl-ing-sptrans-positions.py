from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

PG_USER = "airflow"
PG_PASS = "airflow"
PG_PORT = 5432
PG_HOST = "postgres"

def ensure_database(db_name: str, host: str = PG_HOST):
    """Cria o database se não existir (usa conexão no DB 'postgres')."""
    conn = psycopg2.connect(
        host=host, port=PG_PORT, dbname="postgres",
        user=PG_USER, password=PG_PASS
    )
    conn.autocommit = True
    with conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (db_name,))
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{db_name}" ENCODING \'UTF8\' TEMPLATE template1')
    conn.close()

def make_engine(db):
    eng = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{db}",
        pool_pre_ping=True,
        connect_args={"options": "-c timezone=UTC"},
    )
    with eng.connect() as conn:
        conn.execute(text("select 1"))
    return eng

def reverse_geocode(lat, lon, key):
    r = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params={
            "latlng": f"{lat},{lon}",
            "key": key,
            "result_type": "street_address|route",
            "language": "pt-BR",
        },
        timeout=10
    )
    r.raise_for_status()
    js = r.json()
    if js.get("status") != "OK":
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

def process_positions(**kwargs):
    ensure_database("trusted_sptrans")
    ensure_database("refined_sptrans")

    trusted_engine = make_engine("trusted_sptrans")
    refined_engine = make_engine("refined_sptrans")

    df = pd.read_sql(
        text("SELECT * FROM public.positions ORDER BY event_ts DESC LIMIT 200"),
        trusted_engine
    )

    geo_maps_key = kwargs["params"].get("GOOGLE_MAPS_KEY", "")
    cache = {}
    for idx, row in df.iterrows():
        lat = row.get("lat")
        lon = row.get("lon")
        if pd.isna(lat) or pd.isna(lon):
            continue
        key = (round(float(lat), 4), round(float(lon), 4))
        if key not in cache:
            try:
                cache[key] = reverse_geocode(lat, lon, geo_maps_key) if geo_maps_key else {}
            except Exception:
                cache[key] = {}
        for k, v in cache[key].items():
            df.at[idx, k] = v

    cols = [
        "route_id", "route_code", "direction", "dir_from", "dir_to",
        "vehicle_id", "in_service", "event_ts", "ingestion_ts",
        "formatted_address", "postal_code"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df[cols].to_sql("positions", refined_engine, if_exists="replace", index=False)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "dag-dl-ing-sptrans-positions",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Processa posições e insere no PostgreSQL (refined)",
)

start = EmptyOperator(task_id="start", dag=dag)

process_task = PythonOperator(
    task_id="process_positions",
    python_callable=process_positions,
    params={
        "GOOGLE_MAPS_KEY": Variable.get('GOOGLE_MAPS_KEY'),
    },
    dag=dag,
)

end = EmptyOperator(task_id="end", dag=dag)

start >> process_task >> end