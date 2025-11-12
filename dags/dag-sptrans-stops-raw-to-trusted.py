# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json
from datetime import datetime, timedelta
from typing import Dict, Any, List

import boto3
import duckdb
import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

# ---------- Helpers ----------
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
    con.execute("SET s3_url_style='path'")  # MinIO


def ensure_bucket(s3, bucket: str):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def _flatten_any(x):
    """Retorna lista de dicts, achatando listas/listas de listas e wrappers."""
    out = []
    if x is None:
        return out
    if isinstance(x, dict):
        # se vier com wrappers comuns
        if "records" in x and isinstance(x["records"], list):
            for v in x["records"]:
                out.extend(_flatten_any(v))
        elif "data" in x and isinstance(x["data"], list):
            for v in x["data"]:
                out.extend(_flatten_any(v))
        else:
            out.append(x)
    elif isinstance(x, list):
        for v in x:
            out.extend(_flatten_any(v))
    # demais tipos são ignorados
    return out


def parse_ndjson_or_json_bytes(raw: bytes) -> List[Dict[str, Any]]:
    """
    Aceita:
      - JSON objeto
      - JSON array (até aninhado)
      - NDJSON (1 json por linha; cada linha pode ser obj ou array)
    Retorna SEMPRE uma lista achatada de dicts.
    """
    text = raw.decode("utf-8", errors="ignore").strip()
    if not text:
        return []

    # tenta JSON completo primeiro
    try:
        js = json.loads(text)
        return _flatten_any(js)
    except Exception:
        pass

    # tenta NDJSON
    items: List[Dict[str, Any]] = []
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            js = json.loads(ln)
            items.extend(_flatten_any(js))
        except Exception:
            # ignora linha inválida
            continue

    return items


def _to_int(x):
    try: return int(x) if pd.notna(x) else None
    except: return None


def _to_float(x):
    try: return float(x) if pd.notna(x) else None
    except: return None


def _to_str(x):
    if x is None or (isinstance(x,str) and x.strip()==""): return None
    return str(x)


# ---------- Task principal ----------
def stops_raw_to_trusted(**_):
    # MinIO
    endpoint   = get_var("MINIO_ENDPOINT", "http://minio:9000")
    access_key = get_var("MINIO_ACCESS_KEY", "admin")
    secret_key = get_var("MINIO_SECRET_KEY", "minioadmin")

    # buckets/prefix
    date_now = datetime.now() - timedelta(hours=3)
    reference_date = date_now.strftime("%Y-%m-%d")

    raw_bucket     = get_var("RAW_BUCKET", "raw")
    raw_prefix     = get_var("STOPS_RAW_PREFIX", f"sptrans/stops/{reference_date}")
    trusted_bucket = get_var("TRUSTED_BUCKET", "trusted")
    trusted_prefix = get_var("STOPS_TRUSTED_PREFIX", "sptrans/stops/")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    ensure_bucket(s3, trusted_bucket)

    # lista objetos no raw
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
        for obj in page.get("Contents", []):
            k = (obj.get("Key") or "").strip()
            if k and not k.endswith("/"):
                keys.append(k)
    if not keys:
        raise AirflowSkipException(f"Nenhum arquivo em s3://{raw_bucket}/{raw_prefix}")

    # carrega e normaliza
    rows: List[Dict[str, Any]] = []
    ing_ts = datetime.utcnow().isoformat()

    for k in keys:
        body = s3.get_object(Bucket=raw_bucket, Key=k)["Body"].read()
        items = parse_ndjson_or_json_bytes(body)

        for it in items:
            if not isinstance(it, dict):
                continue

            # mapeamento do payload da SPTrans (cp/np/ed/py/px) + route_id se existir
            rows.append({
                "route_id":     it.get("route_id"),
                "stop_id":      it.get("cp"),
                "stop_name":    it.get("np"),
                "address":      it.get("ed"),
                "lat":          it.get("py"),
                "lon":          it.get("px"),
                "ingestion_ts": ing_ts,
            })

    if not rows:
        raise AirflowSkipException("Nenhuma linha normalizada (stops).")

    df = pd.DataFrame(rows)

    # tipagem
    for c in ["route_id","stop_id"]:
        df[c] = df[c].apply(_to_int)
    df["stop_name"] = df["stop_name"].apply(_to_str)
    df["address"]   = df["address"].apply(_to_str)
    df["lat"]       = df["lat"].apply(_to_float)
    df["lon"]       = df["lon"].apply(_to_float)
    df["ingestion_ts"] = pd.to_datetime(df.get("ingestion_ts"), errors="coerce", utc=True)

    # remove nulos essenciais
    df = df.dropna(subset=["stop_id"])
    if df.empty:
        raise AirflowSkipException("Sem registros válidos após tipagem.")

    # DuckDB -> Parquet particionado (evt_date + route_id)
    con = duckdb.connect()
    duckdb_config(con, endpoint_url=endpoint, access_key=access_key, secret_key=secret_key, use_ssl=endpoint.startswith("https"))
    con.register("stops_df", df)

    trusted_uri = f"s3://{trusted_bucket}/{trusted_prefix}"
    con.execute(f"""
        COPY (
          SELECT
            route_id,
            stop_id,
            stop_name,
            address,
            lat,
            lon,
            ingestion_ts,
            CAST(date_trunc('day', COALESCE(ingestion_ts, now())) AS DATE) AS evt_date
          FROM stops_df
        )
        TO '{trusted_uri}'
        (FORMAT PARQUET, PARTITION_BY (evt_date, route_id), OVERWRITE_OR_IGNORE TRUE);
    """)
    print(f"[OK] Stops -> TRUSTED: {len(df)} linhas em {trusted_uri}")


# ---------- DAG ----------
default_args = {"owner":"airflow","retries":1,"retry_delay":timedelta(minutes=2)}


with DAG(
    dag_id="dag-sptrans-stops-raw-to-trusted",
    description="Stops RAW (MinIO JSON/NDJSON) -> TRUSTED (Parquet particionado)",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:
    PythonOperator(task_id="stops-raw-to-trusted", python_callable=stops_raw_to_trusted)
