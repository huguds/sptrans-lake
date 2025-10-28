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
import logging

# ----------------- Helpers -----------------
def get_var(name: str, default: str | None = None) -> str:
    """Busca primeiro em Airflow Variables, senão env var, senão default."""
    try:
        from airflow.models import Variable
        return Variable.get(name, default_var=os.getenv(name, default))
    except Exception:
        return os.getenv(name, default)


def duckdb_config(
    con: duckdb.DuckDBPyConnection,
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    use_ssl: bool,
):
    con.execute("INSTALL httpfs; LOAD httpfs;")
    host = endpoint_url.replace("http://", "").replace("https://", "")
    con.execute(f"SET s3_endpoint='{host}'")
    con.execute(f"SET s3_access_key_id='{access_key}'")
    con.execute(f"SET s3_secret_access_key='{secret_key}'")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
    con.execute("SET s3_url_style='path'")  # MinIO usa path-style


def ensure_bucket(s3, bucket: str):
    """Cria o bucket no MinIO se não existir."""
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def parse_ndjson_or_json_bytes(raw: bytes) -> List[Dict[str, Any]]:
    """
    Tenta carregar:
      - NDJSON (várias linhas JSON)
      - Único JSON
    Retorna uma lista de objetos (dicts).
    """
    text = raw.decode("utf-8", errors="ignore").strip()
    if not text:
        return []
    if "\n" in text:
        objs = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                objs.append(json.loads(line))
            except Exception:
                # fallback: tenta o payload inteiro como único JSON
                try:
                    return [json.loads(text)]
                except Exception:
                    pass
        if objs:
            return objs
    # único JSON
    try:
        return [json.loads(text)]
    except Exception:
        return []


def normalize_sptrans_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converte payloads do NiFi no formato:
      {"hr": "08:31", "records": [ { "route_code": [...], "route_id": [...], ... } ]}
    para linhas normalizadas.
    Também funciona se 'records' já vier como lista de objetos escalares.
    """
    rows: List[Dict[str, Any]] = []
    
    if payload is None:
        print("Payload is None")
        return rows

    hr = payload.get("hr", "")
    recs = payload.get("records", [])
    
    if not isinstance(recs, list):
        print(f"Ignorando payload com 'records' não é uma lista: {payload}")
        return rows

    for item in recs:
        if not isinstance(item, dict):
            print(f"Ignorando item que não é um dicionário: {item}")
            continue

        # se já é escalar (linha-a-linha)
        has_list = any(isinstance(v, list) for v in item.values())
        if not has_list:
            r = dict(item)
            r["source_hr_local"] = hr
            r["ingestion_ts"] = datetime.utcnow().isoformat()
            rows.append(r)
            continue

        # transpõe listas (repete escalares)
        keys = list(item.keys())
        maxlen = 0
        for v in item.values():
            maxlen = max(maxlen, (len(v) if isinstance(v, list) else 1))

        for i in range(maxlen):
            r: Dict[str, Any] = {}
            for k in keys:
                v = item[k]
                r[k] = (v[i] if isinstance(v, list) and i < len(v) else (v if not isinstance(v, list) else None))
            r["source_hr_local"] = hr
            r["ingestion_ts"] = datetime.utcnow().isoformat()
            rows.append(r)

    return rows


# ----------------- Task principal -----------------
def raw_to_trusted(**ctx):
    """
    Lê arquivos JSON/NDJSON da RAW (MinIO), normaliza e deduplica,
    e grava Parquet particionado por evt_date em trusted via DuckDB.
    """
    # Credenciais/endereços MinIO (Airflow Variables ou env)
    endpoint   = get_var("MINIO_ENDPOINT", "http://minio:9000")
    access_key = get_var("MINIO_ACCESS_KEY", "admin")
    secret_key = get_var("MINIO_SECRET_KEY", "minioadmin")

    # Partição alvo (ex.: ontem; ajuste para 0 se quiser hoje)
    reference_date = datetime.now().strftime("%Y-%m-%d")
    logging.info(f'A data de referência é: {reference_date}')

    # Buckets/prefixos
    raw_bucket     = "raw"
    raw_prefix     = f"olhovivo/posicao/{reference_date}"
    
    trusted_bucket = "trusted"
    trusted_prefix = f"sptrans/positions/"
    
    # S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # garante bucket de destino
    ensure_bucket(s3, trusted_bucket)

    # Lista objetos
    paginator = s3.get_paginator("list_objects_v2")
    page_it   = paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix)

    keys: List[str] = []
    for page in page_it:
        for obj in page.get("Contents", []):
            k = (obj.get("Key") or "").strip()
            if k and not k.endswith("/"):
                keys.append(k)
            logging.info(f'Objetos no Bucket: {k}')

    if not keys:
        raise AirflowSkipException(f"Nenhum arquivo encontrado em s3://{raw_bucket}/{raw_prefix}")

    # Lê e normaliza tudo
    all_rows: List[Dict[str, Any]] = []
    for k in keys:
        body = s3.get_object(Bucket=raw_bucket, Key=k)["Body"].read()
        payloads = parse_ndjson_or_json_bytes(body)
        for p in payloads:
            all_rows.extend(normalize_sptrans_records(p))

    if not all_rows:
        raise AirflowSkipException("Nenhum registro normalizado.")

    # Pandas DataFrame
    df = pd.DataFrame(all_rows)

    # ---------- Tipagem + defaults ----------
    must_cols = [
        "route_id","route_code","direction","dir_from","dir_to",
        "vehicle_id","in_service","event_ts","lat","lon","speed","stop_id","ingestion_ts"
    ]
    for c in must_cols:
        if c not in df.columns:
            df[c] = None

    def _to_int(x):
        try: return int(x) if pd.notna(x) else None
        except: return None

    def _to_bool(x):
        if isinstance(x, bool): return x
        if isinstance(x, (int, float)): return bool(int(x))
        if isinstance(x, str): return x.strip().lower() in ("1","true","t","yes","y","sim")
        return None

    def _to_float(x):
        try: return float(x) if pd.notna(x) else None
        except: return None

    df["route_id"]   = df["route_id"].apply(_to_int)
    df["route_code"] = df["route_code"].astype("string").where(df["route_code"].notna(), None)
    df["direction"]  = df["direction"].apply(_to_int)
    df["dir_from"]   = df["dir_from"].astype("string").where(df["dir_from"].notna(), None)
    df["dir_to"]     = df["dir_to"].astype("string").where(df["dir_to"].notna(), None)
    df["vehicle_id"] = df["vehicle_id"].apply(_to_int)
    df["in_service"] = df["in_service"].apply(_to_bool)
    df["event_ts"]   = pd.to_datetime(df["event_ts"], errors="coerce", utc=True)
    df["lat"]        = df["lat"].apply(_to_float)
    df["lon"]        = df["lon"].apply(_to_float)
    df["speed"]      = df["speed"].apply(_to_float).fillna(0.0)
    df["stop_id"]    = df["stop_id"].astype("string").where(df["stop_id"].notna(), None)
    df["ingestion_ts"] = pd.to_datetime(
        df.get("ingestion_ts", datetime.utcnow().isoformat()), errors="coerce", utc=True
    )

    # Filtra linhas inválidas para a chave
    df = df.dropna(subset=["route_id","vehicle_id","event_ts"])
    if df.empty:
        raise AirflowSkipException("Sem registros válidos após tipagem/filtragem.")

    # Dedup por (route_id, vehicle_id, event_ts) mantendo maior ingestion_ts
    df = df.sort_values(["route_id","vehicle_id","event_ts","ingestion_ts"]).drop_duplicates(
        subset=["route_id","vehicle_id","event_ts"], keep="last"
    )

    # ---------- Gravar em trusted (Parquet particionado) ----------
    con = duckdb.connect()
    duckdb_config(
        con,
        endpoint_url=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        use_ssl=endpoint.startswith("https"),
    )

    # registra o DataFrame nesta conexão
    con.register("norm_df", df)

    # URI COMPLETA no destino S3 (barra final é importante)
    trusted_uri = f"s3://{trusted_bucket}/{trusted_prefix}/"

    con.execute(f"""
        COPY (
          SELECT
            route_id, route_code, direction, dir_from, dir_to,
            vehicle_id, in_service, event_ts, lat, lon, speed, stop_id, ingestion_ts,
            CAST(date_trunc('day', event_ts) AS DATE) AS evt_date
          FROM norm_df
        )
        TO '{trusted_uri}'
        (FORMAT PARQUET, PARTITION_BY (evt_date), OVERWRITE_OR_IGNORE TRUE);
    """)

    print(f"[OK] Gravado {len(df)} registros em {trusted_uri}")


# ----------------- DAG -----------------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="dag-sptrans-raw-to-trusted",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    t_raw_to_trusted = PythonOperator(
        task_id="raw-to-trusted",
        python_callable=raw_to_trusted,
    )

    t_raw_to_trusted
