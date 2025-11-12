# Projeto de Engenharia de Dados — SPTRANS Olho Vivo

# 🛠️ Ferramentas

- NiFi → Ingestão/transformação de dados (API Olho Vivo → MinIO)
- Postgres → Camadas REFINED
- MinIO → Data Lake (raw e trusted)
- Airflow → Orquestração (jobs batch (DuckDB/enriquecimento)
- Python (Pandas/SQLAlchemy) → análises e utilitários locais

# 👷 Como rodar
## 1) Clonar o repositório
```bash
git clone https://github.com/huguds/sptrans-lake.git
cd sptrans-lake
```

## 2) Subir a stack infraestrutura + serviços (Docker Compose)
```bash
  docker compose up -d nifi minio mc postgres pgadmin metabase airflow-init airflow-webserver airflow-scheduler airflow-triggerer 
```

## 3) Subir a stack de observabilidade (Docker Compose)
```bash
docker compose up -d statsd_exporter prometheus grafana cadvisor postgres_exporter blackbox_exporter
```

## 4) (Opcional) Instalar libs Python locais
```pip install -r requirements.txt```

## **Observação**: após instalar bibliotecas Python na sua IDE/Jupyter, reinicie o kernel para reconhecer os pacotes, além disso é necessário criar um arquivo .env com todas as credenciais necessárias, por exemplo:
  - CONFLUENT_VERSION=7.6.1
  - MINIO_ROOT_USER=123
  - MINIO_ROOT_PASSWORD=abc

# 5) Informações sobre o projeto

## Objetivo:
- Construir um pipeline near real-time com camadas RAW → TRUSTED → REFINED:
  - NiFi consome a API Olho Vivo (SPTRANS), normaliza JSON, aplica defaults e salva arquivos no MinIO (raw).
  - Airflow + DuckDB responsável por processar os dados que estão na camada do MinIO (Raw) e enviar para a MinIO (Trusted) e posteriormente para o Postgres (Refined).
  - Airflow executa rotinas (ex.: deduplicação, enriquecimento, carga para refined_sptrans).
  - O Metabase se conecta no banco de dados do Postgres onde está localizado a camada Refined no qual é construído todos os gráficos com as métricas.

## Acesse:
  - NiFi: https://localhost:9443
  - MinIO Console: http://localhost:9001
  - pgAdmin: http://localhost:5433
  - Airflow: http://localhost:8080
  - Metabase: http://localhost:3000
  - Prometheus: http://localhost:9090
  - Grafana: http://localhost:3001

## 6) Armazenamento
- Camadas:
  - Raw - Responsável pelo armazenamento dos dados brutos produzidos pela requisição do NIFI à API da SPTrans.
  - Trusted - Responsável por armazenar os dados processados e tratados pelo DuckDB executado no Airflow.
  - Refined - Camada final de uso do usuário, no qual os dados estão devidamente tratados e padronizados. 

## 7) NiFi
- Importe o template em nifi/template/.
- Configure variáveis/Controller Services:
  - **Aws Credentials** - Passando as credenciais geradas para o futuro envio dos arquivos gerados
- MinIO (Access/Secret), endpoint http://minio:9000

- **Observação**: Para acessar a API é necessário se cadastrar para receber o Access Token para a requisição: https://www.sptrans.com.br/desenvolvedores/api-do-olho-vivo-guia-de-referencia/
- (Opcional) Rate limit com ControlRate (ex.: 1 msg / 2s).

## 8) Postgres (DBs/Tabelas)
- Para acessar:
Bases: refined_sptrans

Tabelas:
- REFINED:
```
-- 1) DB: refined_sptrans
CREATE TABLE IF NOT EXISTS public.rf_positions (
  route_id        INT NOT NULL,
  route_code      TEXT,
  direction       SMALLINT,
  dir_from        TEXT,
  dir_to          TEXT,
  vehicle_id      INT NOT NULL,
  in_service      BOOLEAN,
  event_ts        TIMESTAMPTZ NOT NULL,
  lat             DOUBLE PRECISION,
  lon             DOUBLE PRECISION,
  speed           DOUBLE PRECISION,
  stop_id         TEXT,
  ingestion_ts    TIMESTAMPTZ DEFAULT now(),

  -- Enriquecimento (geocode):
  formatted_address TEXT,
  street            TEXT,
  number            TEXT,
  neighborhood      TEXT,
  city              TEXT,
  state             TEXT,
  postal_code       TEXT,

  PRIMARY KEY (route_id, vehicle_id, event_ts)
);

-- Índices auxiliares (idempotentes)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = 'idx_rf_positions_event_ts' AND n.nspname='public') THEN
    CREATE INDEX idx_rf_positions_event_ts ON public.rf_positions (event_ts DESC);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = 'idx_positions_route_event' AND n.nspname='public') THEN
    CREATE INDEX idx_positions_route_event ON public.rf_positions (route_id, event_ts DESC);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE c.relname = 'idx_rf_positions_vehicle_event' AND n.nspname='public') THEN
    CREATE INDEX idx_rf_positions_vehicle_event ON public.rf_positions (vehicle_id, event_ts DESC);
  END IF;
END$$;
```

-- Paradas por linha (cada linha tem seu conjunto de paradas)
```
CREATE TABLE IF NOT EXISTS public.rf_stops (
  stop_id      BIGINT   NOT NULL,      -- cp
  route_id     INT      NOT NULL,      -- cl
  stop_name    TEXT,                   -- np
  address      TEXT,                   -- ed
  lat          DOUBLE PRECISION,       -- py
  lon          DOUBLE PRECISION,       -- px
  updated_at   TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (stop_id, route_id)
);

-- Índices úteis
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname='idx_rf_stops_route' AND n.nspname='public'
  ) THEN
    CREATE INDEX idx_rf_stops_route ON public.rf_stops(route_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relname='idx_rf_stops_latlon' AND n.nspname='public'
  ) THEN
    CREATE INDEX idx_rf_stops_latlon ON public.rf_stops(lat, lon);
  END IF;
END$$;
```

💡 Exemplos úteis

- Testar conexão do prometheus com a porta aberta de outros serviços (Se Necessário):
  ```sh
  docker exec -it prometheus sh
  wget -qO- http://nifi-n:9404/metrics
  wget -qO- http://statsd_exporter:9102/metrics | head
  ```

- Conferir no Postgres:
  ```
  docker compose exec postgres psql -U airflow -d trusted_sptrans \
    -c "SELECT COUNT(*) FROM public.positions;"
  ```

Deduplicação (REFINED) — exemplo
```
DELETE FROM refined_sptrans.public.positions a
USING refined_sptrans.public.positions b
WHERE a.ctid < b.ctid
  AND a.vehicle_id = b.vehicle_id
  AND a.event_ts  = b.event_ts;
```

🚀 KPIs
-  (veículos ativos, headway, atraso, velocidade média).
- Dashboards (Grafana/Metabase).
- Materialized Views e mais índices.
