# Projeto de Engenharia de Dados — SPTRANS Olho Vivo

# 🛠️ Ferramentas

- NiFi → Ingestão/transformação de dados (API Olho Vivo → Kafka/MinIO)
- Kafka → Streaming de mensagens (tópico sptrans.trusted)
- Kafka Connect (JDBC Sink) → Upsert no Postgres
- Postgres → Camadas TRUSTED e REFINED
- MinIO → Data Lake (raw, trusted, refined)
- Airflow → Orquestração (jobs batch/enriquecimento)
- Python (Pandas/SQLAlchemy) → análises e utilitários locais

# 👷 Como rodar
## 1) Clonar o repositório
```bash

git clone https://github.com/huguds/sptrans-lake.git
cd sptrans-lake
```

## 2) Subir a stack principal (Docker Compose)
```
docker compose --env-file .env.local up -d \
  zookeeper kafka-broker postgres pgadmin minio mc nifi \
  airflow-init airflow-webserver airflow-scheduler airflow-triggerer \
  kafka-connect
```

## 3) (Opcional) Instalar libs Python locais
```pip install -r requirements.txt  # se existir```

- **Observação**: após instalar bibliotecas Python na sua IDE/Jupyter, reinicie o kernel para reconhecer os pacotes, além disso é necessário criar um arquivo .env com todas as credenciais necessárias, por exemplo:
  - CONFLUENT_VERSION=7.6.1
  - MINIO_ROOT_USER=123
  - MINIO_ROOT_PASSWORD=abc

# ℹ️ Informações sobre o projeto

## Objetivo:
- Construir um pipeline near real-time com camadas RAW → TRUSTED → REFINED:
  - NiFi consome a API Olho Vivo (SPTRANS), normaliza JSON, aplica defaults e salva arquivos no MinIO (raw/trusted/refined);
  - publica eventos no Kafka (sptrans.trusted).
  - Kafka Connect (JDBC Sink) lê o tópico e faz UPSERT em trusted_sptrans.public.positions.
  - Airflow executa rotinas (ex.: deduplicação, enriquecimento, carga para refined_sptrans).

✅ Passo a passo (resumo)
## 1) Deploy da Infra:
```
docker compose up -d airflow-webserver  airflow-scheduler airflow-triggerer airflow-init airflow-cli nifi minio postgres pgadmin zookeeper broker kafka-connect
```

Acesse:

- NiFi: https://localhost:9443
- MinIO Console: http://localhost:9001
- pgAdmin: http://localhost:5433
- Airflow: http://localhost:8080
- Kafka Connect: http://localhost:8083

## 2) NiFi
- Importe o template em nifi/template/.
- Configure variáveis/Controller Services:
  - **Aws Credentials** - Passando as credenciais geradas para o futuro envio dos arquivos gerados
  - **Kafka3ConnectionService**
- MinIO (Access/Secret), endpoint http://minio:9000
- Kafka (Bootstrap: kafka-broker:29092, tópico sptrans.trusted)

- **Observação**: Para acessar a API é necessário se cadastrar para receber o Access Token para a requisição: https://www.sptrans.com.br/desenvolvedores/api-do-olho-vivo-guia-de-referencia/
- (Opcional) Rate limit com ControlRate (ex.: 1 msg / 2s).

## 3) Postgres (DBs/Tabelas)

Bases: trusted_sptrans e refined_sptrans.

Tabelas:
- TRUSTED:
```
CREATE TABLE public.positions (
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
  ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (route_id, vehicle_id, event_ts)
);
```

- REFINED:
```
CREATE TABLE public.positions (
   route_id INT,
   route_code VARCHAR(50),
   direction INT,
   dir_from VARCHAR(100),
   dir_to VARCHAR(100),
   vehicle_id INT,
   in_service BOOLEAN,
   event_ts TIMESTAMP,
   speed FLOAT,
   stop_id VARCHAR(50),
   ingestion_ts TIMESTAMP,
   formatted_address VARCHAR(255),
   postal_code VARCHAR(20)
);
```

## 4) Kafka & Kafka Connect

- Listar os conectores para identificar se há algum ativo no momento (Kafka Connect):
```
curl -s http://localhost:8083/connectors | jq .
```

- Listar os tópicos (Kafka):
```
kafka-topics --bootstrap-server localhost:9092 --list
```

- Criar/garantir o tópico (Kafka):
```
docker exec -it kafka-broker bash -lc \
  "kafka-topics --bootstrap-server kafka-broker:29092 \
   --create --if-not-exists --topic sptrans.trusted \
   --replication-factor 1 --partitions 1"
```

- Deletar tópico caso necessário (Kafka):
```
kafka-topics --delete --topic dlq.pg.positions --bootstrap-server localhost:9092
```

- Leitura das mensagem (Kafka):
```
docker exec -it broker bash -lc \
    'kafka-console-consumer --bootstrap-server broker:9092 \
    --topic sptrans-trusted --from-beginning \
    --property print.key=true --property print.value=true --property print.headers=true
```

- Criar/atualizar o conector JDBC (Kafka Connect):
```
curl -s -X PUT http://localhost:8083/connectors/conector-postgres/config \
  -H "Content-Type: application/json" \
  --data-binary @kafka-connect/conectores/conector-postgres.json
```
- **Observação**: Entrar dentro do diretorio no qual estão os conectores (Kafka Connect).

- Status:
```curl -s http://localhost:8083/connectors/conector-postgres/status | jq```

## 5) Airflow

- Cadastre variáveis (Admin → Variables), ex.: GEO_MAPS_KEY.
- Agendamento sugerido: a cada 5 minutos → */5 * * * *.

🧪 Teste rápido (fim a fim)

Ver mensagens no tópico:
```
docker exec -it kafka-broker bash -lc \
  "kafka-console-consumer --bootstrap-server kafka-broker:29092 \
   --topic sptrans.trusted --from-beginning \
   --property print.value=true --max-messages 5"
```

- Conferir no Postgres:
```
docker compose exec postgres psql -U airflow -d trusted_sptrans \
  -c "SELECT COUNT(*) FROM public.positions;"
```

💡 Exemplos úteis

Produzir JSON válido no tópico
```
{"route_id":1114,"route_code":"736I-10","direction":2,"dir_from":"STO. AMARO","dir_to":"JD. INGÁ","vehicle_id":68853,"in_service":true,"event_ts":"2025-10-08T16:06:27Z","lat":-23.65,"lon":-46.66}
```

Deduplicação (REFINED) — exemplo
```
DELETE FROM refined_sptrans.public.positions a
USING refined_sptrans.public.positions b
WHERE a.ctid < b.ctid
  AND a.vehicle_id = b.vehicle_id
  AND a.event_ts  = b.event_ts;
```

🧯 Troubleshooting (curto)

- NiFi → Kafka Timeout/InitProducerId: ver bootstrap.servers = kafka-broker:29092 e conectividade.
- LEADER_NOT_AVAILABLE: aguarde alguns segundos após criar tópico.
- Connector “Struct schema required”: com value.converter.schemas.enable=false, publique JSON flat com campos esperados; ou ative schemas e envie schema+payload.
- Airflow conecta em localhost e falha: dentro do Compose o host é postgres.

🚀 Próximos passos

- Ingerir GTFS (routes, trips, stops, stop_times, shapes) para enriquecer REFINED.
- KPIs (veículos ativos, headway, atraso, velocidade média).
- Dashboards (Grafana/Metabase).
- Materialized Views e mais índices.
