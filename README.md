# Projeto: SPTRANS Olho Vivo

1. Nifi & Minio & Postgres

    - Comando para subir o conteiner
        - docker compose up -d airflow-webserver  airflow-scheduler airflow-triggerer airflow-init airflow-cli nifi minio postgres pgadmin zookeeper broker kafka-connect

    - Acessar o endereço:
        - https://localhost:9443/nifi/#/login (Nifi)
        - https://localhost:9001 (Minio)
        - http://localhost:5433/ (Pgadmin)
    
    1. Passo:
        - Importar a última versão do nifi/template para o Process Group
        - Gerar Access Key & Secret no minio
        - Habilitar:
            - Aws Credentials - Passando as credenciais geradas para o futuro envio dos arquivos gerados
            - Kafka3ConnectionService
    
    2. Passo:
        - Entrar no postgres através do pgadmin e criar dois databases:
            - refined_sptrans
            - trusted_sptrans

        - Após a criação desses database, crie as seguintes tabelas e cada um deles:
            - Trusted:
                CREATE TABLE public.positions (
                    route_id INT NOT NULL,
                    route_code TEXT,
                    direction SMALLINT,
                    dir_from TEXT,
                    dir_to TEXT,
                    vehicle_id INT NOT NULL,
                    in_service BOOLEAN,
                    event_ts VARCHAR NOT NULL,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    speed DOUBLE PRECISION,
                    stop_id TEXT,
                    ingestion_ts TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (route_id, vehicle_id, event_ts)
                );

                - Índices
                CREATE INDEX idx_positions_event_ts ON public.positions(event_ts DESC);
                CREATE INDEX idx_positions_route_event ON public.positions(route_id, event_ts DESC);
                CREATE INDEX idx_positions_vehicle_event ON public.positions(vehicle_id, event_ts DESC);

            - Refined:
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
    3. Passo:
        - Configurar o Kafka-connect:

            1. Listar os conectores para identificar se temos algum ativo no momento:
                - curl -s http://localhost:8083/connectors | jq .

            2. Criar o conector:
                - Entrar dentro do diretorio no qual estão os conectores:
                    /mnt/wsl/docker-desktop-bind-mounts/Ubuntu/8405424490c03a30d2389ec3cc6fd034630b725e946594cb381645fa15497e69/kafka-connect/conectores
                
                - Execute esse comando para criar:
                curl -X PUT http://localhost:8083/connectors/jdbc-sink-connector/config \
                -H "Content-Type: application/json" \
                --data-binary @conector-postgres.json

                - Verifique o status do seu conector:
                    - curl -s http://localhost:8083/connectors/jdbc-sink-connector/status | jq
        
        - Configurar o kafka Broker:
            
            1. Entrar na linha de comando:
                - docker exec -it broker /bin/bash
            
            2. Listar os tópicos:
                - kafka-topics --bootstrap-server localhost:9092 --list
            
            3. Criar tópico caso não exista:
                - kafka-topics --bootstrap-server broker:9092 --create --if-not-exists --topic sptrans-trusted --replication-factor 1 --partitions 1 && kafka-topics --bootstrap-server broker:9092 --describe --topic sptrans-trusted
            
            4. Deletar tópico caso necessário:
                - kafka-topics --delete --topic dlq.pg.positions --bootstrap-server localhost:9092
            
            5. Leitura de mensagens dentro da linha de comando do Broker:
                - docker exec -it broker bash -lc \
                    'kafka-console-consumer --bootstrap-server broker:9092 \
                    --topic sptrans-trusted --from-beginning \
                    --property print.key=true --property print.value=true --property print.headers=true'

* Extra:

    -  ver o container e o estado
        - docker ps -a | grep broker

    - parar (se estiver rodando) e remover
        - docker stop kafka-connect 2>/dev/null || true
        - docker rm kafka-connect