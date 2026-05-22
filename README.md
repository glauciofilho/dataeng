# 🏗️ Engenharia de Dados — Data Lakehouse Local

Stack moderna de engenharia de dados rodando 100% em Docker, com arquitetura **Medallion (Bronze → Silver → Gold)**.

## Tecnologias

| Ferramenta | Papel | UI |
|---|---|---|
| **Apache Airflow** | Orquestração de pipelines | http://localhost:8080 |
| **Apache Spark** | Processamento distribuído (PySpark) | http://localhost:8090 |
| **SeaweedFS** | Object Storage S3-compatível (Produção) | — |
| **Apache Iceberg** | Formato de tabela ACID open table format | — |
| **Iceberg REST Catalog** | Catálogo de metadados leve | http://localhost:8181 |
| **dbt (dbt-trino)** | Transformações SQL declarativas | — |
| **Trino** | SQL engine distribuído sobre Iceberg | http://localhost:8083 |

## Arquitetura

```
[Airflow Orchestrator]
       │
       ├─► DAG 1: Ingestion ──────► SeaweedFS bronze/  (raw parquet)
       │                                  │
       ├─► DAG 2: Spark Processing ───────┤
       │         bronze → silver          │  Iceberg Tables
       │         silver → gold            ▼
       │                           SeaweedFS warehouse/
       │                           ┌─ silver.nyc_taxi   (Iceberg)
       │                           └─ gold.trips_by_day (Iceberg)
       │                              gold.trips_by_zone(Iceberg)
       │                                  │
       └─► DAG 3: dbt ─────────────────────► Trino SQL queries
                   silver → gold views
```

## Pré-requisitos

- Docker Desktop ≥ 4.x com **≥ 8 GB de RAM** alocados
- `make` (opcional, mas recomendado)

> No Windows, instale o `make` via [chocolatey](https://chocolatey.org/): `choco install make`

## Início Rápido

```bash
# 1. Clone / entre no diretório
cd "Engenharia de Dados"

# 2. Suba toda a infraestrutura (primeira vez demora ~5 min para baixar imagens)
make up
# ou: docker compose up -d --build

# 3. Aguarde todos os serviços ficarem healthy (~2-3 min)
make ps

# 4. Acesse o Airflow
#    URL:  http://localhost:8080
#    user: admin  |  senha: admin

# 5. Dispare a pipeline completa
make trigger-pipeline
# ou pelo UI: DAG "04_full_pipeline" → trigger
```

## Serviços & Portas

| Serviço | URL / Porta | Credenciais |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8090 | — |
| Trino UI | http://localhost:8083 | — |
| Iceberg REST | http://localhost:8181 | — |
| PostgreSQL (Prod) | Configurado em `.env` | Externo |
| SeaweedFS S3 (Prod) | Configurado em `.env` | Externo |

## Pipeline de Dados

### DAG 01 — Ingestion
Baixa o dataset **NYC Yellow Taxi Jan/2023** (~45 MB parquet) do site público da TLC e faz upload para o bucket `bronze/` no SeaweedFS.

- Idempotente: pula se o arquivo já existir
- Fonte: `d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet`

### DAG 02 — Spark Processing
Dois jobs PySpark em sequência:

**`bronze_to_silver`**
- Lê o parquet do `bronze/`
- Limpa e tipifica colunas, remove registros inválidos
- Adiciona `trip_duration_min`, `pickup_date`
- Escreve tabela Iceberg particionada por dia em `iceberg.silver.nyc_taxi`

**`silver_to_gold`**
- Lê `iceberg.silver.nyc_taxi`
- Agrega em `iceberg.gold.trips_by_day` (KPIs diários)
- Agrega em `iceberg.gold.trips_by_zone` (performance por zona)

### DAG 03 — dbt Transformations
Executa modelos dbt via Trino:
- `silver/stg_nyc_taxi` → view sobre a tabela Iceberg silver
- `gold/trips_by_day` → tabela Iceberg com KPIs diários
- `gold/trips_by_zone` → tabela Iceberg com ranking de zonas
- `dbt test` valida constraints de qualidade de dados

### DAG 04 — Full Pipeline
Encadeia os DAGs 01 → 02 → 03 automaticamente. **Use este para rodar a pipeline completa.**

## Queries de Exemplo no Trino

```bash
# Abre o Trino CLI
make trino-cli

# ou via Docker diretamente:
docker exec -it trino trino --catalog iceberg --schema gold
```

```sql
-- Top 10 dias com mais corridas
SELECT pickup_date, total_trips, total_revenue_usd
FROM gold.trips_by_day
ORDER BY total_trips DESC
LIMIT 10;

-- Zonas com maior receita média por corrida
SELECT pickup_zone_id, total_pickups, avg_fare_usd
FROM gold.trips_by_zone
ORDER BY avg_fare_usd DESC
LIMIT 10;

-- Ver tabelas Iceberg disponíveis
SHOW TABLES IN iceberg.silver;
SHOW TABLES IN iceberg.gold;

-- Snapshot history de uma tabela Iceberg
SELECT * FROM iceberg.silver."nyc_taxi$snapshots";
```

## Comandos Úteis

```bash
make up                    # Sobe tudo
make down                  # Para tudo
make clean                 # Para + apaga volumes (reset completo)
make logs                  # Logs de todos os serviços
make log svc=trino         # Logs de um serviço específico
make ps                    # Status dos containers

make trigger-pipeline      # Dispara DAG 04 via CLI
make trino-cli             # Abre Trino CLI interativo
make dbt-run               # Roda modelos dbt manualmente
make dbt-test              # Roda dbt test
make dbt-docs              # Gera e serve dbt docs em :8088
```

## Estrutura do Projeto

```
.
├── docker-compose.yml       # Orquestração de todos os serviços
├── .env                     # Variáveis de ambiente
├── Makefile                 # Atalhos de comandos
├── airflow/
│   ├── Dockerfile           # Airflow + Spark provider + dbt-trino
│   ├── requirements.txt
│   └── dags/
│       ├── 01_ingestion_dag.py
│       ├── 02_processing_dag.py
│       ├── 03_dbt_dag.py
│       └── 04_full_pipeline_dag.py
├── spark/
│   ├── Dockerfile           # bitnami/spark + Iceberg JARs
│   └── jobs/
│       ├── bronze_to_silver.py
│       └── silver_to_gold.py
├── trino/
│   └── etc/
│       ├── config.properties
│       ├── jvm.config
│       ├── node.properties
│       └── catalog/iceberg.properties
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── silver/stg_nyc_taxi.sql
│       └── gold/
│           ├── trips_by_day.sql
│           └── trips_by_zone.sql
└── minio/
    └── create-buckets.sh
```

## Troubleshooting

**Airflow não sobe:**
```bash
make log svc=airflow-init
```

**Spark job falha com S3/SeaweedFS error:**
- Verifique se as credenciais e o endpoint de produção do SeaweedFS S3 no arquivo `.env` estão corretos.
- Verifique se os buckets `bronze`, `silver`, `gold` e `warehouse` existem no seu servidor SeaweedFS de produção.

**Trino não conecta no Iceberg catalog:**
```bash
make log svc=iceberg-rest
# O catalog REST precisa estar UP antes do Trino
```

**Falta memória (OOM):**
- Aumente o limite de RAM do Docker Desktop para ≥ 9 GB
- Ou reduza `SPARK_WORKER_MEMORY` no `.env` para `768m`

## Referências

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Apache Iceberg Docs](https://iceberg.apache.org/docs/latest/)
- [Trino Iceberg Connector](https://trino.io/docs/current/connector/iceberg.html)
- [dbt-trino Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup)
- [NYC TLC Trip Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
