# LedgerStream (txpipeline)

## 📌 Visão Geral

**Nome do projeto**: LedgerStream

**Descrição**: pipeline de dados de transações financeiras construído como case de portfólio. O objetivo é aprender e demonstrar a integração das principais ferramentas do ecossistema moderno de engenharia de dados.

**Nível**: médio/difícil

**Diferencial**: integração completa Kafka + Airflow + Kubernetes + observabilidade + DBT.

---

## 🧩 Arquitetura

Fluxo principal:

1. Produtor Python usa Faker para gerar transações fictícias e publica em tópico Kafka (KRaft).
2. Kafka + Schema Registry via Avro/JSON Schema garante compatibilidade de payload.
3. Consumer lê do tópico e publica via API FastAPI (JWT) no PostgreSQL.
4. DBT transforma dados raw em camadas analíticas (staging, mart), com testes de qualidade.
5. Airflow orquestra o pipeline com DAG, retries e alertas.
6. Prometheus + Grafana monitoram lag, latência e volume em tempo real.
7. Docker + Kubernetes orquestram deploys e autoscaling baseado no lag do Kafka.

---

## 🧱 Stack Tecnológica

- Python
- Kafka (KRaft)
- Schema Registry
- FastAPI
- PostgreSQL
- DBT
- Airflow
- Prometheus
- Grafana
- Docker
- Kubernetes

---

## 🛠 Módulos / Pastas

- `producer/` - produtor de transações (producer.py, requirements)
- `consumer/` - serviço Kafka consumer
- `api/` - FastAPI de ingestão e autenticação JWT
- `dbt/` - modelos, seeds e testes DBT
- `airflow/` - DAGs e operadores
- `k8s/` - manifests (Deployment, Service, HPA, ConfigMap, Secret)
- `docker-compose.yml` - ambiente local
- `.env` - variáveis de ambiente

---

## ✅ Funcionalidades

- Ingestão em streaming via Kafka
- Governança de schema com Schema Registry
- API segura com JWT
- Persistência em PostgreSQL
- Transformação analítica com DBT (facts/dimensions)
- Orquestração com Airflow (dag, retries, SLA)
- Observabilidade com Prometheus + Grafana
- Autoscaling do consumer por lag (Kubernetes/KEDA)

---

## 📂 Como rodar localmente

1. `docker compose up -d`
2. Inicie Kafka/KRaft + Schema Registry + PostgreSQL
3. `python producer/producer.py`
4. `uvicorn api.main:app --reload`
5. Inicie Airflow (scheduler + webserver) via compose
6. `dbt deps && dbt seed && dbt run && dbt test`
7. Acesse Grafana e Prometheus para dashboards

---

## 🧪 Como validar

- Eventos aparecem no tópico Kafka
- Consumer consome e API grava no PostgreSQL
- Modelos DBT compilam e testes passam
- DAG Airflow conclui sem falhas
- Dashboards mostram lag e throughput

---

## 🚀 Roadmap recomendados

- Kafka Connect para Data Lake
- Data Warehouse em Snowflake/BigQuery
- Governança de dados (Data Catalog / Data Lineage)
- Feature store para ML
- Chaos testing e testes de performance

---

## 🎯 Caso de uso para portfólio

- Demonstra end-to-end pipeline de dados
- Aborda ingestão streaming, API, transformação, orquestração, observabilidade
- Mostra capacidade de implantação em conteiner e Kubernetes
- Apresenta competências em SRE/DevOps e engenharia de dados
