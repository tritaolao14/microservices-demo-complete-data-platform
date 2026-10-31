# TODO List - Microservices Demo with Data Platform

## Overview
This document outlines the current status and future development tasks for the microservices demo application with integrated data platform.

## Current Status
The project has a partial data platform implementation with foundational infrastructure but missing key processing components.

### Implemented Components
- Kafka (Event Streaming)
- MinIO (Data Lake)
- PostgreSQL (Database)
- Spark/Iceberg Configuration
- Data Processing Scripts
- Trino + dbt + Great Expectations (serving and quality)
- Apache Superset BI dashboards (Trino datasource)

### Missing Components
- Kafka Producer in checkoutservice
- Data Ingestion Service
- Airflow Orchestration
- Debezium CDC

## Sprint Planning

### Sprint 1: Foundation Setup (Weeks 1-4)
**Goal**: Complete basic data platform infrastructure and event streaming

#### Tasks:
- [ ] Configure Kafka Producer in checkoutservice
  - [ ] Add kafka-go library dependency
  - [ ] Implement order event publishing to Kafka topic `orders`
  - [ ] Test event production from checkout service
- [ ] Create Data Ingestion Service (Python)
  - [ ] Set up Python consumer for Kafka `orders` topic
  - [ ] Implement data processing logic
  - [ ] Store processed data to MinIO or PostgreSQL
- [ ] Configure Data Pipeline
  - [ ] Set up basic ETL flow from Kafka to storage
  - [ ] Implement data validation and error handling

### Sprint 2: Data Warehouse & Analytics (Weeks 5-8)
**Goal**: Establish data warehouse and business intelligence capabilities

#### Tasks:
- [x] Implement Debezium CDC for PostgreSQL
  - [x] Configure Debezium connector for product catalog changes
  - [x] Set up Kafka topic for database change events
    - Topic `cdc_product_changes`, 2 partitions, định tuyến bằng SMT `RegexRouter`
  - [x] Serialise CDC output as Avro with Schema Registry
    - Confluent Avro converter đóng gói trong image riêng `src/debezium`
    - `confluentinc/cp-schema-registry` chỉ trong overlay dev
  - [x] Make the register Job reconcile config instead of POST-only
    - Trước đây Job coi `409` là thành công nên thay đổi config trong git bị bỏ qua
  - [x] Capture `analytics.order_items` — bảng traffic thật, thay cho `public.products`
    - Connector `orderitems-postgres`, topic `cdc_order_items` (4 partitions), Avro
    - `Job/debezium-register` và `Job/kafka-create-topic` giờ xử lý nhiều connector/topic
    - Bắt buộc mỗi connector một `topic.prefix` khác nhau, nếu không JMX MBean trùng
      làm task `RUNNING` nhưng slot không active và không có event
- [ ] Setup Data Warehouse
  - [x] Configure Trino or ClickHouse for data querying (Trino 476 + Iceberg JDBC catalog, DPFMD-37)
  - [x] Set up dbt for data transformation (DPFMD-38)
    - Project `src/data_processing/dbt`: `stg_orders_items` (silver) → `fact_orders_items` (gold), grain `(order_id, product_id)`
    - `dbt build` PASS=26 (2 models + 24 tests); reconcile `analytics.order_items` window 2026-10-11..2026-10-15 = 67.013 dòng
    - CronJob `dbt-build` trong `gitops/overlays/dev/dbt.yaml`, chạy sau Bronze sync
  - [x] Add Great Expectations data quality layer (DPFMD-39)
    - Project `src/data_processing/great_expectations`: datasource Trino (`iceberg.gold`) + Postgres (`analytics`), checkpoint `fact_orders_items_awap`
    - 3 validation definitions: fact window integrity, reference totals, cross-catalog reconcile (window 2026-10-11..2026-10-15; 67.013 dòng, units 451.674, revenue 336.524.180,88)
    - CronJob `great-expectations` (`gitops/overlays/dev/great-expectations.yaml`) lúc `15 * * * *`, chạy sau `dbt-build`
    - Trino results materialized into a temp SQLite snapshot before GE validation; price check uses per-unit rounding tolerance
    - Runtime verify PASS (2026-10-31, exit 0): checkpoint `fact_orders_items_awap` PASSED, 67.013 fact items, deltas = 0, price mismatch = 0; xem `docs/data-engineering-patterns.md` §20 → `Đang chạy`
- [x] Deploy BI Dashboard (DPFMD-40)
  - [x] Install Apache Superset or Metabase
    - Superset 4.1.2 image `src/superset/`, deployed by `gitops/overlays/dev/superset.yaml`
    - Dedicated metadata Postgres `superset-metadata` + PVC; initContainer runs `superset db upgrade`/`superset init`
  - [x] Configure connections to data warehouse
    - Datasource `Trino Iceberg` → `trino://superset@trino:8080/iceberg`
    - Idempotent bootstrap `src/superset/bootstrap_superset.py` creates admin, database, datasets, charts, dashboard
  - [x] Create sample dashboards
    - Dashboard `business-overview`: doanh thu theo giờ, sản phẩm bán chạy nhất, cart-but-no-pay
    - Cart metric is a Redis snapshot proxy (`CronJob/cart-snapshot`, `src/data_processing/cart_snapshot.py`) because cartservice emits no events; see `docs/data-engineering-patterns.md` §18

### Sprint 3: Optimization & Expansion (Weeks 9-12)
**Goal**: Optimize performance and expand data platform capabilities

#### Tasks:
- [ ] Performance Optimization
  - [ ] Optimize Kafka consumer performance
  - [ ] Improve data processing efficiency
- [ ] Expand Data Sources
  - [ ] Add additional microservice event sources
  - [ ] Implement data enrichment from external systems
- [ ] Security
  - [ ] Replace plain-text dev DB passwords with SealedSecret or External Secrets
    - [ ] `POSTGRES_PASSWORD` in `kubernetes-manifests/postgresql.yaml`
    - [ ] `debezium-db` Secret in `gitops/overlays/dev/debezium.yaml`
    - [ ] Required before staging/production; plaintext in dev is a temporary exception
  - [ ] Stop storing the connector config in plain text
    - [ ] Kafka Connect keeps the whole connector config, including `database.password`,
          in the `_debezium_connect_configs` topic
    - [ ] Encrypt the config topic or move the password to a credential provider
  - [ ] Add authentication to Schema Registry
    - [ ] `confluentinc/cp-schema-registry` in `gitops/overlays/dev/schema-registry.yaml`
          currently has no auth; it is ClusterIP-only so it is not exposed outside the cluster
    - [ ] Add auth plus a NetworkPolicy before staging/production
- [ ] Documentation & Testing
  - [ ] Complete technical documentation
  - [ ] Write comprehensive test suite
  - [ ] Create user guides and tutorials

## Development Approach

### Agile Methodology
- Use 4-week sprints for iterative development
- Daily standups (15 minutes) for progress tracking
- Sprint reviews to demonstrate completed features
- Sprint retrospectives for process improvement

### CI/CD Pipeline
- Cloud Build or Jenkins for automated testing
- Skaffold for continuous deployment
- Automated monitoring and logging

## Technical Requirements

### Infrastructure
- Kubernetes cluster (minikube, kind, or GKE)
- Helm charts for deployment management
- Terraform for infrastructure as code

### Data Platform Components
- Apache Kafka (Event Streaming)
- MinIO (Object Storage)
- PostgreSQL (Database with CDC support)
- Apache Spark/Iceberg (Data Processing)
- Apache Airflow (Orchestration)
- Apache Superset/Metabase (BI Dashboard)

## Risk Assessment

### Technical Risks
- Kafka integration complexity with Go services
- Data consistency challenges in distributed systems
- Performance bottlenecks in data processing pipelines

### Timeline Risks
- Dependency on external libraries and services
- Potential delays in testing and validation phases

## Success Metrics

### Functional Metrics
- All microservices can communicate with data platform
- Data pipeline processes events in real-time
- BI dashboards display accurate analytics

### Performance Metrics
- Kafka event processing latency < 100ms
- Data warehouse query response time < 2s
- System availability > 99%

## Dependencies

### External Services
- Kubernetes cluster (local or cloud)
- Docker registry for container images
- Cloud storage services (if deployed on GCP)

### Development Tools
- Go 1.25+ for checkoutservice
- Python 3.x for data processing scripts
- Node.js 18+ for microservices (paymentservice, currencyservice)
- Helm 3+ for deployment management

## Notes
This TODO list will be updated regularly as the project progresses and new requirements emerge.