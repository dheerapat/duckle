# Duckle Project Roadmap

## Milestone 1: Foundation for Automation and Extensibility
**Goal**: Transform Duckle from a manual execution library to an automated, extensible platform.

### 1.1 Scheduler/Orchestrator Integration
- Add lightweight scheduler (APScheduler) for time-based pipeline triggers
- Implement webhook support for event-driven pipelines
- Add CLI command: `duckle schedule run --cron "0 2 * * *" pipeline_name`
- Store scheduled jobs in DuckLake metadata for persistence across restarts

### 1.2 Expanded Connector Ecosystem
- Add REST API connector (with pagination, auth headers, rate limiting)
- Add S3/GCS connector for cloud object storage (Parquet/CSV/JSON)
- Implement connector registry/plugin system for community contributions

## Milestone 2: Production Readiness
**Goal**: Make Duckle suitable for production deployment with monitoring and security.

### 2.1 Monitoring & Observability
- Add metrics endpoint (`/metrics`) tracking:
  - Pipeline run duration, success/failure rates
  - Row counts processed per stage (extract/transform/load)
  - Quality check failure rates and rule-specific metrics
- Implement structured JSON logging for log aggregation (ELK/Fluentd)
- Add retry mechanisms with exponential backoff for transient failures
- Create dashboard (basic) for pipeline health and performance trends

### 2.2 Security Enhancements
- Implement role-based access control (RBAC) for API/UI
- Add connection secret management (integrate with HashiCorp Vault/AWS Secrets Manager)
- Implement data masking/anonymization utilities for PII protection
- Add audit logging for data access and pipeline modifications
- Enhance SQL injection prevention with prepared statements where applicable

## Milestone 3: Developer Experience and Usability
**Goal**: Reduce barrier to entry and improve day-to-day usability.

### 3.1 Declarative Pipeline Definitions
- Add YAML/JSON pipeline definition format:
  ```yaml
  name: sales_etl
  sources:
    orders:
      type: postgres
      query: "SELECT * FROM orders WHERE date >= '{{ds}}'"
  transforms:
    - "SELECT date, SUM(amount) as revenue FROM {{input}} GROUP BY date"
  destination:
    name: daily_sales
    layer: gold
  incremental:
    cursor_column: date
    merge_keys: [date]
  ```
- Add schema validation for pipeline definitions (using Pydantic)
- Generate Python API from declarative definitions
- Keep programmatic API for advanced use cases

### 3.2 Documentation & Examples
- Create interactive tutorials (Jupyter notebooks) for common patterns
- Add "getting started" guide with sample dataset (e.g., NYC Taxi)
- Document common patterns: SCD Type 2, surrogate keys, slowly changing dimensions
- Create Architecture Decision Records (ADRs) for key technical decisions
- Improve API documentation with examples for all public methods

## Milestone 4: Advanced Features
**Goal**: Advanced data engineering capabilities.

### 4.1 Data Engineering Productivity Features
- Add data profiling in `Transformer.preview()`:
  - Column stats (null %, distinct values, min/max, histograms)
  - Data type detection and validation
  - Sample value distribution and outlier detection
- Implement pipeline templates for common patterns:
  - `slowly_changing_dimension(template="scd_type_2")`
  - `facts_aggregation(granularity="hour|day|month")`
  - `surrogate_key_generation(source_columns=["id"])`
- Add data contracts: define expected schemas for pipeline inputs/outputs
  - Automatic schema validation and evolution handling
  - Generate documentation from contracts

## Future Considerations
- **UI Development**: React-based dashboard for pipeline monitoring and management
- **Machine Learning Integration**: Feature store and model training pipelines
- **Streaming Support**: Add micro-batch streaming via Apache Kafka or similar
- **Multi-tenancy**: Resource isolation and fair scheduling for team sharing
- **Cloud-Native Deployment**: Helm charts and Kubernetes operator for orchestrated deployment

---
*Last updated: 2026-04-22*
