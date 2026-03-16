# Customer 360 Data Platform

Enterprise-scale Customer 360 platform built on Azure, integrating **policy, claims and telematics data** across insurance business lines. Improved marketing personalisation by **28%**, boosted policy renewal rates by **15%**, and improved BI analytics performance by **35%**.

## Architecture

```
Source Systems
┌─────────┐  ┌─────────┐  ┌──────────────┐
│  Policy  │  │ Claims  │  │  Telematics  │
│   DB     │  │   DB    │  │   Streams    │
└────┬─────┘  └────┬────┘  └──────┬───────┘
     │              │               │
     └──────────────┼───────────────┘
                    │
                    ▼
          ┌──────────────────┐
          │  Azure Data      │
          │  Factory (ADF)   │  ← Orchestration & ingestion
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  Azure Data Lake │
          │  Storage Gen2    │  ← Raw → Bronze → Silver → Gold
          │  (ADLS)          │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  Azure Databricks│  ← PySpark transformation
          │  (Delta Lake)    │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  Azure Synapse   │  ← Analytical warehouse
          │  Analytics       │
          └────────┬─────────┘
                   │
                   ▼
          ┌──────────────────┐
          │  Power BI        │  ← Customer 360 dashboards
          └──────────────────┘
```

## Key Features

- **Medallion architecture** — Bronze / Silver / Gold layers on ADLS Gen2
- **Real-time + batch** integration across policy, claims and telematics
- **Customer 360 profile** — unified single view of each customer
- **28% improvement** in marketing personalisation accuracy
- **15% increase** in policy renewal rates
- **35% faster** BI query performance via Synapse optimisations
- **97% SLA compliance** across 125+ automated pipelines
- **Azure Purview** — 3,500+ catalogued data assets with full lineage

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2, Delta Lake |
| Processing | Azure Databricks, PySpark |
| Warehouse | Azure Synapse Analytics |
| Governance | Azure Purview, Azure Key Vault |
| Security | Azure RBAC, Managed Identity |
| Orchestration | Azure Data Factory, Azure Logic Apps |
| BI | Power BI |
| IaC | Terraform (Azure provider) |

## Project Structure

```
customer-360-platform/
├── src/
│   ├── ingestion/          # ADF pipeline definitions & connectors
│   ├── transformation/     # PySpark Databricks notebooks
│   ├── serving/            # Synapse serving layer
│   └── utils/              # Delta Lake helpers, schema registry
├── sql/                    # Synapse DDL & analytical queries
├── config/                 # Pipeline config & secrets references
├── infrastructure/         # Terraform for Azure resources
└── tests/                  # Unit & integration tests
```

## Results

| Metric | Before | After |
|---|---|---|
| Marketing personalisation | Segment-based | **Profile-based (+28%)** |
| Policy renewal rate | Baseline | **+15%** |
| Pipeline latency | High | **↓31%** |
| BI query performance | Baseline | **+35%** |
| SLA compliance | ~85% | **97%** |
| Data assets catalogued | ~400 | **3,500+** |
| Discovery efficiency | Baseline | **+54%** |

## Author

**Pramod Vishnumolakala** — Senior Data Engineer  
[pramodvishnumolakala@gmail.com](mailto:pramodvishnumolakala@gmail.com) · [LinkedIn](https://linkedin.com/in/pramod-vishnumolakala)
EOF
