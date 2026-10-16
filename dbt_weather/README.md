# Modern End-to-End Weather ELT Pipeline

A containerized, production-ready local ELT data pipeline built using a Modern Data Stack (MDS). This project ingests real-time weather metrics, stores them in an analytical lakehouse, transforms them using analytics engineering principles, orchestrates execution, and syncs curated data into an operational serving database.

---

##  Architecture & Tech Stack

```text
[ Live Weather API ] 
        │
        ▼ (Python Ingestion)
[ DuckDB Local Lakehouse ] 
        │
        ▼ (dbt Transformations)
[ Curated Analytical Models ] 
        │
        ▼ (Post-Load Sync Script)
[ PostgreSQL Serving Layer (Docker) ] 
        ▲
        └─ (Orchestrated via Dagster)
