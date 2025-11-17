# 🏛️ System Architecture – Event-Driven Judicial Case Pipeline (GCP)

This document describes the end-to-end architecture of the **event-driven streaming system** implemented for managing judicial case updates (“expedientes”) on Google Cloud Platform.  
All data used is synthetic; the pipeline replicates the behavior of a real-world legal case lifecycle.

---

# 1. Overview

The system simulates the evolution of court cases by generating procedural events in real time.  
These events are streamed into Google Cloud, processed using Dataflow, stored as an immutable history in BigQuery, and finally exposed through Looker Studio dashboards.

The goal is to demonstrate:

- Event-driven design  
- Streaming ETL  
- Append-only staging  
- Incremental modelling (MERGE)  
- Real-time analytics  

This architecture is equivalent to a modern operational analytics pipeline (OLAP + streaming).

---

# 2. High-Level Architecture Diagram

    A[Jupyter Notebooks<br>Event Generator] -->|Publishes JSON| B(Pub/Sub Topic<br>expedientes-updates)
    B --> C[Dataflow Streaming Pipeline<br>Beam + Streaming Engine]

    C --> D[(BigQuery<br>Expedientes_Staging<br>Append-Only)]
    D -->|Scheduled MERGE| E[(BigQuery<br>Expedientes<br>Master Table)]

    E --> F[Looker Studio Dashboard]

# 3. Component Details
3.1 Event Generator (Python)

A controlled simulator builds realistic judicial updates:

Cases follow procedural sequences depending on type:

Ordinario

Verbal

Updates include:

New procedural state (Estado)

Optional change in claim amount (Cuantia)

ISO-8601 timestamp

Fully coherent transitions (e.g., no skipping states)

Published to Pub/Sub as compact JSON

The generator can produce:

Single events (manual testing)

Bulk events (e.g., 5.000 updates)

 # 3.2 Pub/Sub (Messaging Layer)

Pub/Sub acts as the ingestion buffer between the generator and Dataflow.

Topic:

projects/<project-id>/topics/expedientes-updates


Events are delivered with at-least-once semantics.
Attributes include:

content_type="application/json"

Pub/Sub ensures durability, replayability, and decoupling.

 # 3.3 Dataflow Streaming Pipeline
Purpose

Process each incoming event and write it to BigQuery in append-only fashion.

Key features

Implemented using Apache Beam (Python)

Uses Streaming Engine for autoscaling

Consumes Pub/Sub messages (JSON)

Performs light transformations:

JSON parsing

Validation

Normalization of empty fields

Inserts rows into:

Expedientes_OLAP.Expedientes_Staging

Why append-only?

Enables complete audit trail

Simplifies concurrency

Decouples streaming ingestion from business updates

Prevents partial updates or race conditions

 # 3.4 BigQuery – Staging Layer (Append Only)

Table: Expedientes_Staging

Contains every event emitted by Pub/Sub/Dataflow for each case:

Column	Type	Description
Ref	STRING	Unique case identifier
Estado	ARRAY<STRUCT>	New procedural states
Cuantia	ARRAY<STRUCT>	New amount updates
ingestion_timestamp	TIMESTAMP	Automatic ingestion time

Each event is stored independently.
No overwrites.
Perfect for auditing or replaying state transitions.

 # 3.5 BigQuery – Master Table (Materialization)

Table: Expedientes

Contains the full current case representation, including complete historical arrays of:

All states ever reached (in order)

All quantía changes (in order)

Court assignment

Procedure type

Current state and last amount

The table is maintained with an incremental MERGE, executed every 3 hours by a Scheduled Query.

MERGE responsibilities:

Union staging + master timelines

Remove duplicates (UNION DISTINCT)

Preserve chronological order

Overwrite the arrays only (no other fields touched)

Natural CDC pattern (event sourcing style)

# 3.6 Looker Studio Dashboard

Visualizes:

Distribution of cases by procedure type

Most frequent states (Ordinario / Verbal)

Workload per court (“Juzgado”)

State timelines

Synthetic KPIs

Designed to close the pipeline loop from generation → ingestion → transformation → analytics.

# 4. Data Model
 # 4.1 Procedural State Structure
STRUCT<
    estado STRING,
    timestamp TIMESTAMP
>

 # 4.2 Amount Change Structure
STRUCT<
    importe FLOAT64,
    timestamp TIMESTAMP
>

 # 4.3 Master Table Schema (Expedientes)
Ref STRING,
Procedimiento STRING,
Juzgado STRING,
Cuantia ARRAY<STRUCT<importe FLOAT64, timestamp TIMESTAMP>>,
Estado ARRAY<STRUCT<estado STRING, timestamp TIMESTAMP>>,
FechaCreacion TIMESTAMP

# 5. IAM & Security
Key permissions used:

Pub/Sub Publisher (for generator)

Pub/Sub Subscriber (Dataflow)

BigQuery Data Editor (Dataflow)

BigQuery Job User

Storage Object Admin (for staging/temp buckets)

Service Account User (to run Dataflow workers)

All service accounts follow the principle of least privilege.
No credentials are committed to the repository.

# 6. Scalability

This architecture scales fully:

Pub/Sub scales to millions of messages per second

Dataflow autoscaling keeps cost efficient

BigQuery handles streaming inserts natively

Dashboard refresh is near-real-time

It is suitable for:

Large-volume case management

Judicial analytics

Financial/legal workflows

CDC-style pipelines

# 7. Cost Notes

This project can run on free tier or minimal cost if:

Generator is local

Dataflow uses 1–2 workers

BigQuery scheduled queries run every few hours

Storage buckets have soft delete disabled

# 8. Possible Extensions

Add BigQuery ML to predict case duration

Implement anomaly detection in timelines

Add a Cloud Run REST API for external systems

Store raw Pub/Sub messages in GCS for audit

Build a Dataform project instead of manual SQL

# 9. Conclusion

This project demonstrates a complete production-grade streaming architecture running on Google Cloud, capable of handling complex stateful updates and delivering real-time analytical insights.

It replicates modern data pipelines used in:

Banking

Legal tech

Insurance

Public administration

Workflow management systems

The structure is modular, extendable, and fully reproducible.
