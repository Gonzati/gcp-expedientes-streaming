# 📡 GCP Event-Driven Streaming Pipeline for Judicial Case Management  
*A full end-to-end Data Engineering project on Google Cloud Platform*

This repository contains a complete, production-style streaming architecture implemented on **Google Cloud Platform**, simulating the lifecycle of judicial case files ("expedientes").  
The system generates realistic legal events, streams them in real time, processes them through Dataflow, stores append-only history in BigQuery, and exposes final analytics via Looker Studio.

All data is synthetic.  
This project is fully reproducible.

---

## 🚀 1. Architecture Overview

The solution implements a real-time event-driven pipeline:

    A[Jupyter / Python<br>Event Generator] -->|JSON events| B(Pub/Sub Topic)
    B --> C[Dataflow Streaming Pipeline]
    C --> D[BigQuery: Expedientes_Staging<br>(append-only)]
    D --> E[BigQuery: Expedientes<br>(master table via MERGE)]
    E --> F[Looker Studio Dashboard]

## 🧩 2. Components
🔵 Pub/Sub

Receives JSON messages representing case updates

Each event includes:

Ref (unique case ID)

Updated Estado (procedure state)

Optional updated Cuantia (claim amount)

Timestamps

🟣 Event Generator

Python script that:

Loads current status from BigQuery

Generates coherent next states depending on the procedure (“Ordinario” or “Verbal”)

Publishes events to Pub/Sub

Supports massive loads (e.g., 10k events)

🟡 Dataflow Streaming Pipeline

Reads from Pub/Sub

Parses JSON

Writes append-only rows into Expedientes_Staging in BigQuery

Ensures idempotency and schema consistency

🟢 BigQuery
Tables:

Expedientes → master table

Expedientes_Staging → append-only incoming events

Incremental model:

A scheduled MERGE runs every 3 hours to update case timelines:

Append new states

Append updated amounts (never using duplicates)

Preserve full chronological history

🔍 Looker Studio Dashboard

Visualizes:

Distribution of cases by procedure

Most common procedural states

Workload per court (“Juzgado”)

Timeline of legal states

##🗂 3. Repository Structure
.
├── dataflow/
│   ├── pipeline_expedientes_staging.py
│   └── requirements.txt
├── pubsub/
│   ├── event_generator.py
│   └── samples/
│       └── sample_event.json
├── sql/
│   ├── create_tables.sql
│   ├── merge_expedientes.sql
│   └── queries_demo.sql
├── docs/
│   ├── architecture.md
│   └── dashboard_screenshot.png
├── scripts/
│   └── load_initial_data.py
└── README.md

## ⚙️ 4. How to Reproduce
1️⃣ Create resources

gcloud pubsub topics create expedientes-updates
gcloud pubsub subscriptions create expedientes-sub --topic=expedientes-updates
gcloud storage buckets create gs://your-bucket

2️⃣ Deploy Dataflow streaming job
python pipeline_expedientes_staging.py \
  --runner DataflowRunner \
  --project your-project \
  --region europe-west1 \
  --temp_location gs://your-bucket/temp \
  --staging_location gs://your-bucket/staging \
  --input_topic projects/your-project/topics/expedientes-updates

3️⃣ Load initial synthetic dataset

4️⃣ Generate streaming events

5️⃣ Schedule MERGE in BigQuery

Use the SQL in sql/merge_expedientes.sql

## 📈 5. Example Dashboard

(see docs/dashboard_screenshot.png)

The dashboard includes:

Case distribution by procedure

State progression (Ordinario / Verbal)

Workload per court

Evolution of procedural steps

## 🧪 6. Extending the Project

You can easily add:

ML predictions with BigQuery ML

Latency monitoring with Cloud Monitoring

Audit log tables

CDC (change data capture) patterns

REST API on Cloud Run

✉️ Contact
Ángel Argibay
LinkedIn: www.linkedin.com/in/ángel-argibay-cabo-842504174

Feel free to reach out if you'd like to discuss data engineering, legal tech, or cloud pipelines.
