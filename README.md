<div align="center">

# 🚕 NYC Taxi Big Data End-to-End Pipeline

### A Lambda-Architecture Data Engineering & Machine Learning System

**Batch Training + Real-Time Streaming Inference on 1.5M NYC Taxi Records**

<br/>

![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboards-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

</div>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Architecture & Data Flow](#-architecture--data-flow)
- [Tech Stack](#-tech-stack)
- [Repository Structure](#-repository-structure)
- [Prerequisites](#-prerequisites)
- [Setup Instructions](#-setup-instructions)
- [Step-by-Step Execution Guide](#-step-by-step-execution-guide)
- [Visualization](#-visualization)
- [Author](#-author)

---

## 🔭 Overview

This project implements a **production-style Lambda Architecture** for processing and modeling **1.5 million NYC Taxi trip records**, combining historical batch training with live streaming inference.

It is designed to demonstrate real-world Big Data engineering patterns:

- 🗂️ **Batch Layer** — large-scale historical data is used to train a machine learning model offline using Apache Spark MLlib.
- ⚡ **Speed Layer** — new records are simulated in real time through Apache Kafka and scored on-the-fly by a Spark Structured Streaming job using the pre-trained model.
- ☁️ **Serving Layer** — all raw data, trained model artifacts, and prediction outputs are persisted in **Azure Data Lake Storage Gen2**, decoupling storage from compute entirely.
- 📊 **Presentation Layer** — Power BI connects directly to the data lake to visualize actual vs. predicted values in near real time.

The result is an architecture that mirrors how modern data platforms separate **cheap, containerized local compute** from **durable, scalable cloud storage** — a pattern widely used in industry-grade data platforms.

---

## 🏗️ Architecture & Data Flow



```mermaid
flowchart TD
    A[("🗃️ Raw Dataset<br/>1.5M NYC Taxi Records")] --> B{Data Split}
    B -->|1,000,000 rows| C[batch.csv]
    B -->|500,000 rows| D[stream.csv]

    subgraph CLOUD["☁️ Azure Data Lake Storage Gen2"]
        C --> E[("Container:<br/>batch-data")]
        H[("Container:<br/>predictions-output")]
    end

    subgraph BATCH["🗂️ Batch Layer — Training Path"]
        E --> F["PySpark Training Job<br/>(train_model.py)<br/>Spark MLlib"]
        F --> G[("Trained Model Artifact<br/>saved back to Azure")]
    end

    subgraph SPEED["⚡ Speed Layer — Inference Path"]
        D --> P["Python Producer<br/>(producer.py)<br/>row-by-row publish"]
        P --> K[("Apache Kafka Topic")]
        Z[["Zookeeper"]] -.manages.-> K
        K --> S["Spark Structured Streaming<br/>(spark.py)"]
        G -.loads pre-trained model.-> S
        S --> H
    end

    subgraph BI["📊 Visualization"]
        H --> PBI["Power BI<br/>abfss:// live connection"]
    end

    style CLOUD fill:#0078D4,color:#fff,stroke:#003a63
    style BATCH fill:#E25A1C,color:#fff,stroke:#7a2e0c
    style SPEED fill:#231F20,color:#fff,stroke:#000
    style BI fill:#F2C811,color:#000,stroke:#8a6d00
```
![alt text](ProjectFlow.jpeg)

### Data Flow Summary

| Stage | Description |
|---|---|
| **1. Data Splitting** | The raw 1.5M-record dataset is split into `batch.csv` (1,000,000 rows) for training and `stream.csv` (500,000 rows) for live simulation. |
| **2. Batch Ingestion** | `batch.csv` is uploaded to the `batch-data` container in ADLS Gen2. |
| **3. Model Training** | A PySpark job (`train_model.py`) reads the batch data from Azure, trains an ML model with Spark MLlib, and writes the model artifact back to Azure. |
| **4. Stream Simulation** | `producer.py` reads `stream.csv` row by row and publishes each record to a Kafka topic, simulating a live taxi-trip event feed. |
| **5. Real-Time Inference** | A Spark Structured Streaming application (`spark.py`) consumes the Kafka topic, loads the pre-trained model from Azure, and scores each incoming record in real time. |
| **6. Output Persistence** | Predictions (actual + predicted values) are written back to the `predictions-output` container in ADLS Gen2 via the `abfss://` protocol. |
| **7. Visualization** | Power BI connects directly to the ADLS Gen2 endpoint to render live dashboards comparing actual vs. predicted fare/trip values. |

---

## 🧰 Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Processing & ML** | ![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white) ![Spark MLlib](https://img.shields.io/badge/Spark%20MLlib-E25A1C?style=flat-square&logo=apachespark&logoColor=white) | Batch training and distributed processing |
| **Stream Processing** | ![Spark Streaming](https://img.shields.io/badge/Spark%20Structured%20Streaming-E25A1C?style=flat-square&logo=apachespark&logoColor=white) | Real-time consumption of Kafka events and inference |
| **Message Broker** | ![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white) ![Zookeeper](https://img.shields.io/badge/Zookeeper-Coordination-lightgrey?style=flat-square) | Real-time event ingestion and topic coordination |
| **Cloud Storage** | ![Azure ADLS Gen2](https://img.shields.io/badge/Azure%20ADLS%20Gen2-0078D4?style=flat-square&logo=microsoftazure&logoColor=white) | HDFS-compatible hierarchical cloud storage for raw data, models, and predictions |
| **Containerization** | ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white) ![Docker Compose](https://img.shields.io/badge/Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white) | Local orchestration of Kafka, Zookeeper, and Spark |
| **Language** | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white) | Producer scripts and PySpark job logic |
| **Visualization** | ![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=black) | Live dashboards for actual vs. predicted trip values |

---

## 📁 Repository Structure

```
Texi-BigData/
├── docker-compose.yml       # Local orchestration: Kafka, Zookeeper, Spark
├── producer.py               # Publishes stream.csv rows to Kafka topic
├── spark.py                  # Spark Structured Streaming consumer + real-time inference
├── train_model.py            # Batch PySpark job — trains the ML model on ADLS Gen2 data
├── train_model/               # Training pipeline resources/artifacts
├── SparkML.ipynb             # Exploratory notebook for model development
├── saved_lgb_model.onnx      # Exported trained model artifact
├── requirements.txt          # Python dependencies
├── docs/                       # 📸 Add architecture & dashboard screenshots here
└── README.md
```

---

## ✅ Prerequisites

Before running this project, make sure you have:

- 🐳 **Docker & Docker Compose** installed and running
- 🐍 **Python 3.9+** with `pip`
- ☁️ An **Azure Subscription** with a **Storage Account** configured for **ADLS Gen2** (Hierarchical Namespace **enabled**)
- 🔑 Your **Storage Account Name** and **Access Key** (Access keys → key1, under the storage account's "Security + networking" blade)
- ✨ **Apache Spark 3.x** binaries available locally (or via the Dockerized Spark image) with `spark-submit` on your `PATH`
- 📊 **Power BI Desktop** (Windows) for the final visualization step

---

## ⚙️ Setup Instructions

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/Asoussa/Texi-BigData.git
cd Texi-BigData
```

### 2️⃣ Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate      # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3️⃣ Provision Azure Data Lake Storage Gen2

1. Create a **Storage Account** in the Azure Portal with **Hierarchical Namespace = Enabled**.
2. Create two containers:
   - `batch-data`
   - `predictions-output`
3. Upload `batch.csv` to the `batch-data` container.
4. Copy the **Storage Account Key** — it will be passed into Spark's Hadoop configuration in the execution steps below.

### 4️⃣ Split the Raw Dataset

If not already split, divide the raw 1.5M-record NYC Taxi dataset:

```bash
python split_dataset.py \
  --input nyc_taxi_raw.csv \
  --batch-output batch.csv \
  --stream-output stream.csv \
  --batch-size 1000000 \
  --stream-size 500000
```

> `batch.csv` → upload to Azure (`batch-data`) · `stream.csv` → keep locally for the Kafka producer.

### 5️⃣ Spin Up Local Infrastructure (Docker)

```bash
docker-compose up -d
```

Verify all containers (Zookeeper, Kafka, Spark) are healthy:

```bash
docker ps
```

Create the Kafka topic used for streaming ingestion:

```bash
docker exec -it kafka kafka-topics.sh --create \
  --topic nyc-taxi-stream \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

## 🚀 Step-by-Step Execution Guide

### Step 1 — Train the Batch Model

Runs a Spark job that reads `batch.csv` from ADLS Gen2, trains the model with Spark MLlib, and writes the artifact back to Azure.

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
  --conf spark.hadoop.fs.azure.account.key.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net=<STORAGE_ACCOUNT_KEY> \
  train_model.py \
  --input abfss://batch-data@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/batch.csv \
  --model-output abfss://batch-data@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/models/taxi_model
```

### Step 2 — Start the Kafka Producer

Streams `stream.csv` row by row into the Kafka topic, simulating live taxi trip events.

```bash
python producer.py \
  --input stream.csv \
  --topic nyc-taxi-stream \
  --bootstrap-server localhost:9092
```

### Step 3 — Run the Spark Streaming Inference Job

Consumes the Kafka topic in real time, loads the pre-trained model from Azure, scores incoming records, and writes predictions back to ADLS Gen2.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
  --conf spark.hadoop.fs.azure.account.key.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net=<STORAGE_ACCOUNT_KEY> \
  spark.py \
  --kafka-bootstrap-server localhost:9092 \
  --kafka-topic nyc-taxi-stream \
  --model-path abfss://batch-data@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/models/taxi_model \
  --output abfss://predictions-output@<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/live-predictions
```

### Step 4 — Verify the Output

Check that predictions are landing in the `predictions-output` container via the Azure Portal, Azure Storage Explorer, or the CLI:

```bash
az storage fs file list \
  --account-name <STORAGE_ACCOUNT_NAME> \
  --file-system predictions-output \
  --path live-predictions
```

---

## 📊 Visualization

Power BI connects **directly** to the ADLS Gen2 endpoint (no intermediate database required):

1. Open **Power BI Desktop** → **Get Data** → **Azure** → **Azure Data Lake Storage Gen2**.
2. Enter the endpoint URL:
   ```
   https://<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net/predictions-output
   ```
3. Authenticate using the **Storage Account Key**.
4. Load the `live-predictions` data and build visuals comparing **actual vs. predicted** trip values — e.g., fare amount, trip duration, or distance.
5. Set an appropriate **refresh schedule** to keep the dashboard current as new predictions arrive.


![alt text](PowerBi.jpeg)
---

## 👤 Authors

**Abdallah Mohamed**, **Mostafa Ehab**, **Youssef ElDwaltly**, **Mina Maged**

If you found this project useful or interesting, consider ⭐ starring the repo!