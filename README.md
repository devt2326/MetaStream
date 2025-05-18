# MetaStream: Real-Time Event Streaming and Analytics Pipeline

MetaStream is a real-time data engineering pipeline that simulates social media engagement events and processes them using Apache Kafka, Apache Spark, and Parquet for analytics and visualization.

---

## 🚀 Overview

This project demonstrates a production-style streaming architecture:

* **Kafka** to simulate and stream social events (likes, shares, comments, follows)
* **Spark Structured Streaming** to process events from Kafka in real time
* **Parquet** as a storage layer for optimized querying
* **JupyterLab in Docker** to explore and analyze the ingested data interactively

---

## 🧱 Architecture

```text
                 +---------------------+
                 |  Event Producer     | (Python script)
                 |  (Kafka producer)   |
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |   Apache Kafka      |
                 |  Topic: social_events|
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |   Apache Spark      |
                 | Structured Streaming|
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |   Parquet Storage   |
                 |  /parquet_output/   |
                 +---------------------+
                           |
                           v
                 +---------------------+
                 |   Jupyter Notebook  |
                 |   Analytics + Viz   |
                 +---------------------+
```

---

## 📁 File Structure

```
MetaStream/
├── kafka/
│   ├── docker-compose.yml
│   └── event_producer.py
├── spark/
│   ├── stream_processor.py
│   └── run_spark.sh
├── parquet_output/
│   ├── checkpoint/
│   └── _spark_metadata/
├── stream-submit.ipynb
├── docker-compose.yml
├── docker-compose.spark.yml
└── README.md
```

---

## 🛠️ Technologies Used

* **Python** for producer and ETL orchestration
* **Apache Kafka** for message streaming
* **Apache Spark** for distributed stream processing
* **Parquet** for optimized columnar storage
* **JupyterLab** (inside Docker) for analytics
* **Docker Compose** for orchestration

---

## 🧪 How to Run

1. **Start Kafka & Zookeeper**:

```bash
cd kafka
docker compose up -d
```

2. **Start Spark Cluster with Jupyter**:

```bash
cd ..
docker compose -f docker-compose.spark.yml up --build
```

3. **Run Event Producer**:

```bash
python kafka/event_producer.py
```

4. **Start Stream Processor in Jupyter Notebook**:
   Open `http://localhost:8888/lab` using the printed token. Then run `stream-submit.ipynb`.

5. **Explore Parquet Output**:

```python
# Inside notebook
df = spark.read.parquet("/home/jovyan/work/parquet_output/")
df.createOrReplaceTempView("events")
spark.sql("SELECT event_type, COUNT(*) FROM events GROUP BY event_type").show()
```

---

## 📊 Example Queries

```sql
SELECT location, COUNT(*) FROM events GROUP BY location;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
SELECT hour(timestamp), COUNT(*) FROM events GROUP BY hour(timestamp);
```

---

## 📌 Highlights

* Simulates realistic user engagement events (with timestamp, location, device, etc.)
* Real-time ingestion and append-mode storage
* Easily extendable for windowed aggregation or alerts
* Ideal for learning **data pipelines**, **streaming**, and **analytics at scale**

---

## 📦 Future Enhancements

* Integrate with **Streamlit** or **Dash** for visual dashboards
* Add **data quality validation** and alerting
* Add **windowed aggregations** and **sliding window joins**

---

## 📜 License

MIT License. This project is for educational purposes and showcases common industry design patterns for real-time analytics.

---

## 👨‍💻 Author

Built by Dev Thakkar as a Meta-style data engineering capstone simulation. 
