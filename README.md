Real-Time E-Commerce Analytics with Kafka and Spark

A production-style real-time data streaming pipeline built using Apache Kafka and Apache Spark Structured Streaming.
This project simulates large-scale e-commerce order processing, real-time alerting, and analytical aggregation.

Project Overview

This project processes high-volume streaming data and performs:
Real-time ingestion of e-commerce orders
High-value order detection and alerting
Time-window based revenue aggregation
Fault-tolerant stream processing
Batch analytics on streaming output
It reflects a real-world streaming data architecture, not a toy or demo setup.

System Architecture:

CSV File (100K+ Orders)
        ↓
Kafka Producer
        ↓
Kafka Topic: orders
        ↓
Spark Structured Streaming
        ├── High-Value Alerts → Kafka Topic: alerts
        └── Revenue Aggregation → Parquet Files
                                          ↓
                                  Spark Batch Analytics
                                  
Tech Stack

Apache Kafka – Distributed event streaming platform
Apache Spark 3.5.1 – Stream and batch processing
Spark Structured Streaming
Docker & Docker Compose
Python (PySpark & kafka-python)
Parquet: Columnar analytics storage format

Key Features:

1- Real-Time Streaming:

Orders are continuously streamed through Kafka
Spark processes events incrementally with low latency

2- High-Value Order Alerts 🚨:

Computes order value as price × quantity
Triggers alerts when threshold is exceeded
Alerts are published to a separate Kafka topic

3- Windowed Revenue Analytics

Revenue aggregated per category
Uses time windows and watermarking
Safely handles late-arriving events

4- Fault Tolerance

Spark checkpointing ensures:
Exactly-once processing
Offset tracking
Automatic recovery after failures

5- Batch Analytics:

Streaming output stored as Parquet
Separate Spark batch job computes final revenue summaries


Scalability Notes:

Designed to scale with:
    Higher Kafka partitions
    Larger datasets (millions of records)
    Multiple Spark executors
Uses Parquet for efficient analytical workloads
Easily portable to cloud environments
