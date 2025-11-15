# Claims Data Engineering Platform

### Project Overview

This project sets up a complete, containerized data engineering environment for processing claims data. It leverages a modern data stack, including Change Data Capture (CDC), real-time stream processing, a data lakehouse, and a powerful query engine.

The architecture is designed to capture data changes from a transactional PostgreSQL database, process them in real-time using Spark, store them in a Delta Lake table in an AWS S3 bucket, and make them available for analytics through Trino and Metabase.

### Architecture

The platform is composed of several services orchestrated by Docker Compose:

1.  **Data Ingestion (CDC):**
    *   **PostgreSQL (`postgres`):** The source transactional database containing claims data. It's configured with `wal_level=logical` to enable Change Data Capture.
    *   **Debezium (`connect`):** Captures row-level changes (inserts, updates, deletes) from the PostgreSQL database in real-time.
    *   **Kafka (`kafka`):** Debezium publishes the captured change events to Kafka topics, creating a durable, real-time stream of data modifications.

2.  **Stream Processing:**
    *   **PySpark (`pyspark-app`):** A Spark environment for running data processing jobs. It's intended to consume data from Kafka topics, perform transformations, and write the results to the data lake.
    *   **JupyterLab (`jupyter-lab`):** An interactive development environment for creating and testing PySpark applications and notebooks.

3.  **Data Lakehouse & Querying:**
    *   **AWS S3:** The data lakehouse storage layer. Processed data is stored in Delta Lake format in a designated S3 bucket, making it scalable and durable.
    *   **Trino (`trino`):** A high-performance, distributed SQL query engine. It is configured with a Delta Lake connector to query the data stored in the S3 bucket directly. It uses AWS Glue as its metastore to manage table schemas.
    *   **Metabase (`metabase`):** A user-friendly business intelligence and visualization tool. It connects to Trino, allowing for easy exploration, dashboarding, and analysis of the claims data in the Delta Lake.

### Use Cases & Problems Solved

This architecture is designed to solve several common challenges in data engineering:

*   **Unified Data Platform (Single Source of Truth):** By centralizing data from operational databases into a Delta Lake, this project creates a single, reliable source for all analytics. This eliminates data silos and ensures consistency across reports and dashboards.

*   **Real-Time Analytics:** Traditional analytics often relies on slow, nightly batch jobs. This platform enables real-time dashboards and monitoring by capturing data changes as they happen. For example, you can monitor incoming claims, track processing status, or detect fraudulent activity with minimal delay.

*   **Decoupling Analytics from Operational Systems:** Running complex, long-running analytical queries directly against a production database can degrade its performance. This architecture offloads the analytical workload to a separate, optimized system (Spark and Trino), protecting the performance of the source application.

*   **Data Auditing and Historical Analysis:** Delta Lake's "time travel" feature versions the data with every change. This allows you to query the state of a claim or policy at any point in time, which is invaluable for auditing, debugging data pipelines, and reproducing historical reports.

*   **Scalable and Future-Proof Data Platform:** The components are highly scalable and handle schema evolution gracefully. If columns are added to the source tables, the pipeline can adapt without breaking, making the platform robust and easy to maintain.
