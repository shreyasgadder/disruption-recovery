# ETG Disruption Recovery Pipeline

## The Mission
This project implements a disruption recovery engine for an Online Travel Agency. It processes a high-volume airline event stream (5 million records) and reconciles it against an internal Booking Master to identify conflicts and prioritize customer recovery.

The architecture follows a memory-efficient Medallion pattern (Bronze, Silver, Gold), ensuring data integrity during high-scale flight disruptions.

## Quick Start
1.  **Environment Setup**:
    Start the Postgres database, Airflow, and the Spark streaming consumer (which runs invisibly in the background).
    ```bash
    docker-compose up -d --build
    ```
    *Wait for all containers to be Up and Healthy.*

2.  **Run Phase A (Production Audit)**:
    Run the standalone streaming audit on your local machine (completely separate from Docker/Airflow).
    ```bash
    python scripts/phase_a_audit.py
    ```

3.  **Start Phase B (Streaming Simulation)**:
    Start the event trickler on your local machine to mimic a real-time stream into the input directory.
    ```bash
    python scripts/simulate_stream.py
    ```
    *(Note: You do not need to run `spark_streamer.py` yourself. The Docker container `egt_spark_streamer` runs it automatically 24/7, catching the files you trickle in.)*

4.  **Monitor via Airflow**: 
    -   Access the UI at `http://localhost:8080` (U: `airflow`, P: `airflow`).
    -   Unpause the `etg_recovery_mission` DAG.
    -   This DAG acts as a dashboard, querying the Gold layer database every 5 minutes and printing the recovery status and VIP alerts.

## Repository Structure
-   `/src`: Core logic and processors.
    -   `spark_processors.py`: Data cleaning, reconciliation, and aggregation modules.
    -   `spark_streamer.py`: Robust polling-based Spark consumer.
-   `/dags`: Airflow orchestration and Medallion monitoring.
-   `/scripts`: 
    -   `phase_a_audit.py`: Restricted RAM streaming auditor (Standalone CLI).
    -   `simulate_stream.py`: Stream simulation.
-   `/infrastructure`: Database initialization and service configs.

## Requirements
-   Docker and Docker Compose.
-   Minimum 6GB RAM (WSL2 recommended).
-   Stack: PySpark 3.5.4, Airflow 2.10.4, PostgreSQL 15.
