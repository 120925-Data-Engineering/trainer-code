# Setting Up Apache Airflow

## Learning Objectives
- Install Apache Airflow using Docker Compose
- Configure essential Airflow settings for integration with Spark and Kafka
- Verify your Airflow installation is working correctly
- Understand the components that need to be running for Airflow to function

## Why This Matters

In the previous weeks, you learned to process data with Spark and stream events with Kafka. But how do you ensure these jobs run on schedule? How do you handle dependencies between tasks? How do you monitor failures and retries?

This is where **Apache Airflow** enters your data engineering toolkit. Before you can orchestrate workflows, you need a properly configured Airflow environment. Getting the setup right from the start prevents debugging headaches later and ensures your development environment mirrors production.

Throughout this training, you will use **Airflow alongside Spark and Kafka** to build complete data pipelines. While this week focuses on Airflow fundamentals, the environment we set up now will support your work with these integrated technologies in upcoming lessons.

## The Concept

### What is Apache Airflow?

Apache Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Originally created at Airbnb in 2014, it became an Apache Top-Level Project in 2019 and is now the industry standard for workflow orchestration.

### Installation with Docker Compose

Docker Compose provides a consistent, isolated environment that closely mirrors production deployments. It also makes it straightforward to add additional services like Spark and Kafka when needed.

#### Step 1: Create Required Directories

Before starting, create the necessary directories for Airflow:

```bash
mkdir -p ./dags ./logs ./plugins ./config
```

#### Step 2: Set Up the Docker Compose File

Create a `docker-compose.yml` file with the following configuration:

```yaml
# docker-compose.yml
version: '3.8'

x-airflow-common: &airflow-common
  image: apache/airflow:2.7.3
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
    - AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
    - AIRFLOW__CORE__LOAD_EXAMPLES=false
    - AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key-here
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db init
        airflow users create \
          --username admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com \
          --password admin
    restart: on-failure

  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", "airflow jobs check --job-type SchedulerJob --hostname $(hostname)"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s

volumes:
  postgres-db-volume:
```

#### Step 3: Initialize and Start Airflow

Run the following commands to start your Airflow environment:

```bash
# Initialize the database and create admin user
docker-compose up airflow-init

# Start all services in detached mode
docker-compose up -d
```

#### Step 4: Verify the Installation

Check that all services are running:

```bash
# View running containers
docker-compose ps

# Check container logs for any errors
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
```

Access the Airflow UI at `http://localhost:8080` and log in with:
- **Username:** admin
- **Password:** admin

### Essential Configuration

Airflow configuration can be set via environment variables in your `docker-compose.yml`. The naming convention converts config file settings to environment variables:

| Config File Setting | Environment Variable |
|---------------------|---------------------|
| `[core] dags_folder` | `AIRFLOW__CORE__DAGS_FOLDER` |
| `[core] executor` | `AIRFLOW__CORE__EXECUTOR` |
| `[webserver] web_server_port` | `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` |

Common configuration options:

```yaml
environment:
  # Core settings
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__CORE__LOAD_EXAMPLES=false
  - AIRFLOW__CORE__DAG_DIR_LIST_INTERVAL=300
  
  # Scheduler settings
  - AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5
  
  # Webserver settings
  - AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
```

### Components That Must Be Running

For Airflow to function, you need:

1. **Metadata Database**: Stores DAG state, task history, connections
2. **Webserver**: Provides the UI for monitoring and management
3. **Scheduler**: Monitors DAGs and triggers task execution
4. **Executor**: Actually runs the tasks (can be local or distributed)

```
+-------------+     +-------------+     +-------------+
|  Scheduler  |---->|   Database  |<----|  Webserver  |
+-------------+     +-------------+     +-------------+
       |
       v
+-------------+
|   Executor  |
+-------------+
```

### Stopping and Cleaning Up

When you need to stop your Airflow environment:

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clears all data)
docker-compose down -v
```

## Summary

- Docker Compose is the recommended method for setting up Airflow locally
- The setup includes PostgreSQL as the metadata database, a webserver, and a scheduler
- Configuration is done via environment variables in the `docker-compose.yml`
- This environment will support integration with Spark and Kafka in future lessons
- Always verify your installation before creating DAGs

## Additional Resources

- [Apache Airflow Official Documentation](https://airflow.apache.org/docs/)
- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Astronomer Getting Started Guide](https://docs.astronomer.io/learn/get-started-with-airflow)
