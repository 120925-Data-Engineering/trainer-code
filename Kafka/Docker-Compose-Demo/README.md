# Kafka & Spark Demo Notes

## 1. Environment Management

These commands control the infrastructure (Kafka, Zookeeper, Spark Cluster). Run these from the folder containing `docker-compose.yaml`.

**Start the Cluster**
Downloads images and starts all services in the background.

```bash
docker compose up -d

```

**Stop the Cluster**
Stops and removes containers and networks.

```bash
docker compose down

```

**Nuclear Reset**
Use this if Kafka is crashing or data is corrupted. It deletes the containers and the persistent data volumes.

```bash
docker compose down --volumes
docker compose up -d

```

## 2. Monitoring & Logging

**Check Running Services**
Ensure `kafka`, `spark-master`, and `generator` are listed as "Up".

```bash
docker ps

```

**Check Data Generator**
View the output of the Python producer to ensure it is sending data.

* Press `CTRL+C` to exit the log view.

```bash
docker logs -f generator

```

## 3. Submitting the Spark Job

This command connects to the Spark Master container and submits the Python script. It includes the necessary Maven packages for Kafka support.

### Option A: Windows (PowerShell)

* **Note:** This assumes you are using PowerShell. If using Command Prompt, change the backticks (`) to carets (`^`). This command will work in **GitBash** ***AS IS!***

```powershell
docker exec -it spark-master spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
  /app/spark_kafka_stream.py

```

### Option B: UNIX / macOS / Linux

* **Note:** If using Git Bash on Windows, use this version but prepend `MSYS_NO_PATHCONV=1` to the command to prevent path conversion errors.

```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/opt/bitnami/spark/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/spark_kafka_stream.py

```

## 4. Common Troubleshooting

**Error: "File not found: /app/spark_kafka_stream.py"**

* **Cause:** The `scripts` folder on your host machine is empty or the file is named incorrectly.
* **Fix:** Ensure your local folder structure matches: `kafka-spark-lab/scripts/spark_kafka_stream.py`.

**Error: "NoBrokersAvailable"**

* **Cause:** The Python script started before Kafka was fully initialized.
* **Fix:** Restart the generator container.

```bash
docker compose restart generator

```

**Error: "AnalysisException: Table or view not found"**

* **Cause:** Connection to Kafka failed, or the topic does not exist yet.
* **Fix:** Ensure the generator is running (`docker logs generator`) and check that your script points to `kafka:9092` (not localhost).
