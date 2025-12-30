# QC Study Guide - WIP

## Apache Airflow (25 Questions)

### Core Concepts

1. What is Apache Airflow and what problem does it solve?
2. What is a DAG? Why is it called "directed acyclic"?
3. What is the Airflow Scheduler and what does it do?
4. What is the Airflow Executor? Name three types.
5. What is `catchup` and when would you disable it?
6. What is `start_date` and how does it affect DAG runs?
7. What is `execution_date` vs the actual run time?

### Operators & Tasks

8. What is an Operator in Airflow?
2. What is the difference between `BashOperator` and `PythonOperator`?
3. How do you define task dependencies using `>>` and `<<`?
4. What is a Sensor? Give an example use case.
5. What is `trigger_rule`? Name two options.
6. How do retries work? What is `retry_delay`?
7. What is the difference between `BranchPythonOperator` and `ShortCircuitOperator`?

### XComs, Variables & Templating

15. What is an XCom?
2. How do you push and pull XCom values?
3. What is an Airflow Variable?
4. What is an Airflow Connection?
5. What is Jinja templating in Airflow?
6. What does `{{ ds }}` represent?
7. What is `default_args` and why is it useful?

### Operations

22. A task is stuck in "queued" state. What would you check?
2. How do you manually trigger a DAG run?
3. What is backfilling and when would you use it?
4. How do you view logs for a specific task run?

---

## Apache Kafka (25 Questions)

### Core Concepts

1. What is Apache Kafka and what problem does it solve?
2. What is the publish-subscribe messaging pattern?
3. How does Kafka differ from a traditional message queue?
4. What is a Kafka broker?
5. What is the role of Zookeeper in Kafka?

### Topics & Partitions

6. What is a Kafka topic?
2. What is a partition?
3. Why are partitions important for scalability?
4. What is a message key and how does it affect partitioning?
5. What is partition ordering guarantee?

### Producers

11. What is a Kafka producer?
2. What is the `acks` configuration? What do values 0, 1, and "all" mean?
3. What is producer batching?
4. What is `linger.ms`?

### Consumers

15. What is a Kafka consumer?
2. What is a consumer group?
3. What happens when you add a consumer to a consumer group?
4. What is an offset?
5. What is consumer commit?
6. What is consumer lag?
7. What is a rebalance?

### Delivery & Replication

22. What are the three message delivery semantics?
2. What is replication factor?
3. What is ISR (In-Sync Replica)?
4. What is the default message retention period?

---

## Apache Spark (30 Questions)

### Core Concepts

1. What is Apache Spark and what problem does it solve?
2. How does Spark differ from Hadoop MapReduce?
3. What is in-memory processing and why is it faster?
4. What is PySpark?

### RDDs & DataFrames

5. What is an RDD (Resilient Distributed Dataset)?
2. What does "resilient" mean in RDD?
3. When would you use RDDs vs DataFrames?
4. What is a DataFrame in Spark?
5. How does a DataFrame differ from an RDD?
6. What is the Catalyst optimizer?

### Transformations & Actions

11. What is a transformation in Spark?
2. What is an action in Spark?
3. What is lazy evaluation?
4. Why does Spark use lazy evaluation?
5. What is the difference between `map()` and `flatMap()`?

### Spark SQL

16. What is Spark SQL?
2. How do you register a DataFrame as a temporary view?
3. How do you run SQL queries on a DataFrame?

### Cluster Architecture

19. What is the Spark Driver?
2. What is an Executor?
3. What is a Spark job, stage, and task? How are they related?

### Partitioning & Performance

22. What is a partition in Spark?
2. What is a shuffle operation? Why are shuffles expensive?
3. What is `repartition()` vs `coalesce()`?
4. What is a broadcast variable?
5. What is an accumulator?

### Spark Submit & Caching

27. What is `spark-submit`?
2. What is the difference between `--deploy-mode client` and `cluster`?
3. What is `cache()` in Spark?
4. What is `persist()` and what are the different storage levels?

---

## Quick Definitions

Be prepared to define these terms in one or two sentences:

### Airflow

- DAG
- Operator
- XCom
- Backfill

### Kafka

- Topic
- Partition
- Offset
- Consumer Group
- ISR

### Spark

- RDD
- DataFrame
- Lazy Evaluation
- Shuffle
- Executor
