# Exercise: SparkSession Setup and Configuration

## Overview
**Day:** Monday  
**Mode:** Implementation (Code Lab)  
**Duration:** 1.5-2 hours  
**Topics:** SparkSession creation, configuration, builder pattern

## Learning Objectives
By completing this exercise, you will be able to:
- Create SparkSession objects using the builder pattern
- Configure SparkSession with various settings
- Access and inspect session properties
- Understand getOrCreate() behavior

## Prerequisites
- Week 1 PySpark fundamentals
- Understanding of RDDs and SparkContext (from Week 1)
- Reading: `intro-to-spark-sql.md`, `sparksession.md`

---

## Core Tasks

### Task 1: Basic SparkSession Creation (20 mins)
Navigate to `starter_code/exercise_spark_session.py` and complete the following:

1. Create a SparkSession with:
   - App name: "MyFirstSparkSQLApp"
   - Master: "local[*]"
   
2. Print the following information:
   - Spark version
   - Application ID
   - Default parallelism

3. Create a simple DataFrame with 3 columns and 5 rows to verify your session works.

### Task 2: Configuration Exploration (20 mins)
1. Create a NEW SparkSession (use a different variable name) with:
   - App name: "ConfiguredApp"
   - `spark.sql.shuffle.partitions` set to 50
   - `spark.driver.memory` set to "2g" (note: this must be set at start)
   
2. Verify your configuration by printing:
   - The value of `spark.sql.shuffle.partitions`
   - At least 3 other configuration values

3. Try changing `spark.sql.shuffle.partitions` at runtime. Does it work?

### Task 3: getOrCreate() Behavior (15 mins)
1. Call `SparkSession.builder.appName("DifferentName").getOrCreate()` 
2. Check which app name is actually used
3. Explain in a comment why this happens

### Task 4: Session Cleanup (10 mins)
1. Properly stop your SparkSession
2. Verify the session has stopped by checking `spark._jsc.sc().isStopped()`

---

## Stretch Goals (Optional)
- Enable Hive support and explore the difference
- List all available configuration options using `spark.sparkContext.getConf().getAll()`
- Create a helper function that creates a SparkSession with your preferred defaults

---

## Definition of Done
- [ ] SparkSession created successfully with builder pattern
- [ ] Configuration values printed and verified
- [ ] getOrCreate() behavior explained in comments
- [ ] Session properly stopped
- [ ] Code runs without errors

---

## Hints
- Use `spark.sparkContext` to access SparkContext properties
- Use `spark.conf.get("key")` to read configuration values
- Use `spark.conf.set("key", "value")` to change mutable configs
- Not all configurations can be changed after session starts
