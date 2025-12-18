# Exercise: Context Comparison

## Overview
**Day:** Monday  
**Mode:** Hybrid (Conceptual + Implementation)  
**Duration:** 1-1.5 hours  
**Topics:** SparkSession vs SparkContext, legacy contexts

## Learning Objectives
By completing this exercise, you will be able to:
- Explain the relationship between SparkSession and SparkContext
- Access SparkContext, SQLContext from SparkSession
- Understand when to use each entry point
- Convert between RDDs and DataFrames

## Prerequisites
- Completed `exercise_spark_session.py`
- Reading: `spark-session-vs-spark-context.md`, `sparksqlcontext-vs-hivecontext.md`

---

## Core Tasks

### Task 1: Understanding the Relationship (20 mins)
Navigate to `exercise_context_comparison.py` and:

1. Create a SparkSession
2. Access the SparkContext via `spark.sparkContext`
3. Prove they are connected by:
   - Printing the app name from both SparkSession and SparkContext
   - Verifying they share the same application ID

4. **Answer these questions in comments:**
   - Q1: Can you create a SparkContext after SparkSession exists?
   - Q2: What happens if you try?

### Task 2: RDD vs DataFrame Operations (25 mins)
Using the SAME SparkSession:

1. Create an RDD using `sparkContext.parallelize([1, 2, 3, 4, 5])`
2. Create a DataFrame with the same data using `spark.createDataFrame()`
3. Perform a simple transformation on each (double the values)
4. Convert: RDD to DataFrame, then DataFrame back to RDD

**Answer in comments:**
- Q3: Which approach felt more natural?
- Q4: What data type are the elements in the RDD from df.rdd?

### Task 3: Broadcast and Accumulator Access (15 mins)
1. Create a broadcast variable with a lookup dictionary using `sparkContext.broadcast()`
2. Create an accumulator using `sparkContext.accumulator(0)`
3. Use both in an RDD operation

**Answer in comments:**
- Q5: Why are these accessed via SparkContext instead of SparkSession?

---

## Conceptual Questions
Answer these in the designated section of your code file:

1. In a new PySpark 3.x project, which entry point would you use and why?

2. You inherit legacy Spark 1.x code that uses SQLContext. What is the minimal change to modernize it?

3. Draw (in ASCII or describe) the relationship between SparkSession, SparkContext, SQLContext, and HiveContext.

---

## Definition of Done
- [ ] SparkSession and SparkContext connection verified
- [ ] RDD and DataFrame transformations completed
- [ ] Conversions between RDD and DataFrame working
- [ ] Broadcast and accumulator examples working
- [ ] All conceptual questions answered in comments
- [ ] Code runs without errors

---

## Hints
- `spark.sparkContext` gives you access to SparkContext
- `df.rdd` converts DataFrame to RDD (elements are Row objects)
- `spark.createDataFrame(rdd, schema)` converts RDD to DataFrame
- Broadcast: `sc.broadcast(data)`, access with `.value`
- Accumulator: `sc.accumulator(0)`, update with `.add(n)`
