# General Interview Preparation

## Learning Objectives

- Understand common PySpark interview question categories
- Learn strategies for whiteboard coding and technical interviews
- Prepare behavioral interview responses for data engineering roles
- Practice articulating project experience effectively

## Why This Matters

Technical skills alone do not guarantee success in interviews. Data engineering interviews typically assess:

- **Technical depth**: Your understanding of Spark, data systems, and programming
- **Problem-solving ability**: How you approach unfamiliar challenges
- **Communication skills**: Your ability to explain complex concepts
- **Experience**: How you have applied knowledge in real projects

This preparation session consolidates your Week 1 and Week 2 PySpark knowledge into a job-ready skillset, aligning with our Weekly Epic of *Mastering Spark SQL and DataFrames*.

## The Concept

### Interview Question Categories

Data engineering interviews typically include:

| Category | Examples | Preparation Focus |
|----------|----------|-------------------|
| **Conceptual** | RDD vs DataFrame, when to use each | Understand trade-offs and reasoning |
| **Coding** | Transform data, write aggregations | Practice common patterns |
| **Optimization** | Improve slow queries, reduce shuffles | Know performance techniques |
| **System Design** | Design a data pipeline | Understand architecture patterns |
| **Behavioral** | Tell me about a challenging project | Prepare STAR format stories |

### Common PySpark Interview Topics

**Week 1 Topics (RDD Foundation):**

- RDD operations: transformations vs actions
- Partitioning and parallelism
- Broadcast variables and accumulators
- Spark architecture: driver, executors, cluster managers

**Week 2 Topics (Spark SQL and DataFrames):**

- SparkSession vs SparkContext
- DataFrame operations and optimizations
- Joins and their performance implications
- Caching and persistence strategies
- Bucketing and partitioning for performance

### The STAR Method for Behavioral Questions

Structure your answers with:

- **Situation**: Context and background
- **Task**: What you needed to accomplish
- **Action**: What you specifically did
- **Result**: The outcome and what you learned

### Whiteboard Coding Strategies

1. **Clarify requirements** before coding
2. **Think out loud** as you work
3. **Start simple**, then optimize
4. **Handle edge cases** (nulls, empty data)
5. **Test your solution** with examples

## Common Interview Questions

### Conceptual Questions

**Q: What is the difference between RDD and DataFrame?**

**A**: RDDs (Resilient Distributed Datasets) are the low-level, fundamental data structure in Spark. They provide fine-grained control but require manual optimization. DataFrames are a higher-level abstraction built on RDDs that represent data as distributed tables with named columns and schemas.

Key differences:

- **Optimization**: DataFrames benefit from the Catalyst optimizer, which automatically optimizes query plans. RDDs require manual optimization.
- **Schema**: DataFrames have schemas; RDDs are untyped (in Python).
- **Performance**: DataFrames are generally faster due to Tungsten memory management and code generation.
- **API**: DataFrames support SQL-like operations; RDDs use functional transformations.

**When to use each**: Use DataFrames for structured data processing (most cases). Use RDDs when you need fine-grained control, custom partitioning, or are working with unstructured data.

---

**Q: Explain the difference between narrow and wide transformations.**

**A**:

- **Narrow transformations** (like `map`, `filter`) process data within each partition without data movement. Each output partition depends on only one input partition.
- **Wide transformations** (like `groupBy`, `join`, `repartition`) require data to be shuffled across the network because output partitions depend on multiple input partitions.

Wide transformations are expensive because they require:

1. Writing intermediate data to disk
2. Network transfer between nodes
3. Reading and combining data on destination nodes

Minimizing wide transformations improves performance.

---

**Q: What is the Catalyst optimizer?**

**A**: Catalyst is Spark SQL's query optimizer. It takes your logical query plan (what you want to compute) and transforms it into an optimized physical plan (how to compute it efficiently).

Key optimizations include:

- **Predicate pushdown**: Filters are pushed to the data source
- **Column pruning**: Only necessary columns are read
- **Join reordering**: Joins are ordered for efficiency
- **Constant folding**: Constant expressions are pre-computed

### Coding Questions

**Q: Given a DataFrame of employees with columns (id, name, department, salary), find the top 3 highest-paid employees in each department.**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Top Earners").getOrCreate()

# Sample data
data = [
    (1, "Alice", "Engineering", 80000),
    (2, "Bob", "Engineering", 75000),
    (3, "Charlie", "Engineering", 70000),
    (4, "Diana", "Engineering", 65000),
    (5, "Eve", "Marketing", 60000),
    (6, "Frank", "Marketing", 55000),
    (7, "Grace", "Marketing", 50000)
]

df = spark.createDataFrame(data, ["id", "name", "department", "salary"])

# Window function approach
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

result = df.withColumn("rank", row_number().over(window_spec)) \
           .filter(col("rank") <= 3) \
           .drop("rank")

result.show()
```

**Key points to mention:**

- Window functions are efficient for ranking within groups
- `row_number()` gives unique ranks; `rank()` allows ties
- Alternative approaches: groupBy with collect_list and slice

---

**Q: How would you find duplicate records in a DataFrame?**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder.appName("Find Duplicates").getOrCreate()

data = [
    ("Alice", "alice@email.com"),
    ("Bob", "bob@email.com"),
    ("Alice", "alice@email.com"),  # Duplicate
    ("Charlie", "charlie@email.com"),
    ("Alice", "alice@email.com")   # Duplicate
]

df = spark.createDataFrame(data, ["name", "email"])

# Method 1: Find which records are duplicated
duplicates = df.groupBy(df.columns) \
               .agg(count("*").alias("count")) \
               .filter(col("count") > 1)

print("Duplicate records:")
duplicates.show()

# Method 2: Show all duplicate rows
df_with_count = df.groupBy(df.columns).count()
duplicate_records = df.join(df_with_count.filter(col("count") > 1).drop("count"),
                           df.columns, "inner")
```

---

**Q: Write code to join two DataFrames and handle cases where keys might be null.**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

spark = SparkSession.builder.appName("Safe Join").getOrCreate()

# DataFrames with potential null keys
users = spark.createDataFrame([
    (1, "Alice"),
    (None, "Bob"),
    (2, "Charlie")
], ["id", "name"])

orders = spark.createDataFrame([
    (1, 100),
    (None, 200),
    (3, 300)
], ["user_id", "amount"])

# Standard join (nulls do not match)
print("Standard join (nulls excluded):")
users.join(orders, users.id == orders.user_id, "full").show()

# To match nulls, use coalesce or eqNullSafe
print("Null-safe join:")
users.join(orders, users.id.eqNullSafe(orders.user_id), "full").show()
```

### Optimization Questions

**Q: A Spark job is running slowly. How would you diagnose and fix it?**

**A**: I would follow a systematic approach:

1. **Check Spark UI**: Look at stage timelines, task distribution, and shuffle read/write sizes.

2. **Identify the bottleneck**:
   - **Data skew**: A few partitions taking much longer than others
   - **Shuffle overhead**: Large shuffle read/write values
   - **Memory issues**: Frequent spilling to disk

3. **Common solutions**:
   - **Data skew**: Salting keys, broadcast joins for small tables
   - **Excessive shuffles**: Reduce operations like `groupBy`, use `repartition` strategically
   - **Memory**: Increase executor memory, use appropriate caching
   - **Wrong join type**: Use broadcast join for small tables

4. **Specific techniques**:
   - Cache intermediate DataFrames used multiple times
   - Adjust `spark.sql.shuffle.partitions` based on data size
   - Enable Adaptive Query Execution (`spark.sql.adaptive.enabled`)
   - Use bucketing for repeated joins on the same key

---

**Q: When would you use `cache()` vs `persist()`?**

**A**: Both store DataFrames for reuse, but:

- `cache()` is shorthand for `persist(MEMORY_AND_DISK)`
- `persist()` allows choosing storage levels

Use `cache()` when memory is sufficient and you want the default behavior.

Use `persist()` with specific levels when:

- Memory is limited: `MEMORY_ONLY_SER` (serialized, less memory)
- Memory is very limited: `DISK_ONLY`
- Critical data: `MEMORY_AND_DISK_2` (replicated)

Only cache DataFrames used multiple times. Do not cache one-time computations.

### Behavioral Questions

**Q: Tell me about a challenging data engineering project you worked on.**

**Example Answer (STAR format)**:

**Situation**: At my previous role, we had a daily ETL job that processed 500GB of event data but was taking 6+ hours to complete, often failing overnight.

**Task**: I was responsible for reducing the job runtime to under 2 hours while ensuring data quality.

**Action**: I analyzed the job using Spark UI and found three issues:

1. Data skew in the join on user_id
2. Too many small partitions (20,000)
3. No caching of repeatedly used data

I addressed these by:

- Implementing broadcast joins for the user dimension table
- Repartitioning to 200 partitions based on our cluster size
- Adding strategic caching for the main event DataFrame
- Adding data quality checks using PySpark validation

**Result**: The job runtime dropped to 1.5 hours with 99.9% success rate. I documented the optimizations so the team could apply similar techniques.

---

**Q: How do you stay current with data engineering technologies?**

**Example Answer**: I take a multi-pronged approach:

- I follow technical blogs from Databricks and Apache Spark
- I participate in online communities and forums
- I work on personal projects to try new features
- I take online courses when learning major new technologies
- I share knowledge with my team through tech talks and documentation

## Interview Tips

### Before the Interview

- Review fundamental concepts from both weeks
- Practice coding without IDE assistance
- Prepare 3-5 project stories using STAR format
- Research the company's data infrastructure

### During the Interview

- Listen carefully to the full question before answering
- Ask clarifying questions: "Is the data sorted?" "What is the data volume?"
- Think out loud to show your reasoning
- Start with a working solution, then optimize
- Discuss trade-offs: "This approach uses more memory but is faster..."

### Common Mistakes to Avoid

- Jumping to code without understanding requirements
- Ignoring null handling and edge cases
- Not considering performance implications
- Giving one-word answers to behavioral questions
- Not asking questions about the role and team

## Summary

- Interview preparation covers conceptual, coding, optimization, and behavioral questions
- Understand the trade-offs between RDDs, DataFrames, and their operations
- Practice common coding patterns: joins, aggregations, window functions
- Know optimization techniques: caching, partitioning, broadcast joins
- Prepare STAR-format stories for behavioral questions
- Think out loud and discuss trade-offs during technical questions
- Ask clarifying questions before jumping to solutions

## Additional Resources

- [Spark Interview Questions (Databricks)](https://www.databricks.com/blog/2020/03/17/how-to-prepare-for-a-spark-interview.html)
- [PySpark Coding Practice (LeetCode)](https://leetcode.com/problemset/) - Many problems can be solved with Spark
- [Spark Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Data Engineering Interview Preparation](https://www.interviewquery.com/blog-data-engineering-interview-questions/)
