# Hadoop vs Spark

## Learning Objectives
- Compare MapReduce and Spark processing models
- Understand the advantages of in-memory computing
- Identify when to use Hadoop vs Spark in real-world scenarios
- Recognize performance differences and trade-offs

## Why This Matters
Before Spark, Hadoop MapReduce was the dominant framework for distributed data processing. Understanding the evolution from Hadoop to Spark helps you appreciate why Spark has become the preferred choice for modern data engineering. Many organizations still run both technologies, and knowing their differences enables you to make informed architectural decisions.

## The Concept

### Hadoop MapReduce: The Foundation
Hadoop MapReduce was revolutionary when introduced in 2006. It enabled processing of massive datasets across commodity hardware clusters. However, it has significant limitations:

**How MapReduce Works:**
1. **Map Phase:** Input data is split and processed in parallel
2. **Shuffle Phase:** Intermediate results are sorted and transferred
3. **Reduce Phase:** Results are aggregated to produce final output

**MapReduce Limitations:**
- **Disk I/O Overhead:** Intermediate results are written to disk after each stage
- **Batch Processing Only:** Not designed for iterative algorithms or interactive queries
- **Complex Programming Model:** Requires verbose Java code for simple operations
- **Latency:** Multiple disk reads/writes create significant delays

### Apache Spark: The Modern Approach
Spark addresses MapReduce limitations through several innovations:

**In-Memory Processing:**
- Keeps intermediate results in memory across operations
- Reduces disk I/O by 10-100x for many workloads

**DAG Execution Engine:**
- Optimizes the entire computation as a Directed Acyclic Graph
- Combines multiple operations into efficient stages
- Minimizes data shuffling

**Unified API:**
- Single framework for batch, streaming, ML, and graph processing
- Concise APIs in Python, Scala, Java, and R

### Side-by-Side Comparison

| Aspect | Hadoop MapReduce | Apache Spark |
|--------|------------------|--------------|
| **Processing** | Disk-based | In-memory |
| **Speed** | Slower (disk I/O) | Up to 100x faster |
| **Ease of Use** | Verbose Java code | Concise Python/Scala |
| **Processing Type** | Batch only | Batch + Streaming |
| **Iterative Algorithms** | Poor performance | Excellent performance |
| **Fault Tolerance** | Disk replication | RDD lineage |
| **Resource Usage** | Disk-heavy | Memory-heavy |

### Performance Benchmark Example

For a typical log analysis job processing 100GB of data:

```
MapReduce:
- Time: ~30 minutes
- Disk writes: 5 stages x 100GB = 500GB I/O

Spark:
- Time: ~3 minutes
- Disk writes: Minimal (only final output)
```

### When to Use Each

**Choose Hadoop MapReduce when:**
- Budget constraints limit available memory
- Processing truly massive datasets (petabytes) with limited memory
- Existing infrastructure is heavily invested in MapReduce
- Simple, single-pass processing jobs

**Choose Spark when:**
- Iterative algorithms (machine learning, graph processing)
- Interactive data exploration
- Real-time or near-real-time processing needed
- Complex multi-stage pipelines
- Data fits reasonably in cluster memory

## Code Example

The same word count operation in both frameworks illustrates the difference:

**MapReduce (Java):**
```java
public class WordCount {
    public static class TokenizerMapper 
        extends Mapper<Object, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
    
    public static class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
```

**Spark (Python):**
```python
from pyspark import SparkContext

sc = SparkContext("local", "WordCount")
text_file = sc.textFile("input.txt")

counts = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("output")
sc.stop()
```

The Spark version accomplishes the same task in a fraction of the code.

## Summary
- Hadoop MapReduce processes data using disk-based operations between stages
- Spark uses in-memory processing for dramatically faster performance
- Spark provides a unified, concise API across multiple languages
- MapReduce remains relevant for specific use cases with constrained memory
- Spark excels at iterative algorithms, interactive queries, and streaming
- Modern data engineering predominantly uses Spark for new workloads

## Additional Resources
- [Spark vs Hadoop MapReduce - Databricks](https://www.databricks.com/glossary/hadoop)
- [Apache Spark FAQ](https://spark.apache.org/faq.html)
- [MapReduce Tutorial - Apache Hadoop](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
