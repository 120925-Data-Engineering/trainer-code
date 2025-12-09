# Data Loading and Saving Through RDDs

## Learning Objectives
- Load data from various file formats into RDDs
- Save RDD data to different output formats
- Work with textFile, wholeTextFiles, and sequence files
- Handle different data sources and sinks

## Why This Matters
Data engineering involves moving data between systems. Understanding how to efficiently load and save data through RDDs enables you to build robust ETL pipelines. Whether reading from local files, HDFS, or cloud storage, these patterns form the foundation of data processing workflows.

Building on the RDD concepts from yesterday, today focuses on practical data I/O operations.

## The Concept

### Loading Text Files

#### textFile()
The most common way to load data. Each line becomes an element in the RDD.

```python
# Load a single file
lines = sc.textFile("data/input.txt")

# Load multiple files
all_logs = sc.textFile("logs/*.log")

# Load from HDFS
hdfs_data = sc.textFile("hdfs://namenode:9000/data/input.txt")

# Load from S3
s3_data = sc.textFile("s3a://bucket-name/path/to/file.txt")
```

**Key behaviors:**
- Returns RDD[String] where each element is one line
- Automatically handles compressed files (.gz, .bz2)
- Supports wildcards for multiple files
- Partitions based on HDFS block size or specified minPartitions

```python
# Control partitioning
data = sc.textFile("large_file.txt", minPartitions=10)
```

#### wholeTextFiles()
Reads entire files as key-value pairs (filename, content).

```python
# Returns (filename, entire_content) pairs
files = sc.wholeTextFiles("documents/")

for filename, content in files.take(2):
    print(f"File: {filename}")
    print(f"Content length: {len(content)} characters")
```

**Use cases:**
- Small files where line-by-line processing is not appropriate
- Processing documents as complete units
- Preserving file-level context

### Saving Text Files

#### saveAsTextFile()
Writes RDD elements as text files.

```python
rdd = sc.parallelize(["line 1", "line 2", "line 3"])
rdd.saveAsTextFile("output/results")
```

**Output structure:**
```
output/results/
    _SUCCESS          # Marker file indicating completion
    part-00000        # Partition 0 data
    part-00001        # Partition 1 data
    ...
```

**Important considerations:**
- Output directory must not exist (or will error)
- Creates one file per partition
- Use `coalesce()` or `repartition()` to control output file count

```python
# Single output file
rdd.coalesce(1).saveAsTextFile("output/single_file")

# Specific number of files
rdd.repartition(4).saveAsTextFile("output/four_files")
```

### Working with Structured Data

#### CSV Files
```python
# Load CSV
csv_rdd = sc.textFile("data.csv")

# Skip header and parse
header = csv_rdd.first()
data_rdd = csv_rdd.filter(lambda line: line != header) \
                   .map(lambda line: line.split(","))

# Access fields
names = data_rdd.map(lambda fields: fields[0])
```

#### JSON Files
```python
import json

# Load JSON lines (one JSON object per line)
json_rdd = sc.textFile("data.jsonl")
parsed = json_rdd.map(lambda line: json.loads(line))

# Access fields
names = parsed.map(lambda obj: obj.get("name"))
```

### Sequence Files

Sequence files are Hadoop's binary key-value format.

#### Reading Sequence Files
```python
# Returns (key, value) pairs
seq_data = sc.sequenceFile("path/to/seqfile", 
                            "org.apache.hadoop.io.Text",
                            "org.apache.hadoop.io.IntWritable")
```

#### Writing Sequence Files
```python
# RDD must contain (key, value) tuples
pair_rdd = sc.parallelize([("key1", 1), ("key2", 2), ("key3", 3)])
pair_rdd.saveAsSequenceFile("output/sequence")
```

### Object Files (Pickle)

Python-specific serialization using pickle.

```python
# Save as pickle
rdd.saveAsPickleFile("output/pickled")

# Load pickle file
loaded = sc.pickleFile("output/pickled")
```

**Caution:** Pickle files are Python-version specific and not portable to other languages.

### Handling Compression

Spark automatically handles compressed files:

```python
# Reading compressed files (automatic detection)
gz_data = sc.textFile("data.txt.gz")    # gzip
bz_data = sc.textFile("data.txt.bz2")   # bzip2

# Writing with compression
rdd.saveAsTextFile("output/compressed", 
    compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
```

### Data Source Patterns

| Source | Read Method | Write Method |
|--------|-------------|--------------|
| Text files | textFile() | saveAsTextFile() |
| Whole files | wholeTextFiles() | N/A |
| Sequence files | sequenceFile() | saveAsSequenceFile() |
| Python objects | pickleFile() | saveAsPickleFile() |
| Hadoop formats | newAPIHadoopFile() | saveAsNewAPIHadoopFile() |

## Code Example

```python
from pyspark import SparkContext
import json
import os

sc = SparkContext("local[*]", "DataLoadingSaving")

# Create sample data directory
os.makedirs("sample_data", exist_ok=True)

# Create sample CSV
with open("sample_data/employees.csv", "w") as f:
    f.write("id,name,department,salary\n")
    f.write("1,Alice,Engineering,75000\n")
    f.write("2,Bob,Sales,60000\n")
    f.write("3,Charlie,Engineering,80000\n")
    f.write("4,Diana,HR,55000\n")

# Create sample JSON lines
with open("sample_data/products.jsonl", "w") as f:
    products = [
        {"id": 1, "name": "Laptop", "price": 999.99},
        {"id": 2, "name": "Mouse", "price": 29.99},
        {"id": 3, "name": "Keyboard", "price": 79.99}
    ]
    for p in products:
        f.write(json.dumps(p) + "\n")

print("=== Loading CSV ===")
csv_rdd = sc.textFile("sample_data/employees.csv")
header = csv_rdd.first()
employees = csv_rdd.filter(lambda line: line != header) \
                    .map(lambda line: line.split(","))

print("Employees:")
for emp in employees.collect():
    print(f"  {emp[1]} - {emp[2]} - ${emp[3]}")

# Calculate average salary by department
dept_salaries = employees.map(lambda e: (e[2], int(e[3])))
dept_totals = dept_salaries.combineByKey(
    lambda salary: (salary, 1),
    lambda acc, salary: (acc[0] + salary, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)
dept_averages = dept_totals.mapValues(lambda x: x[0] / x[1])

print("\nAverage Salary by Department:")
for dept, avg in dept_averages.collect():
    print(f"  {dept}: ${avg:,.2f}")

print("\n=== Loading JSON ===")
json_rdd = sc.textFile("sample_data/products.jsonl")
products = json_rdd.map(lambda line: json.loads(line))

print("Products:")
for product in products.collect():
    print(f"  {product['name']}: ${product['price']}")

total_value = products.map(lambda p: p['price']).reduce(lambda a, b: a + b)
print(f"\nTotal catalog value: ${total_value}")

print("\n=== Saving Results ===")
# Save department averages
import shutil
output_dir = "output/dept_averages"
if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

dept_averages.map(lambda x: f"{x[0]},{x[1]:.2f}") \
             .coalesce(1) \
             .saveAsTextFile(output_dir)

print(f"Saved to {output_dir}")

# Show saved content
saved = sc.textFile(output_dir)
print("Saved content:")
for line in saved.collect():
    print(f"  {line}")

# Clean up
shutil.rmtree("sample_data")
shutil.rmtree("output")

sc.stop()
```

**Output:**
```
=== Loading CSV ===
Employees:
  Alice - Engineering - $75000
  Bob - Sales - $60000
  Charlie - Engineering - $80000
  Diana - HR - $55000

Average Salary by Department:
  Engineering: $77,500.00
  Sales: $60,000.00
  HR: $55,000.00

=== Loading JSON ===
Products:
  Laptop: $999.99
  Mouse: $29.99
  Keyboard: $79.99

Total catalog value: $1109.97

=== Saving Results ===
Saved to output/dept_averages
Saved content:
  Engineering,77500.00
  Sales,60000.00
  HR,55000.00
```

## Summary
- `textFile()` loads text data line by line into an RDD
- `wholeTextFiles()` loads entire files as (filename, content) pairs
- `saveAsTextFile()` writes RDD to text files (one per partition)
- Use `coalesce(1)` for single output file, `repartition(n)` for specific count
- Spark automatically handles compressed files (.gz, .bz2)
- Sequence files provide efficient binary key-value storage
- Parse CSV and JSON manually in RDD operations or use Spark SQL DataFrames

## Additional Resources
- [External Datasets](https://spark.apache.org/docs/latest/rdd-programming-guide.html#external-datasets)
- [Spark Supported File Formats](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Hadoop Integration](https://spark.apache.org/docs/latest/rdd-programming-guide.html#hadoop-inputformat-output-format)
