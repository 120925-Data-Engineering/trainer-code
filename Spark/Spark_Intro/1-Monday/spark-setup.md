# Spark Setup

## Learning Objectives
- Install Apache Spark on a local machine
- Configure required Java dependencies
- Set up environment variables correctly
- Verify the Spark installation works properly

## Why This Matters
A properly configured local Spark environment is essential for development and testing. Before deploying to production clusters, you will write and debug your Spark applications locally. Getting the setup right ensures you can focus on learning Spark concepts rather than troubleshooting configuration issues.

## The Concept

### Prerequisites

Before installing Spark, ensure you have:

1. **Java Development Kit (JDK) 8 or 11**
   - Spark runs on the Java Virtual Machine (JVM)
   - JDK 8 and 11 are the most widely supported versions
   - JDK 17 support is available in recent Spark versions

2. **Python 3.8+** (for PySpark)
   - Required for running PySpark applications
   - Recommended: Use a virtual environment

3. **Sufficient System Resources**
   - Minimum: 4GB RAM, 10GB disk space
   - Recommended: 8GB+ RAM for comfortable development

### Installation Steps

#### Step 1: Install Java

**Windows:**
1. Download JDK from [Oracle](https://www.oracle.com/java/technologies/downloads/) or use [OpenJDK](https://adoptium.net/)
2. Run the installer
3. Verify installation:
```cmd
java -version
```

**macOS:**
```bash
brew install openjdk@11
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

#### Step 2: Download Apache Spark

1. Visit [Apache Spark Downloads](https://spark.apache.org/downloads.html)
2. Select:
   - Spark release: Latest stable version (e.g., 3.5.0)
   - Package type: "Pre-built for Apache Hadoop"
3. Download the `.tgz` file

#### Step 3: Extract Spark

**Windows (PowerShell):**
```powershell
Expand-Archive spark-3.5.0-bin-hadoop3.tgz -DestinationPath C:\spark
```

**macOS/Linux:**
```bash
tar -xzf spark-3.5.0-bin-hadoop3.tgz
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
```

#### Step 4: Configure Environment Variables

**Windows:**
1. Open System Properties > Environment Variables
2. Add new System Variables:
   - `SPARK_HOME` = `C:\spark\spark-3.5.0-bin-hadoop3`
   - `JAVA_HOME` = `C:\Program Files\Java\jdk-11`
3. Edit `Path` variable, add:
   - `%SPARK_HOME%\bin`

**macOS/Linux (add to ~/.bashrc or ~/.zshrc):**
```bash
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Apply changes:
```bash
source ~/.bashrc
```

#### Step 5: Install PySpark (Optional but Recommended)

Using pip provides a cleaner Python integration:
```bash
pip install pyspark
```

### Verification Steps

#### Test Spark Shell
```bash
spark-shell
```

You should see the Spark logo and a `scala>` prompt. Type `:quit` to exit.

#### Test PySpark Shell
```bash
pyspark
```

You should see the Spark logo and a `>>>` prompt.

#### Run a Quick Test
In the PySpark shell:
```python
# Create a simple RDD
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Perform an operation
print(rdd.reduce(lambda a, b: a + b))
# Output: 15
```

### Common Installation Issues

| Issue | Solution |
|-------|----------|
| `JAVA_HOME not set` | Verify JAVA_HOME points to JDK directory |
| `Python version mismatch` | Set `PYSPARK_PYTHON` environment variable |
| `Permission denied` | Run terminal as administrator (Windows) or use sudo (Linux) |
| `Spark command not found` | Verify PATH includes `$SPARK_HOME/bin` |

### Recommended Directory Structure

```
C:\spark\                     # SPARK_HOME
  spark-3.5.0-bin-hadoop3\
    bin\                      # Executables (spark-shell, pyspark)
    conf\                     # Configuration files
    jars\                     # Spark JAR files
    python\                   # PySpark library
    sbin\                     # Admin scripts
```

## Code Example

Create a simple test script `test_spark.py`:

```python
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("InstallationTest") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Display the DataFrame
df.show()

# Print Spark version
print(f"Spark Version: {spark.version}")

# Stop the session
spark.stop()
```

Run it:
```bash
python test_spark.py
```

Expected output:
```
+-------+---+
|   name|age|
+-------+---+
|  Alice| 25|
|    Bob| 30|
|Charlie| 35|
+-------+---+

Spark Version: 3.5.0
```

## Summary
- Spark requires Java (JDK 8 or 11) as a prerequisite
- Download Spark from the official Apache website
- Configure `SPARK_HOME` and `PATH` environment variables
- PySpark can be installed via pip for easier Python integration
- Verify installation using `spark-shell` or `pyspark` commands
- Test with a simple RDD or DataFrame operation

## Additional Resources
- [Spark Installation Guide](https://spark.apache.org/docs/latest/index.html#downloading)
- [PySpark Installation - PyPI](https://pypi.org/project/pyspark/)
- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
