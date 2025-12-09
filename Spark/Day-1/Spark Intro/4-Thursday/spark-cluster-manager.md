# Spark Cluster Manager

## Learning Objectives
- Understand the role of cluster managers in Spark
- Compare Standalone, YARN, Mesos, and Kubernetes cluster managers
- Identify when to use each cluster manager type
- Configure Spark for different cluster environments

## Why This Matters
In production, Spark runs on clusters managed by cluster managers. Choosing the right cluster manager depends on your infrastructure, team expertise, and workload requirements. Understanding these options helps you deploy Spark applications in the most suitable environment for your organization.

## The Concept

### What is a Cluster Manager?

A cluster manager is responsible for:
- **Resource allocation:** Distributing CPU, memory across applications
- **Job scheduling:** Running multiple applications concurrently
- **Monitoring:** Tracking node health and application status
- **Scaling:** Adding or removing resources dynamically

```
+-------------------+
|  Spark Application|
+-------------------+
         |
         v
+-------------------+
|  Cluster Manager  |
|  (YARN, K8s, etc) |
+-------------------+
    |    |    |
    v    v    v
+------+ +------+ +------+
|Node 1| |Node 2| |Node 3|
+------+ +------+ +------+
```

### Supported Cluster Managers

#### 1. Standalone Mode
Spark's built-in simple cluster manager.

**Pros:**
- Easy to set up
- No external dependencies
- Good for dedicated Spark clusters

**Cons:**
- Limited scheduling capabilities
- Single workload type (Spark only)
- Basic resource management

**Configuration:**
```python
spark = SparkSession.builder \
    .master("spark://master-host:7077") \
    .appName("StandaloneApp") \
    .getOrCreate()
```

**Cluster setup:**
```bash
# Start master
$SPARK_HOME/sbin/start-master.sh

# Start workers
$SPARK_HOME/sbin/start-worker.sh spark://master-host:7077
```

#### 2. Apache YARN
Hadoop's resource manager, widely used in enterprise environments.

**Pros:**
- Integrates with Hadoop ecosystem
- Mature and battle-tested
- Dynamic resource allocation
- Multi-tenant support

**Cons:**
- Requires Hadoop infrastructure
- More complex setup
- HDFS dependency for some features

**Configuration:**
```python
spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.submit.deployMode", "cluster") \
    .appName("YARNApp") \
    .getOrCreate()
```

**Submit command:**
```bash
spark-submit --master yarn \
             --deploy-mode cluster \
             --num-executors 4 \
             app.py
```

#### 3. Apache Mesos
General-purpose cluster manager (now less common).

**Pros:**
- Fine-grained resource sharing
- Multi-framework support
- Flexible scheduling

**Cons:**
- Complex to operate
- Declining adoption
- Less community support

**Configuration:**
```python
spark = SparkSession.builder \
    .master("mesos://mesos-master:5050") \
    .appName("MesosApp") \
    .getOrCreate()
```

#### 4. Kubernetes
Container orchestration platform, increasingly popular.

**Pros:**
- Cloud-native deployment
- Container isolation
- Auto-scaling capabilities
- Wide cloud provider support

**Cons:**
- Requires Kubernetes expertise
- Container overhead
- More complex networking

**Configuration:**
```python
spark = SparkSession.builder \
    .master("k8s://https://kubernetes-api:6443") \
    .config("spark.kubernetes.container.image", "spark:3.5.0") \
    .appName("K8sApp") \
    .getOrCreate()
```

**Submit command:**
```bash
spark-submit --master k8s://https://k8s-api:6443 \
             --deploy-mode cluster \
             --conf spark.kubernetes.container.image=spark:3.5.0 \
             app.py
```

### Comparison Table

| Feature | Standalone | YARN | Mesos | Kubernetes |
|---------|------------|------|-------|------------|
| Complexity | Low | Medium | High | Medium |
| Multi-tenant | Limited | Yes | Yes | Yes |
| Dynamic Resources | Limited | Yes | Yes | Yes |
| Container Support | No | Yes | Yes | Native |
| Cloud Native | No | No | No | Yes |
| Ecosystem | Spark only | Hadoop | Any | Cloud/Container |

### Deployment Modes

Regardless of cluster manager, Spark supports two deployment modes:

#### Client Mode
- Driver runs on the client machine
- Good for interactive development
- Results returned directly to client

```bash
spark-submit --deploy-mode client app.py
```

#### Cluster Mode
- Driver runs on a cluster node
- Better for production workloads
- Survives client disconnection

```bash
spark-submit --deploy-mode cluster app.py
```

### Resource Configuration

Configure resources based on your cluster manager:

```python
spark = SparkSession.builder \
    .appName("ResourceConfig") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

### Choosing a Cluster Manager

**Choose Standalone when:**
- Quick setup for testing/development
- Dedicated Spark cluster
- Simple workload requirements

**Choose YARN when:**
- Existing Hadoop infrastructure
- Enterprise multi-tenant environment
- Integration with Hive, HBase, etc.

**Choose Kubernetes when:**
- Cloud-native infrastructure
- Container-based deployments
- Need for auto-scaling
- Multi-cloud strategy

## Code Example

```python
from pyspark.sql import SparkSession
import os

def get_cluster_info():
    """
    Demonstrate connecting to different cluster managers.
    The actual cluster is determined by environment.
    """
    
    # Detect environment and configure accordingly
    cluster_type = os.environ.get("SPARK_CLUSTER_TYPE", "local")
    
    if cluster_type == "standalone":
        master = os.environ.get("SPARK_MASTER", "spark://localhost:7077")
    elif cluster_type == "yarn":
        master = "yarn"
    elif cluster_type == "kubernetes":
        k8s_api = os.environ.get("K8S_API", "https://localhost:6443")
        master = f"k8s://{k8s_api}"
    else:
        # Default to local mode for development
        master = "local[*]"
    
    print(f"Connecting to cluster: {master}")
    
    # Build SparkSession with appropriate configuration
    builder = SparkSession.builder \
        .appName("ClusterManagerDemo") \
        .master(master)
    
    # Add cluster-specific configurations
    if cluster_type == "yarn":
        builder = builder \
            .config("spark.submit.deployMode", "client") \
            .config("spark.dynamicAllocation.enabled", "true")
    
    elif cluster_type == "kubernetes":
        builder = builder \
            .config("spark.kubernetes.container.image", "spark:3.5.0") \
            .config("spark.kubernetes.namespace", "spark")
    
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    
    # Display cluster information
    print("\n" + "=" * 50)
    print("CLUSTER INFORMATION")
    print("=" * 50)
    print(f"Application ID: {sc.applicationId}")
    print(f"Application Name: {sc.appName}")
    print(f"Master: {sc.master}")
    print(f"Deploy Mode: {os.environ.get('SPARK_DEPLOY_MODE', 'N/A')}")
    print(f"Default Parallelism: {sc.defaultParallelism}")
    
    # Get executor information (if available)
    try:
        # In cluster mode, this gives executor count
        executor_count = sc._jsc.sc().getExecutorMemoryStatus().size()
        print(f"Executors: {executor_count}")
    except Exception:
        print("Executors: (not available in local mode)")
    
    print("=" * 50)
    
    return spark

def run_sample_job(spark):
    """Run a sample job to verify cluster connectivity."""
    
    sc = spark.sparkContext
    
    print("\n=== Running Sample Job ===")
    
    # Create a distributed dataset
    data = sc.parallelize(range(10000), numSlices=10)
    
    # Perform transformations
    result = data.map(lambda x: x * 2) \
                 .filter(lambda x: x % 4 == 0) \
                 .reduce(lambda a, b: a + b)
    
    print(f"Result: {result}")
    print("Job completed successfully!")
    
    return result

def main():
    spark = None
    try:
        spark = get_cluster_info()
        run_sample_job(spark)
        
    except Exception as e:
        print(f"Error: {e}")
        raise
        
    finally:
        if spark:
            print("\nStopping SparkSession...")
            spark.stop()
            print("Done.")

if __name__ == "__main__":
    main()
```

**Output (local mode):**
```
Connecting to cluster: local[*]

==================================================
CLUSTER INFORMATION
==================================================
Application ID: local-1234567890
Application Name: ClusterManagerDemo
Master: local[*]
Deploy Mode: N/A
Default Parallelism: 8
Executors: (not available in local mode)
==================================================

=== Running Sample Job ===
Result: 24995000
Job completed successfully!

Stopping SparkSession...
Done.
```

## Summary
- Cluster managers handle resource allocation, scheduling, and monitoring
- Standalone is Spark's simple built-in cluster manager
- YARN integrates well with existing Hadoop infrastructure
- Kubernetes is ideal for cloud-native, containerized deployments
- Mesos offers fine-grained sharing but has declining adoption
- Choose your cluster manager based on existing infrastructure and expertise
- All cluster managers support client and cluster deployment modes

## Additional Resources
- [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Running on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html)
- [Running on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
