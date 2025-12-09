# Spark Cluster Mode

## Learning Objectives
- Understand client vs cluster deploy modes
- Identify when to use each mode
- Configure applications for proper deployment

## Why This Matters
Deploy mode determines where your driver runs. Choosing the right mode affects job reliability, debugging, and resource usage in production environments.

## The Concept

### Client Mode

Driver runs on the machine where spark-submit is executed.

```
+----------------+           +----------------+
|  Your Machine  |           |    Cluster     |
|   (Driver)     |<--------->|   (Executors)  |
+----------------+           +----------------+
```

**Best for:**
- Interactive development
- Debugging
- Results returned locally

```bash
spark-submit --deploy-mode client app.py
```

### Cluster Mode

Driver runs on a cluster node.

```
+----------------+           +------------------+
|  Your Machine  |  submit   |     Cluster      |
|   (Client)     |---------->| Driver+Executors |
+----------------+           +------------------+
```

**Best for:**
- Production jobs
- Long-running applications
- Client can disconnect

```bash
spark-submit --deploy-mode cluster app.py
```

### Comparison

| Aspect | Client Mode | Cluster Mode |
|--------|-------------|--------------|
| Driver location | Local machine | Cluster node |
| Log access | Local terminal | Cluster logs |
| Network | Requires stable connection | Can disconnect |
| Debugging | Easier | Harder |
| Production use | Not recommended | Recommended |

### Configuration

```bash
# Client mode (explicit)
spark-submit --master yarn --deploy-mode client app.py

# Cluster mode  
spark-submit --master yarn --deploy-mode cluster app.py
```

## Summary
- Client mode: driver on local machine, good for development
- Cluster mode: driver in cluster, required for production
- Choose based on stability and debugging needs

## Additional Resources
- [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
