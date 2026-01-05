# Snowflake Architecture

## Learning Objectives

- Understand Snowflake's three-layer architecture
- Explain the role of each layer: Cloud Services, Query Processing, and Database Storage
- Describe how virtual warehouses provide compute resources
- Recognize how compute separation enables Snowflake's unique capabilities

## Why This Matters

Snowflake's architecture is fundamentally different from traditional data warehouses, and understanding this architecture is essential for optimizing performance, managing costs, and designing effective data solutions. As you continue your journey *From Data Lakes to Data Warehouses*, this architectural knowledge will inform your decisions about warehouse sizing, concurrency management, and workload isolation.

## The Concept

Snowflake's architecture consists of three independent layers, each with distinct responsibilities. This separation is the foundation of Snowflake's flexibility, scalability, and ease of use.

```
+------------------------------------------+
|           Cloud Services Layer            |
|  (Authentication, Metadata, Optimization) |
+------------------------------------------+
                    |
+------------------------------------------+
|         Query Processing Layer            |
|     (Virtual Warehouses / Compute)        |
+------------------------------------------+
                    |
+------------------------------------------+
|         Database Storage Layer            |
|    (Centralized, Cloud Object Storage)    |
+------------------------------------------+
```

### Layer 1: Database Storage

The **Database Storage layer** is where all data is stored. Snowflake manages this layer entirely, abstracting away the complexities of cloud object storage.

**Key Characteristics:**

- **Centralized Storage**: All data is stored in a single, centralized location
- **Cloud Object Storage**: Uses the underlying cloud provider's storage (S3, Azure Blob, GCS)
- **Columnar Format**: Data is organized in a compressed, columnar micro-partition format
- **Automatic Optimization**: Snowflake automatically handles compression, encryption, and organization

**Micro-Partitions:**

Snowflake organizes data into **micro-partitions**, which are immutable, compressed units typically containing 50-500 MB of uncompressed data.

Benefits of micro-partitions:
- Automatic clustering based on ingestion order
- Efficient pruning during query execution
- Enables time travel by retaining historical micro-partitions

```
Table: ORDERS
+----------------+----------------+----------------+
| Micro-Partition| Micro-Partition| Micro-Partition|
|      001       |      002       |      003       |
+----------------+----------------+----------------+
| order_id: 1-100| order_id: 101- | order_id: 201- |
| date: Jan 1-5  | date: Jan 6-10 | date: Jan 11-15|
+----------------+----------------+----------------+
```

**Storage Costs:**

You pay for storage based on the average compressed data stored per month. Key factors:
- Compression is automatic and typically achieves 4x-5x reduction
- Time Travel data counts toward storage
- Fail-Safe data is charged separately

### Layer 2: Query Processing (Virtual Warehouses)

The **Query Processing layer** executes queries using **virtual warehouses**. Virtual warehouses are independent compute clusters that can be started, stopped, and resized on demand.

**Key Characteristics:**

- **Independent Compute**: Each warehouse is an isolated compute cluster
- **On-Demand Scaling**: Start, stop, suspend, and resize warehouses instantly
- **No Shared State**: Warehouses have local caches but share no other resources
- **Concurrent Access**: Multiple warehouses can query the same data simultaneously

**Virtual Warehouse Sizes:**

| Size | Approx. Compute Power | Credit/Hour |
|------|----------------------|-------------|
| X-Small | 1 node | 1 |
| Small | 2 nodes | 2 |
| Medium | 4 nodes | 4 |
| Large | 8 nodes | 8 |
| X-Large | 16 nodes | 16 |
| 2X-Large | 32 nodes | 32 |
| ... | ... | ... |

Each size increase doubles the compute resources and typically doubles the cost per hour.

**Warehouse Operations:**

```sql
-- Create a virtual warehouse
CREATE WAREHOUSE my_warehouse
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300          -- Suspend after 5 minutes of inactivity
    AUTO_RESUME = TRUE          -- Resume when query is submitted
    INITIALLY_SUSPENDED = TRUE; -- Start in suspended state

-- Resize a warehouse (instant, no data movement)
ALTER WAREHOUSE my_warehouse SET WAREHOUSE_SIZE = 'LARGE';

-- Suspend a warehouse (stop billing)
ALTER WAREHOUSE my_warehouse SUSPEND;

-- Resume a warehouse
ALTER WAREHOUSE my_warehouse RESUME;
```

**Multi-Cluster Warehouses (Enterprise Edition):**

For high-concurrency workloads, multi-cluster warehouses automatically add compute clusters as demand increases:

```sql
CREATE WAREHOUSE high_concurrency_wh
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    SCALING_POLICY = 'STANDARD';  -- or 'ECONOMY'
```

### Layer 3: Cloud Services

The **Cloud Services layer** is the brain of Snowflake. It coordinates all activities, manages metadata, and provides shared services across all accounts.

**Key Responsibilities:**

| Service | Description |
|---------|-------------|
| **Authentication** | User login, SSO, MFA, key pair authentication |
| **Access Control** | RBAC (Role-Based Access Control), object privileges |
| **Metadata Management** | Table definitions, statistics, partition metadata |
| **Query Parsing & Optimization** | SQL compilation, query plan generation |
| **Transaction Management** | ACID compliance, concurrency control |
| **Infrastructure Management** | Cluster provisioning, health monitoring |

**Query Optimization:**

The Cloud Services layer includes a sophisticated query optimizer that:
- Prunes micro-partitions based on query predicates
- Generates efficient execution plans
- Caches query results for instant repeat queries
- Collects statistics for optimization decisions

**Result Caching:**

Snowflake caches query results in the Cloud Services layer. If the same query is executed again and the underlying data has not changed, results are returned instantly without consuming warehouse compute.

```sql
-- First execution: Uses warehouse compute
SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-01';
-- Result cached

-- Second execution: Returns from cache (no compute cost)
SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-01';
```

## How the Layers Work Together

Consider a query execution flow:

1. **User submits query** to the Cloud Services layer
2. **Cloud Services** authenticates user, checks permissions, parses SQL
3. **Query Optimizer** generates execution plan, identifies relevant micro-partitions
4. **Virtual Warehouse** (Query Processing) executes the plan
5. **Warehouse** reads required data from Database Storage
6. **Results** are returned to user and cached in Cloud Services

```
User Query
    |
    v
[Cloud Services] -> Parse -> Optimize -> Plan
    |
    v
[Virtual Warehouse] -> Execute
    |
    v
[Database Storage] -> Read Micro-Partitions
    |
    v
Results -> Cache -> Return to User
```

## Compute Separation: Why It Matters

The separation of compute (virtual warehouses) from storage enables several key capabilities:

**1. Workload Isolation:**
```sql
-- Create separate warehouses for different teams/workloads
CREATE WAREHOUSE etl_warehouse       WITH WAREHOUSE_SIZE = 'LARGE';
CREATE WAREHOUSE analytics_warehouse WITH WAREHOUSE_SIZE = 'MEDIUM';
CREATE WAREHOUSE datascience_warehouse WITH WAREHOUSE_SIZE = 'SMALL';
```

**2. Cost Optimization:**
- Run ETL jobs on larger warehouses for faster completion
- Use smaller warehouses for ad-hoc queries
- Suspend warehouses when not in use

**3. Concurrency Without Contention:**
- Multiple warehouses query the same tables simultaneously
- No locking or resource contention between workloads

**4. Independent Scaling:**
- Scale up: Increase warehouse size for faster queries
- Scale out: Add clusters for more concurrent queries

## Summary

- Snowflake's architecture has three layers: **Database Storage**, **Query Processing**, and **Cloud Services**
- **Database Storage** uses cloud object storage with automatic compression and micro-partitioning
- **Query Processing** uses **virtual warehouses** - independent, scalable compute clusters
- **Cloud Services** handles metadata, optimization, security, and coordination
- **Compute separation** enables workload isolation, cost optimization, and unlimited concurrency
- Understanding this architecture is essential for effective Snowflake performance tuning and cost management

## Additional Resources

- [Snowflake Documentation: Architecture Overview](https://docs.snowflake.com/en/user-guide/intro-key-concepts#snowflake-architecture)
- [Snowflake Documentation: Virtual Warehouses](https://docs.snowflake.com/en/user-guide/warehouses)
- [Snowflake Blog: Understanding Micro-Partitions](https://www.snowflake.com/blog/how-foundationdb-powers-snowflake-metadata-forward/)
