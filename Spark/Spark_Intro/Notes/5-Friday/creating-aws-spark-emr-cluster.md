# Creating AWS Spark EMR Cluster

## Learning Objectives
- Create EMR clusters through AWS Console and CLI
- Select appropriate instance types for Spark workloads
- Configure cluster settings for optimal performance
- Understand EMR cluster architecture

## Why This Matters
Amazon EMR (Elastic MapReduce) is a managed platform for running Spark at scale in the cloud. Understanding how to create and configure EMR clusters enables you to deploy production Spark workloads without managing infrastructure. This knowledge is essential for data engineers working in AWS environments.

This builds on everything you have learned this week about Spark, preparing you to run distributed Spark jobs in a real cloud environment.

## The Concept

### What is Amazon EMR?

Amazon EMR is a cloud-native big data platform that:
- Runs Spark, Hadoop, Hive, and other frameworks
- Automatically provisions and configures clusters
- Integrates with S3, Glue, and other AWS services
- Provides managed scaling and monitoring

### EMR Cluster Architecture

```
+------------------------------------------+
|              EMR Cluster                  |
|  +----------------+  +----------------+   |
|  |  Primary Node  |  |   Core Nodes   |   |
|  |  - YARN RM     |  |  - Executors   |   |
|  |  - Spark Driver|  |  - HDFS        |   |
|  |  - Web UIs     |  |  - Processing  |   |
|  +----------------+  +----------------+   |
|                      +----------------+   |
|                      |   Task Nodes   |   |
|                      |  - Executors   |   |
|                      |  - Spot/temp   |   |
|                      +----------------+   |
+------------------------------------------+
                    |
                    v
            +---------------+
            |  Amazon S3    |
            | (Data Lake)   |
            +---------------+
```

### Node Types

| Node Type | Purpose | HDFS? | Scaling |
|-----------|---------|-------|---------|
| Primary | Cluster management, YARN RM | Yes | Fixed (1) |
| Core | Data storage, processing | Yes | Careful scaling |
| Task | Processing only | No | Flexible scaling |

### Instance Type Selection

For Spark workloads, consider:

**Memory-Optimized (r5, r6g series):**
- Good for caching, large datasets
- Examples: r5.xlarge, r5.2xlarge

**Compute-Optimized (c5, c6g series):**
- Good for CPU-intensive transformations
- Examples: c5.xlarge, c5.2xlarge

**General Purpose (m5, m6g series):**
- Balanced workloads
- Examples: m5.xlarge, m5.2xlarge

### Creating via AWS Console

#### Step 1: Navigate to EMR
1. Open AWS Console
2. Search for "EMR"
3. Click "Create cluster"

#### Step 2: Cluster Configuration
```
Name: my-spark-cluster
Release: emr-7.0.0
Applications: Spark
```

#### Step 3: Hardware Configuration
```
Primary node:
  - Instance type: m5.xlarge
  - Count: 1

Core nodes:
  - Instance type: r5.xlarge
  - Count: 4

Task nodes (optional):
  - Instance type: r5.xlarge
  - Count: 0-10 (auto-scaling)
```

#### Step 4: Security and Access
```
EC2 key pair: my-key-pair
IAM roles:
  - Service role: EMR_DefaultRole
  - Instance profile: EMR_EC2_DefaultRole
```

### Creating via AWS CLI

```bash
aws emr create-cluster \
    --name "my-spark-cluster" \
    --release-label emr-7.0.0 \
    --applications Name=Spark \
    --instance-groups \
        InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
        InstanceGroupType=CORE,InstanceCount=4,InstanceType=r5.xlarge \
    --use-default-roles \
    --ec2-attributes KeyName=my-key-pair \
    --log-uri s3://my-bucket/emr-logs/
```

### Cluster Configuration Options

#### Bootstrap Actions
Scripts that run during cluster provisioning:

```bash
aws emr create-cluster \
    --bootstrap-actions \
        Path=s3://my-bucket/scripts/install-deps.sh,Name="Install Dependencies"
```

Example bootstrap script:
```bash
#!/bin/bash
sudo pip3 install pandas numpy scikit-learn
```

#### Spark Configuration
Pass Spark configurations during cluster creation:

```bash
aws emr create-cluster \
    --configurations '[
        {
            "Classification": "spark-defaults",
            "Properties": {
                "spark.executor.memory": "8g",
                "spark.driver.memory": "4g",
                "spark.sql.shuffle.partitions": "200"
            }
        }
    ]'
```

#### Auto-Scaling
Configure automatic scaling based on metrics:

```json
{
    "Rules": [
        {
            "Name": "Scale-Out",
            "Action": {
                "SimpleScalingPolicyConfiguration": {
                    "AdjustmentType": "CHANGE_IN_CAPACITY",
                    "ScalingAdjustment": 2,
                    "CoolDown": 300
                }
            },
            "Trigger": {
                "CloudWatchAlarmDefinition": {
                    "MetricName": "YARNMemoryAvailablePercentage",
                    "ComparisonOperator": "LESS_THAN",
                    "Threshold": 20,
                    "Period": 300,
                    "EvaluationPeriods": 3
                }
            }
        }
    ]
}
```

### Cluster Lifecycle

```
STARTING -> BOOTSTRAPPING -> RUNNING -> TERMINATING -> TERMINATED
                                |
                        (Your jobs run here)
```

**Modes:**
- **Transient:** Cluster terminates after steps complete
- **Long-running:** Cluster stays running until manually terminated

### Cost Optimization

#### Spot Instances
Use spot instances for task nodes:

```bash
aws emr create-cluster \
    --instance-groups \
        InstanceGroupType=CORE,InstanceCount=2,InstanceType=r5.xlarge \
        InstanceGroupType=TASK,InstanceCount=4,InstanceType=r5.xlarge,BidPrice=0.10
```

#### Right-sizing
- Start small, monitor, and scale
- Use CloudWatch metrics to optimize
- Consider Graviton (ARM) instances for cost savings

### Accessing the Cluster

#### SSH Access
```bash
# Connect to primary node
ssh -i my-key.pem hadoop@ec2-xx-xx-xx-xx.compute.amazonaws.com
```

#### Spark Shell on Cluster
```bash
# After SSH
spark-shell --master yarn
pyspark --master yarn
```

#### Web UIs (via SSH tunnel)
```bash
# Create SSH tunnel
ssh -i my-key.pem -ND 8157 hadoop@ec2-xx-xx-xx-xx.compute.amazonaws.com

# Access via browser with SOCKS proxy:
# - Spark UI: http://primary-node:18080
# - YARN RM: http://primary-node:8088
```

## Code Example

**Create cluster using boto3 (Python):**

```python
import boto3
import time

def create_emr_cluster():
    """Create an EMR cluster for Spark workloads."""
    
    client = boto3.client('emr', region_name='us-east-1')
    
    # Spark configuration
    spark_config = {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.executor.memory": "8g",
            "spark.executor.cores": "4",
            "spark.driver.memory": "4g",
            "spark.sql.shuffle.partitions": "200",
            "spark.dynamicAllocation.enabled": "true"
        }
    }
    
    # Create cluster
    response = client.run_job_flow(
        Name='pyspark-training-cluster',
        ReleaseLabel='emr-7.0.0',
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hadoop'}
        ],
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'r5.xlarge',
            'InstanceCount': 3,  # 1 primary + 2 core
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2KeyName': 'my-key-pair'
        },
        Configurations=[spark_config],
        LogUri='s3://my-bucket/emr-logs/',
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        Tags=[
            {'Key': 'Environment', 'Value': 'Training'},
            {'Key': 'Project', 'Value': 'PySpark-Week1'}
        ]
    )
    
    cluster_id = response['JobFlowId']
    print(f"Cluster created: {cluster_id}")
    
    return cluster_id

def wait_for_cluster(cluster_id):
    """Wait for cluster to be ready."""
    
    client = boto3.client('emr', region_name='us-east-1')
    
    print("Waiting for cluster to start...")
    
    while True:
        response = client.describe_cluster(ClusterId=cluster_id)
        status = response['Cluster']['Status']['State']
        
        print(f"  Status: {status}")
        
        if status == 'WAITING':
            print("Cluster is ready!")
            return True
        elif status in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            print("Cluster failed to start")
            return False
        
        time.sleep(30)

def get_cluster_info(cluster_id):
    """Get cluster details."""
    
    client = boto3.client('emr', region_name='us-east-1')
    
    response = client.describe_cluster(ClusterId=cluster_id)
    cluster = response['Cluster']
    
    print("\n" + "=" * 50)
    print("CLUSTER INFORMATION")
    print("=" * 50)
    print(f"ID: {cluster['Id']}")
    print(f"Name: {cluster['Name']}")
    print(f"Status: {cluster['Status']['State']}")
    print(f"Release: {cluster['ReleaseLabel']}")
    
    # Get primary node DNS
    if 'MasterPublicDnsName' in cluster:
        print(f"Primary DNS: {cluster['MasterPublicDnsName']}")
    
    print("=" * 50)

def terminate_cluster(cluster_id):
    """Terminate the cluster."""
    
    client = boto3.client('emr', region_name='us-east-1')
    
    client.terminate_job_flows(JobFlowIds=[cluster_id])
    print(f"Termination initiated for: {cluster_id}")

def main():
    """Main function demonstrating cluster lifecycle."""
    
    # Create cluster
    cluster_id = create_emr_cluster()
    
    # Wait for it to be ready
    if wait_for_cluster(cluster_id):
        # Get cluster info
        get_cluster_info(cluster_id)
        
        print("\nCluster is ready for job submission!")
        print("To submit a job, use:")
        print(f"  aws emr add-steps --cluster-id {cluster_id} ...")
        
        # In practice, you would submit jobs here
        # Then terminate when done:
        # terminate_cluster(cluster_id)

if __name__ == "__main__":
    main()
```

## Summary
- EMR is AWS's managed service for running Spark clusters
- Clusters consist of Primary, Core, and Task nodes
- Use the Console for learning, CLI/SDK for automation
- Choose instance types based on workload (memory vs compute)
- Configure Spark settings during cluster creation
- Use Spot instances for Task nodes to reduce costs
- Access cluster via SSH or submit jobs remotely
- Terminate clusters when not in use to control costs

## Additional Resources
- [Amazon EMR Documentation](https://docs.aws.amazon.com/emr/)
- [EMR Best Practices](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-instances-guidelines.html)
- [EMR with Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html)
