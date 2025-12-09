# Cluster Step Execution

## Learning Objectives
- Understand EMR steps and step types
- Chain multiple steps together
- Handle step failures appropriately

## Why This Matters
EMR steps are the building blocks for automated data pipelines. Understanding how to chain steps and handle failures enables reliable, production-grade workflows.

## The Concept

### What are EMR Steps?

Steps are units of work submitted to an EMR cluster. Each step runs a command or script.

```
Cluster
   |
   +-- Step 1: Ingest data (Spark)
   |
   +-- Step 2: Transform (Spark)  
   |
   +-- Step 3: Export (Custom script)
```

### Step Types

| Type | Use Case |
|------|----------|
| Spark | PySpark/Scala Spark jobs |
| Custom JAR | Java applications |
| Streaming | Spark Streaming |
| Script | Shell scripts |

### Adding Steps

```bash
aws emr add-steps \
    --cluster-id j-XXXXX \
    --steps '[
        {
            "Type": "Spark",
            "Name": "Step 1",
            "ActionOnFailure": "CONTINUE",
            "Args": ["s3://bucket/job1.py"]
        },
        {
            "Type": "Spark", 
            "Name": "Step 2",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "Args": ["s3://bucket/job2.py"]
        }
    ]'
```

### Failure Handling

| ActionOnFailure | Behavior |
|-----------------|----------|
| CONTINUE | Run next step |
| CANCEL_AND_WAIT | Stop steps, keep cluster |
| TERMINATE_CLUSTER | Terminate on failure |

### Monitoring Steps

```bash
# List steps
aws emr list-steps --cluster-id j-XXXXX

# Describe specific step
aws emr describe-step --cluster-id j-XXXXX --step-id s-XXXXX
```

### Step States

```
PENDING -> RUNNING -> COMPLETED
                   -> FAILED
                   -> CANCELLED
```

## Summary
- Steps are sequential units of work
- Chain steps for multi-stage pipelines
- Use ActionOnFailure for error handling
- Monitor via CLI or Console

## Additional Resources
- [EMR Steps](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-work-with-steps.html)
