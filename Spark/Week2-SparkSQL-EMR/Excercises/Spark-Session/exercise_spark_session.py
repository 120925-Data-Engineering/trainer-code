"""
Exercise: SparkSession Setup and Configuration
===============================================
Week 2, Monday

Complete the TODOs below to practice creating and configuring SparkSession objects.
"""

from pyspark.sql import SparkSession

# =============================================================================
# TASK 1: Basic SparkSession Creation
# =============================================================================

# TODO 1a: Create a SparkSession with:
#   - App name: "MyFirstSparkSQLApp"
#   - Master: "local[*]"
# HINT: Use SparkSession.builder.appName(...).master(...).getOrCreate()

spark = None  # Replace with your code

# TODO 1b: Print the following information:
#   - Spark version
#   - Application ID
#   - Default parallelism
# HINT: Access these via spark.version, spark.sparkContext.applicationId, etc.

print("=== Task 1: Basic SparkSession ===")
# Your print statements here


# TODO 1c: Create a simple DataFrame with 3 columns and 5 rows
# to verify your session works

data = []  # Replace with sample data
columns = []  # Replace with column names
df = None  # Create DataFrame

# Show the DataFrame
# df.show()


# =============================================================================
# TASK 2: Configuration Exploration
# =============================================================================

print("\n=== Task 2: Configuration ===")

# TODO 2a: Print the value of spark.sql.shuffle.partitions
# HINT: Use spark.conf.get("spark.sql.shuffle.partitions")

print(f"Shuffle partitions: ")  # Complete this


# TODO 2b: Print at least 3 other configuration values
# Some options: spark.driver.memory, spark.executor.memory, spark.sql.adaptive.enabled


# TODO 2c: Try changing spark.sql.shuffle.partitions at runtime
# Does it work? Add a comment explaining what happens.

# Your code here


# =============================================================================
# TASK 3: getOrCreate() Behavior
# =============================================================================

print("\n=== Task 3: getOrCreate() Behavior ===")

# TODO 3a: Create another reference using getOrCreate with a DIFFERENT app name
spark2 = None  # Replace with SparkSession.builder.appName("DifferentName").getOrCreate()


# TODO 3b: Check which app name is actually used
# print(f"spark app name: {spark.sparkContext.appName}")
# print(f"spark2 app name: {spark2.sparkContext.appName}")


# TODO 3c: Are spark and spark2 the same object? Check with 'is' operator


# TODO 3d: EXPLAIN IN A COMMENT: Why does getOrCreate() behave this way?
# Your explanation:
#
#
#


# =============================================================================
# TASK 4: Session Cleanup
# =============================================================================

print("\n=== Task 4: Cleanup ===")

# TODO 4a: Stop the SparkSession properly
# HINT: Use spark.stop()


# TODO 4b: Verify the session has stopped
# HINT: Check spark.sparkContext._jsc.sc().isStopped() before stopping


# =============================================================================
# STRETCH GOALS (Optional)
# =============================================================================

# Stretch 1: Create a helper function that builds a SparkSession with your
# preferred default configurations

def create_my_spark_session(app_name, shuffle_partitions=100):
    """
    Creates a SparkSession with custom defaults.
    
    Args:
        app_name: Name of the Spark application
        shuffle_partitions: Number of shuffle partitions (default: 100)
    
    Returns:
        SparkSession object
    """
    # TODO: Implement this function
    pass


# Stretch 2: Enable Hive support
# HINT: Use .enableHiveSupport() in the builder chain
# Note: This may fail if Hive is not configured - that's okay!


# Stretch 3: List all configuration options
# HINT: spark.sparkContext.getConf().getAll() returns all settings
