"""
Exercise: Column Management
===========================
Week 2, Wednesday

Practice adding, removing, and transforming columns on product inventory data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, lower, trim, concat, concat_ws,
    split, substring, regexp_replace, coalesce, current_date
)

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Columns").master("local[*]").getOrCreate()

# Product inventory data (messy data for cleaning!)
inventory = spark.createDataFrame([
    (1, "  LAPTOP pro  ", "Electronics", 999.99, 50, None),
    (2, "  phone X ", "Electronics", 799.99, 100, "NY"),
    (3, "Winter JACKET", "Clothing", 149.99, 200, "CA"),
    (4, " running shoes ", "Clothing", 89.99, None, "TX"),
    (5, "coffee MAKER", "Home", 49.99, 75, None),
    (6, "  Desk Lamp  ", "Home", 29.99, 120, "NY")
], ["product_id", "product_name", "category", "price", "quantity", "warehouse"])

print("=== Exercise: Column Management ===")
print("\nRaw Inventory Data:")
inventory.show(truncate=False)

# =============================================================================
# TASK 1: String Cleaning (20 mins)
# =============================================================================

print("\n--- Task 1: String Cleaning ---")

# TODO 1a: Clean product_name: trim whitespace, convert to title case
# HINT: trim() removes whitespace, initcap() for title case


# TODO 1b: Standardize category to lowercase


# TODO 1c: Create a "product_code" column by:
# - Taking first 3 letters of category (uppercase)
# - Adding the product_id
# - Example: "ELE-1" for Electronics product 1


# =============================================================================
# TASK 2: Handling Nulls (15 mins)
# =============================================================================

print("\n--- Task 2: Handling Nulls ---")

# TODO 2a: Replace null warehouse with "CENTRAL"


# TODO 2b: Replace null quantity with 0


# TODO 2c: Create an "in_stock" boolean column (quantity > 0 or not null)


# =============================================================================
# TASK 3: Calculated Columns (20 mins)
# =============================================================================

print("\n--- Task 3: Calculated Columns ---")

# TODO 3a: Add "inventory_value" = price * quantity (handle nulls!)


# TODO 3b: Add "price_tier" based on price:
# - "Budget" if price < 50
# - "Mid" if 50 <= price < 200
# - "Premium" if price >= 200


# TODO 3c: Add "last_updated" column with today's date


# =============================================================================
# TASK 4: Removing and Renaming (10 mins)
# =============================================================================

print("\n--- Task 4: Removing and Renaming ---")

# TODO 4a: Drop the "warehouse" column


# TODO 4b: Rename columns:
# - product_id -> id
# - product_name -> name


# =============================================================================
# TASK 5: Complete Data Pipeline (25 mins)
# =============================================================================

print("\n--- Task 5: Complete Data Pipeline ---")

# Create a clean, analysis-ready version of the data:
# 1. Clean product_name (trim, title case)
# 2. Fill null warehouse with "CENTRAL"
# 3. Fill null quantity with 0
# 4. Add inventory_value column
# 5. Add price_tier column
# 6. Add last_updated column
# 7. Rename product_id to id, product_name to name
# 8. Drop warehouse column
# 9. Order columns: id, name, category, price, quantity, inventory_value, price_tier, last_updated

clean_inventory = None  # Your pipeline here

# clean_inventory.show()


# =============================================================================
# CHALLENGE: Extract and Parse (15 mins)
# =============================================================================

print("\n--- Challenge: String Parsing ---")

# Product descriptions
descriptions = spark.createDataFrame([
    ("Widget A - Size: Large, Color: Blue"),
    ("Gadget B - Size: Medium, Color: Red"),
    ("Tool C - Size: Small, Color: Green")
], ["description"])

# TODO 6a: Extract just the product name (before the dash)


# TODO 6b: Extract the size value


# TODO 6c: Extract the color value


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
