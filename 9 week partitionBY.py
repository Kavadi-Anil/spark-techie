# DataFrame Writer API
# ====================

orders_schema = "order_id LONG, order_date STRING, cust_id LONG, order_status STRING"

order_df = spark.read.format("csv").schema(orders_schema).load("location")

order_df.show()

# Check number of partitions
order_df.rdd.getNumPartitions()
# Output: 9 partitions for 1 GB data (approx. 128MB per partition)

# Writer API Usage
# ================

order_df.write.format("csv").mode("overwrite").option("path", "location").save()
# If there are 9 partitions, it writes 9 partition files.

# Default format is Parquet
# 128MB CSV => ~13MB in Parquet (compressed)

order_df.write.format("json").mode("overwrite").option("path", "location").save()
# ~289MB per partition (takes more time to write)

order_df.write.format("orc").mode("overwrite").option("path", "location").save()
# ~7MB per partition

# Append mode
order_df.write.format("orc").mode("append").option("path", "location").save()

# Error if file exists
order_df.write.format("orc").mode("errorifexists").option("path", "location").save()

# Ignore if file exists
order_df.write.format("orc").mode("ignore").option("path", "location").save()

# AVRO format fails if required packages are not imported
order_df.write.format("avro").mode("overwrite").option("path", "location").save()

# Writing Modes
# =============
# 1. overwrite – replaces existing data
# 2. ignore – skips write if path exists
# 3. append – adds data to existing files
# 4. errorifexists – throws error if path exists

# Important: Always mention FOLDER path in `.option("path", "folder")`

# Partition By
# ============

# Without partitioning
order_df.write.format("csv").mode("overwrite").option("path", "location").save()

# Query like this:
# SELECT * FROM orders WHERE order_id = 123 AND order_date = '2024-05-28'
# ...will scan all partitions (not optimized)

# Register as temp view
order_df.createOrReplaceTempView("orders")

spark.sql("SELECT count(*) FROM orders WHERE order_status = 'CLOSED'")
# This query is slow if data is not partitioned properly

# Optimized partitioning by a filterable column
order_df.write \
    .format("csv") \
    .partitionBy("order_status") \
    .option("path", "location") \
    .save()

# If order_status has 9 unique values → creates 9 folders (partitions)
# Query: SELECT * FROM orders WHERE order_status = 'CLOSED' → Reads only 1 partition
# This is called **Partition Pruning**

# Avoid partitioning by high-cardinality columns (e.g., order_id)
# Prefer columns with fewer distinct values (low cardinality)

# Inefficient queries:
# SELECT * FROM orders WHERE order_id = 342 → Scans all partitions
# SELECT count(*) FROM orders → Scans all rows

# Loading CSV without headers
customer_df = spark.read.format("csv").option("inferSchema", True).load("location")

# Converting unnamed DataFrame to named
customer_final_df = customer_df.toDF("customer_id", "customer_name", "customer_state", "customer_city")

# 2-level Partitioning
customer_final_df.write \
    .format("csv") \
    .mode("overwrite") \
    .partitionBy("customer_state", "customer_city") \
    .option("path", "location") \
    .save()

# Example:
# 50 states = 50 folders → each state with 20 cities = 20 subfolders inside each state

# Query: SELECT COUNT(*) FROM orders WHERE state = 'CA' AND city = 'San Diego'
# → Reads only 1 partition file

# Partition pruning: the engine knows which partition files to read

# Important:
# Only partition by columns with a small number of distinct values (e.g., country, state, region)
# Do NOT partition by customer_id or order_id (high cardinality → thousands of small folders)
