Bucketing in Spark
===================

Sometimes you want to filter on a high-cardinality column like `customer_id`.  
But partitioning by such columns creates too many partitions — thousands or more.  
This is where **bucketing** helps.

✅ Bucketing is useful when:
- You have a large number of distinct values.
- You want to optimize joins or filtering.
- You want to limit the number of output files without creating excessive partitions.

Concept:
========
- Instead of thousands of partitions, define a fixed number of buckets.
- Data is split into those buckets using a **hash function** on the bucket column.

Example:
--------

customer_df.write \
  .format("parquet") \
  .mode("overwrite") \
  .bucketBy(4, "customer_id") \  # 4 buckets using customer_id
  .option("path", "/path/to/output") \
  .saveAsTable("database.customer_table")  # saveAsTable is required for bucketing

📌 Note:
- You must create the database before using `saveAsTable`.
- Bucketing only works with `saveAsTable()`.

Combining Partitioning and Bucketing
====================================

orders_df.write \
  .format("parquet") \
  .mode("overwrite") \
  .partitionBy("order_status") \  # Partition directory-wise
  .bucketBy(4, "order_id") \      # Bucket file-wise within each partition
  .saveAsTable("database.orders_table")

If `order_status` has 9 values, and `order_id` is bucketed into 4:
→ Total files = 9 partitions × 4 buckets = 36 files

Query Optimization:
-------------------
- SELECT * FROM orders WHERE order_status = 'CLOSED' AND order_id = 123
  → Reads only from the matching partition and bucket

- SELECT * FROM orders WHERE order_status = 'CLOSED'
  → Reads from one partition, all 4 buckets

- SELECT * FROM orders WHERE order_id = 123
  → Reads all partitions, one matching bucket

Hashing Example:
================

customer_id: 1 to 12
Bucket ID = customer_id % 4

Distribution:
- Bucket 0 → 4, 8, 12
- Bucket 1 → 1, 5, 9
- Bucket 2 → 2, 6, 10
- Bucket 3 → 3, 7, 11

Tips:
=====
- Use `partitionBy()` for **low-cardinality** columns.
- Use `bucketBy()` for **high-cardinality** columns.

Example:
--------
If you have 16 GB data, partitioned into 12 folders, and 4 buckets per partition:
→ Total files = 12 × 4 = 48 files
