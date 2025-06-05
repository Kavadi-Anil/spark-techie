📌 Ways to Create a DataFrame in Spark

# 1️⃣ From external data sources (CSV, Parquet, Avro, etc.)
df1 = spark.read.format("csv").option("header", "true").load("path/to/file.csv")

# 2️⃣ From Spark SQL query (e.g., from a temporary view or table)
df2 = spark.sql("SELECT * FROM orders")

# 3️⃣ From an existing table (e.g., managed or external in Hive/Metastore)
df3 = spark.table("database_name.table_name")

# 4️⃣ From a range of numbers (creates a single-column DataFrame)
df4 = spark.range(5)           # 0 to 4
df5 = spark.range(1, 4)        # 1 to 3
df6 = spark.range(1, 8, 2)     # 1, 3, 5, 7

# 5️⃣ From a list of tuples using createDataFrame
data = [(1, "Alice"), (2, "Bob")]
df7 = spark.createDataFrame(data, ["id", "name"])

# 6️⃣ From local list using RDD and then converting to DataFrame
rdd = spark.sparkContext.parallelize(data)
df8 = spark.createDataFrame(rdd, ["id", "name"])

# 7️⃣ Again, directly using createDataFrame from data + schema
df9 = spark.createDataFrame([(10, "Carol"), (11, "Dan")], schema=["id", "name"])


from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data with mixed types in the 'age' field
data = [
    (1, "Alice", 30),
    (2, "Bob", "twenty"),
    (3, "Charlie", 25),
    (4, "David", "NA"),
    (5, "Eve", "forty")
]

# Step 1: Create DataFrame without column names
df1 = spark.createDataFrame(data)
df1.show()

# Output:
# +---+--------+-------+
# | _1|     _2 |   _3  |
# +---+--------+-------+
# |  1|  Alice |    30 |
# |  2|    Bob |twenty |
# |  3|Charlie |    25 |
# |  4|  David |    NA |
# |  5|    Eve | forty |
# +---+--------+-------+

# Step 2: Assign custom column names
df2 = df1.toDF("id", "name", "age")
df2.show()

# Step 3: Rename columns to capital letters
df3 = df2.toDF("ID", "NAME", "AGE")
df3.show()

# Step 4: Create DataFrame with data and schema in one step
columns = ["ID", "NAME", "AGE"]
df4 = spark.createDataFrame(data, schema=columns)
df4.show()

# Final Output:
# +---+--------+-------+
# | ID|    NAME|   AGE |
# +---+--------+-------+
# |  1|   Alice|     30|
# |  2|     Bob|twenty |
# |  3| Charlie|     25|
# |  4|   David|     NA|
# |  5|     Eve| forty |
# +---+--------+-------+




from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample data (string datetime format)
data = [
    (1, "Alice", "2024-05-01 10:30:00"),
    (2, "Bob", "2024-05-01 11:45:00"),
    (3, "Charlie", "2024-05-01 12:15:00")
]

# Define column names
columns = ["id", "name", "created_at"]

# Create DataFrame with 'created_at' as string
df = spark.createDataFrame(data, schema=columns)

# Print schema before conversion
print("📌 Schema Before Conversion:")
df.printSchema()

# Expected output:
# root
#  |-- id: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- created_at: string (nullable = true)

# Convert 'created_at' to TimestampType
df = df.withColumn("created_at", to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))

# Print schema after conversion
print("\n📌 Schema After Conversion:")
df.printSchema()

# Expected output:
# root
#  |-- id: integer (nullable = true)
#  |-- name: string (nullable = true)
#  |-- created_at: timestamp (nullable = true)





