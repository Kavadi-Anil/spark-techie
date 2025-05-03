rdd to dataframe 
=============== 
python list is not distributed across the cluster but covertting
to dataframe does 

rdd is  already ditributed but now u want to structured it bciove
coverting to dataframe 

# 📁 Sample orders.txt file (place this in a text file):
# Each line should look like this:

# 1,2013-07-25,11599,CLOSED
# 2,2013-07-25,256,PENDING_PAYMENT
# 3,2013-07-25,12111,COMPLETE
# 4,2013-07-25,8827,CLOSED
# 5,2013-07-25,11318,COMPLETE

# ✅ Spark code to read it as RDD and convert to DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Step 1: Read text file as RDD
rdd = spark.sparkContext.textFile("path/to/orders.txt")

# Step 2: Split and cast fields using split()[index]
rdd_typed = rdd.map(lambda x: (
    int(x.split(",")[0]),        # order_id
    x.split(",")[1],             # order_date (as string)
    int(x.split(",")[2]),        # customer_id
    x.split(",")[3]              # order_status
))

# Step 3: Define schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),  # can convert later to DateType
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])

# Step 4: Create DataFrame from RDD and schema
df = spark.createDataFrame(rdd_typed, schema)

# Step 5: Show schema and data
df.printSchema()
df.show()

# Expected Output:
# root
#  |-- order_id: integer (nullable = true)
#  |-- order_date: string (nullable = true)
#  |-- customer_id: integer (nullable = true)
#  |-- order_status: string (nullable = true)
#
# +--------+-----------+------------+--------------+
# |order_id|order_date|customer_id |order_status  |
# +--------+-----------+------------+--------------+
# |1       |2013-07-25 |11599       |CLOSED        |
# |2       |2013-07-25 |256         |PENDING_PAYMENT|
# |3       |2013-07-25 |12111       |COMPLETE      |
# |4       |2013-07-25 |8827        |CLOSED        |
# |5       |2013-07-25 |11318       |COMPLETE      |
# +--------+-----------+------------+--------------+

















from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Sample RDD
rdd = spark.sparkContext.parallelize([
    (1, "2013-07-25", 11599, "CLOSED"),
    (2, "2013-07-25", 256, "PENDING_PAYMENT"),
    (3, "2013-07-25", 12111, "COMPLETE")
])

# Column name list for toDF()
column_names = ["order_id", "order_date", "customer_id", "order_status"]

# 🔹 Method 1: Using spark.createDataFrame(rdd, schema)
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])

df1 = spark.createDataFrame(rdd, schema)
print("📌 Method 1: createDataFrame with schema")
df1.printSchema()
df1.show()

# 📤 Output (df1.printSchema()):
# root
#  |-- order_id: integer (nullable = true)
#  |-- order_date: string (nullable = true)
#  |-- customer_id: integer (nullable = true)
#  |-- order_status: string (nullable = true)

# 🔹 Method 2: Using spark.createDataFrame(rdd).toDF(*column_names)
df2 = spark.createDataFrame(rdd).toDF(*column_names)
print("📌 Method 2: createDataFrame then toDF with column names")
df2.printSchema()
df2.show()

# 📤 Output (df2.printSchema()):
# root
#  |-- order_id: long (nullable = true)
#  |-- order_date: string (nullable = true)
#  |-- customer_id: long (nullable = true)
#  |-- order_status: string (nullable = true)

# 🔹 Method 3: Using rdd.toDF(*column_names)
df3 = rdd.toDF(*column_names)
print("📌 Method 3: rdd.toDF with column names")
df3.printSchema()
df3.show()

# 📤 Output (df3.printSchema()):
# root
#  |-- order_id: long (nullable = true)
#  |-- order_date: string (nullable = true)
#  |-- customer_id: long (nullable = true)
#  |-- order_status: string (nullable = true) 



from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ================================
# 📌 Different Ways to Create DataFrame
# ================================

# 1️⃣ From files using spark.read
df_csv = spark.read.option("header", "true").csv("path/to/file.csv")
df_parquet = spark.read.parquet("path/to/file.parquet")

# 2️⃣ From a table using SQL
df_sql = spark.sql("SELECT * FROM orders")

# 3️⃣ From a table using spark.table()
df_table = spark.table("default.orders")

# 4️⃣ From a range of numbers
df_range1 = spark.range(5)         # 0 to 4
df_range2 = spark.range(1, 6)      # 1 to 5
df_range3 = spark.range(1, 10, 2)  # 1, 3, 5, 7, 9

# 5️⃣ From local Python list of tuples (in memory)
data = [(1, "Alice"), (2, "Bob")]
df_list = spark.createDataFrame(data, ["id", "name"])

# 6️⃣ From RDD to DataFrame
rdd = spark.sparkContext.parallelize([
    (1, "2013-07-25", 11599, "CLOSED"),
    (2, "2013-07-25", 256, "PENDING_PAYMENT")
])

# 6.1) Using schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True)
])
df_rdd_schema = spark.createDataFrame(rdd, schema)

# 6.2) Using toDF() with column names
df_rdd_names = rdd.toDF("order_id", "order_date", "customer_id", "order_status")

