nested schema
================
{"customer_id":1,"fullname":{"firstname":"sumit","lastname":"mittal"},"city":"bangalore"}
{"customer_id":2,"fullname":{"firstname":"ram","lastname":"kumar"},"city":"hyderabad"}
{"customer_id":3,"fullname":{"firstname":"vijay","lastname":"shankar"},"city":"pune"}







from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Path to your JSON file (update this to your actual file path)
json_path = "/path/to/customers.json"

# Define schema for nested JSON
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("fullname", StructType([
        StructField("firstname", StringType(), True),
        StructField("lastname", StringType(), True)
    ])),
    StructField("city", StringType(), True)
])

# Read JSON file with schema
df = spark.read.schema(schema).json(json_path)

# Show schema and data
df.printSchema()
df.show(truncate=False)





from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# 📁 Path to your JSON file (update this)
json_path = "/path/to/customers.json"

# 🧱 DDL schema (with nested struct)
ddl_schema = "customer_id INT, fullname STRUCT<firstname:STRING, lastname:STRING>, city STRING"

# ✅ Read JSON file using DDL schema
df = spark.read.schema(ddl_schema).json(json_path)

# 📌 Show schema and all data
df.printSchema()
df.show(truncate=False)

# ✅ DataFrame API query: select only firstname and city
result_df = df.select("fullname.firstname", "city")

# 📌 Show the result of the query
result_df.show()




root
 |-- customer_id: integer (nullable = true)
 |-- fullname: struct (nullable = true)
 |    |-- firstname: string (nullable = true)
 |    |-- lastname: string (nullable = true)
 |-- city: string (nullable = true)



+---------+----------+
|firstname|city      |
+---------+----------+
|sumit    |bangalore |
|ram      |hyderabad |
|vijay    |pune      |
+---------+----------+
