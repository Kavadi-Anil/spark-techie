from pyspark.sql import SparkSession

# ✅ Start Spark session
spark = SparkSession.builder.getOrCreate()

# ✅ Sample data with one exact duplicate row
data = [
    (1, "kapil", 24),
    (1, "satish", 34),
    (2, "kapil", 24),
    (1, "satish", 34)  # Duplicate row
]

# ✅ Create DataFrame with column names
df = spark.createDataFrame(data, ["order_id", "name", "age"])

# ✅ Show original DataFrame
print("🔹 Original DataFrame:")
df.show()

# ▶️ Output:
# +--------+--------+---+
# |order_id|name    |age|
# +--------+--------+---+
# |1       |kapil   |24 |
# |1       |satish  |34 |
# |2       |kapil   |24 |
# |1       |satish  |34 |
# +--------+--------+---+

# ✅ Remove exact duplicate rows
df1 = df.distinct()

# ✅ Show distinct DataFrame
print("🔹 DataFrame after applying distinct():")
df1.show()

# ▶️ Output:
# +--------+--------+---+
# |order_id|name    |age|
# +--------+--------+---+
# |1       |kapil   |24 |
# |1       |satish  |34 |
# |2       |kapil   |24 |
# +--------+--------+---+





# ✅ Select distinct order_id values (not full row distinct)
df.select("order_id").distinct().show()

# ▶️ Output:
# +--------+
# |order_id|
# +--------+
# |1       |
# |2       |
# +--------+

# 🔍 Explanation:
# - df.select("order_id") → picks only the 'order_id' column
# - .distinct() → removes repeated values (e.g., 1 appears multiple times but shown once)
# - .show() → displays the result



# ✅ Remove duplicates based on subset of columns: name and age
df.dropDuplicates(["name", "age"]).show()

# ▶️ Output:
# +--------+--------+---+
# |order_id|name    |age|
# +--------+--------+---+
# |1       |kapil   |24 |
# |1       |satish  |34 |
# +--------+--------+---+

# ✅ Remove duplicates based only on one column: order_id
df.dropDuplicates(["order_id"]).show()

# ▶️ Output:
# +--------+--------+---+
# |order_id|name    |age|
# +--------+--------+---+
# |1       |kapil   |24 |
# |2       |kapil   |24 |
# +--------+--------+---+

# 🔍 Explanation:
# - ✅ Use `distinct()` → removes duplicates based on the entire row.
# - ✅ Use `dropDuplicates([...])` → removes duplicates **based on subset of columns**.
#     It keeps the **first occurrence** and drops the rest based on the column(s) you specify.
