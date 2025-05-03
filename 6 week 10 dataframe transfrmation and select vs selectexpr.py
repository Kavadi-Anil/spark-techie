




+--------------+---------+-----------+--------+--------+-------------+
|order_item_id |order_id |product_id |quantity|subtotal|product_price|
+--------------+---------+-----------+--------+--------+-------------+
|1             |100      |200        |2       |400.0   |200.0        |
|2             |101      |201        |1       |150.0   |150.0        |
|3             |102      |202        |3       |300.0   |100.0        |
|4             |103      |203        |5       |500.0   |100.0        |
|5             |104      |204        |2       |180.0   |90.0         |
+--------------+---------+-----------+--------+--------+-------------+


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# ✅ Start Spark session
spark = SparkSession.builder.getOrCreate()

# ✅ Simulated order_items data (like read from CSV without header, using inferSchema)
order_items_data = [
    (1, 100, 200, 2, 400.0, 200.0),
    (2, 101, 201, 1, 150.0, 150.0),
    (3, 102, 202, 3, 300.0, 100.0),
    (4, 103, 203, 5, 500.0, 100.0),
    (5, 104, 204, 2, 180.0, 90.0)
]

# ✅ Create raw DataFrame
raw_df = spark.createDataFrame(order_items_data)

# ✅ Assign column names
refined_df = raw_df.toDF("order_item_id", "order_id", "product_id", "quantity", "subtotal", "product_price")

# ✅ Drop the 'subtotal' column
df1 = refined_df.drop("subtotal")
df1.show(truncate=False)

# ▶️ Output:
# +--------------+---------+-----------+--------+-------------+
# |order_item_id |order_id |product_id |quantity|product_price|
# +--------------+---------+-----------+--------+-------------+
# |1             |100      |200        |2       |200.0        |
# |2             |101      |201        |1       |150.0        |
# |3             |102      |202        |3       |100.0        |
# |4             |103      |203        |5       |100.0        |
# |5             |104      |204        |2       |90.0         |
# +--------------+---------+-----------+--------+-------------+

# ✅ Select all columns
df1.select("*").show(truncate=False)

# ✅ Select only specific columns
df1.select("order_item_id", "product_id").show(truncate=False)

# ▶️ Output:
# +--------------+-----------+
# |order_item_id |product_id |
# +--------------+-----------+
# |1             |200        |
# |2             |201        |
# |3             |202        |
# |4             |203        |
# |5             |204        |
# +--------------+-----------+

# ✅ Add new column 'subtotal' using expr()
df1.select("*", expr("product_price * quantity AS subtotal")).show(truncate=False)

# ▶️ Output:
# +--------------+---------+-----------+--------+-------------+--------+
# |order_item_id |order_id |product_id |quantity|product_price|subtotal|
# +--------------+---------+-----------+--------+-------------+--------+
# |1             |100      |200        |2       |200.0        |400.0   |
# |2             |101      |201        |1       |150.0        |150.0   |
# |3             |102      |202        |3       |100.0        |300.0   |
# |4             |103      |203        |5       |100.0        |500.0   |
# |5             |104      |204        |2       |90.0         |180.0   |
# +--------------+---------+-----------+--------+-------------+--------+

# ✅ Do the same using selectExpr
df1.selectExpr("*", "product_price * quantity AS subtotal").show(truncate=False)

# ✅ Rename 'product_price' to 'unit_price'
renamed_df = df1.withColumnRenamed("product_price", "unit_price")
renamed_df.show(truncate=False)

# ▶️ Output:
# +--------------+---------+-----------+--------+----------+
# |order_item_id |order_id |product_id |quantity|unit_price|
# +--------------+---------+-----------+--------+----------+
# |1             |100      |200        |2       |200.0     |
# |2             |101      |201        |1       |150.0     |
# |3             |102      |202        |3       |100.0     |
# |4             |103      |203        |5       |100.0     |
# |5             |104      |204        |2       |90.0      |
# +--------------+---------+-----------+--------+----------+





from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# ✅ Simulate products table (as if read from a location)
products_data = [
    (1, 10, "nike shoes", "running shoes", 100.0, "nike.com"),
    (2, 11, "armour tee", "training t-shirt", 50.0, "armour.com"),
    (3, 12, "nike socks", "sport socks", 30.0, "nike.com/socks"),
    (4, 13, "armour cap", "baseball cap", 20.0, "armour.com/cap"),
    (5, 14, "puma hoodie", "winter hoodie", 60.0, "puma.com/hoodie")
]

products_df = spark.createDataFrame(products_data).toDF(
    "product_id", "product_category_id", "product_name", "product_description", "product_price", "product_url"
)

# Show original products table
products_df.show(truncate=False)




+-----------+---------------------+------------+-------------------+-------------+------------------+
|product_id |product_category_id |product_name|product_description|product_price|product_url       |
+-----------+---------------------+------------+-------------------+-------------+------------------+
|1          |10                   |nike shoes  |running shoes      |100.0        |nike.com          |
|2          |11                   |armour tee  |training t-shirt   |50.0         |armour.com        |
|3          |12                   |nike socks  |sport socks        |30.0         |nike.com/socks    |
|4          |13                   |armour cap  |baseball cap       |20.0         |armour.com/cap    |
|5          |14                   |puma hoodie |winter hoodie      |60.0         |puma.com/hoodie   |
+-----------+---------------------+------------+-------------------+-------------+------------------+



# ✅ Adjust prices using CASE WHEN + expr
adjusted_df = products_df.withColumn(
    "product_price",
    expr("""
        CASE 
            WHEN product_name LIKE '%nike%' THEN product_price * 1.2
            WHEN product_name LIKE '%armour%' THEN product_price * 1.1
            ELSE product_price
        END
    """)
)

# Show adjusted prices
adjusted_df.show(truncate=False)


+-----------+---------------------+------------+-------------------+-------------+------------------+
|product_id |product_category_id |product_name|product_description|product_price|product_url       |
+-----------+---------------------+------------+-------------------+-------------+------------------+
|1          |10                   |nike shoes  |running shoes      |120.0        |nike.com          |
|2          |11                   |armour tee  |training t-shirt   |55.0         |armour.com        |
|3          |12                   |nike socks  |sport socks        |36.0         |nike.com/socks    |
|4          |13                   |armour cap  |baseball cap       |22.0         |armour.com/cap    |
|5          |14                   |puma hoodie |winter hoodie      |60.0         |puma.com/hoodie   |
+-----------+---------------------+------------+-------------------+-------------+------------------+



