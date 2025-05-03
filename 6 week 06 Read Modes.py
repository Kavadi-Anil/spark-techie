📌 Spark CSV Reading Modes with Mixed Data Types

Sample Input CSV (age column has mixed types):

+---+--------+--------+
|id | name   | age    |
+---+--------+--------+
| 1 | Alice  | 30     |
| 2 | Bob    | twenty |
| 3 | Charlie| 25     |
| 4 | David  | NA     |
| 5 | Eve    | forty  |
+---+--------+--------+

Schema used:
id INT, name STRING, age INT

============================================================

1️⃣ Mode: failFast (strict mode)

- Spark will immediately throw an error when it encounters a malformed or invalid record.
- Use this when data accuracy is critical.

Example:
df = spark.read \
    .format("csv") \
    .schema(schema) \
    .option("mode", "failFast") \
    .option("header", "true") \
    .load("path")

Output:
❌ ERROR: Fails on row 2 (age = "twenty")

============================================================

2️⃣ Mode: permissive (default)

- Invalid values are replaced with null without affecting the rest of the dataset.

Example:
df = spark.read \
    .format("csv") \
    .schema(schema) \
    .option("mode", "permissive") \
    .option("header", "true") \
    .load("path")

Output:
+---+--------+-----+
|id | name   | age |
+---+--------+-----+
| 1 | Alice  | 30  |
| 2 | Bob    | null|
| 3 | Charlie| 25  |
| 4 | David  | null|
| 5 | Eve    | null|
+---+--------+-----+

============================================================

3️⃣ Mode: dropMalformed

- Entire rows with any schema issues are dropped from the result.

Example:
df = spark.read \
    .format("csv") \
    .schema(schema) \
    .option("mode", "dropMalformed") \
    .option("header", "true") \
    .load("path")

Output:
+---+--------+-----+
|id | name   | age |
+---+--------+-----+
| 1 | Alice  | 30  |
| 3 | Charlie| 25  |
+---+--------+-----+

Rows with invalid "age" values ("twenty", "NA", "forty") are completely dropped.
