📌 Spark SQL – Databases & Table Creation

🔹 By default, Spark SQL creates tables in the default database if no database is specified.

🔹 List all available databases:
spark.sql("SHOW DATABASES").show()

🔹 Filter databases using SQL LIKE:
spark.sql("SHOW DATABASES LIKE 'test%'").show()

🔹 Filter databases using DataFrame filter:
spark.sql("SHOW DATABASES").filter("databaseName LIKE 'test%'").show()

🔹 Switch to a specific database:
spark.sql("USE retail")

🔹 Show all tables in the current database:
spark.sql("SHOW TABLES").show()

🔹 Create a table with just the name (no schema):
spark.sql("CREATE TABLE IF NOT EXISTS employees")

🔹 Create an empty table if it does not exist with just table and column names:
spark.sql("CREATE TABLE IF NOT EXISTS employees (id INT, name STRING)")

🔹 Create a table only if it does not already exist with full schema and datatypes:
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees (
        id INT,
        name STRING,
        department STRING
    )
""")

🔸 Note:
In Python environments (like PySpark, Jupyter), always use triple quotes (""") for multi-line SQL strings. Using a multi-line string with single quotes causes a syntax error.

📌 Spark – Persistence

Persistence means keeping a DataFrame or RDD in memory (or disk) to avoid recomputation in future actions.

Common levels:
- MEMORY_AND_DISK (default)
- MEMORY_ONLY
- DISK_ONLY

Example:
df.persist()       # Caches in memory and disk  
df.cache()         # Same as persist()  
df.unpersist()     # Removes cached data

📌 Spark SQL – Temporary Views

🔹 Temporary views are created from DataFrames to run SQL queries.

🔹 Create a temp view:
df.createOrReplaceTempView("employees_view")

🔹 Query the view:
spark.sql("SELECT * FROM employees_view").show()

🔹 Temp views are not persistent. They exist only during the current Spark session and are not stored in the metastore.

📌 Spark SQL – DataFrame from Temp Table

🔹 Insert data into a permanent table from a temporary view:
spark.sql("INSERT INTO db_name.table_name SELECT * FROM temp_view_name")

🔹 Ensure that the schema of the temp view matches the target table.

🔹 Example:
spark.sql("INSERT INTO retail.employees SELECT * FROM employees_view")

🔹 You can also create a DataFrame by reading from a SQL table:
df = spark.sql("SELECT * FROM db_name.table_name")

📌 Spark SQL – Describe Table

🔹 View schema of a table:
spark.sql("DESCRIBE TABLE employees").show()

Sample output:
+-----------+----------+--------+
| col_name  | data_type| comment|
+-----------+----------+--------+
| id        | int      | null   |
| name      | string   | null   |
| department| string   | null   |
+-----------+----------+--------+

🔹 View extended metadata (location, format, etc.):
spark.sql("DESCRIBE TABLE EXTENDED employees").show(truncate=False)

Includes:
- Column definitions
- Detailed table information:
  - Database: default
  - Table: employees
  - Type: MANAGED or EXTERNAL
  - Location: table path
  - Provider: file format
  - Schema: struct<...>

📌 Spark SQL – Show Create Table

🔹 View the SQL statement that defines the table:
spark.sql("SHOW CREATE TABLE employees").show(truncate=False)

Sample output:
+------------------------------------------------------------------+
|createtab_stmt                                                   |
+------------------------------------------------------------------+
|CREATE TABLE employees (id INT, name STRING ...) USING parquet ...|
+------------------------------------------------------------------+

📌 Spark SQL – Dropping Managed Tables

🔹 Dropping a managed table deletes both its metadata (from metastore) and its data (from storage).

🔹 Example:
spark.sql("DROP TABLE IF EXISTS employees")

🔹 Managed tables are fully controlled by Spark, including where the data is stored.

📌 Spark SQL – External Tables

🔹 External tables only store metadata in the metastore. The data itself remains in the specified path.

🔹 Dropping an external table removes only the metadata. The data remains untouched in its location.

🔹 Example:
spark.sql("""
    CREATE TABLE IF NOT EXISTS ext_employees (
        id INT,
        name STRING
    )
    USING PARQUET
    OPTIONS (path "/mnt/external/employees")
""")

📌 Spark SQL – Check if Table is Managed or External

🔹 Use DESCRIBE EXTENDED to find the table type:
spark.sql("DESCRIBE EXTENDED table_name").show(truncate=False)

🔹 Look for the value of "Type" in the output:
- MANAGED means it is a managed table
- EXTERNAL means it is an external table
