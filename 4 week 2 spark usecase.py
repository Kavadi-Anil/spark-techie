
Perform Various Transformations
================================

Given columns:
- order_id
- date
- customer_id
- order_status

Tasks:
------

1. Count the orders under each status  
   → Example: (CLOSED, 1), (PENDING_PAYMENT, 1), ...

2. Find the premium customers  
   → Top 10 customers who placed the most number of orders

3. Count of distinct customers who placed at least one order  
   → Use distinct on customer_id

4. Find the customer who has placed the maximum number of CLOSED orders  

Sample Data:
------------
1, 2013-07-25 00:00:00.0, 11599, CLOSED  
2, 2013-07-25 00:00:00.0, 256, PENDING_PAYMENT  
3, 2013-07-25 00:00:00.0, 12111, COMPLETE  
4, 2013-07-25 00:00:00.0, 8827, CLOSED  
5, 2013-07-25 00:00:00.0, 11318, COMPLETE  

Example Outputs:
----------------
(CLOSED, 1)  
(PENDING_PAYMENT, 1)  
(COMPLETE, 1)  
(CLOSED, 1)  
(COMPLETE, 1)







# Spark RDD Use Case
# =========================

# Read the file as an RDD
order_rdd = spark.sparkContext.textFile("location")

# Preview the first 5 lines (Action)
order_rdd.take(5)

# 1. Count the orders under each status
# Example Output: ('CLOSED', 1), ('PENDING_PAYMENT', 1), ...

mapped_rdd = order_rdd.map(lambda x: (x.split(",")[3], 1))
mapped_rdd.take(5)
# [('CLOSED', 1), ('PENDING_PAYMENT', 1), ...]

reduced_rdd = mapped_rdd.reduceByKey(lambda x, y: x + y)
reduced_rdd.collect()

# Sort by count in descending order
sorted_status = reduced_rdd.sortBy(lambda x: x[1], ascending=False)
sorted_status.collect()

# 2. Find the premium customers (Top customers by order count)
customer_mapped = order_rdd.map(lambda x: (x.split(",")[2], 1))
customer_mapped.take(5)

customer_agg = customer_mapped.reduceByKey(lambda x, y: x + y)
customer_agg.take(10)

# Sort customers by order count in descending order
customer_sorted = customer_agg.sortBy(lambda x: x[1], ascending=False)
customer_sorted.take(10)

# 3. Distinct count of customers who placed at least one order
distinct_customers = order_rdd.map(lambda x: x.split(",")[2]).distinct()
distinct_count = distinct_customers.count()
print("Distinct Customers:", distinct_count)

# Total Orders
total_orders = order_rdd.count()
print("Total Orders:", total_orders)

# 4. Which customer has the maximum number of CLOSED orders

# Filter only closed orders
filtered_orders = order_rdd.filter(lambda x: x.split(",")[3] == "CLOSED")

# Map to (customer_id, 1)
filter_mapped = filtered_orders.map(lambda x: (x.split(",")[2], 1))

# Aggregate by customer_id
filter_agg = filter_mapped.reduceByKey(lambda x, y: x + y)

# Sort to find the customer with max CLOSED orders
closed_sorted = filter_agg.sortBy(lambda x: x[1], ascending=False)
closed_sorted.take(1)
