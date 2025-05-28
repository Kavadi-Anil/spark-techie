Use Case: Word Count on Large File (e.g., 10 TB)
=================================================

You are given a use case where you have to develop a logic to process a huge file (e.g., 10 TB).

✅ Step-by-step approach:
--------------------------
1. Start your development using sample data.
2. Once the logic gives correct results on sample data, replace it with the actual 10 TB file.

Goal:
-----
Develop a logic to find the **frequency of each word** in the file.

Using RDD and parallelize for development/testing:

```python
# Sample logic to find word frequencies
result = spark.sparkContext \
    .parallelize(words) \
    .map(lambda x: x.lower()) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .collect()

print(result)







Spark Core API
==================

# Task: Perform word count using RDD transformations

# Step 1: Sample word list
words = ["big", "data", "trendy", "tech", "more", "SUPER", "super"]

# Step 2: Create an RDD using parallelize
words_rdd = spark.sparkContext.parallelize(words)

# Step 3: Normalize all words to lowercase
words_normalized = words_rdd.map(lambda x: x.lower())
words_normalized.collect()
# Output: ['big', 'data', 'trendy', 'tech', 'more', 'super', 'super']

# Step 4: Map each word to a tuple (word, 1)
mapped_words = words_normalized.map(lambda x: (x, 1))

# Step 5: Aggregate word counts using reduceByKey
agg_result = mapped_words.reduceByKey(lambda x, y: x + y)

# Step 6: Collect the result
agg_result.collect()
# Example Output: [('big', 1), ('data', 1), ('super', 2), ...]
