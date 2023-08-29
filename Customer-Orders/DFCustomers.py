from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc
import sys

# Create a SparkContext
sc = SparkContext(appName="CustomerOrdersDF")

# Get the filename from command-line arguments
customer_orders_file = sys.argv[1]

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the customer-orders.csv file with comma-separated values and without headers
orders_df = spark.read.csv(customer_orders_file, header=False, inferSchema=True)

# Convert the columns to appropriate data types
orders_df = orders_df.withColumn("_c0", orders_df["_c0"].cast("integer"))
orders_df = orders_df.withColumn("_c1", orders_df["_c1"].cast("integer"))
orders_df = orders_df.withColumn("_c2", orders_df["_c2"].cast("float"))

# Rename the columns
orders_df = orders_df.withColumnRenamed("_c0", "customer_id")
orders_df = orders_df.withColumnRenamed("_c1", "product_id")
orders_df = orders_df.withColumnRenamed("_c2", "order_amount")

# Calculate the total amount spent by each customer
total_spent_df = orders_df.groupBy("customer_id").agg(sum("order_amount").alias("total_amount_spent"))

# Sort the results based on the total amount spent in descending order
sorted_results = total_spent_df.orderBy(desc("total_amount_spent"))

# Display the total amount spent by each customer
sorted_results.show()

# Stop the SparkContext
sc.stop()
