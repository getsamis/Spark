from pyspark import SparkContext
import sys

# Create a SparkContext
sc = SparkContext(appName="CustomerOrdersRDD")

# Get the filename from command-line arguments
customer_orders_file = sys.argv[1]

# Load the CSV file into an RDD
orders_rdd = sc.textFile(customer_orders_file)

# Split each line of the RDD by comma to extract the customer ID and order amount
split_rdd = orders_rdd.map(lambda line: line.split(","))

# Convert the customer ID and order amount to integers and floats
customer_orders_rdd = split_rdd.map(lambda x: (int(x[0]), float(x[2])))

# Calculate the total amount spent by each customer
total_spent_rdd = customer_orders_rdd.reduceByKey(lambda x, y: x + y)

# Sort the results based on the total amount spent in descending order
sorted_results = total_spent_rdd.sortBy(lambda x: x[1], ascending=False)

# Print the total amount spent by each customer
for customer in sorted_results.collect():
    print("Customer ID: {}, Total Amount Spent: {:.2f}".format(customer[0], customer[1]))

# Stop the SparkContext
sc.stop()
