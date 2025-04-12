from pyspark import SparkConf, SparkContext

# Prerequisites: Ensure PySpark is installed and the file exists at the specified path.

# Initialize SparkContext
conf = SparkConf().setAppName("customer_purchase")
sc = SparkContext(conf=conf)

# Read the CSV file into an RDD
file_path = "/home/ubuntu/project/pyspark/files/customerorders-201008-180523.csv"
rdd = sc.textFile(file_path)

# Split the CSV lines and extract relevant fields (CustomerID and PurchaseAmount)
rdd_mapped = rdd.map(lambda line: line.split(",")) \
                .map(lambda fields: (fields[0], float(fields[2])))  # (CustomerID, PurchaseAmount)

# Aggregate purchase amounts per customer
rdd_aggregated = rdd_mapped.reduceByKey(lambda x, y: x + y)

# Sort the aggregated results by ascending purchase amount
rdd_sorted = rdd_aggregated.sortBy(lambda x: x[1], ascending=True)

# Collect and print the results
results = rdd_sorted.collect()
for customer, total in results:
    print(f"CustomerID: {customer}, TotalPurchase: {total}")