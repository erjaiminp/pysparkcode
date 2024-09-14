# Read a file using PySpark and convert text to date, float, integer

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark_conf = SparkConf()
spark_conf.set("spark.app.name", "convert_text_to_date")
spark_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# Struct_Type
schema_orders = StructType([

    StructField("order_date", StringType(), True),
    StructField("pages_visited", IntegerType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("tshirt_category", StringType(), True),
    StructField("tshirt_price", FloatType(), True),
    StructField("tshirt_quantity", IntegerType(), True),
])

# schema_order = ("order_date String, pages_visited Integer, "
#                 "order_id String, customer_id String, "
#                 "tshirt_category String, "
#                 "tshirt_price Float, tshirt_quantity Integer")

# Here we are reading the file using defined schema 'schema_orders' header from file
orders_stg_df = spark.read.schema(schema_orders).option("header", True)\
    .csv("/home/ubuntu/project/pyspark/files/orders.csv")

orders_stg_df.printSchema()

# Converting Date in String to Date Datatype
orders_df = orders_stg_df.select(to_date(col("order_date"), "yyyy/MM/dd").alias("order_date"),
                                 "pages_visited",
                                 # convert Text to int: col("pages_visited").cast("int")
                                 "order_id",
                                 "customer_id",
                                 "tshirt_category",
                                 "tshirt_price",
                                 # convert Text to float: col("tshirt_price").cast("float"),
                                 "tshirt_quantity"
                                 # convert Text to int: col("tshirt_quantity").cast("int")
                                 )

orders_df.show()

orders_df.printSchema()

spark.stop()
