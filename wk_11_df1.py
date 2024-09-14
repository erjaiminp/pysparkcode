from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark_conf=SparkConf()
spark_conf.set("spark.app.name","df1")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

#struct type
order_schema=StructType([
    StructField("customer_id",IntegerType()),
    StructField("product_id",IntegerType()),
    StructField("amount_spent",FloatType())
])

ordersddl = "customer_id Integer, product_id Integer, amountspent Float"

orderdf = spark.read.schema(ordersddl).csv("E:/Oracle/Softwares/BD/shared/customerorders-201008-180523.csv")
orderdf.printSchema()
orderdf.show()

spark.stop()