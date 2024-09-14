from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf=SparkConf()
spark_conf.set("spark.app.name","dfrw")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

order_schema = "customer_id Integer,product_id Integer,amount_spent Float"

orderdf = spark.read.format("csv").schema(order_schema).option("path","E:/Oracle/Softwares/BD/shared/customerorders-201008-180523.csv").load()

#orderdf.show()

orderdf.write.format("csv").mode("overwrite").option("header",True).option("path","E:/Oracle/Softwares/BD/shared/cust_orders").save()

spark.stop()
