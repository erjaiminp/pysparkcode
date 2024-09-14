from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","regex")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

myregex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'
orderdf=spark.read.text("E:/Oracle/Softwares/BD/shared/orders_new.txt")
orderdf1 = orderdf.select(regexp_extract('value',myregex,1).alias("order_id"),regexp_extract('value',myregex,2).alias("trans_dt"),regexp_extract('value',myregex,3).alias("customer_id"),regexp_extract('value',myregex,4).alias("status"))
orderdf1.show()
orderdf1.select("order_id","status").show()

spark.stop()
