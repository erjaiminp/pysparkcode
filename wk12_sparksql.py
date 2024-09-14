from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_conf=SparkConf()
spark_conf.set("spark.app.name","spark_sql")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

orderdf = spark.read.csv("E:/Oracle/Softwares/BD/shared/customerorders-201008-180523.csv")
orderdf.createOrReplaceTempView("orders")
orderdf1 = spark.sql("select * from orders")
orderdf1.show()

spark.stop()