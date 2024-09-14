from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark_conf=SparkConf()
spark_conf.set("spark.app.name","udf")
spark_conf.set("spark.master","local[*]")
spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

age_schema = StructType([
    StructField("name",StringType()),
    StructField("age",IntegerType()),
    StructField("city",StringType())
])

def adult_status(age):
    if (age>18):
        return "Y"
    else:
        return "N"

parseAgeFunction = udf(adult_status,StringType())

agedf=spark.read.format("csv").schema(age_schema).option("path","E:/Oracle/Softwares/BD/shared/dataset.txt").load()
agedf1 = agedf.withColumn("adult",parseAgeFunction("age"))

agedf1.printSchema()
agedf1.show()

spark.stop()