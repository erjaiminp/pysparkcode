from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark_conf=SparkConf()
spark_conf.set("spark.app.name","sql_udf")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()
age_schema = StructType([
    StructField("name",StringType()),
    StructField("age",IntegerType()),
    StructField("city",StringType()),
])

agedf=spark.read.format("csv").schema(age_schema).option("path","E:/Oracle/Softwares/BD/shared/dataset.txt").load()

def adult_flg(age):
    if (age>18):
        return "Y"
    else:
        return "N"

spark.udf.register("parseAgeFUnction",adult_flg,StringType())

agedf.createOrReplaceTempView("aged")
agedf1=spark.sql("select name,age,city,parseAgeFUnction(age) as age_status from aged")

agedf1.show()

for x in spark.catalog.listFunctions():
    print(x)

spark.stop()