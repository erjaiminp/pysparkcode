from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark_conf=SparkConf()
spark_conf.set("spark.app.name","sqlex")
spark_conf.set("spark.master","local[*]")

spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()

age_schema = StructType([
    StructField("name",StringType()),
    StructField("age",IntegerType()),
    StructField("city",StringType()),
])

agedf=spark.read.format("csv").schema(age_schema).option("path","E:/Oracle/Softwares/BD/shared/dataset.txt").load()
agedf.createOrReplaceTempView("aged")

agedf1=spark.sql("select name,age,city,case when age>18 then 'y' else 'n' end as adult from aged")

agedf1.show()

spark.stop()