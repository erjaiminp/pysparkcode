from pyspark import SparkContext

sc=SparkContext("local[*]","biglog")
rdd1=sc.textFile("E:/Oracle/Softwares/BD/shared/biglog-201105-152517.txt")
rdd2=rdd1.map(lambda x: (x.split(",")[0],1))
rdd3=rdd2.reduceByKey(lambda x,y: (x+y))

rdd4=rdd3.collect()

for a in rdd4:
    print(a)