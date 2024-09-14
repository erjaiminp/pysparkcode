from pyspark import SparkContext

sc=SparkContext("local[*]","customer_spent")

rdd1=sc.textFile("E:/Oracle/Softwares/BD/shared/customerorders-201008-180523.csv")
rdd2=rdd1.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))
rdd3=rdd2.reduceByKey(lambda x,y: (x+y)).filter(lambda x: x[1]>5000)

rdd4=rdd3.collect()

for a in rdd4:
    print(a)

