from pyspark import SparkContext

sc=SparkContext("local[*]","campaign_spent")

rdd1=sc.textFile("E:/Oracle/Softwares/BD/shared/bigdatacampaigndata-201014-183159.csv")
rdd2=rdd1.map(lambda x: (float(x.split(",")[10]),x.split(",")[0]))
rdd3=rdd2.flatMapValues(lambda x: x.split(" "))
rdd4=rdd3.map(lambda x: (x[1].lower(),x[0]))
rdd5=rdd4.reduceByKey(lambda x,y: (x+y)).sortBy(lambda x: x[1],ascending=False).take(20)

#rdd6=rdd5.collect()

for a in rdd5:
    print(a)