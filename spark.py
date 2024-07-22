import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.rdd import RDD


spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
sc = spark.sparkContext
# input = "C:\Users\premv\PycharmProjects\project\com\prem\input\input_text.txt"

# data = ["prem is ","very good person","he plays cricket","he eats healthy food"]

# rdd = spark.sparkContext.parallize(data)

rdd_input = sc.textFile("input_text")
counts=rdd_input.flatMap(lambda line:line.split()).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)

output = counts.collect()

# for(word,count) in output:
#     print(word,count)

counts.coalesce(1).saveAsTextFile("output1.txt")



