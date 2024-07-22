import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import broadcast
#creating spark session
spark=SparkSession.builder.master("local[*]").appName('PySpark broadcast variable').getOrCreate()

states = {"NY":"New York", "CA":"California", "FL":"Florida"}

broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
("Michael","Rose","USA","NY"),
("Robert","William","USA","CA"),
("Maria","Jones","USA","FL")
]


columns = ["firstname","lastname","country","state"]

# creating a dataframe
df = spark.createDataFrame(data = data, schema = columns)

df.printSchema()

df.show(truncate=False)


#defining a UDF in pyspark
def state_convert(code):
    return broadcastStates.value[code]


df2 = df.rdd.map(lambda a: (a[0],a[1],a[2],state_convert(a[3]))).toDF(columns)

df2.show(truncate=False)