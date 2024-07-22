import findspark
import  json,requests
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark=SparkSession.builder.master("local[*]").appName('PySpark broadcast variable').getOrCreate()
#Sample Dataset (customer_purchases.csv):
#customer_id,purchase_amount,purchase_date

data = [(1,100,"2023-01-15"),
        (2,150,"2023-02-20"),
        (1,200,"2023-03-10"),
        (3,50,"2023-04-05"),
        (2,120,"2023-05-15"),
        (1,300,"2023-06-25")]

columns=["customer_id","purchase_amount","purchase_date"]

df=spark.createDataFrame(data, columns)
df.printSchema()
df.show()

total_purchase_by_customer = df.groupBy("customer_id").agg(F.sum("purchase_amount").alias("total_puchase_amount"))
total_purchase_by_customer.printSchema()
total_purchase_by_customer.show()
