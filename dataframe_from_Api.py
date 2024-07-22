import findspark
import  json,requests
findspark.init()
from pyspark.sql import SparkSession


spark=SparkSession.builder.master("local[*]").appName('PySpark broadcast variable').getOrCreate()


# Create Python function to read data from API
def read_api(url: str):
    normalized_data = dict()
    data = requests.get(api_url).json()
    normalized_data["_data"] = data # Normalize payload to handle array situtations
    return json.dumps(normalized_data)


api_url = r"https://api.coindesk.com/v1/bpi/currentprice.json"
# api_url = "https://api.wazirx.com/sapi/v1/tickers/24hr"

# Read data into Data Frame
# Create payload rdd
payload = json.loads(read_api(api_url))
payload_rdd = spark.sparkContext.parallelize([payload])
# Read from JSON
df = spark.read.json(payload_rdd)
df.printSchema()
df.select("_data").printSchema()

# Expand root element to read Struct Data
df.select("_data.*").show(truncate=False)
