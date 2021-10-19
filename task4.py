from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
# task 5 connect to MongoDB
spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/precp.hpcp") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/precp.hpcp") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()





# DF STRUCTURED
df = spark.read.csv("./data.csv", header=True)
df.printSchema()
df.show()
df.write.format("mongo").mode("append").save()
df_distinct = df.select(countDistinct("STATION_NAME"))
df_distinct.show()
df.groupBy("STATION_NAME").count().show(58,truncate = False) 



