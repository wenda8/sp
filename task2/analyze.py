from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, lower, array_contains, split, explode, array, lit
import os
os.environ["JAVA_HOME"] = "D:/Java/Java8"

spark = SparkSession.builder \
    .appName("TelegramChannelAnalysis") \
    .config("spark.local.dir", "D:/xuexi/sp/temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .schema("time TIMESTAMP, source STRING, message STRING, has_media BOOLEAN") \
    .parquet("D:/xuexi/sp/telegram_data")

keywords = ["fight", "error"]  

message_count_anomalies = df.withWatermark("time", "10 minutes") \
    .groupBy(
        window(col("time"), "5 minutes"),
        col("source")
    ) \
    .agg(count("message").alias("message_count")) \
    .where("message_count > 10")  


keyword_anomalies = df.withColumn("word", explode(split(lower(col("message")), "\\s+"))) \
    .where(array_contains(array(*[lit(k) for k in keywords]), col("word"))) \
    .groupBy(
        window(col("time"), "5 minutes"),
        col("source")
    ) \
    .agg(count("word").alias("keyword_count")) \
    .where("keyword_count > 0") 


query1 = message_count_anomalies.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query2 = keyword_anomalies.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
