# streaming/debug.py
# spark streaming test
from pyspark.sql import SparkSession
import os
import sys

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

# spark session 생성 (kafka 자동 다운로드 포함)
spark = SparkSession.builder \
    .appName("SkyWatcher-Debug") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

## 로그 레벨 조정 (지저분한 로그 끄기)
spark.sparkContext.setLogLevel("WARN")

# Kafka에서 데이터 읽기 (ReadStream)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw_flight_data") \
    .option("startingOffsets", "latest") \
    .load()

## Kafka 데이터는 key, value가 바이너리로 옴 -> String으로 변환
df_string = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# 콘솔 출력
query = df_string.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("🚀 Spark Streaming이 Kafka 데이터를 기다리는 중...")
query.awaitTermination()