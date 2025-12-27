# stream/main.py
# spark streaming main file
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, substring, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import os

# spark session 생성 (kafka 자동 다운로드 + postgres driver load 포함)
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'

spark = SparkSession.builder \
    .appName("SkyWatcher-Debug") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# 스키마 정의 - icao24, callsign 등 필요한 필드만 정의 (ERD 참고)
schema = StructType([
    StructField("timestamp", LongType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("country", StringType()),
    StructField("longitude", FloatType()), # 올바른 이름! (126.x)
    StructField("latitude", FloatType()),  # 올바른 이름! (36.x)
    StructField("altitude", FloatType()),
    StructField("velocity", FloatType())
])

# Kafka에서 데이터 읽기 (Source)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw_flight_data") \
    .option("startingOffsets", "latest") \
    .load()

# 데이터 파싱 & 전처리 (Transformation)
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# [핵심 1] Geofencing: 한국 상공 필터링
# 위도: 33~39, 경도: 124~132
korea_flights = parsed_stream.filter(
    (col("latitude") >= 33.0) & (col("latitude") <= 39.0) &
    (col("longitude") >= 124.0) & (col("longitude") <= 132.0)
)

# [핵심 2] 데이터 가공: 시간 변환 & 항공사 추출 & 컬럼 매핑
# DB 컬럼명과 맞춰줘야 함 (active_flights, flight_logs 테이블 구조 참고)
final_stream = korea_flights \
    .withColumn("updated_at", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumn("created_at", from_unixtime(col("timestamp")).cast("timestamp")) \
    .withColumn("airline", substring(col("callsign"), 1, 3)) \
    .withColumnRenamed("baro_altitude", "altitude") \
    .withColumnRenamed("longitude", "lon") \
    .withColumnRenamed("latitude", "lat") \
    .select(
        "icao24", "callsign", "lat", "lon", "velocity", "altitude", "airline", "updated_at", "created_at"
    )

# DB 저장 로직 (Sink) - foreachBatch 사용
# 이유: 하나의 스트림으로 두 개의 테이블(Logs, Active)에 동시에 저장하기 위함
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    print(f"Batch {batch_id}: Writing {batch_df.count()} rows to Postgres...")

    # A. flight_logs 테이블에 저장 (Append Mode: 계속 쌓기)
    # 로그용 데이터만 선택 (id는 serial이라 자동 생성되므로 제외)
    logs_df = batch_df.select("icao24", "lat", "lon", "velocity", "altitude", "airline", "created_at")
    logs_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/skywatcher") \
        .option("dbtable", "flight_logs") \
        .option("user", "admin") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    # B. active_flights 테이블에 저장 (Upsert 구현을 위한 임시 조치)
    # append: 넣기, upsert: 덮어쓰기
    active_df = batch_df.select("icao24", "callsign", "lat", "lon", "velocity", "altitude", "airline", "updated_at")
    active_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/skydb") \
        .option("dbtable", "active_flights") \
        .option("user", "skyuser") \
        .option("password", "skypass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("upsert") \
        .save()

# 스트리밍 시작
query = final_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()