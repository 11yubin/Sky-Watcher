# stream/main.py
# spark streaming main file
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, substring, from_utc_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, StringType
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

# Geofencing: 한국 상공 필터링
# 위도: 33~39, 경도: 124~132
korea_flights = parsed_stream.filter(
    (col("latitude") >= 33.0) & (col("latitude") <= 39.0) &
    (col("longitude") >= 124.0) & (col("longitude") <= 132.0)
)

# 항공사 코드 매핑 함수 (UDF)
def map_airline_code(callsign):
    if not callsign:
        return "Unknown"
    
    # 1. 앞 3글자 추출 및 대문자 변환
    code = callsign[:3].upper()
    
    # 2. 공백이 포함된 경우 (예: "MG1") 처리
    # 혹시 모를 데이터 오염 방지
    code = code.strip()

    mapping = {
        # --- 🇰🇷 대한민국 (국적기 & LCC & 화물) ---
        'KAL': 'Korean Air',          # 대한항공
        'AAR': 'Asiana Airlines',     # 아시아나항공
        'APZ': 'Air Premia',
        'JJA': 'Jeju Air',            # 제주항공
        'JNA': 'Jin Air',             # 진에어
        'TWB': 'T\'way Air',          # 티웨이항공
        'ESR': 'Eastar Jet',          # 이스타항공
        'ASV': 'Air Seoul',           # 에어서울
        'ABL': 'Air Busan',           # 에어부산
        'EOK': 'Aero K',              # 에어로케이
        'AIH': 'Air Incheon',         # 에어인천 (화물)
        'HKE': 'HK Express',          # 홍콩 익스프레스 (한국 취항 많음)

        # --- 🇯🇵 일본 ---
        'ANA': 'All Nippon Airways',  # 전일본공수
        'JAL': 'Japan Airlines',      # 일본항공
        'APJ': 'Peach Aviation',      # 피치항공
        'SKY': 'Skymark Airlines',    # 스카이마크
        'JTA': 'Japan Transocean',    # 일본 트랜스오션
        'SJO': 'J-Air',               # J-Air (JAL 자회사)
        'JJP': 'Jetstar Japan',

        # --- 🇨🇳 중국 / 🇭🇰 홍콩 / 🇹🇼 대만 ---
        'CCA': 'Air China',           # 중국국제항공
        'CES': 'China Eastern',       # 중국동방항공
        'CSN': 'China Southern',      # 중국남방항공
        'CHH': 'Hainan Airlines',     # 하이난항공
        'CQH': 'Spring Airlines',     # 춘추항공
        'CPA': 'Cathay Pacific',      # 캐세이퍼시픽 (리스트엔 없지만 추가 추천)
        'HDA': 'Cathay Dragon',       # 캐세이 드래곤
        'CKK': 'China Cargo',
        'AXM': 'Air Asia',

        # --- 🇺🇸 미국 & 🇪🇺 유럽 & 🌏 기타 ---
        'DAL': 'Delta Air Lines',     # 델타항공
        'UAL': 'United Airlines',     # 유나이티드항공
        'ACA': 'Air Canada',          # 에어캐나다
        'FIN': 'Finnair',             # 핀에어
        'THY': 'Turkish Airlines',    # 터키항공
        'UAE': 'Emirates',            # 에미레이트항공
        'QTR': 'Qatar Airways',       # 카타르항공
        'SIA': 'Singapore Airlines',  # 싱가포르항공
        'JST': 'Jetstar Airways',     # 젯스타
        'PTA': 'Jetstar Asia',        # 젯스타 아시아
        'CEB': 'Cebu Pacific',        # 세부퍼시픽
        'AIQ': 'Thai AirAsia',        # 타이 에어아시아

        # --- 📦 글로벌 화물 (Cargo) ---
        'UPS': 'UPS Airlines',
        'FDX': 'FedEx Express',
        'GTI': 'Atlas Air',           # 아틀라스 항공 (화물)
        'CKS': 'Kalitta Air',         # 칼리타 에어 (화물)

        # --- 🇲🇳 몽골 (특이 케이스) ---
        'MGL': 'MIAT Mongolian',      # 미아트 몽골항공
        'MG1': 'MIAT Mongolian',      # (데이터 오류 보정)
        'MG6': 'MIAT Mongolian',      # (데이터 오류 보정)
    }
    
    # 매핑에 없으면 그냥 코드(KAL 등) 그대로 반환
    return mapping.get(code, code)

airline_udf = udf(map_airline_code, StringType())

# 데이터 가공: 시간 변환 & 항공사 추출 & 컬럼 매핑
final_stream = korea_flights \
    .withColumn("updated_at", from_utc_timestamp(from_unixtime(col("timestamp")), "Asia/Seoul")) \
    .withColumn("created_at", from_utc_timestamp(from_unixtime(col("timestamp")), "Asia/Seoul")) \
    .withColumn("airline", airline_udf(col("callsign"))) \
    .withColumnRenamed("baro_altitude", "altitude") \
    .withColumnRenamed("longitude", "lon") \
    .withColumnRenamed("latitude", "lat") \
    .select(
        "icao24", "callsign", "lat", "lon", "velocity", "altitude", "airline", "updated_at", "created_at"
    )

# Upsert 실행 함수
def execute_upsert_query(spark_session):
    try:
        # Py4J로 Java Driver 접근
        driver_manager = spark_session._sc._gateway.jvm.java.sql.DriverManager
        con = driver_manager.getConnection("jdbc:postgresql://postgres:5432/skywatcher", "admin", "password")
        stmt = con.createStatement()
        
        # Staging -> Active 덮어쓰기 쿼리
        upsert_sql = """
        INSERT INTO active_flights (icao24, callsign, lat, lon, velocity, altitude, airline, updated_at)
        SELECT icao24, callsign, lat, lon, velocity, altitude, airline, updated_at FROM staging_flights
        ON CONFLICT (icao24) 
        DO UPDATE SET 
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            velocity = EXCLUDED.velocity,
            altitude = EXCLUDED.altitude,
            updated_at = EXCLUDED.updated_at,
            callsign = EXCLUDED.callsign,
            airline = EXCLUDED.airline;
        """
        stmt.execute(upsert_sql)
        stmt.close()
        con.close()
    except Exception as e:
        print(f"  -> Upsert Query Failed: {str(e)}")

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

    # B. Staging 테이블을 거쳐서 Active 테이블로 Upsert
    try:
        active_df = batch_df.select("icao24", "callsign", "lat", "lon", "velocity", "altitude", "airline", "updated_at")
        
        # 1. 임시 테이블(staging_flights)에 덮어쓰기 (Overwrite)
        active_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/skywatcher") \
            .option("dbtable", "staging_flights") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        # 2. 실제 Upsert 함수 호출
        execute_upsert_query(batch_df.sparkSession)
        print("  -> Upserted to active_flights 🔄")
        
    except Exception as e:
        print(f"  -> Error doing upsert: {str(e)}")

# 스트리밍 시작
query = final_stream.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()