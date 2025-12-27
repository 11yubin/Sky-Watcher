-- init.sql
-- docker 초기 설정시 테이블 생성 자동화 위한 파일

-- 1. 권한 부여
GRANT ALL PRIVILEGES ON DATABASE skywatcher TO skyuser;

-- 2. 기존 테이블 삭제 (초기화용 안전장치)
DROP TABLE IF EXISTS active_flights;
DROP TABLE IF EXISTS flight_logs;
DROP TABLE IF EXISTS hourly_flight_stats;

-- 3. 실시간 현황 테이블
CREATE TABLE active_flights (
    icao24 VARCHAR(20) PRIMARY KEY,  -- Upsert용 PK
    callsign VARCHAR(20),
    lat FLOAT,
    lon FLOAT,
    velocity FLOAT,
    altitude FLOAT,
    airline VARCHAR(50),
    updated_at TIMESTAMP
);

-- 4. 로그 테이블
CREATE TABLE flight_logs (
    id SERIAL PRIMARY KEY,           -- Auto Increment ID
    icao24 VARCHAR(20),
    lat FLOAT,
    lon FLOAT,
    velocity FLOAT,
    altitude FLOAT,
    airline VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. 인덱스 생성 (조회 성능 최적화)
CREATE INDEX idx_logs_created_at ON flight_logs(created_at);

-- 6. 데이터 마트 (통계용)
CREATE TABLE hourly_flight_stats (
    stat_time TIMESTAMP,
    total_flights INT,
    avg_velocity FLOAT,
    top_airline VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);