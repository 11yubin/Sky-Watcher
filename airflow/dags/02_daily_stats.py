# 02_daily_stats.py
# 하루치 간단 통계 테이블
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

def calculate_stats():
    conn = psycopg2.connect(
        host="postgres", database="skywatcher", user="admin", password="password", port="5432"
    )
    cursor = conn.cursor()

    # 요약 테이블이 없으면 만들기
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            summary_date DATE PRIMARY KEY,
            total_flights INT,
            avg_altitude FLOAT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)

    # 통계 쿼리 (어제 데이터 요약)
    query = """
        INSERT INTO daily_stats (summary_date, total_flights, avg_altitude)
        SELECT 
            CURRENT_DATE - INTERVAL '1 day', 
            COUNT(*), 
            AVG(altitude)
        FROM flight_logs
        WHERE created_at::date = CURRENT_DATE - INTERVAL '1 day'
        ON CONFLICT (summary_date) DO UPDATE 
        SET total_flights = EXCLUDED.total_flights, avg_altitude = EXCLUDED.avg_altitude;
    """
    
    cursor.execute(query)
    conn.commit()
    print("📊 일일 통계 요약 완료!")
    
    cursor.close()
    conn.close()

with DAG(
    dag_id='skywatcher_daily_stats',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['analytics']
) as dag:

    stats_task = PythonOperator(
        task_id='calculate_daily_stats',
        python_callable=calculate_stats
    )