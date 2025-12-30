# 01_cleanup_logs.py
# 3일 지난 flight_logs 모조리 지워주는 함수
from airflow import DAG
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from datetime import datetime, timedelta
import psycopg2

def delete_old_logs():
    try:
        # SkyWatcher DB에 직접 접속 (Airflow 컨테이너 -> Postgres 컨테이너)
        conn = psycopg2.connect(
            host="postgres",       # skywatcher의 postgres가 아니라 airflow-postgres
            database="skywatcher",
            user="admin",
            password="password",
            port="5432"
        )
        cursor = conn.cursor()
        
        # 3일 지난 데이터 삭제 쿼리 실행
        query = "DELETE FROM flight_logs WHERE created_at < NOW() - INTERVAL '3 days';"
        cursor.execute(query)
        
        deleted_count = cursor.rowcount
        conn.commit()
        
        print(f"🧹 [청소 완료] 3일 지난 로그 {deleted_count}건을 삭제했습니다.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"🔥 DB 연결 또는 삭제 실패: {e}")
        raise e

# DAG 설정
default_args = {
    'owner': 'skywatcher',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='skywatcher_db_cleanup',      # Airflow 화면에 뜰 이름
    default_args=default_args,
    description='Delete flight logs older than 3 days',
    start_date=datetime(2023, 1, 1),     # 과거 날짜로 잡아야 바로 실행 가능
    schedule_interval='@daily',          # 매일 0시 0분에 실행
    catchup=False,                       # 밀린 거 실행 안 함 (중요!)
    tags=['maintenance']
) as dag:

    # 3. 태스크 정의
    cleanup_task = PythonOperator(
        task_id='delete_old_rows',
        python_callable=delete_old_logs
    )

    cleanup_task