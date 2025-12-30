# 03_check_spark.py
# spark 실행 여부를 10분마다 체크하고, 안되면 다시 실행 -> 컨테이너를 감시하는 dag
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import docker

def check_and_restart_spark():
    client = docker.from_env()
    
    target_container = 'skywatcher-spark'
    
    try:
        container = client.containers.get(target_container)
        print(f"👀 상태 확인: {target_container} is {container.status}")
        
        if container.status != 'running':
            print("🚨 스파크 정지 상태! 재시작 프로세스 가동 ⚡️")
            container.restart()
            print("✅ 재시작 완료.")
        else:
            print("✅ 스파크는 튼튼합니다.")
            
    except Exception as e:
        print(f"❌ 컨테이너를 찾을 수 없거나 에러 발생: {e}")

with DAG(
    dag_id='skywatcher_health_check',
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/10 * * * *', # 10분마다 실행
    catchup=False,
    tags=['monitoring']
) as dag:

    check_task = PythonOperator(
        task_id='check_spark_container',
        python_callable=check_and_restart_spark
    )