# 03_check_spark.py
# spark 실행 여부를 10분마다 체크하고, 안되면 다시 실행 -> 컨테이너를 감시하는 dag
# 26.02.12 추가 -> Slack 알림
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import docker

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

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

def task_fail_slack_alert(context):
    """
    Task 실패 시 Slack으로 알림을 전송하는 콜백 함수
    """
    slack_webhook_token = "slack_conn"  # Airflow Admin > Connections에 설정할 Conn Id
    
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    exception = context.get('exception')

    slack_msg = f"""
    :red_circle: Task Failed.
    *Dag*: {dag_id}
    *Task*: {task_id}
    *Execution Date*: {execution_date}
    *Log Url*: {log_url}
    *Exception*: {exception}
    """

    slack_hook = SlackWebhookHook(
        slack_webhook_conn_id=slack_webhook_token
    )
    
    return slack_hook.send(text=slack_msg, username='Airflow Alert')

with DAG(
    dag_id='skywatcher_health_check',
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/10 * * * *', # 10분마다 실행
    catchup=False,
    tags=['monitoring'],
    default_args={
        'on_failure_callback': task_fail_slack_alert
    }
) as dag:

    check_task = PythonOperator(
        task_id='check_spark_container',
        python_callable=check_and_restart_spark
    )