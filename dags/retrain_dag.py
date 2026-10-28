# -*- coding: utf-8 -*-
"""주간 재학습 DAG: 데이터 준비 -> 학습 -> 골든셋 검증 -> 챔피언 승격.

이 파일은 "무엇을 어떤 순서로, 언제" 실행할지만 적는다.
실제 일은 jaeyun1/retrain 이미지 안의 stages.py가 한다.
Task 하나 = 파드 하나(KubernetesPodOperator)이고, 네 Task는 직렬로 실행된다.
"""
import pendulum
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "jaeyun1/retrain:v1"
NAMESPACE = "airflow"
SECRET_NAME = "retrain-secrets"   # MLFLOW_TRACKING_URI, FEEDBACK_DATABASE_URL, KMA_API_KEY
REJECTED_EXIT_CODE = 99           # stages.py promote 가 "승격 안 함"일 때 내는 종료코드


def stage(name: str) -> KubernetesPodOperator:
    """stages.py 의 단계 하나를 파드로 실행하는 Task를 만든다."""
    return KubernetesPodOperator(
        task_id=name,
        name=f"retrain-{name}",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="IfNotPresent",
        cmds=["python", "stages.py"],
        # {{ run_id }} : 이번 DAG 실행의 고유 ID. 네 Task가 같은 MLflow run을 찾는 열쇠다.
        arguments=[name, "--airflow-run-id", "{{ run_id }}"],
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=SECRET_NAME))],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "200m", "memory": "256Mi"},
            limits={"memory": "1Gi"},
        ),
        # 종료코드 99 = 실패가 아니라 "건너뜀(skipped)"으로 표시
        skip_on_exit_code=REJECTED_EXIT_CODE,
        get_logs=True,
        on_finish_action="delete_pod",
        startup_timeout_seconds=300,
        retries=1,
        retry_delay=pendulum.duration(minutes=2),
        execution_timeout=pendulum.duration(minutes=30),
    )


with DAG(
    dag_id="weekly_retrain",
    description="매주 월요일 03:00(KST) 재학습 -> 골든셋 검증 -> 통과 시에만 champion 승격",
    schedule="0 3 * * 1",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    catchup=False,          # 밀린 주간 실행을 한꺼번에 돌리지 않는다
    max_active_runs=1,      # 재학습이 겹쳐서 돌지 않게 한다
    tags=["mlops", "retrain"],
) as dag:
    prepare = stage("prepare")
    train = stage("train")
    validate = stage("validate")
    promote = stage("promote")

    prepare >> train >> validate >> promote
