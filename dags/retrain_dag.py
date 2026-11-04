# -*- coding: utf-8 -*-
"""주간 재학습 DAG: 데이터 준비 -> 학습 -> 골든셋 검증 -> 챔피언 승격.

이 파일은 "무엇을 어떤 순서로, 언제" 실행할지만 적는다.
실제 일은 jaeyun1/retrain 이미지 안의 stages.py가 한다.
Task 하나 = 파드 하나(KubernetesPodOperator)이고, 4개의 Task는 직렬로 실행된다.
"""
import pendulum
from airflow.sdk import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

IMAGE = "jaeyun1/retrain:v2"
NAMESPACE = "ai"                      # [EKS] Airflow가 ai 네임스페이스에 있으므로 재학습 파드도 ai에 띄운다
MLFLOW_URI = "http://mlflow.ai.svc:5000"   # [EKS] 클러스터 안의 MLflow 주소
REJECTED_EXIT_CODE = 99           # stages.py promote 가 "승격 안 함"일 때 내는 종료코드


def secret_env(env_name: str, secret_name: str, key: str) -> k8s.V1EnvVar:
    """Secret의 값을 환경변수로 넣는다 (값이 코드와 Git에 남지 않는다)."""
    return k8s.V1EnvVar(
        name=env_name,
        value_from=k8s.V1EnvVarSource(secret_key_ref=k8s.V1SecretKeySelector(name=secret_name, key=key)),
    )


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
        # [EKS] 별도 Secret을 만들지 않고 이미 있는 Secret을 직접 참조한다
        env_vars=[
            k8s.V1EnvVar(name="MLFLOW_TRACKING_URI", value=MLFLOW_URI),
            k8s.V1EnvVar(name="FEEDBACK_TABLE", value="feedbacks"),
            secret_env("FEEDBACK_DATABASE_URL", "airflow-feedback-db", "connection"),  # 피드백 DB 읽기 전용
            secret_env("KMA_API_KEY", "kma-secret", "API_KEY"),
        ],
        # [EKS] 피드백 DB 접속(verify-full)에 쓰는 RDS CA 인증서를 /etc/postgresql/ca.crt로 마운트
        volumes=[k8s.V1Volume(
            name="db-ca",
            secret=k8s.V1SecretVolumeSource(
                secret_name="airflow-feedback-db",
                items=[k8s.V1KeyToPath(key="ca.crt", path="ca.crt")],
            ),
        )],
        volume_mounts=[k8s.V1VolumeMount(name="db-ca", mount_path="/etc/postgresql", read_only=True)],
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
