from datetime import datetime

from airflow.sdk import DAG, task
from kubernetes.client import models as k8s

with DAG(
    dag_id="hello_k8s",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    @task
    def hello():
        import socket
        print("hello from pod:", socket.gethostname())

    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={"cpu": "250m", "memory": "512Mi"},
                                limits={"memory": "1Gi"},
                            ),
                        )
                    ]
                )
            )
        }
    )
    def hello_with_resources():
        import socket
        print("resources overridden, pod:", socket.gethostname())

    hello() >> hello_with_resources()
