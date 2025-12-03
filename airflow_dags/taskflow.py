from airflow.sdk import dag, task, chain
from pendulum import datetime


user="user19"


@dag(
    dag_id=f"{user}_taskflow_dag",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    tags=[f"{user}"]
)
def taskflow_dag():
    @task
    def my_task_1():
        import time

        time.sleep(5)
        print(1)

    @task
    def my_task_2():
        print(2)

    chain(my_task_1(), my_task_2())


taskflow_dag()