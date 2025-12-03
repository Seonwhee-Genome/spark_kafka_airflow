from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime


def my_task_1_func():
    import time

    time.sleep(5)
    print(1)


user="user19"


with DAG(
    dag_id=f"{user}_traditional_syntax_dag",
    start_date=datetime(2025, 4, 1),
    schedule="@daily",
    tags=[f"{user}"]
):
    my_task_1 = PythonOperator(
        task_id="my_task_1",
        python_callable=my_task_1_func,
    )

    my_task_2 = PythonOperator(
        task_id="my_task_2",
        python_callable=lambda: print(2),
    )

    my_task_1 >> my_task_2
