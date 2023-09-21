from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task
from datetime import datetime

my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")

with DAG(
        dag_id="consumer",
        schedule=[my_file,my_file_2],
        start_date=datetime(2023, 9, 18),
        catchup=False
):
    @task()
    def read_dataset():
        with open(my_file.uri, "a+") as f:
            print(f.read())

    @task()
    def read_dataset_2():
        with open(my_file_2.uri, "a+") as f:
            print(f.read())


    read_dataset() >> read_dataset_2()
