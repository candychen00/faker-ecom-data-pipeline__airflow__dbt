from airflow.sdk import dag, task

@dag(
        schedule=None,
        tags=['klc']
)
def test_dag():

    @task
    def a():
        print("Task A !!!!!!!!!!")

    a()

test_dag()