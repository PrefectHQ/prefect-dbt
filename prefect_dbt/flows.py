"""This is an example flows module"""
from prefect import flow

from prefect_dbt.tasks import goodbye_prefect_dbt, hello_prefect_dbt


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_dbt)
    print(goodbye_prefect_dbt)
