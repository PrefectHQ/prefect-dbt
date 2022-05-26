from prefect import flow

from prefect_dbt.tasks import goodbye_prefect_dbt, hello_prefect_dbt


def test_hello_prefect_dbt():
    @flow
    def test_flow():
        return hello_prefect_dbt()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-dbt!"


def goodbye_hello_prefect_dbt():
    @flow
    def test_flow():
        return goodbye_prefect_dbt()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-dbt!"
