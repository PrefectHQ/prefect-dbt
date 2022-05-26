from prefect_dbt.flows import hello_and_goodbye


def test_hello_and_goodbye_flow():
    flow_state = hello_and_goodbye()
    assert flow_state.is_completed
