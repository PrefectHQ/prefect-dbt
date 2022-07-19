from prefect_dbt.cli.configs.base import TargetConfigs


def test_target_configs_get_configs():
    target_configs = TargetConfigs(
        type="snowflake", schema_="schema_input", threads=5, extras={"extra_input": 1}
    )
    assert target_configs.get_configs() == dict(
        type="snowflake", schema="schema_input", threads=5, extra_input=1
    )
