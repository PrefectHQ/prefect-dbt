import pytest
from prefect_snowflake.credentials import SnowflakeConnector
from pydantic import SecretBytes, SecretStr

from prefect_dbt.cli.configs import SnowflakeTargetConfigs


def test_snowflake_target_configs_get_configs():
    connector_kwargs = dict(
        account="account",
        user="user",
        password="password",
        schema="schema",
        database="database",
        warehouse="warehouse",
    )

    snowflake_connector = SnowflakeConnector(**connector_kwargs)
    configs_kwargs = {"credentials": snowflake_connector}

    configs = SnowflakeTargetConfigs(**configs_kwargs)
    actual = configs.get_configs()
    expected = dict(
        account="account",
        user="user",
        password="password",
        type="snowflake",
        schema="schema",
        database="database",
        warehouse="warehouse",
        threads=4,
    )
    for k, v in actual.items():
        actual_v = (
            v.get_secret_value() if isinstance(v, (SecretBytes, SecretStr)) else v
        )
        expected_v = expected[k]
        assert actual_v == expected_v


def test_snowflake_target_configs_get_configs_missing_schema():
    snowflake_connector = SnowflakeConnector(
        account="account",
        user="user",
        password="password",
        database="database",
        warehouse="warehouse",
    )
    configs = SnowflakeTargetConfigs(credentials=snowflake_connector)
    with pytest.raises(ValueError, match="The keyword, schema"):
        configs.get_configs()
