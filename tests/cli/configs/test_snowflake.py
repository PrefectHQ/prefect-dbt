from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector
from pydantic import SecretBytes, SecretStr

from prefect_dbt.cli.configs import SnowflakeTargetConfigs


def test_snowflake_target_configs_get_configs():
    credentials = SnowflakeCredentials(
        account="account",
        user="user",
        password="password",
    )
    connector_kwargs = dict(
        schema="schema",
        database="database",
        warehouse="warehouse",
        credentials=credentials,
    )

    snowflake_connector = SnowflakeConnector(**connector_kwargs)
    configs_kwargs = {"connector": snowflake_connector}

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
        authenticator="snowflake",
        threads=4,
    )
    for k, v in actual.items():
        actual_v = (
            v.get_secret_value() if isinstance(v, (SecretBytes, SecretStr)) else v
        )
        expected_v = expected[k]
        assert actual_v == expected_v
