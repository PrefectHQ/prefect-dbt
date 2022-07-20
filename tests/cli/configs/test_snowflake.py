from prefect_snowflake.credentials import SnowflakeCredentials
from pydantic import SecretBytes, SecretStr

from prefect_dbt.cli.configs import SnowflakeTargetConfigs


def test_snowflake_target_configs_get_configs():
    snowflake_credentials = SnowflakeCredentials(
        account="account", user="user", password="password"
    )
    configs = SnowflakeTargetConfigs(
        credentials=snowflake_credentials, type="snowflake", schema="schema"
    )
    actual = configs.get_configs()
    expected = dict(
        account="account",
        user="user",
        password="password",
        type="snowflake",
        schema="schema",
        threads=4,
    )
    for k, v in actual.items():
        actual_v = (
            v.get_secret_value() if isinstance(v, (SecretBytes, SecretStr)) else v
        )
        expected_v = expected[k]
        assert actual_v == expected_v
