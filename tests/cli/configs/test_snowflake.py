import pytest
from prefect_snowflake.credentials import SnowflakeCredentials
from pydantic import SecretBytes, SecretStr

from prefect_dbt.cli.configs import SnowflakeTargetConfigs


@pytest.mark.parametrize("schema_in_target_configs", [True, False])
def test_snowflake_target_configs_get_configs(schema_in_target_configs):
    configs_kwargs = dict()
    credentials_kwargs = dict(account="account", user="user", password="password")
    if schema_in_target_configs:
        configs_kwargs["schema"] = "schema"
    else:
        credentials_kwargs["schema"] = "schema"

    snowflake_credentials = SnowflakeCredentials(**credentials_kwargs)
    configs_kwargs["credentials"] = snowflake_credentials

    configs = SnowflakeTargetConfigs(**configs_kwargs)
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


def test_snowflake_target_configs_get_configs_missing_schema():
    snowflake_credentials = SnowflakeCredentials(
        account="account", user="user", password="password"
    )
    configs = SnowflakeTargetConfigs(credentials=snowflake_credentials)
    with pytest.raises(ValueError, match="The keyword, schema"):
        configs.get_configs()


def test_snowflake_target_configs_get_configs_duplicate_schema():
    snowflake_credentials = SnowflakeCredentials(
        account="account", user="user", password="password", schema="schema"
    )
    configs = SnowflakeTargetConfigs(credentials=snowflake_credentials, schema="schema")
    with pytest.raises(ValueError, match="The keyword, schema"):
        configs.get_configs()
