import pytest

from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_dbt.cloud.credentials import DbtCloudCredentials


@pytest.fixture
def dbt_cloud_credentials():
    return DbtCloudCredentials(api_key="my_api_key", account_id=123456789)


@pytest.fixture
def dbt_cli_profile():
    target_configs = dict(
        type="snowflake",
        account="account",
        user="user",
        password="password",
        role="role",
        database="database",
        warehouse="warehouse",
        schema="schema",
        threads=4,
        client_session_keep_alive=False,
        query_tag="query_tag",
    )
    global_configs = dict(
        send_anonymous_usage_stats=False,
        use_colors=True,
        partial_parse=False,
        printer_width=88,
        write_json=True,
        warn_error=False,
        log_format=True,
        debug=True,
        version_check=True,
        fail_fast=True,
        use_experimental_parser=True,
        static_parser=False,
    )
    return DbtCliProfile(
        name="jaffle_shop",
        target="dev",
        target_configs=target_configs,
        global_configs=global_configs,
    )


@pytest.fixture
def dbt_cli_profile_bare():
    target_configs = dict(
        type="custom",
        account="fake",
    )
    return DbtCliProfile(
        name="prefecto",
        target="testing",
        target_configs=target_configs,
    )
