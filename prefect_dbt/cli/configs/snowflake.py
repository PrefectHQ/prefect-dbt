"""Module containing models for Snowflake configs"""
from pathlib import Path
from typing import Optional, Union

from pydantic import Extra

from prefect_dbt.cli.configs.base import TargetConfigs


class SnowflakeTargetConfigs(TargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and
    settings, specific to Snowflake.
    This is the base Snowflake TargetConfigs model;
    ideally use the TargetConfigs containing authentication keywords, e.g.
    `prefect_dbt.cli.snowflake.SnowflakeUserPasswordTargetConfigs`,
    but if the desired TargetConfigs is missing, use this class with
    extra keywords, or help contribute a pull request.
    To find valid keys, head to the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
    page.
    """

    account: str
    role: str
    database: str
    warehouse: str
    client_session_keep_alive: bool = False
    query_tag: Optional[str] = None
    connect_retries: int = 0
    connect_timeout: int = 10
    retry_on_database_errors: bool = False
    retry_all: bool = False


class SnowflakeUserPasswordTargetConfigs(SnowflakeTargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and settings, specific to Snowflake,
    with user and password authentication. Descriptions can be found at the
    [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile) page.
    """

    user: str
    password: str
    authenticator: str


class SnowflakeKeyPairTargetConfigs(SnowflakeTargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and settings, specific to Snowflake,
    with key pair authentication. Descriptions can be found at the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile) page.
    """

    private_key_path: Union[Path, str]
    private_key_passphrase: str


class SnowflakeSsoTargetConfigs(SnowflakeTargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and settings, specific to Snowflake,
    with SSO authentication. Descriptions can be found at the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile) page.
    """

    authenticator: str = "externalbrowser"
