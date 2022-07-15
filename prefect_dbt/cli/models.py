"""Module containing models for configs"""
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, Extra, Field


class TargetConfigs(BaseModel, extra=Extra.allow):
    """
    Target configs contain credentials and
    settings, specific to the warehouse you're connecting to.
    To find valid keys, head to the [Available adapters](
    https://docs.getdbt.com/docs/available-adapters) page and
    click the desired adapter's "Profile Setup" hyperlink.
    """

    type: str
    schema_: str = Field(alias="schema")
    threads: int = 4


class SnowflakeTargetConfigs(TargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and
    settings, specific to Snowflake.
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
    with user and password authentication. To find valid keys, head to the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile) page.
    """  # noqa

    user: str
    password: str
    authenticator: str


class SnowflakeKeyPairTargetConfigs(SnowflakeTargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and settings, specific to Snowflake,
    with key pair authentication. To find valid keys, head to the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
    page.
    """

    private_key_path: Union[Path, str]
    private_key_passphrase: str


class SnowflakeSsoTargetConfigs(SnowflakeTargetConfigs, extra=Extra.allow):
    """
    Target configs contain credentials and settings, specific to Snowflake,
    with SSO authentication. To find valid keys, head to the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
    page.
    """

    authenticator: str = "externalbrowser"


class GlobalConfigs(BaseModel, extra=Extra.allow):
    """
    Global configs control things like the visual output
    of logs, the manner in which dbt parses your project,
    and what to do when dbt finds a version mismatch
    or a failing model. Valid keys can be found [here](
    https://docs.getdbt.com/reference/global-configs)
    """

    send_anonymous_usage_stats: Optional[bool] = None
    use_colors: Optional[bool] = None
    partial_parse: Optional[bool] = None
    printer_width: Optional[int] = None
    write_json: Optional[bool] = None
    warn_error: Optional[bool] = None
    log_format: Optional[bool] = None
    debug: Optional[bool] = None
    version_check: Optional[bool] = None
    fail_fast: Optional[bool] = None
    use_experimental_parser: Optional[bool] = None
    static_parser: Optional[bool] = None
