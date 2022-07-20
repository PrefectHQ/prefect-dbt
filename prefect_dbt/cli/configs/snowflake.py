"""Module containing models for Snowflake configs"""
from prefect_dbt.cli.configs.base import MissingExtrasRequireError, TargetConfigs

try:
    from prefect_snowflake.credentials import SnowflakeCredentials
except ModuleNotFoundError as e:
    raise MissingExtrasRequireError("Snowflake") from e


class SnowflakeTargetConfigs(TargetConfigs):
    """
    Target configs contain credentials and
    settings, specific to Snowflake.
    To find valid keys, head to the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
    page.

    Args:
        credentials: The credentials to use to authenticate; if there are
            overlapping keys between credentials and TargetConfigs,
            e.g. schema, credentials takes precedence.
    """

    credentials: SnowflakeCredentials
