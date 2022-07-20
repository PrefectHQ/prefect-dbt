"""Module containing models for Snowflake configs"""
from typing import Any, Dict, Literal, Optional

from pydantic import Field

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
            duplicate keys between credentials and TargetConfigs,
            e.g. schema, an error will be raised.
    """

    type: Literal["snowflake"] = "snowflake"
    schema_: Optional[str] = Field(default=None, alias="schema")
    credentials: SnowflakeCredentials

    def get_configs(self) -> Dict[str, Any]:
        configs_json = super().get_configs()
        configs_json.pop("connect_params")
        if "schema" not in configs_json:
            raise ValueError(
                "The keyword, schema, must be provided in either "
                "SnowflakeCredentials or SnowflakeTargetConfigs"
            )
        return configs_json
