"""Module containing models for Snowflake configs"""
from typing import Any, Dict, Optional

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

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

    Examples:
        Instantiate SnowflakeTargetConfigs.
        ```python
        from prefect_dbt.cli.configs import SnowflakeTargetConfigs
        from prefect_snowflake.credentials import SnowflakeCredentials

        credentials = SnowflakeCredentials(
            schema="schema",
            user="user",
            password="password",
            account="account",
            role="role",
            database="database",
            warehouse="warehouse",
        )
        target_configs = SnowflakeTargetConfigs(
            credentials=credentials
        )
        ```
    """

    _block_type_name = "dbt CLI Snowflake Target Configs"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/8cb73be51575a659667f6471a24153f5/dbt-bit_tm.png?h=250"  # noqa
    _code_example = """/
    ```python
        from prefect_dbt.cli.configs import SnowflakeTargetConfigs
        
        dbt_cli_snowflake_target_configs = SnowflakeTargetConfigs.load("BLOCK_NAME")
    ```"""  # noqa

    type: Literal["snowflake"] = "snowflake"
    schema_: Optional[str] = Field(default=None, alias="schema")
    credentials: SnowflakeCredentials

    def get_configs(self) -> Dict[str, Any]:
        """
        Returns the dbt configs specific to Snowflake profile.

        Returns:
            A configs JSON.
        """
        configs_json = super().get_configs()
        configs_json.pop("connect_params")
        if "schema" not in configs_json:
            raise ValueError(
                "The keyword, schema, must be provided in either "
                "SnowflakeCredentials or SnowflakeTargetConfigs"
            )
        return configs_json
