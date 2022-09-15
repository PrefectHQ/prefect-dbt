"""Module containing models for Snowflake configs"""
from typing import Any, Dict, Optional

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from pydantic import Field

from prefect_dbt.cli.configs.base import (
    CredentialsTargetConfigs,
    MissingExtrasRequireError,
)

try:
    from prefect_snowflake.database import SnowflakeConnector
except ModuleNotFoundError as e:
    raise MissingExtrasRequireError("Snowflake") from e


class SnowflakeTargetConfigs(CredentialsTargetConfigs):
    """
    Target configs contain credentials and
    settings, specific to Snowflake.
    To find valid keys, head to the [Snowflake Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/snowflake-profile)
    page.

    Attributes:
        credentials: The credentials to use to authenticate; if there are
            duplicate keys between credentials and TargetConfigs,
            e.g. schema, an error will be raised.

    Examples:
        Load stored SnowflakeTargetConfigs:
        ```python
        from prefect_dbt.cli.configs import SnowflakeTargetConfigs

        snowflake_target_configs = SnowflakeTargetConfigs.load("BLOCK_NAME")
        ```

        Instantiate SnowflakeTargetConfigs.
        ```python
        from prefect_dbt.cli.configs import SnowflakeTargetConfigs
        from prefect_snowflake.credentials import SnowflakeCredentials
        from prefect_snowflake.database import SnowflakeConnector

        credentials = SnowflakeCredentials(
            user="user",
            password="password",
            account="account.region.aws",
            role="role",
        )
        connector = SnowflakeConnector(
            schema="public",
            database="database",
            warehouse="warehouse",
            credentials=credentials,
        )
        target_configs = SnowflakeTargetConfigs(
            connector=connector
        )
        ```
    """

    _block_type_name = "dbt CLI Snowflake Target Configs"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/9a001902ed43a84c6c96d23b24622e19/dbt-bit_tm.png?h=250"  # noqa
    _nested_fields = ("connector",) + CredentialsTargetConfigs._nested_fields

    type: Literal["snowflake"] = "snowflake"
    schema_: Optional[str] = Field(default=None, alias="schema")
    connector: SnowflakeConnector

    def get_configs(self) -> Dict[str, Any]:
        """
        Returns the dbt configs specific to Snowflake profile.

        Returns:
            A configs JSON.
        """
        configs_json = super().get_configs()
        return configs_json
