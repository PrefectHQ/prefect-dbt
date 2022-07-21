"""Module containing credentials for interacting with dbt CLI"""
from typing import Any, Dict, Optional

from prefect.blocks.core import Block

from prefect_dbt.cli.configs import GlobalConfigs, TargetConfigs


class DbtCliProfile(Block):
    """
    Profile for use across dbt CLI tasks and flows.

    Args:
        name (str): Profile name used for populating profiles.yml.
        target (str): The default target your dbt project will use.
        target_configs (TargetConfigs): Target configs contain credentials and
            settings, specific to the warehouse you're connecting to.
            To find valid keys, head to the [Available adapters](
            https://docs.getdbt.com/docs/available-adapters) page and
            click the desired adapter's "Profile Setup" hyperlink.
        global_configs (GlobalConfigs): Global configs control
            things like the visual output of logs, the manner
            in which dbt parses your project, and what to do when
            dbt finds a version mismatch or a failing model.
            Valid keys can be found [here](
            https://docs.getdbt.com/reference/global-configs).

    Examples:
        Get a dbt Snowflake profile from DbtCliProfile by using SnowflakeTargetConfigs:
        ```python
        from prefect_dbt.cli import DbtCliProfile
        from prefect_dbt.cli.configs import SnowflakeTargetConfigs
        from prefect_snowflake.credentials import SnowflakeCredentials

        snowflake_credentials = SnowflakeCredentials(
            schema="schema",
            user="user",
            password="password",
            account="account",
            role="role",
            database="database",
            warehouse="warehouse",
        )
        target_configs = SnowflakeTargetConfigs(
            credentials=snowflake_credentials
        )
        dbt_cli_profile = DbtCliProfile(
            name="jaffle_shop",
            target="dev",
            target_configs=target_configs,
        )
        profile = dbt_cli_profile.get_profile()
        ```

        Get a dbt Redshift profile from DbtCliProfile by using generic TargetConfigs:
        ```python
        from prefect_dbt.cli import DbtCliProfile
        from prefect_dbt.cli.configs import GlobalConfigs, TargetConfigs

        target_configs_extras = dict(
            host="hostname.region.redshift.amazonaws.com",
            user="username",
            password="password1",
            port=5439,
            dbname="analytics",
        )
        target_configs = TargetConfigs(
            type="redshift",
            schema="schema",
            threads=4,
            extras=target_configs_extras
        )
        dbt_cli_profile = DbtCliProfile(
            name="jaffle_shop",
            target="dev",
            target_configs=target_configs,
        )
        profile = dbt_cli_profile.get_profile()
        ```

        Load saved dbt CLI profile:
        ```python
        from prefect_dbt.cli import DbtCliProfile
        profile = DbtCliProfile.load("my-dbt-credentials").get_profile()
        ```
    """

    _block_type_name = "dbt CLI Profile"
    _logo_url = "https://asset.brandfetch.io/idofJOT4bu/idxrwTdDC-.svg"
    _code_example = """/
    ```python
        from prefect_dbt.cli import DbtCliProfile
        dbt_cli_profile = DbtCliProfile.load("BLOCK_NAME")
    ```"""

    name: str
    target: str
    target_configs: TargetConfigs
    global_configs: Optional[GlobalConfigs] = None

    def get_profile(self) -> Dict[str, Any]:
        """
        Returns the dbt profile, likely used for writing to profiles.yml.

        Returns:
            A JSON compatible dictionary with the expected format of profiles.yml.
        """
        profile = {
            "config": self.global_configs.get_configs() if self.global_configs else {},
            self.name: {
                "target": self.target,
                "outputs": {self.target: self.target_configs.get_configs()},
            },
        }
        return profile
