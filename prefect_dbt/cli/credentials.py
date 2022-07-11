"""Module containing credentials for interacting with dbt CLI"""
from dataclasses import dataclass
from typing import Any, Dict

from prefect.blocks.core import Block


@dataclass
class DbtCliProfile(Block):
    """
    Profile for use across dbt CLI tasks and flows.

    Args:
        name: Profile name used for populating profiles.yml.
        target: The default target your dbt project will use.
        target_configs: Target configs contain credentials and
            settings, specific to the warehouse you're connecting to.
            To find valid keys, head to the [Available adapters](
            https://docs.getdbt.com/docs/available-adapters) page and
            click the desired adapter's "Profile Setup" hyperlink.
        global_configs: Global configs control things like the visual output
            of logs, the manner in which dbt parses your project,
            and what to do when dbt finds a version mismatch
            or a failing model. Valid keys can be found [here](
            https://docs.getdbt.com/reference/global-configs)

    Examples:
        Get a dbt Snowflake profile from DbtCliProfile:
        ```python
        from prefect_dbt.cli import DbtCliProfile

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
            static_parser=False
        )
        dbt_cli_profile = DbtCliProfile(
            name="jaffle_shop",
            target="dev",
            target_configs=target_configs,
            global_configs=global_configs,
        )
        profile = dbt_cli_profile.get_profile()
        ```

    Load saved dbt CLI profile:
        ```python
        from prefect_dbt.cloud import DbtCliProfile
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
    target_configs: Dict[str, Any]
    global_configs: Dict[str, Any] = None

    def get_profile(self):
        """
        Returns the class's profile.
        """
        profile = {
            "config": self.global_configs or {},
            self.name: {
                "target": self.target,
                "outputs": {self.target: self.target_configs},
            },
        }
        return profile
