"""Module containing credentials for interacting with dbt CLI"""
from typing import Any, Dict, Optional

from prefect.blocks.core import Block
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


class GlobalConfigs(BaseModel, extra=Extra.allow):
    """
    Global configs control things like the visual output
    of logs, the manner in which dbt parses your project,
    and what to do when dbt finds a version mismatch
    or a failing model. Valid keys can be found [here](
    https://docs.getdbt.com/reference/global-configs)
    """

    send_anonymous_usage_stats: bool = None
    use_colors: bool = None
    partial_parse: bool = None
    printer_width: int = None
    write_json: bool = None
    warn_error: bool = None
    log_format: bool = None
    debug: bool = None
    version_check: bool = None
    fail_fast: bool = None
    use_experimental_parser: bool = None
    static_parser: bool = None


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
            https://docs.getdbt.com/reference/global-configs).

    Examples:
        Get a dbt Snowflake profile from DbtCliProfile:
        ```python
        from prefect_dbt.cli.credentials import (
            DbtCliProfile, TargetConfigs, GlobalConfigs
        )

        target_configs = TargetConfigs(
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
        global_configs = GlobalConfigs(
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
    target_configs: TargetConfigs
    global_configs: Optional[GlobalConfigs] = None

    def block_initialization(self):
        self.target_configs = self._parse_configs(self.target_configs)
        self.global_configs = self._parse_configs(self.global_configs)

    @staticmethod
    def _parse_configs(configs: BaseModel) -> Dict[str, Any]:
        """
        Helper function to parse configs when passed either as
        dict or pydantic model.
        """
        if configs is None:
            return {}
        elif isinstance(configs, BaseModel):
            return {
                k.rstrip("_"): v for k, v in configs.dict().items() if v is not None
            }

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
