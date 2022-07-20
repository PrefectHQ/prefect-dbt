"""Module containing models for base configs"""

import abc
from typing import Any, Dict, Optional

from prefect.blocks.core import Block
from pydantic import Field, SecretBytes, SecretStr


class DbtConfigs(Block, abc.ABC):
    """
    Abstract class for other dbt Configs.

    Args:
        extras: Extra target configs' keywords, not yet added
            to prefect-dbt, but available in dbt; if there are
            duplicate keys between extras and TargetConfigs,
            an error will be raised.
    """

    extras: Optional[Dict[str, Any]] = None

    def _populate_configs_json(
        self, configs_json: Dict[str, Any], dict_: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Recursively populate configs_json.
        """
        for key, value in dict_.items():
            key = key.rstrip("_")
            if value is not None:
                if key in ["extras", "credentials"]:
                    configs_json = self._populate_configs_json(configs_json, value)
                else:
                    if key in configs_json.keys():
                        raise ValueError(
                            f"The keyword, {key}, has already been provided in "
                            f"TargetConfigs; remove duplicated keywords to continue"
                        )
                    if isinstance(value, (SecretStr, SecretBytes)):
                        value = value.get_secret_value()
                    # key needs to be rstripped because schema alias doesn't get used
                    configs_json[key] = value
        return configs_json

    def get_configs(self) -> Dict[str, Any]:
        """
        Returns the dbt configs, likely used eventually for writing to profiles.yml.

        Returns:
            A configs JSON.
        """
        return self._populate_configs_json({}, self.dict())


class TargetConfigs(DbtConfigs):
    """
    Target configs contain credentials and
    settings, specific to the warehouse you're connecting to.
    To find valid keys, head to the [Available adapters](
    https://docs.getdbt.com/docs/available-adapters) page and
    click the desired adapter's "Profile Setup" hyperlink.

    Args:
        type: The name of the database warehouse
        schema: The schema that dbt will build objects into;
            in BigQuery, a schema is actually a dataset.
        threads: The number of threads representing the max number
            of paths through the graph dbt may work on at once.
    """

    type: str
    schema_: str = Field(alias="schema")
    threads: int = 4


class GlobalConfigs(DbtConfigs):
    """
    Global configs control things like the visual output
    of logs, the manner in which dbt parses your project,
    and what to do when dbt finds a version mismatch
    or a failing model. Docs can be found [here](
    https://docs.getdbt.com/reference/global-configs).

    Args:
        send_anonymous_usage_stats:
        use_colors:
        partial_parse:
        printer_width:
        write_json:
        warn_error:
        log_format:
        debug:
        version_check:
        fail_fast:
        use_experimental_parser:
        static_parser:
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


class MissingExtrasRequireError(ImportError):
    def __init__(self, service, *args, **kwargs):
        msg = (
            f"To use {service.title()}TargetConfigs, "
            f'execute `pip install "prefect-dbt[{service.lower()}]"`'
        )
        super().__init__(msg, *args, **kwargs)
