"""Module containing models for base configs"""

from typing import Any, Dict, Optional

from prefect.blocks.core import Block


class DbtConfigs(Block):
    """
    Abstract class for other dbt Configs.

    Args:
        extras: Extra target configs' keywords, not yet added
            to prefect-dbt, but available in dbt.
    """

    _is_anonymous = True

    extras: Optional[Dict[str, Any]] = None

    def get_configs(self):
        """
        Helper function to parse configs.
        """
        configs_json = {}
        for key, val in self.dict().items():
            if val is not None:
                if key in ["extras", "credentials"]:
                    configs_json.update({k.rstrip("_"): v for k, v in val.items() if v})
                else:
                    configs_json[key.rstrip("_")] = val
        return configs_json


class TargetConfigs(DbtConfigs):
    """
    Target configs contain credentials and
    settings, specific to the warehouse you're connecting to.
    To find valid keys, head to the [Available adapters](
    https://docs.getdbt.com/docs/available-adapters) page and
    click the desired adapter's "Profile Setup" hyperlink.

    Args:
        type: The name of the database warehouse
        schema_: The schema that dbt will build objects into;
            in BigQuery, a schema is actually a dataset.
        threads: The number of threads representing the max number
            of paths through the graph dbt may work on at once.
    """

    type: str
    # cannot name this schema, even with Field alias; will handle in get_configs
    schema_: str
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
