"""Module containing models for base configs"""

from typing import Optional

from pydantic import BaseModel, Extra, Field


class TargetConfigs(BaseModel, extra=Extra.allow):
    """
    Target configs contain credentials and
    settings, specific to the warehouse you're connecting to.
    This is the base dbt TargetConfigs model;
    ideally use the TargetConfigs containing authentication keywords, e.g.
    `prefect_dbt.cli.snowflake.SnowflakeUserPasswordTargetConfigs`,
    but if the desired TargetConfigs is missing, use this class with
    extra keywords, or help contribute a pull request.
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


class GlobalConfigs(BaseModel, extra=Extra.allow):
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
