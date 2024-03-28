import os
from pathlib import Path, PosixPath
from prefect import task, get_run_logger
from typing import Any, Dict, List, Optional, Union

from pydantic import VERSION as PYDANTIC_VERSION

from dbt.cli.main import dbtRunner, dbtRunnerResult
from prefect_dbt.cli.credentials import DbtCliProfile

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, validator
else:
    from pydantic import Field, validator

@task
def dbt_build_task(
    profiles_dir: Optional[Union[Path, str]] = None,
    project_dir: Optional[Union[Path, str]] = None,
    overwrite_profiles: bool = False,
    dbt_cli_profile: Optional[DbtCliProfile] = None,
    tags: Optional[List[str]] = None,
    **shell_run_command_kwargs: Dict[str, Any],
):
    logger = get_run_logger()
    logger.info("Running dbt build task.")
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = ["build"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        logger.info(f"{r.node.name}: {r.status}")

