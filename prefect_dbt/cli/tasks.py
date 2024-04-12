import os
import json
from pathlib import Path
from prefect import task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from typing import Any, Dict, List, Optional, Union

from pydantic import VERSION as PYDANTIC_VERSION

from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import RunStatus
from prefect_dbt.cli.credentials import DbtCliProfile

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, validator
else:
    from pydantic import Field, validator

@task
def dbt_build_task(
    project_dir: str,
    artifact_key: str = "dbt-build-task-summary"
):
    logger = get_run_logger()
    logger.info("Running dbt build task.")
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = ["build"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    if res.exception is not None:
        logger.error(f"dbt build task failed with exception: {res.exception}")
        raise res.exception
    
    if res.success:
        logger.info(f"dbt build task succeeded.")
    else:
        logger.error(f"dbt build task failed.")

    #Create Summary Markdown Artifact
    successful_runs = []
    failed_runs = []
    skipped_runs = []
    for r in res.result.results:
        match r.status:
            case RunStatus.Success:
                successful_runs.append(r)
            case RunStatus.Error:
                failed_runs.append(r)
            case RunStatus.Skipped:
                skipped_runs.append(r)

    successful_runs_str = "\n".join([f"{r.node.name}" for r in successful_runs])
    failed_runs_str = "\n".join([f"{r.node.name}" for r in failed_runs])
    skipped_runs_str = "\n".join([f"{r.node.name}" for r in skipped_runs])

    markdown = f"""# DBT Build Task Summary\n## Successful Runs\n\n{successful_runs_str}\n\n## Failed Runs\n{failed_runs_str}\n\n## Skipped Runs\n{skipped_runs_str}\n\n"""

    create_markdown_artifact(
        markdown=markdown,
        key=artifact_key,
        description="DBT Build Task Summary",
    )

