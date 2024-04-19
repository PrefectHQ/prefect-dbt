import os
from pathlib import Path
from typing import Optional, Union

import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.results import NodeStatus
from prefect import get_run_logger, task
from prefect.artifacts import create_markdown_artifact
from pydantic import VERSION as PYDANTIC_VERSION

from prefect_dbt.cli.credentials import DbtCliProfile

if PYDANTIC_VERSION.startswith("2."):
    pass
else:
    pass


@task
def dbt_build_task(
    profiles_dir: Optional[Union[Path, str]] = None,
    project_dir: Optional[Union[Path, str]] = None,
    overwrite_profiles: bool = False,
    dbt_cli_profile: Optional[DbtCliProfile] = None,
    dbt_client: Optional[dbtRunner] = None,
    create_artifact: bool = True,
    artifact_key: str = "dbt-build-task-summary",
):
    logger = get_run_logger()
    logger.info("Running dbt build task.")

    # Initialize client if not passed in
    if not dbt_client:
        dbt_client = dbtRunner()

    if profiles_dir is None:
        profiles_dir = os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt")
    profiles_dir = Path(profiles_dir).expanduser()

    # https://docs.getdbt.com/dbt-cli/configure-your-profile
    # Note that the file always needs to be called profiles.yml,
    # regardless of which directory it is in.
    profiles_path = profiles_dir / "profiles.yml"
    logger.debug(f"Using this profiles path: {profiles_path}")

    # write the profile if overwrite or no profiles exist
    if overwrite_profiles or not profiles_path.exists():
        if dbt_cli_profile is None:
            raise ValueError("Provide `dbt_cli_profile` keyword for writing profiles")
        profile = dbt_cli_profile.get_profile()
        profiles_dir.mkdir(exist_ok=True)
        with open(profiles_path, "w+") as f:
            yaml.dump(profile, f, default_flow_style=False)
        logger.info(f"Wrote profile to {profiles_path}")
    elif dbt_cli_profile is not None:
        raise ValueError(
            f"Since overwrite_profiles is False and profiles_path ({profiles_path}) "
            f"already exists, the profile within dbt_cli_profile could not be used; "
            f"if the existing profile is satisfactory, do not pass dbt_cli_profile"
        )

    # create CLI args as a list of strings
    cli_args = ["build"]

    # append the options
    cli_args.append("--profiles-dir")
    cli_args.append(profiles_dir)
    if project_dir is not None:
        project_dir = Path(project_dir).expanduser()
        cli_args.append("--project-dir")
        cli_args.append(project_dir)

    # run the command
    res: dbtRunnerResult = dbt_client.invoke(cli_args)

    if res.exception is not None:
        logger.error(f"dbt build task failed with exception: {res.exception}")
        raise res.exception

    if res.success:
        logger.info("dbt build task succeeded.")
    else:
        logger.error("dbt build task failed.")

    if create_artifact:
        create_dbt_task_artifact(artifact_key=artifact_key, results=res, mode="Build")


def create_dbt_task_artifact(
    artifact_key: str, results: dbtRunnerResult, mode: str = "Build"
):
    # Create Summary Markdown Artifact
    successful_runs = []
    failed_runs = []
    skipped_runs = []
    for r in results.result.results:
        if r.status == NodeStatus.Success or r.status == NodeStatus.Pass:
            successful_runs.append(r)
        elif (
            r.status == NodeStatus.Fail
            or r.status == NodeStatus.Error
            or r.status == NodeStatus.RuntimeErr
        ):
            failed_runs.append(r)
        elif r.status == NodeStatus.Skipped:
            skipped_runs.append(r)

    markdown = "# DBT Build Task Summary"

    if failed_runs != []:
        failed_runs_str = ""
        for r in failed_runs:
            failed_runs_str += f"**{r.node.name}**\nNode Type: {r.node.resource_type}\nNode Path: {r.node.original_file_path}"
            if r.message:
                message = r.message.replace("\n", ".")
                failed_runs_str += f"\nError Message: {message}\n"
        markdown += f"""\n## Failed Runs\n\n{failed_runs_str}\n\n"""

    successful_runs_str = "\n".join([f"**{r.node.name}**" for r in successful_runs])
    markdown += f"""\n## Successful Runs\n\n{successful_runs_str}\n\n"""

    skipped_runs_str = "\n".join([f"**{r.node.name}**" for r in skipped_runs])
    markdown += f"""## Skipped Runs\n{skipped_runs_str}\n\n"""

    create_markdown_artifact(
        markdown=markdown,
        key=artifact_key,
    )
