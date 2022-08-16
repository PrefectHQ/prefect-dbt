"""Module containing tasks and flows for interacting with dbt CLI"""
import os
from pathlib import Path
from shutil import which
from typing import Any, Dict, List, Optional, Union

import yaml
from prefect import get_run_logger, task
from prefect_shell.commands import shell_run_command

from prefect_dbt.cli.credentials import DbtCliProfile


@task
async def trigger_dbt_cli_command(
    command: str,
    profiles_dir: Optional[Union[Path, str]] = None,
    project_dir: Optional[Union[Path, str]] = None,
    overwrite_profiles: bool = False,
    dbt_cli_profile: Optional[DbtCliProfile] = None,
    **shell_run_command_kwargs: Dict[str, Any],
) -> Union[List[str], str]:
    """
    Task for running dbt commands.

    If no profiles.yml file is found or if overwrite_profiles flag is set to True, this
    will first generate a profiles.yml file in the profiles_dir directory. Then run the dbt
    CLI shell command.

    Args:
        command: The dbt command to be executed.
        profiles_dir: The directory to search for the profiles.yml file. Setting this
            appends the `--profiles-dir` option to the command provided. If this is not set,
            will try using the DBT_PROFILES_DIR environment variable, but if that's also not
            set, will use the default directory `$HOME/.dbt/`.
        project_dir: The directory to search for the dbt_project.yml file.
            Default is the current working directory and its parents.
        overwrite_profiles: Whether the existing profiles.yml file under profiles_dir
            should be overwritten with a new profile.
        dbt_cli_profile: Profiles class containing the profile written to profiles.yml.
            Note! This is optional and will raise an error if profiles.yml already exists
            under profile_dir and overwrite_profiles is set to False.
        **shell_run_command_kwargs: Additional keyword arguments to pass to
            [shell_run_command](https://prefecthq.github.io/prefect-shell/commands/#prefect_shell.commands.shell_run_command).

    Returns:
        last_line_cli_output (str): The last line of the CLI output will be returned
            if `return_all` in `shell_run_command_kwargs` is False. This is the default
            behavior.
        full_cli_output (List[str]): Full CLI output will be returned if `return_all`
            in `shell_run_command_kwargs` is True.

    Examples:
        Execute `dbt debug` with a pre-populated profiles.yml.
        ```python
        from prefect import flow
        from prefect_dbt.cli.commands import trigger_dbt_cli_command

        @flow
        def trigger_dbt_cli_command_flow():
            result = trigger_dbt_cli_command("dbt debug")
            return result

        trigger_dbt_cli_command_flow()
        ```

        Execute `dbt debug` without a pre-populated profiles.yml.
        ```python
        from prefect import flow
        from prefect_dbt.cli.credentials import DbtCliProfile
        from prefect_dbt.cli.commands import trigger_dbt_cli_command
        from prefect_dbt.cli.configs import SnowflakeTargetConfigs
        from prefect_snowflake.credentials import SnowflakeCredentials

        @flow
        def trigger_dbt_cli_command_flow():
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
            dbt_cli_profile = DbtCliProfile(
                name="jaffle_shop",
                target="dev",
                target_configs=target_configs,
            )
            result = trigger_dbt_cli_command(
                "dbt debug",
                overwrite_profiles=True,
                dbt_cli_profile=dbt_cli_profile
            )
            return result

        trigger_dbt_cli_command_flow()
        ```
    """  # noqa
    # check if variable is set, if not check env, if not use expected default
    logger = get_run_logger()
    if not which("dbt"):
        raise ImportError(
            "dbt-core needs to be installed to use this task; run "
            '`pip install "prefect-dbt[cli]"'
        )
    elif not command.startswith("dbt"):
        await shell_run_command.fn(command="dbt --help")
        raise ValueError(
            "Command is not a valid dbt sub-command; see dbt --help above,"
            "or use prefect_shell.commands.shell_run_command for non-dbt related "
            "commands instead"
        )

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

    # append the options
    command += f" --profiles-dir {profiles_dir}"
    if project_dir is not None:
        project_dir = Path(project_dir).expanduser()
        command += f" --project-dir {project_dir}"

    # fix up empty shell_run_command_kwargs
    shell_run_command_kwargs = shell_run_command_kwargs or {}

    logger.info(f"Running dbt command: {command}")
    result = await shell_run_command.fn(command=command, **shell_run_command_kwargs)
    return result
