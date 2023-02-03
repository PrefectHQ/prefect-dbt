"""Module containing tasks and flows for interacting with dbt CLI"""
import os
from pathlib import Path
from shutil import which
from typing import Any, Dict, List, Optional, Union

import yaml
from prefect import get_run_logger, task
from prefect_shell.commands import ShellOperation, shell_run_command
from pydantic import Field, root_validator

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


class DbtCoreOperation(ShellOperation):
    """
    A block representing a dbt operation, containing multiple dbt and shell commands.

    For long-lasting operations, use the trigger method and utilize the block as a
    context manager for automatic closure of processes when context is exited.
    If not, manually call the close method to close processes.

    For short-lasting operations, use the run method. Context is automatically managed
    with this method.

    Attributes:
        commands: A list of commands to execute sequentially.
        stream_output: Whether to stream output.
        env: A dictionary of environment variables to set for the shell operation.
        working_directory: The working directory context the commands
            will be executed within.
        shell: The shell to use to execute the commands.
        extension: The extension to use for the temporary file.
            if unset defaults to `.ps1` on Windows and `.sh` on other platforms.
        profiles_dir: The directory to search for the profiles.yml file.
            Setting this appends the `--profiles-dir` option to the dbt commands
            provided. If this is not set, will try using the DBT_PROFILES_DIR
            environment variable, but if that's also not
            set, will use the default directory `$HOME/.dbt/`.
        project_dir: The directory to search for the dbt_project.yml file.
            Default is the current working directory and its parents.
        overwrite_profiles: Whether the existing profiles.yml file under profiles_dir
            should be overwritten with a new profile.
        dbt_cli_profile: Profiles class containing the profile written to profiles.yml.
            Note! This is optional and will raise an error if profiles.yml already
            exists under profile_dir and overwrite_profiles is set to False.
    """

    _block_type_name = "dbt Core Operation"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/9a001902ed43a84c6c96d23b24622e19/dbt-bit_tm.png?h=250"  # noqa
    _documentation_url = "https://prefecthq.github.io/prefect-dbt/cli/commands/#prefect_dbt.cli.commands.DbtCoreOperation"  # noqa

    profiles_dir: Optional[Path] = Field(
        default=None,
        description=(
            "The directory to search for the profiles.yml file. "
            "Setting this appends the `--profiles-dir` option to the dbt commands "
            "provided. If this is not set, will try using the DBT_PROFILES_DIR "
            "environment variable, but if that's also not "
            "set, will use the default directory `$HOME/.dbt/`."
        ),
    )
    project_dir: Optional[Path] = Field(
        default=None,
        description=(
            "The directory to search for the dbt_project.yml file. "
            "Default is the current working directory and its parents."
        ),
    )
    overwrite_profiles: bool = Field(
        default=False,
        description=(
            "Whether the existing profiles.yml file under profiles_dir "
            "should be overwritten with a new profile."
        ),
    )
    dbt_cli_profile: Optional[DbtCliProfile] = Field(
        default=None,
        description=(
            "Profiles class containing the profile written to profiles.yml. "
            "Note! This is optional and will raise an error if profiles.yml already "
            "exists under profile_dir and overwrite_profiles is set to False."
        ),
    )

    @root_validator(pre=True)
    def _check_installation(cls, values):
        """
        Check that dbt is installed.
        """
        if not which("dbt"):
            raise ImportError(
                "dbt-core needs to be installed to use this task; run "
                '`pip install "prefect-dbt[cli]"'
            )
        return values

    @root_validator
    def _check_profiles(cls, values):
        """
        Check that the profiles_dir and dbt_cli_profile are valid.
        """
        profiles_dir = values.get("profiles_dir")
        if profiles_dir is None:
            if values.get("env", {}).get("DBT_PROFILES_DIR") is not None:
                profiles_dir = values["env"]["DBT_PROFILES_DIR"]
            else:
                profiles_dir = os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt")
        profiles_dir = Path(profiles_dir).expanduser()
        values["profiles_dir"] = profiles_dir

        # https://docs.getdbt.com/dbt-cli/configure-your-profile
        # Note that the file always needs to be called profiles.yml,
        # regardless of which directory it is in.
        profiles_path = profiles_dir / "profiles.yml"
        overwrite_profiles = values.get("overwrite_profiles")
        dbt_cli_profile = values.get("dbt_cli_profile")
        if not profiles_path.exists() or overwrite_profiles:
            if dbt_cli_profile is None:
                raise ValueError(
                    "Since overwrite_profiles is True or profiles_path is empty, "
                    "need `dbt_cli_profile` to write a profile"
                )
            profile = dbt_cli_profile.get_profile()
            profiles_dir.mkdir(exist_ok=True)
            with open(profiles_path, "w+") as f:
                yaml.dump(profile, f, default_flow_style=False)
        elif dbt_cli_profile is not None:
            raise ValueError(
                f"Since overwrite_profiles is False and profiles_path {profiles_path} "
                f"already exists, the profile within dbt_cli_profile couldn't be used; "
                f"if the existing profile is satisfactory, do not set dbt_cli_profile"
            )
        return values

    @root_validator
    def _process_commands(cls, values):
        """
        Append profiles-dir and project-dir options to dbt commands, and ensures
        that at least one command is a valid dbt sub-command.
        """
        project_dir = values.get("project_dir")
        profiles_dir = values.get("profiles_dir")

        commands = []
        has_dbt_command = False
        for command in values.get("commands"):
            if command.startswith("dbt "):
                has_dbt_command = True
                command += f" --profiles-dir {profiles_dir}"
                if project_dir is not None:
                    project_dir = Path(project_dir).expanduser()
                    command += f" --project-dir {project_dir}"
            commands.append(command)
        if not has_dbt_command:
            raise ValueError(
                "None of the commands are a valid dbt sub-command; see dbt --help, "
                "or use prefect_shell.ShellOperation for non-dbt related "
                "commands instead"
            )
        values["commands"] = commands
        return values
