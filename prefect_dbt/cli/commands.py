import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from prefect import get_run_logger, task
from prefect_shell.utils import shell_run_command

from prefect_dbt.cli.credentials import DbtCliCredentials


@task
def trigger_dbt_cli_command(
    command: str,
    profiles_dir: Optional[Union[Path, str]] = None,
    overwrite_profiles: bool = False,
    dbt_cli_credentials: DbtCliCredentials = None,
    set_dbt_profiles_dir_env_var: bool = True,
    shell_run_command_kwargs: Dict[str, Any] = None,
) -> Union[List, str]:
    """
    Task for running dbt commands.

    If no profiles.yml file is found or if overwrite_profiles flag is set to True, this
    will first generate a profiles.yml file in the profiles_dir directory. Then run the dbt
    CLI shell command.

    command: The dbt command to be executed.
    profiles_dir: The directory to search for the profiles.yml file. If this is not set,
        will try using the DBT_PROFILES_DIR environment variable, but if that's also not
        set, will use the default directory `$HOME/.dbt/`.
    overwrite_profiles: Whether the existing profiles.yml file under profiles_dir
        should be overwritten with a new profile.
    dbt_cli_credentials: Credentials class containing the profile written to profiles.yml.
        Note! This has no effect if profiles.yml already exists under profile_dir and
        overwrite_profiles is set to False.
    set_dbt_profiles_dir_env_var: Whether DBT_PROFILES_DIR should be set to the
        utilized profiles_dir.
    shell_run_command_kwargs: Additional keyword arguments to pass to
        [shell_run_command](https://prefecthq.github.io/prefect-shell/commands/#prefect_shell.commands.shell_run_command).

    Returns:
        If return_all (default is False) is passed to shell_run_command_kwargs,
        returns all lines as a list; else the last line as a string.
    """  # noqa
    # check if variable is set, if not check env, if not use expected default
    logger = get_run_logger()
    if profiles_dir is None:
        profiles_dir = os.getenv("DBT_PROFILES_DIR", Path.home() / ".dbt")
    profiles_dir = Path(profiles_dir)
    logger.debug(f"Using this profiles directory: {profiles_dir}")

    # https://docs.getdbt.com/dbt-cli/configure-your-profile
    # Note that the file always needs to be called profiles.yml,
    # regardless of which directory it is in.
    profiles_path = profiles_dir / "profiles.yml"

    if overwrite_profiles or not profiles_path.exists():
        if dbt_cli_credentials is None:
            raise ValueError("dbt_cli_credentials must be set for writing profiles!")
        profile = dbt_cli_credentials.get_profile()
        profiles_dir.mkdir(exist_ok=True)
        with open(profiles_path, "w+") as f:
            yaml.dump(profile, f, default_flow_style=False)
    elif dbt_cli_credentials is not None:
        logger.warning(
            f"Since overwrite_profiles is False and profiles_path ({profiles_path}) "
            f"already exists, the profile within dbt_cli_credentials was NOT used"
        )

    if set_dbt_profiles_dir_env_var:
        os.environ["DBT_PROFILES_DIR"] = profiles_dir

    logger.info(f"Running dbt command: {command}")
    shell_run_command_kwargs = shell_run_command_kwargs or {}
    return shell_run_command(command=command, **shell_run_command_kwargs)
