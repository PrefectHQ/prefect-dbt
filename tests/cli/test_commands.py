import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from prefect import flow

from prefect_dbt.cli.commands import trigger_dbt_cli_command


async def mock_shell_run_command_fn(**kwargs):
    return kwargs


@pytest.fixture(autouse=True)
def mock_shell_run_command(monkeypatch):
    _mock_shell_run_command = MagicMock(fn=mock_shell_run_command_fn)
    monkeypatch.setattr(
        "prefect_dbt.cli.commands.shell_run_command", _mock_shell_run_command
    )


@pytest.fixture
def profiles_dir(tmp_path):
    return tmp_path / ".dbt"


def test_trigger_dbt_cli_command_not_dbt():
    @flow
    def test_flow():
        return trigger_dbt_cli_command("ls")

    with pytest.raises(ValueError, match="Command is not a valid dbt sub-command"):
        test_flow()


def test_trigger_dbt_cli_command(profiles_dir, dbt_cli_profile_bare):
    @flow
    def test_flow():
        return trigger_dbt_cli_command(
            "dbt ls", profiles_dir=profiles_dir, dbt_cli_profile=dbt_cli_profile_bare
        )

    result = test_flow()
    assert result == {"command": f"dbt ls --profiles-dir {profiles_dir}"}


def test_trigger_dbt_cli_command_run_twice_overwrite(
    profiles_dir, dbt_cli_profile, dbt_cli_profile_bare
):
    @flow
    def test_flow():
        trigger_dbt_cli_command(
            "dbt ls", profiles_dir=profiles_dir, dbt_cli_profile=dbt_cli_profile
        )
        run_two = trigger_dbt_cli_command(
            "dbt ls",
            profiles_dir=profiles_dir,
            dbt_cli_profile=dbt_cli_profile_bare,
            overwrite_profiles=True,
        )
        return run_two

    result = test_flow()
    assert result == {"command": f"dbt ls --profiles-dir {profiles_dir}"}
    with open(profiles_dir / "profiles.yml", "r") as f:
        actual = yaml.safe_load(f)
    expected = {
        "config": {},
        "prefecto": {
            "target": "testing",
            "outputs": {
                "testing": {
                    "type": "custom",
                    "schema": "my_schema",
                    "threads": 4,
                    "account": "fake",
                }
            },
        },
    }
    assert actual == expected


def test_trigger_dbt_cli_command_run_twice_exists(
    profiles_dir, dbt_cli_profile, dbt_cli_profile_bare
):
    @flow
    def test_flow():
        trigger_dbt_cli_command(
            "dbt ls", profiles_dir=profiles_dir, dbt_cli_profile=dbt_cli_profile
        )
        run_two = trigger_dbt_cli_command(
            "dbt ls",
            profiles_dir=profiles_dir,
            dbt_cli_profile=dbt_cli_profile_bare,
        )
        return run_two

    with pytest.raises(ValueError, match="Since overwrite_profiles is False"):
        test_flow()


def test_trigger_dbt_cli_command_missing_profile(profiles_dir):
    @flow
    def test_flow():
        return trigger_dbt_cli_command(
            "dbt ls",
            profiles_dir=profiles_dir,
        )

    with pytest.raises(
        ValueError, match="Provide `dbt_cli_profile` keyword for writing profiles"
    ):
        test_flow()


def test_trigger_dbt_cli_command_find_home(dbt_cli_profile_bare):
    home_dbt_dir = Path.home() / ".dbt"
    if (home_dbt_dir / "profiles.yml").exists():
        dbt_cli_profile = None
    else:
        dbt_cli_profile = dbt_cli_profile_bare

    @flow
    def test_flow():
        return trigger_dbt_cli_command(
            "dbt ls", dbt_cli_profile=dbt_cli_profile, overwrite_profiles=False
        )

    result = test_flow()
    assert result == {"command": f"dbt ls --profiles-dir {home_dbt_dir}"}


def test_trigger_dbt_cli_command_find_env(profiles_dir, dbt_cli_profile_bare):
    @flow
    def test_flow():
        return trigger_dbt_cli_command("dbt ls", dbt_cli_profile=dbt_cli_profile_bare)

    os.environ["DBT_PROFILES_DIR"] = str(profiles_dir)
    result = test_flow()
    assert result == {"command": f"dbt ls --profiles-dir {profiles_dir}"}


def test_trigger_dbt_cli_command_project_dir(profiles_dir, dbt_cli_profile_bare):
    @flow
    def test_flow():
        return trigger_dbt_cli_command(
            "dbt ls",
            profiles_dir=profiles_dir,
            project_dir="project",
            dbt_cli_profile=dbt_cli_profile_bare,
        )

    result = test_flow()
    assert result == {
        "command": f"dbt ls --profiles-dir {profiles_dir} --project-dir project"
    }


def test_trigger_dbt_cli_command_shell_kwargs(profiles_dir, dbt_cli_profile_bare):
    @flow
    def test_flow():
        return trigger_dbt_cli_command(
            "dbt ls",
            return_all=True,
            profiles_dir=profiles_dir,
            dbt_cli_profile=dbt_cli_profile_bare,
        )

    result = test_flow()
    assert result == {
        "command": f"dbt ls --profiles-dir {profiles_dir}",
        "return_all": True,
    }
