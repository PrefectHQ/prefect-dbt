import pytest

from prefect_dbt.cli.credentials import DbtCliProfile


def test_dbt_cli_profile_init():
    target_configs = dict(type="snowflake")
    dbt_cli_profile = DbtCliProfile(
        name="test_name", target="dev", target_configs=target_configs
    )
    assert dbt_cli_profile.name == "test_name"
    assert dbt_cli_profile.target == "dev"
    assert dbt_cli_profile.target_configs == {"type": "snowflake"}


def test_dbt_cli_profile_init_validation_failed():
    with pytest.raises(
        ValueError, match="A `type` must be specified in `target_configs`"
    ):
        DbtCliProfile(name="test_name", target="dev", target_configs={})


def test_dbt_cli_profile_get_profile():
    target_configs = dict(type="snowflake")
    global_configs = dict(use_colors=False)
    dbt_cli_profile = DbtCliProfile(
        name="test_name",
        target="dev",
        target_configs=target_configs,
        global_configs=global_configs,
    )
    actual = dbt_cli_profile.get_profile()
    expected = {
        "config": global_configs,
        "test_name": {
            "target": "dev",
            "outputs": {"dev": target_configs},
        },
    }
    assert actual == expected
