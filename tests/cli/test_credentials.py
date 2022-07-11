import pytest
from pydantic.error_wrappers import ValidationError

from prefect_dbt.cli.credentials import DbtCliProfile, GlobalConfigs, TargetConfigs


@pytest.mark.parametrize("configs_type", ["dict", "model"])
def test_dbt_cli_profile_init(configs_type):
    target_configs = dict(type="snowflake", schema="schema")
    global_configs = dict(use_colors=False)
    if configs_type == "model":
        target_configs = TargetConfigs.parse_obj(target_configs)
        global_configs = GlobalConfigs.parse_obj(global_configs)

    dbt_cli_profile = DbtCliProfile(
        name="test_name",
        target="dev",
        target_configs=target_configs,
        global_configs=global_configs,
    )
    assert dbt_cli_profile.name == "test_name"
    assert dbt_cli_profile.target == "dev"
    assert dbt_cli_profile.target_configs == {
        "type": "snowflake",
        "schema": "schema",
        "threads": 4,
    }
    assert dbt_cli_profile.global_configs == {"use_colors": False}


def test_dbt_cli_profile_init_validation_failed():
    with pytest.raises(ValidationError, match="2 validation errors for TargetConfigs"):
        DbtCliProfile(name="test_name", target="dev", target_configs={"field": "abc"})


def test_dbt_cli_profile_get_profile():
    target_configs = dict(type="snowflake", schema="analysis")
    global_configs = dict(use_colors=False)
    dbt_cli_profile = DbtCliProfile(
        name="test_name",
        target="dev",
        target_configs=target_configs,
        global_configs=global_configs,
    )
    actual = dbt_cli_profile.get_profile()
    expected_target_configs = target_configs.copy()
    expected_target_configs["threads"] = 4
    expected = {
        "config": global_configs,
        "test_name": {
            "target": "dev",
            "outputs": {"dev": expected_target_configs},
        },
    }
    assert actual == expected
