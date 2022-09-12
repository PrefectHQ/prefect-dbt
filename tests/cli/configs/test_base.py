import pytest

from prefect_dbt.cli.configs.base import TargetConfigs


def test_target_configs_get_configs():
    target_configs = TargetConfigs(
        type="snowflake",
        schema="schema_input",
        threads=5,
        extras={"extra_input": 1, "null_input": None},
        _is_anonymous=False,
    )
    assert hasattr(target_configs, "_is_anonymous")
    # get_configs ignore private attrs
    assert target_configs.get_configs() == dict(
        type="snowflake", schema="schema_input", threads=5, extra_input=1
    )


def test_target_configs_get_configs_duplicate_keys():
    with pytest.raises(ValueError, match="The keyword, schema"):
        target_configs = TargetConfigs(
            type="snowflake",
            schema="schema_input",
            threads=5,
            extras={"extra_input": 1, "schema": "something else"},
        )
        target_configs.get_configs()
