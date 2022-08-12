import pytest
from prefect_gcp.credentials import GcpCredentials

from prefect_dbt.cli.configs import BigQueryTargetConfigs


@pytest.mark.parametrize("project_in_target_configs", [True, False])
def test_gcp_target_configs_get_configs(project_in_target_configs):
    configs_kwargs = dict(schema="schema")
    credentials_kwargs = dict(service_account_file="/path/to/file.json")
    if project_in_target_configs:
        configs_kwargs["project"] = "project"
    else:
        credentials_kwargs["project"] = "project"

    gcp_credentials = GcpCredentials(**credentials_kwargs)
    configs_kwargs["credentials"] = gcp_credentials

    configs = BigQueryTargetConfigs(**configs_kwargs)
    actual = configs.get_configs()
    expected = {
        "type": "bigquery",
        "schema": "schema",
        "threads": 4,
        "project": "project",
        "method": "service-account",
        "keyfile": "/path/to/file.json",
    }
    assert actual == expected


def test_gcp_target_configs_get_configs_missing_schema():
    gcp_credentials = GcpCredentials(service_account_file="service_account_file")
    configs = BigQueryTargetConfigs(credentials=gcp_credentials, schema="schema")
    with pytest.raises(ValueError, match="The keyword, project"):
        configs.get_configs()


def test_gcp_target_configs_get_configs_duplicate_project():
    gcp_credentials = GcpCredentials(
        service_account_file="service_account_file", project="project"
    )
    configs = BigQueryTargetConfigs(
        credentials=gcp_credentials, schema="schema", project="project"
    )
    with pytest.raises(ValueError, match="The keyword, project"):
        configs.get_configs()
