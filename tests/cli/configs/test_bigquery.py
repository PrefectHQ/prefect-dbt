import json

import pytest
from prefect_gcp.credentials import GcpCredentials

from prefect_dbt.cli.configs import BigQueryTargetConfigs


@pytest.fixture()
def service_account_file(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "google.auth.crypt._cryptography_rsa.serialization.load_pem_private_key",
        lambda *args, **kwargs: args[0],
    )
    _service_account_file = tmp_path / "gcp.json"
    with open(_service_account_file, "w") as f:
        json.dump(
            {
                "client_email": "my_email",
                "token_uri": "my_token_uri",
                "private_key": "my_key",
                "project_id": "my_project",
            },
            f,
        )
    return _service_account_file


@pytest.mark.parametrize("project_in_target_configs", [True, False])
def test_gcp_target_configs_get_configs(
    service_account_file, project_in_target_configs
):
    configs_kwargs = dict(schema="schema")
    credentials_kwargs = dict(service_account_file=service_account_file)

    if project_in_target_configs:
        configs_kwargs["project"] = "my_project"
    else:
        credentials_kwargs["project"] = "my_project"

    gcp_credentials = GcpCredentials(**credentials_kwargs)
    configs_kwargs["credentials"] = gcp_credentials

    configs = BigQueryTargetConfigs(**configs_kwargs)
    actual = configs.get_configs()
    expected = {
        "type": "bigquery",
        "schema": "schema",
        "threads": 4,
        "project": "my_project",
        "method": "service-account",
        "keyfile": str(service_account_file),
    }
    assert actual == expected


@pytest.fixture()
def service_account_info_dict(monkeypatch):
    monkeypatch.setattr(
        "google.auth.crypt._cryptography_rsa.serialization.load_pem_private_key",
        lambda *args, **kwargs: args[0],
    )
    _service_account_info = {
        "project_id": "my_project",
        "token_uri": "my-token-uri",
        "client_email": "my-client-email",
        "private_key": "my-private-key",
    }
    return _service_account_info


def test_gcp_target_configs_get_configs_service_account_info(service_account_info_dict):
    gcp_credentials = GcpCredentials(service_account_info=service_account_info_dict)
    configs = BigQueryTargetConfigs(
        credentials=gcp_credentials, project="my_project", schema="my_schema"
    )
    actual = configs.get_configs()
    expected = {
        "type": "bigquery",
        "schema": "my_schema",
        "threads": 4,
        "project": "my_project",
        "method": "service-account-json",
        "keyfile_json": service_account_info_dict,
    }
    assert actual == expected
