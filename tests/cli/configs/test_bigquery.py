import json
from unittest.mock import MagicMock, seal

import pytest
from prefect_gcp.credentials import GcpCredentials

from prefect_dbt.cli.configs import BigQueryTargetConfigs


@pytest.fixture()
def service_account_info_dict(monkeypatch):
    monkeypatch.setattr(
        "google.auth.crypt._cryptography_rsa.serialization.load_pem_private_key",
        lambda *args, **kwargs: args[0],
    )
    _service_account_info = {
        "project_id": "service_project",
        "token_uri": "my-token-uri",
        "client_email": "my-client-email",
        "private_key": "my-private-key",
    }
    return _service_account_info


@pytest.fixture()
def service_account_file(monkeypatch, tmp_path, service_account_info_dict):
    monkeypatch.setattr(
        "google.auth.crypt._cryptography_rsa.serialization.load_pem_private_key",
        lambda *args, **kwargs: args[0],
    )
    _service_account_file = tmp_path / "gcp.json"
    with open(_service_account_file, "w") as f:
        json.dump(service_account_info_dict, f)
    return _service_account_file


@pytest.fixture
def google_auth(monkeypatch):
    google_auth_mock = MagicMock(name="google_auth")
    default_credentials_mock = MagicMock(
        name="default_credentials",
        quota_project_id="my_project",
    )
    google_auth_mock.default.side_effect = lambda *args, **kwargs: (
        default_credentials_mock,
        None,
    )
    monkeypatch.setattr("google.auth", google_auth_mock)
    return google_auth_mock


class TestBigQueryTargetConfigs:
    def test_get_configs_service_account_file(self, service_account_file):
        gcp_credentials = GcpCredentials(service_account_file=service_account_file)
        configs = BigQueryTargetConfigs(
            credentials=gcp_credentials, project="my_project", schema="my_schema"
        )
        actual = configs.get_configs()
        expected = {
            "type": "bigquery",
            "schema": "my_schema",
            "threads": 4,
            "project": "my_project",
            "method": "service-account",
            "keyfile": str(service_account_file),
        }
        assert actual == expected

    def test_get_configs_service_account_info(self, service_account_info_dict):
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

    def test_get_configs_service_account_info_extras(self, service_account_info_dict):
        gcp_credentials = GcpCredentials(service_account_info=service_account_info_dict)
        configs = BigQueryTargetConfigs(
            credentials=gcp_credentials,
            project="my_project",
            schema="my_schema",
            extras={"execution_project": "my_exe_project"},
        )
        actual = configs.get_configs()
        expected = {
            "type": "bigquery",
            "schema": "my_schema",
            "threads": 4,
            "project": "my_project",
            "execution_project": "my_exe_project",
            "method": "service-account-json",
            "keyfile_json": service_account_info_dict,
        }
        assert actual == expected

    def test_get_configs_gcloud_cli_refresh_token(self, google_auth):
        gcp_credentials = GcpCredentials()
        configs = BigQueryTargetConfigs(
            credentials=gcp_credentials, project="my_project", schema="my_schema"
        )
        google_credentials = MagicMock(
            refresh_token="my_refresh_token",
            token_uri="my_token_uri",
            client_id="my_client_id",
            client_secret="my_client_secret",
        )
        seal(google_credentials)
        gcp_credentials.get_credentials_from_service_account = (
            lambda: google_credentials
        )
        actual = configs.get_configs()
        expected = {
            "method": "oauth-secrets",
            "type": "bigquery",
            "schema": "my_schema",
            "threads": 4,
            "project": "my_project",
            "refresh_token": "my_refresh_token",
            "token_uri": "my_token_uri",
            "client_id": "my_client_id",
            "client_secret": "my_client_secret",
        }
        assert actual == expected

    def test_get_configs_gcloud_cli_temporary_token(self, google_auth):
        gcp_credentials = GcpCredentials()
        configs = BigQueryTargetConfigs(
            credentials=gcp_credentials, project="my_project", schema="my_schema"
        )
        google_credentials = MagicMock(
            token="my_token", refresh=lambda *args, **kwargs: "refreshed"
        )
        seal(google_credentials)
        gcp_credentials.get_credentials_from_service_account = (
            lambda: google_credentials
        )
        actual = configs.get_configs()
        expected = {
            "method": "oauth-secrets",
            "type": "bigquery",
            "schema": "my_schema",
            "threads": 4,
            "project": "my_project",
            "token": "my_token",
        }
        assert actual == expected

    def test_get_configs_project_from_service_account_file(self, service_account_file):
        gcp_credentials = GcpCredentials(service_account_file=service_account_file)
        configs = BigQueryTargetConfigs(credentials=gcp_credentials, schema="schema")
        actual = configs.get_configs()
        assert actual["project"] == "service_project"

    def test_get_configs_project_from_credentials(self, service_account_file):
        gcp_credentials = GcpCredentials(
            service_account_file=service_account_file, project="credentials_project"
        )
        configs = BigQueryTargetConfigs(credentials=gcp_credentials, schema="schema")
        actual = configs.get_configs()
        assert actual["project"] == "credentials_project"

    def test_get_configs_project_from_target_configs(self, service_account_file):
        gcp_credentials = GcpCredentials(
            service_account_file=service_account_file, project="credentials_project"
        )
        configs = BigQueryTargetConfigs(
            credentials=gcp_credentials, schema="schema", project="configs_project"
        )
        actual = configs.get_configs()
        assert actual["project"] == "configs_project"
