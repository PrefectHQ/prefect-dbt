import pytest

from prefect_dbt.cloud.credentials import DbtCloudCredentials


@pytest.fixture
def dbt_cloud_credentials():
    return DbtCloudCredentials(api_key="my_api_key", account_id=123456789)
