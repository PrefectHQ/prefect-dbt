"""Module containing credentials for interacting with dbt Cloud"""
from dataclasses import dataclass

from prefect_dbt.cloud.clients import DbtCloudAdministrativeClient


@dataclass
class DbtCloudCredentials:
    """
    Credentials class for credential use across dbt Cloud tasks and flows.

    Args:
        api_key: API key to authenticate with the dbt Cloud administrative API.
        account_id: ID of dbt Cloud account with which to interact.
        domain: Domain at which the dbt Cloud API is hosted.

    Examples:
        Use DbtCloudCredentials instance to trigger a job run:
        ```python
        from prefect_dbt.cloud import DbtCloudCredentials

        credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

        async with dbt_cloud_credentials.get_administrative_client() as client:
            client.trigger_job_run(job_id=1)
        ```
    """

    api_key: str
    account_id: int
    domain: str = "cloud.getdbt.com"

    def get_administrative_client(self):
        """
        Returns a newly instantiated client for working with the dbt Cloud
        administrative API.
        """
        return DbtCloudAdministrativeClient(
            api_key=self.api_key, account_id=self.account_id, domain=self.domain
        )
