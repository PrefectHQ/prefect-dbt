"""Module containing credentials for interacting with dbt Cloud"""
from prefect.blocks.core import Block
from pydantic import SecretStr

from prefect_dbt.cloud.clients import DbtCloudAdministrativeClient


class DbtCloudCredentials(Block):
    """
    Credentials block for credential use across dbt Cloud tasks and flows.

    Args:
        api_key (SecretStr): API key to authenticate with the dbt Cloud
            administrative API. Refer to the [Authentication docs](
            https://docs.getdbt.com/dbt-cloud/api-v2#section/Authentication)
            for retrieving the API key.
        account_id (int): ID of dbt Cloud account with which to interact.
        domain (Optional[str]): Domain at which the dbt Cloud API is hosted.

    Examples:
        Use DbtCloudCredentials instance to trigger a job run:
        ```python
        from prefect_dbt.cloud import DbtCloudCredentials

        credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

        async with dbt_cloud_credentials.get_administrative_client() as client:
            client.trigger_job_run(job_id=1)
        ```

        Load saved dbt Cloud credentials within a flow:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run


        @flow
        def trigger_dbt_cloud_job_run_flow():
            credentials = DbtCloudCredentials.load("my-dbt-credentials")

            trigger_dbt_cloud_job_run(dbt_cloud_credentials=credentials, job_id=1)


        trigger_dbt_cloud_job_run_flow()
        ```
    """

    _block_type_name = "dbt Cloud Credentials"
    _logo_url = "https://asset.brandfetch.io/idofJOT4bu/idxrwTdDC-.svg"
    _code_example = """/
    ```python
        from prefect_dbt.cloud import DbtCloudCredentials

        dbt_cloud_credentials = DbtCloudCredentials.load("BLOCK_NAME")
    ```"""

    api_key: SecretStr
    account_id: int
    domain: str = "cloud.getdbt.com"

    def get_administrative_client(self):
        """
        Returns a newly instantiated client for working with the dbt Cloud
        administrative API.
        """
        return DbtCloudAdministrativeClient(
            api_key=self.api_key.get_secret_value(),
            account_id=self.account_id,
            domain=self.domain,
        )
