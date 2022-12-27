"""Module containing credentials for interacting with dbt Cloud"""
from typing import Literal

from prefect.blocks.abstract import CredentialsBlock
from pydantic import Field, SecretStr

from prefect_dbt.cloud.clients import (
    DbtCloudAdministrativeClient,
    DbtCloudMetadataClient,
)


class DbtCloudCredentials(CredentialsBlock):
    """
    Credentials block for credential use across dbt Cloud tasks and flows.

    Attributes:
        api_key (SecretStr): API key to authenticate with the dbt Cloud
            administrative API. Refer to the [Authentication docs](
            https://docs.getdbt.com/dbt-cloud/api-v2#section/Authentication)
            for retrieving the API key.
        account_id (int): ID of dbt Cloud account with which to interact.
        domain (Optional[str]): Domain at which the dbt Cloud API is hosted.

    Examples:
        Load stored dbt Cloud credentials:
        ```python
        from prefect_dbt.cloud import DbtCloudCredentials

        dbt_cloud_credentials = DbtCloudCredentials.load("BLOCK_NAME")
        ```

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
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/9a001902ed43a84c6c96d23b24622e19/dbt-bit_tm.png?h=250"  # noqa

    api_key: SecretStr = Field(
        default=...,
        title="API Key",
        description="A dbt Cloud API key to use for authentication.",
    )
    account_id: int = Field(
        default=..., title="Account ID", description="The ID of your dbt Cloud account."
    )
    domain: str = Field(
        default="cloud.getdbt.com",
        description="The base domain of your dbt Cloud instance.",
    )

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

    def get_metadata_client(self):
        """
        Returns a newly instantiated client for working with the dbt Cloud
        metadata API.
        """
        return DbtCloudMetadataClient(
            api_key=self.api_key.get_secret_value(),
            domain=f"metadata.{self.domain}",
        )

    def get_client(self, client_type: Literal["administrative", "meatadata"]):
        """
        Returns a newly instantiated client for working with the dbt Cloud API.
        """
        get_client_method = getattr(self, f"get_{client_type}_client", None)
        if get_client_method is None:
            raise ValueError(f"'{client_type}' is not a supported client type.")
        return get_client_method()
