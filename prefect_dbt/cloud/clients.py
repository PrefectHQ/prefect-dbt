"""Module containing clients for interacting with the dbt Cloud API"""
from typing import Optional

import prefect
from httpx import AsyncClient, Response

from prefect_dbt.cloud.models import TriggerJobRunOptions


class DbtCloudAdministrativeClient:
    """
    Client for interacting with the dbt cloud Administrative API.

    Args:
        api_key: API key to authenticate with the dbt Cloud administrative API.
        account_id: ID of dbt Cloud account with which to interact.
        domain: Domain at which the dbt Cloud API is hosted.
    """

    def __init__(self, api_key: str, account_id: int, domain: str):
        self._closed = False
        self._started = False

        self._admin_client = AsyncClient(
            headers={
                "Authorization": f"Bearer {api_key}",
                "user-agent": f"prefect-{prefect.__version__}",
            },
            base_url=f"https://{domain}/api/v2/accounts/{account_id}",
        )

    async def trigger_job_run(
        self, job_id: int, options: Optional[TriggerJobRunOptions] = None
    ) -> Response:
        """
        Sends a request to the [trigger job run endpoint](https://docs.getdbt.com/dbt-cloud/api-v2#tag/Jobs/operation/triggerRun)
        to initiate a job run.

        Args:
            job_id: The ID of the job to trigger.
            options: An optional TriggerJobRunOptions instance to specify overrides for the triggered job run.

        Returns:
            The response from the dbt Cloud administrative API.
        """  # noqa
        if options is None:
            options = TriggerJobRunOptions()

        response = await self._admin_client.post(
            url=f"/jobs/{job_id}/run/",
            json=options.dict(exclude_none=True),
        )

        response.raise_for_status()

        return response

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError(
                "The client cannot be started again after it has been closed."
            )
        if self._started:
            raise RuntimeError("The client cannot be started more than once.")

        self._started = True

        return self

    async def __aexit__(self, *exc):
        self._closed = True
        await self._admin_client.__aexit__()
