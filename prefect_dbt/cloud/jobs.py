"""Module containing tasks and flows for interacting with dbt Cloud"""
from typing import Optional

from httpx import Response
from prefect import task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions


@task
async def trigger_job_run(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    options: Optional[TriggerJobRunOptions] = None,
) -> Response:
    """
    A task to trigger a dbt Cloud job run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        job_id: The ID of the job to trigger.
        options: An optional TriggerJobRunOptions instance to specify overrides
            for the triggered job run.

    Returns:
        The response from the dbt Cloud administrative API.
    """
    async with dbt_cloud_credentials.get_administrative_client() as client:
        response = await client.trigger_job_run(job_id=job_id, options=options)

    return response
