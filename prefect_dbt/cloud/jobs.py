"""Module containing tasks and flows for interacting with dbt Cloud"""
from typing import Optional

from httpx import HTTPStatusError, Response
from prefect import task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.utils import extract_user_message


class JobRunTriggerFailed(Exception):
    """Raised when a dbt Cloud job trigger fails"""

    pass


class GetRunFailed(Exception):
    """Raised when unable to retrieve dbt Cloud run"""

    pass


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

    Example:
        Trigger a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_job_run


        @flow
        def trigger_job_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            trigger_job_run(dbt_cloud_credentials=credentials, job_id=1)


        trigger_job_run_flow()
        ```

        Trigger a dbt Cloud job run with overrides:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_job_run
        from prefect_dbt.cloud.models import TriggerJobRunOptions


        @flow
        def trigger_job_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            trigger_job_run(
                dbt_cloud_credentials=credentials,
                job_id=1,
                options=TriggerJobRunOptions(
                    git_branch="staging",
                    schema_override="dbt_cloud_pr_123",
                    dbt_version_override="0.18.0",
                    target_name_override="staging",
                    timeout_seconds_override=3000,
                    generate_docs_override=True,
                    threads_override=8,
                    steps_override=[
                        "dbt seed",
                        "dbt run --fail-fast",
                        "dbt test --fail fast",
                    ],
                ),
            )


        trigger_job_run_flow()
        ```
    """  # noqa
    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.trigger_job_run(job_id=job_id, options=options)
    except HTTPStatusError as ex:
        raise JobRunTriggerFailed(extract_user_message(ex)) from ex
    return response


@task
async def get_run(dbt_cloud_credentials: DbtCloudCredentials, run_id: int):
    """
    A task to retrieve information about a dbt Cloud job run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the job to trigger.

    Returns:
        The response from the dbt Cloud administrative API.

    Example:
        Get status of a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import get_run

        @flow
        def get_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return get_run(
                dbt_cloud_credentials=credentials,
                run_id=42
            )

        get_run_flow()
        ```
    """  # noqa
    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.get_run(run_id=run_id)
    except HTTPStatusError as ex:
        raise GetRunFailed(extract_user_message(ex)) from ex
    return response
