"""Module containing tasks and flows for interacting with dbt Cloud"""
import asyncio
from enum import Enum
from typing import Dict, Optional

from httpx import HTTPStatusError
from prefect import flow, task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.utils import extract_user_message


class DbtCloudJobRunTriggerFailed(Exception):
    """Raised when a dbt Cloud job trigger fails"""

    pass


class DbtCloudGetRunFailed(Exception):
    """Raised when unable to retrieve dbt Cloud run"""

    pass


class DbtCloudJobRunFailed(Exception):
    """Raised when a triggered job run fails"""

    pass


class DbtCloudJobRunCancelled(Exception):
    """Raised when a triggered job run is cancelled"""

    pass


class DbtCloudJobRunTimedOut(Exception):
    """
    Raised when a triggered job run does not complete in the configured max
    wait seconds
    """

    pass


class DbtCloudJobRunStatus(Enum):
    """dbt Cloud Job statuses."""

    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    FAILED = 20
    CANCELLED = 30
    TERMINAL_STATUSES = (SUCCESS, FAILED, CANCELLED)

    @classmethod
    def is_terminal_status_code(cls, status_code: int) -> bool:
        """
        Returns True if a status code is terminal for a job run.
        Return False otherwise.
        """
        return status_code in cls.TERMINAL_STATUSES.value


@task(
    name="Trigger dbt Cloud job run",
    description="Triggers a dbt Cloud job run for the job "
    "with the given job_id and optional overrides.",
    retries=3,
    retry_delay_seconds=10,
)
async def trigger_job_run(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    options: Optional[TriggerJobRunOptions] = None,
) -> Dict:
    """
    A task to trigger a dbt Cloud job run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        job_id: The ID of the job to trigger.
        options: An optional TriggerJobRunOptions instance to specify overrides
            for the triggered job run.

    Returns:
        The run data returned from the dbt Cloud administrative API.

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
        raise DbtCloudJobRunTriggerFailed(extract_user_message(ex)) from ex
    return response.json()["data"]


@task(
    name="Get dbt Cloud job run details",
    description="Retrieves details of a dbt Cloud job run "
    "for the run with the given run_id.",
    retries=3,
    retry_delay_seconds=10,
)
async def get_run(dbt_cloud_credentials: DbtCloudCredentials, run_id: int) -> Dict:
    """
    A task to retrieve information about a dbt Cloud job run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the job to trigger.

    Returns:
        The run data returned by the dbt Cloud administrative API.

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
        raise DbtCloudGetRunFailed(extract_user_message(ex)) from ex
    return response.json()["data"]


@flow
async def trigger_job_run_and_wait_for_completion(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
    max_wait_seconds: Optional[int] = None,
    poll_frequency_seconds: int = 10,
):
    """
    Flow that triggers a job run and waits for the triggered run to complete.
    """
    trigger_job_run_state = await trigger_job_run(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
        options=trigger_job_run_options,
    )
    triggered_run_data = await trigger_job_run_state.result()
    run_id = triggered_run_data["id"]

    seconds_waited_for_run_completion = 0
    while not max_wait_seconds or seconds_waited_for_run_completion <= max_wait_seconds:
        get_run_state = await get_run(
            dbt_cloud_credentials=dbt_cloud_credentials, run_id=run_id
        )
        run_data = await get_run_state.result()
        run_status_code = run_data.get("status")
        if DbtCloudJobRunStatus.is_terminal_status_code(run_status_code):
            if run_status_code == DbtCloudJobRunStatus.SUCCESS.value:
                return run_data
            elif run_status_code == DbtCloudJobRunStatus.FAILED.value:
                raise DbtCloudJobRunFailed(
                    f"Triggered job run with ID: {run_id} failed."
                )
            elif run_status_code == DbtCloudJobRunStatus.CANCELLED.value:
                raise DbtCloudJobRunCancelled(
                    f"Triggered job run with ID: {run_id} was cancelled."
                )

        await asyncio.sleep(poll_frequency_seconds)
        seconds_waited_for_run_completion += poll_frequency_seconds

    raise DbtCloudJobRunTimedOut(
        f"Max wait time exceeded while waiting for job run with ID: {run_id}"
    )
