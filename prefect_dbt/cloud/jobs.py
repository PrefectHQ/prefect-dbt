"""Module containing tasks and flows for interacting with dbt Cloud jobs"""
import asyncio
from enum import Enum
from typing import Dict, Optional

from httpx import HTTPStatusError
from prefect import flow, get_run_logger, task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.runs import (
    DbtCloudListRunArtifactsFailed,
    get_dbt_cloud_run_info,
    list_dbt_cloud_run_artifacts,
)
from prefect_dbt.cloud.utils import extract_user_message


class DbtCloudJobRunTriggerFailed(Exception):
    """Raised when a dbt Cloud job trigger fails"""

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

    @classmethod
    def is_terminal_status_code(cls, status_code: int) -> bool:
        """
        Returns True if a status code is terminal for a job run.
        Return False otherwise.
        """
        return status_code in [cls.SUCCESS.value, cls.FAILED.value, cls.CANCELLED.value]


@task(
    name="Trigger dbt Cloud job run",
    description="Triggers a dbt Cloud job run for the job "
    "with the given job_id and optional overrides.",
    retries=3,
    retry_delay_seconds=10,
)
async def trigger_dbt_cloud_job_run(
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
    logger = get_run_logger()

    logger.info(f"Triggering run for job with ID {job_id}")

    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.trigger_job_run(job_id=job_id, options=options)
    except HTTPStatusError as ex:
        raise DbtCloudJobRunTriggerFailed(extract_user_message(ex)) from ex

    run_data = response.json()["data"]

    logger.info(
        f"Run successfully triggered for job with ID {job_id}. "
        "You can view the status of this run at "
        f"https://{dbt_cloud_credentials.domain}/#/accounts/"
        f"{dbt_cloud_credentials.account_id}/projects/{run_data['project_id']}/"
        f"runs/{run_data['id']}/"
    )

    return run_data


@flow(
    name="Trigger dbt Cloud job run and wait for completions",
    description="Triggers a dbt Cloud job run and waits for the"
    "triggered run to complete.",
)
async def trigger_dbt_cloud_job_run_and_wait_for_completion(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
    max_wait_seconds: int = 900,
    poll_frequency_seconds: int = 10,
) -> Dict:
    """
    Flow that triggers a job run and waits for the triggered run to complete.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        job_id: The ID of the job to trigger.
        trigger_job_run_options: An optional TriggerJobRunOptions instance to
            specify overrides for the triggered job run.
        max_wait_seconds: Maximum number of seconds to wait for job to complete
        poll_frequency_seconds: Number of seconds to wait in between checks for
            run completion.

    Returns:
        The run data returned by the dbt Cloud administrative API.

    Examples:
        Trigger a dbt Cloud job and wait for completion as a stand alone flow:
        ```python
        import asyncio

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion

        asyncio.run(
            trigger_dbt_cloud_job_run_and_wait_for_completion(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                ),
                job_id=1
            )
        )
        ```

        Trigger a dbt Cloud job and wait for completion as a sub-flow:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion

        @flow
        def my_flow():
            ...
            run_result = trigger_dbt_cloud_job_run_and_wait_for_completion(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                ),
                job_id=1
            )
            ...

        my_flow()
        ```

        Trigger a dbt Cloud job with overrides:
        ```python
        import asyncio

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion
        from prefect_dbt.cloud.models import TriggerJobRunOptions

        asyncio.run(
            trigger_dbt_cloud_job_run_and_wait_for_completion(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                ),
                job_id=1,
                trigger_job_run_options=TriggerJobRunOptions(
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
        )
        ```

    """  # noqa
    logger = get_run_logger()

    trigger_job_run_future = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
        options=trigger_job_run_options,
    )
    triggered_run_data = await trigger_job_run_future.result()
    run_id = triggered_run_data["id"]

    seconds_waited_for_run_completion = 0
    while seconds_waited_for_run_completion <= max_wait_seconds:
        get_run_future = await get_dbt_cloud_run_info(
            dbt_cloud_credentials=dbt_cloud_credentials,
            run_id=run_id,
            wait_for=[trigger_job_run_future],
        )
        run_data = await get_run_future.result()
        run_status_code = run_data.get("status")

        if run_status_code == DbtCloudJobRunStatus.SUCCESS.value:
            try:
                list_run_artifacts_state = await list_dbt_cloud_run_artifacts(
                    dbt_cloud_credentials=dbt_cloud_credentials, run_id=run_id
                )
                run_data["artifact_paths"] = await list_run_artifacts_state.result()
                return run_data
            except DbtCloudListRunArtifactsFailed as ex:
                logger.warning(
                    "Unable to retrieve artifacts for job run with ID %s. Reason: %s",
                    run_id,
                    ex,
                )

            return run_data
        elif run_status_code == DbtCloudJobRunStatus.FAILED.value:
            raise DbtCloudJobRunFailed(f"Triggered job run with ID: {run_id} failed.")
        elif run_status_code == DbtCloudJobRunStatus.CANCELLED.value:
            raise DbtCloudJobRunCancelled(
                f"Triggered job run with ID {run_id} was cancelled."
            )

        logger.info(
            "dbt Cloud job run with ID %i has status %s. Waiting for %i seconds.",
            run_id,
            DbtCloudJobRunStatus(run_status_code).name,
            poll_frequency_seconds,
        )
        await asyncio.sleep(poll_frequency_seconds)
        seconds_waited_for_run_completion += poll_frequency_seconds

    raise DbtCloudJobRunTimedOut(
        f"Max wait time of {max_wait_seconds} seconds exceeded while waiting "
        "for job run with ID {run_id}"
    )
