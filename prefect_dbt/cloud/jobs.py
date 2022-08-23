"""Module containing tasks and flows for interacting with dbt Cloud jobs"""
from typing import Dict, Optional

from httpx import HTTPStatusError
from prefect import flow, get_run_logger, task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.runs import (
    DbtCloudJobRunCancelled,
    DbtCloudJobRunFailed,
    DbtCloudJobRunStatus,
    DbtCloudListRunArtifactsFailed,
    list_dbt_cloud_run_artifacts,
    wait_for_dbt_cloud_job_run,
)
from prefect_dbt.cloud.utils import extract_user_message


class DbtCloudJobRunTriggerFailed(Exception):
    """Raised when a dbt Cloud job trigger fails"""

    pass


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

    Examples:
        Trigger a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run


        @flow
        def trigger_dbt_cloud_job_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            trigger_dbt_cloud_job_run(dbt_cloud_credentials=credentials, job_id=1)


        trigger_dbt_cloud_job_run_flow()
        ```

        Trigger a dbt Cloud job run with overrides:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run
        from prefect_dbt.cloud.models import TriggerJobRunOptions


        @flow
        def trigger_dbt_cloud_job_run_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            trigger_dbt_cloud_job_run(
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


        trigger_dbt_cloud_job_run()
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


@task(
    name="Get dbt Cloud job run ID",
    description="Extracts the run ID from a trigger job run API response",
)
def get_run_id(obj: Dict):
    """
    Task that extracts the run ID from a trigger job run API response,

    This task is mainly used to maintain dependency tracking between the
    `trigger_dbt_cloud_job_run` task and downstream tasks/flows that use the run ID.

    Args:
        obj: The JSON body from the trigger job run response.
    """
    id = obj.get("id")
    if id is None:
        raise RuntimeError("Unable to determine run ID for triggered job.")
    return id


@flow(
    name="Trigger dbt Cloud job run and wait for completion",
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

    Raises:
        DbtCloudJobRunCancelled: The triggered dbt Cloud job run was cancelled.
        DbtCloudJobRunFailed: The triggered dbt Cloud job run failed.
        RuntimeError: The triggered dbt Cloud job run ended in an unexpected state.

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

    triggered_run_data_future = await trigger_dbt_cloud_job_run.submit(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
        options=trigger_job_run_options,
    )
    run_id_future = get_run_id.submit(triggered_run_data_future)

    final_run_status, run_data = await wait_for_dbt_cloud_job_run(
        run_id=run_id_future,
        dbt_cloud_credentials=dbt_cloud_credentials,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=poll_frequency_seconds,
    )

    if final_run_status == DbtCloudJobRunStatus.SUCCESS:
        try:
            list_run_artifacts_future = await list_dbt_cloud_run_artifacts.submit(
                dbt_cloud_credentials=dbt_cloud_credentials,
                run_id=run_id_future,
            )
            run_data["artifact_paths"] = await list_run_artifacts_future.result()
        except DbtCloudListRunArtifactsFailed as ex:
            logger.warning(
                "Unable to retrieve artifacts for job run with ID %s. Reason: %s",
                run_id_future.result(),
                ex,
            )
        logger.info(
            "dbt Cloud job run with ID %s completed successfully!",
            run_id_future.result(),
        )
        return run_data
    elif final_run_status == DbtCloudJobRunStatus.CANCELLED:
        raise DbtCloudJobRunCancelled(
            f"Triggered job run with ID {run_id_future.result()} was cancelled."
        )
    elif final_run_status == DbtCloudJobRunStatus.FAILED:
        raise DbtCloudJobRunFailed(
            f"Triggered job run with ID: {run_id_future.result()} failed."
        )
    else:
        raise RuntimeError(
            f"Triggered job run with ID: {run_id_future.result()} ended with unexpected"
            f"status {final_run_status.value}."
        )
