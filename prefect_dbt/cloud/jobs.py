"""Module containing tasks and flows for interacting with dbt Cloud jobs"""
from typing import Any, Dict, List, Optional

from httpx import HTTPStatusError
from prefect import flow, get_run_logger, task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.runs import (
    DbtCloudJobRunCancelled,
    DbtCloudJobRunFailed,
    DbtCloudJobRunStatus,
    DbtCloudListRunArtifactsFailed,
    get_dbt_cloud_run_artifact,
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

    Example:
        ```python
        from prefect import flow
        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, get_run_id


        @flow
        def trigger_run_and_get_id():
            dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key",
                    account_id=123456789
                )

            triggered_run_data = trigger_dbt_cloud_job_run(
                dbt_cloud_credentials=dbt_cloud_credentials,
                job_id=job_id,
                options=trigger_job_run_options,
            )
            run_id = get_run_id.submit(triggered_run_data)
            return run_id

        trigger_run_and_get_id()
        ```
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
    retry_filtered_models_attempts: int = 3,
    retry_status_filters: List[str] = ["error", "fail"],
    retry_downstream_nodes: bool = True,
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
        retry_filtered_models_attempts: Number of times to retry models selected by `retry_status_filters`.
        retry_status_filters: A list of statuses to filter the models by.
        retry_downstream_nodes: Whether to also retry nodes downstream of the filtered models.

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
    run_id = (await triggered_run_data_future.result()).get("id")
    if id is None:
        raise RuntimeError("Unable to determine run ID for triggered job.")

    final_run_status, run_data = await wait_for_dbt_cloud_job_run(
        run_id=run_id,
        dbt_cloud_credentials=dbt_cloud_credentials,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=poll_frequency_seconds,
    )

    if final_run_status == DbtCloudJobRunStatus.SUCCESS:
        try:
            list_run_artifacts_future = await list_dbt_cloud_run_artifacts.submit(
                dbt_cloud_credentials=dbt_cloud_credentials,
                run_id=run_id,
            )
            run_data["artifact_paths"] = await list_run_artifacts_future.result()
        except DbtCloudListRunArtifactsFailed as ex:
            logger.warning(
                "Unable to retrieve artifacts for job run with ID %s. Reason: %s",
                run_id,
                ex,
            )
        logger.info(
            "dbt Cloud job run with ID %s completed successfully!",
            run_id,
        )
        return run_data
    elif final_run_status == DbtCloudJobRunStatus.CANCELLED:
        raise DbtCloudJobRunCancelled(
            f"Triggered job run with ID {run_id} was cancelled."
        )
    elif final_run_status == DbtCloudJobRunStatus.FAILED:
        while retry_filtered_models_attempts > 0:
            logger.info(
                f"Retrying job run with ID: {run_id} "
                f"{retry_filtered_models_attempts} more times"
            )
            try:
                retry_filtered_models_attempts -= 1
                run_data = await (
                    retry_dbt_cloud_job_run_subset_and_wait_for_completion(
                        dbt_cloud_credentials=dbt_cloud_credentials,
                        run_id=run_id,
                        trigger_job_run_options=trigger_job_run_options,
                        max_wait_seconds=max_wait_seconds,
                        poll_frequency_seconds=poll_frequency_seconds,
                        retry_status_filters=retry_status_filters,
                        retry_downstream_nodes=retry_downstream_nodes,
                    )
                )
                return run_data
            except Exception:
                pass
        else:
            raise DbtCloudJobRunFailed(f"Triggered job run with ID: {run_id} failed.")
    else:
        raise RuntimeError(
            f"Triggered job run with ID: {run_id} ended with unexpected"
            f"status {final_run_status.value}."
        )


def _filter_model_names_by_status(
    run_artifact: Dict[str, Any], retry_status_filters: List[str]
):
    """
    Filters model names from model results by status.
    """
    model_results = run_artifact.get("results", [])

    model_names = set()
    for model_result in model_results:
        if model_result.get("status") in retry_status_filters:
            model_id = model_result["unique_id"]
            model_name = model_id.split(".")[-1]
            model_names.add(model_name)

    if len(model_names) == 0:
        raise ValueError(
            f"No valid model names were found using the filters: {retry_status_filters}"
        )
    return model_names


def _build_trigger_job_run_options(
    retry_downstream_nodes: bool,
    model_names: List[str],
    trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
):
    """ """
    graph_operator = "+ " if retry_downstream_nodes else " "
    retry_command = f"dbt build --select {graph_operator.join(model_names)}"
    if retry_downstream_nodes:
        # Convert: ...stg_customers+ stg_payments
        # To: ...stg_payments+ stg_orders+
        retry_command += graph_operator.rstrip()

    if trigger_job_run_options is None:
        trigger_job_run_options_override = TriggerJobRunOptions(
            steps_override=[retry_command]
        )
    else:
        trigger_job_run_options_override = trigger_job_run_options.copy()
        trigger_job_run_options_override.steps_override = [retry_command]
    return trigger_job_run_options_override


@flow(
    name="Retry subset of dbt Cloud job run and wait for completion",
    description=(
        "Retries a subset of dbt Cloud job run, filtered by select statuses, "
        "and waits for the triggered retry to complete."
    ),
)
async def retry_dbt_cloud_job_run_subset_and_wait_for_completion(
    dbt_cloud_credentials: DbtCloudCredentials,
    run_id: int,
    trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
    max_wait_seconds: int = 900,
    poll_frequency_seconds: int = 10,
    retry_status_filters: List[str] = ["error", "fail"],
    retry_downstream_nodes: bool = True,
) -> Dict:
    """
    Flow that retrys a subset of dbt Cloud job run, filtered by select statuses,
    and waits for the triggered retry to complete.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        trigger_job_run_options: An optional TriggerJobRunOptions instance to
            specify overrides for the triggered job run.
        max_wait_seconds: Maximum number of seconds to wait for job to complete
        poll_frequency_seconds: Number of seconds to wait in between checks for
            run completion.
        run_id: The ID of the job run to retry.
        retry_status_filters: A list of statuses to filter the models by.
        retry_downstream_nodes: Whether to also retry nodes downstream of the filtered models.

    Raises:
        ValueError: If `trigger_job_run_options.steps_override` is set by the user.

    Returns:
        The run data returned by the dbt Cloud administrative API.

    Examples:
        Retry a subset of models in a dbt Cloud job run and wait for completion:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import retry_dbt_cloud_job_run_subset_and_wait_for_completion

        @flow
        def retry_dbt_cloud_job_run_subset_and_wait_for_completion_flow():
            credentials = DbtCloudCredentials.load("MY_BLOCK_NAME")
            retry_dbt_cloud_job_run_subset_and_wait_for_completion(
                dbt_cloud_credentials=credentials,
                run_id=88640123,
                retry_status_filters=("error", "fail", "warn"),
                retry_downstream_nodes=True,
            )

        retry_dbt_cloud_job_run_subset_and_wait_for_completion_flow()
        ```
    """  # noqa
    if trigger_job_run_options.steps_override is not None:
        raise ValueError(
            "Do not set `steps_override` in `trigger_job_run_options` "
            "because this flow will automatically set it"
        )

    run_artifact = await get_dbt_cloud_run_artifact.submit(
        dbt_cloud_credentials=dbt_cloud_credentials,
        run_id=run_id,
        path="run_results.json",
    )
    model_names = _filter_model_names_by_status(
        run_artifact=run_artifact, retry_status_filters=retry_status_filters
    )
    trigger_job_run_options_override = _build_trigger_job_run_options(
        retry_downstream_nodes=retry_downstream_nodes,
        model_names=model_names,
        trigger_job_run_options=trigger_job_run_options,
    )

    job_id = run_artifact["metadata"]["env"]["DBT_CLOUD_JOB_ID"]
    run_data = trigger_dbt_cloud_job_run_and_wait_for_completion(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
        retry_filtered_models_attempts=0,
        trigger_job_run_options=trigger_job_run_options_override,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=poll_frequency_seconds,
    )
    return run_data
