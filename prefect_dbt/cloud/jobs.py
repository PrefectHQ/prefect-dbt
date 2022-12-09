"""Module containing tasks and flows for interacting with dbt Cloud jobs"""
import asyncio
import shlex
import time
from json import JSONDecodeError
from typing import Any, Dict, Optional

from httpx import HTTPStatusError
from prefect import flow, get_run_logger, task
from prefect.blocks.core import Block
from prefect.context import FlowRunContext
from prefect.exceptions import MissingContextError
from prefect.logging import get_logger
from prefect.utilities.asyncutils import sync_compatible

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.models import TriggerJobRunOptions
from prefect_dbt.cloud.runs import (
    DbtCloudGetRunFailed,
    DbtCloudJobRunCancelled,
    DbtCloudJobRunFailed,
    DbtCloudJobRunStatus,
    DbtCloudJobRunTimedOut,
    DbtCloudListRunArtifactsFailed,
    get_dbt_cloud_run_artifact,
    get_dbt_cloud_run_info,
)
from prefect_dbt.cloud.utils import extract_user_message

EXE_COMMANDS = ("build", "run", "test", "seed", "snapshot")


class DbtCloudJobRunTriggerFailed(Exception):
    """Raised when a dbt Cloud job trigger fails."""


class DbtCloudGetJobFailed(Exception):
    """Raised when unable to retrieve dbt Cloud job."""


class DbtCloudJobRunIsRunning(Exception):
    """Raised when dbt Cloud job run is still running."""


@task(
    name="Get dbt Cloud job details",
    description="Retrieves details of a dbt Cloud job "
    "for the job with the given job_id.",
    retries=3,
    retry_delay_seconds=10,
)
async def get_dbt_cloud_job_info(
    dbt_cloud_credentials: DbtCloudCredentials,
    job_id: int,
    order_by: Optional[str] = None,
) -> Dict:
    """
    A task to retrieve information about a dbt Cloud job.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        job_id: The ID of the job to get.

    Returns:
        The job data returned by the dbt Cloud administrative API.

    Example:
        Get status of a dbt Cloud job:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import get_job

        @flow
        def get_job_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return get_job(
                dbt_cloud_credentials=credentials,
                job_id=42
            )

        get_job_flow()
        ```
    """  # noqa
    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.get_job(
                job_id=job_id,
                order_by=order_by,
            )
    except HTTPStatusError as ex:
        raise DbtCloudGetJobFailed(extract_user_message(ex)) from ex
    return response.json()["data"]


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

    if "project_id" in run_data and "id" in run_data:
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


async def _build_trigger_job_run_options(
    dbt_cloud_credentials: DbtCloudCredentials,
    trigger_job_run_options: TriggerJobRunOptions,
    run_id: str,
    run_info: Dict[str, Any],
    job_info: Dict[str, Any],
):
    """
    Compiles a list of steps (commands) to retry, then either build trigger job
    run options from scratch if it does not exist, else overrides the existing.
    """
    generate_docs = job_info.get("generate_docs", False)
    generate_sources = job_info.get("generate_sources", False)

    steps_override = []
    for run_step in run_info["run_steps"]:
        status = run_step["status_humanized"].lower()
        # Skipping cloning, profile setup, and dbt deps - always the first three
        # steps in any run, and note, index starts at 1 instead of 0
        if run_step["index"] <= 3 or status == "success":
            continue
        # get dbt build from "Invoke dbt with `dbt build`"
        command = run_step["name"].partition("`")[2].partition("`")[0]

        # These steps will be re-run regardless if
        # generate_docs or generate_sources are enabled for a given job
        # so if we don't skip, it'll run twice
        freshness_in_command = (
            "dbt source snapshot-freshness" in command
            or "dbt source freshness" in command
        )
        if "dbt docs generate" in command and generate_docs:
            continue
        elif freshness_in_command and generate_sources:
            continue

        # find an executable command like `build` or `run`
        # search in a list so that there aren't false positives, like
        # `"run" in "dbt run-operation"`, which is True; we actually want
        # `"run" in ["dbt", "run-operation"]` which is False
        command_components = shlex.split(command)
        for exe_command in EXE_COMMANDS:
            if exe_command in command_components:
                break
        else:
            exe_command = ""

        is_exe_command = exe_command in EXE_COMMANDS
        is_not_success = status in ("error", "skipped", "cancelled")
        is_skipped = status == "skipped"
        if (not is_exe_command and is_not_success) or (is_exe_command and is_skipped):
            # if no matches like `run-operation`, we will be rerunning entirely
            # or if it's one of the expected commands and is skipped
            steps_override.append(command)
        else:
            # errors and failures are when we need to inspect to figure
            # out the point of failure
            try:
                run_artifact_future = await get_dbt_cloud_run_artifact.with_options(
                    retries=0, retry_delay_seconds=0
                ).submit(
                    dbt_cloud_credentials=dbt_cloud_credentials,
                    run_id=run_id,
                    path="run_results.json",
                    step=run_step["index"],
                )
                run_artifact = await run_artifact_future.result()
            except JSONDecodeError:
                # get the run results scoped to the step which had an error
                # an error here indicates that either:
                # 1) the fail-fast flag was set, in which case
                #    the run_results.json file was never created; or
                # 2) there was a problem on dbt Cloud's side saving
                #    this artifact
                steps_override.append(command)
            else:
                # we only need to find the individual nodes for those run commands
                run_results = run_artifact["results"]
                # select nodes that were not successful
                # note "fail" here instead of "cancelled" because
                # nodes do not have a cancelled state
                run_nodes = " ".join(
                    run_result["unique_id"].split(".")[2]
                    for run_result in run_results
                    if run_result["status"] in ("error", "skipped", "fail")
                )

                select_arg = None
                if "-s" in command_components:
                    select_arg = "-s"
                elif "--select" in command_components:
                    select_arg = "--select"

                # prevent duplicate --select/-s statements
                if select_arg is not None:
                    # dbt --fail-fast run, -s, bad_mod --vars '{"env": "prod"}' to:
                    # dbt --fail-fast run -s other_mod bad_mod --vars '{"env": "prod"}'
                    command_start, select_arg, command_end = command.partition(
                        select_arg
                    )
                    modified_command = (
                        f"{command_start} {select_arg} {run_nodes} {command_end}"
                    )
                else:
                    # dbt --fail-fast, build, --vars '{"env": "prod"}' to:
                    # dbt --fail-fast build --select bad_model --vars '{"env": "prod"}'
                    dbt_global_args, exe_command, exe_args = command.partition(
                        exe_command
                    )
                    modified_command = (
                        f"{dbt_global_args} {exe_command} -s {run_nodes} {exe_args}"
                    )
                steps_override.append(modified_command)

    if trigger_job_run_options is None:
        trigger_job_run_options_override = TriggerJobRunOptions(
            steps_override=steps_override
        )
    else:
        trigger_job_run_options_override = trigger_job_run_options.copy()
        trigger_job_run_options_override.steps_override = steps_override
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
            )

        retry_dbt_cloud_job_run_subset_and_wait_for_completion_flow()
        ```
    """  # noqa
    if trigger_job_run_options and trigger_job_run_options.steps_override is not None:
        raise ValueError(
            "Do not set `steps_override` in `trigger_job_run_options` "
            "because this flow will automatically set it"
        )

    run_info_future = await get_dbt_cloud_run_info.submit(
        dbt_cloud_credentials=dbt_cloud_credentials,
        run_id=run_id,
        include_related=["run_steps"],
    )
    run_info = await run_info_future.result()

    job_id = run_info["job_id"]
    job_info_future = await get_dbt_cloud_job_info.submit(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
    )
    job_info = await job_info_future.result()

    trigger_job_run_options_override = await _build_trigger_job_run_options(
        dbt_cloud_credentials=dbt_cloud_credentials,
        trigger_job_run_options=trigger_job_run_options,
        run_id=run_id,
        run_info=run_info,
        job_info=job_info,
    )

    # to circumvent `RuntimeError: The task runner is already started!`
    flow_run_context = FlowRunContext.get()
    task_runner_type = type(flow_run_context.task_runner)

    run_data = await trigger_dbt_cloud_job_run_and_wait_for_completion.with_options(
        task_runner=task_runner_type()
    )(
        dbt_cloud_credentials=dbt_cloud_credentials,
        job_id=job_id,
        retry_filtered_models_attempts=0,
        trigger_job_run_options=trigger_job_run_options_override,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=poll_frequency_seconds,
    )
    return run_data


class DbtCloudJob(Block):
    """
    placeholder
    """

    _block_schema_capabilities = [
        "trigger-job",
    ]

    dbt_cloud_credentials: DbtCloudCredentials
    job_id: int
    max_wait_seconds: int = 900
    poll_frequency_seconds: int = 10

    @property
    def logger(self):
        """
        placeholder
        """
        try:
            return get_run_logger()
        except MissingContextError:
            return get_logger()

    @sync_compatible
    async def trigger(
        self, trigger_job_run_options: Optional[TriggerJobRunOptions] = None
    ):
        """
        placeholder
        """
        try:
            async with self.dbt_cloud_credentials.get_administrative_client() as client:
                response = await client.trigger_job_run(
                    job_id=self.job_id, options=trigger_job_run_options
                )
                self.logger.info(f"Triggered job {self.job_id!r}")
        except HTTPStatusError as ex:
            raise DbtCloudJobRunTriggerFailed(extract_user_message(ex)) from ex

        run_data = response.json()["data"]
        run_id = run_data.get("id")
        run = DbtCloudJobRun(
            dbt_cloud_credentials=self.dbt_cloud_credentials,
            run_id=run_id,
            poll_frequency_seconds=self.poll_frequency_seconds,
            max_wait_seconds=self.max_wait_seconds,
        )
        return run

    @sync_compatible
    async def get_job(self, order_by: Optional[str] = None):
        """
        placeholder
        """
        try:
            async with self.dbt_cloud_credentials.get_administrative_client() as client:
                response = await client.get_job(
                    job_id=self.job_id,
                    order_by=order_by,
                )
        except HTTPStatusError as ex:
            raise DbtCloudGetJobFailed(extract_user_message(ex)) from ex
        return response.json()["data"]

    async def trigger_retry(
        self,
        run_id: int,
        trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
    ):
        """
        placeholder
        """
        run_info = await self.get_run(run_id=run_id)
        job_info = await self.get_job()

        trigger_job_run_options_override = await _build_trigger_job_run_options(
            dbt_cloud_credentials=self.dbt_cloud_credentials,
            trigger_job_run_options=trigger_job_run_options,
            run_id=run_id,
            run_info=run_info,
            job_info=job_info,
        )

        run = await self.trigger(
            trigger_job_run_options=trigger_job_run_options_override,
        )
        return run


class DbtCloudJobRun(object):  # NOT A BLOCK
    """
    placeholder
    """

    def __init__(
        self,
        dbt_cloud_credentials: DbtCloudCredentials,
        run_id: int,
        max_wait_seconds: int,
        poll_frequency_seconds: int,
    ):
        self.dbt_cloud_credentials = dbt_cloud_credentials
        self.run_id = run_id
        self.max_wait_seconds = max_wait_seconds
        self.poll_frequency_seconds = poll_frequency_seconds

    @property
    def logger(self):
        """
        placeholder
        """
        try:
            return get_run_logger()
        except MissingContextError:
            return get_logger()

    @sync_compatible
    async def get_run(self, run_id):
        """
        placeholder
        """
        try:
            async with self.dbt_cloud_credentials.get_administrative_client() as client:
                response = await client.get_run(run_id=run_id)
        except HTTPStatusError as ex:
            raise DbtCloudGetRunFailed(extract_user_message(ex)) from ex
        run_data = response.json()["data"]
        return run_data

    @sync_compatible
    async def get_status(self, run_id) -> int:
        """
        placeholder
        """
        run_data = await self.get_run(run_id)
        run_status_code = run_data.get("status")
        return run_status_code

    def status_is_terminal(self, run_status_code) -> bool:
        return DbtCloudJobRunStatus.is_terminal_status_code(run_status_code)

    @sync_compatible
    async def wait_for_completion(
        self,
        max_wait_seconds: int = 900,
        poll_frequency_seconds: int = 10,
    ):
        """
        placeholder
        """
        start_time = time.time()
        last_run_status_code = run_status_code = None
        while not self.status_is_terminal(run_status_code):
            # get status info
            run_status_code = await self.get_status(self.run_id)
            if run_status_code != last_run_status_code:
                self.logger.info(
                    "dbt Cloud job run with ID %i has new status %s.",
                    self.run_id,
                    DbtCloudJobRunStatus(run_status_code).name,
                )
                last_run_status_code = run_status_code
            # check for timeout
            elapsed_time_seconds = time.time() - start_time
            if elapsed_time_seconds > max_wait_seconds:
                raise DbtCloudJobRunTimedOut(
                    f"Max wait time of {max_wait_seconds} seconds exceeded "
                    "while waiting for job run with ID {self.run_id}"
                )
            await asyncio.sleep(poll_frequency_seconds)

    @sync_compatible
    async def fetch_results(self, step: Optional[int] = None):
        """
        placeholder
        """
        run_data = await self.get_run(self.run_id)
        run_status = DbtCloudJobRunStatus(run_data.get("status"))
        if run_status == DbtCloudJobRunStatus.SUCCESS:
            try:
                async with self.dbt_cloud_credentials.get_administrative_client() as client:  # noqa
                    response = await client.list_run_artifacts(
                        run_id=self.run_id, step=step
                    )
                run_data["artifact_paths"] = response.json()["data"]
                self.logger.info(
                    "dbt Cloud job run with ID %s completed successfully!",
                    self.run_id,
                )
            except HTTPStatusError as ex:
                raise DbtCloudListRunArtifactsFailed(extract_user_message(ex)) from ex
            return run_data
        elif run_status == DbtCloudJobRunStatus.CANCELLED:
            raise DbtCloudJobRunCancelled(
                f"Triggered job run with ID {self.run_id} was cancelled."
            )
        elif run_status == DbtCloudJobRunStatus.FAILED:
            raise DbtCloudJobRunFailed(
                f"Triggered job run with ID: {self.run_id} failed."
            )
        else:
            raise DbtCloudJobRunIsRunning(
                f"Triggered job run with ID: {self.run_id} is still running; "
                "use wait_for_completion() to wait for completion before calling fetch."
            )


@flow(
    name="Trigger dbt Cloud job run and wait for completion",
    description="Triggers a dbt Cloud job run and waits for the"
    "triggered run to complete.",
)
async def trigger_dbt_cloud_job_run_and_wait_for_completion(
    dbt_cloud_job: DbtCloudJob,
    trigger_job_run_options: Optional[TriggerJobRunOptions] = None,
    retry_filtered_models_attempts: int = 3,  # TODO: implement this
) -> Dict:
    """
    Flow that triggers a job run and waits for the triggered run to complete.
    """  # noqa
    logger = get_run_logger()

    run = await dbt_cloud_job.trigger(trigger_job_run_options=trigger_job_run_options)
    await run.wait_for_completion()
    while retry_filtered_models_attempts > 0:
        try:
            result = await run.fetch_results()
            return result
        except DbtCloudJobRunFailed:
            logger.info(
                f"Retrying job run with ID: {run.run_id} "
                f"{retry_filtered_models_attempts} more times"
            )
            run_id = run.run_id
            run = await dbt_cloud_job.trigger_retry(
                run_id=run_id, trigger_job_run_options=trigger_job_run_options
            )
            retry_filtered_models_attempts -= 1
