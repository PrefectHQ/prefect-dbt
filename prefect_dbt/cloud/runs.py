"""Module containing tasks and flows for interacting with dbt Cloud job runs"""
from typing import Dict, List, Optional, Union

from httpx import HTTPStatusError
from prefect import task

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.utils import extract_user_message


class DbtCloudGetRunFailed(Exception):
    """Raised when unable to retrieve dbt Cloud run"""

    pass


class DbtCloudListRunArtifactsFailed(Exception):
    """Raised when unable to list dbt Cloud run artifacts"""

    pass


class DbtCloudGetRunArtifactFailed(Exception):
    """Raised when unable to get a dbt Cloud run artifact"""

    pass


@task(
    name="Get dbt Cloud job run details",
    description="Retrieves details of a dbt Cloud job run "
    "for the run with the given run_id.",
    retries=3,
    retry_delay_seconds=10,
)
async def get_dbt_cloud_run_info(
    dbt_cloud_credentials: DbtCloudCredentials, run_id: int
) -> Dict:
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


@task(
    name="List dbt Cloud job artifacts",
    description="Fetches a list of artifact files generated for a completed run.",
    retries=3,
    retry_delay_seconds=10,
)
async def list_dbt_cloud_run_artifacts(
    dbt_cloud_credentials: DbtCloudCredentials, run_id: int, step: Optional[int] = None
) -> List[str]:
    """
    A task to list the artifact files generated for a completed run.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the run to list run artifacts for.
        step: The index of the step in the run to query for artifacts. The
            first step in the run has the index 1. If the step parameter is
            omitted, then this method will return the artifacts compiled
            for the last step in the run.

    Returns:
        A list of paths to artifact files that can be used to retrieve the generated artifacts.

    Example:
        List artifacts of a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import list_dbt_cloud_run_artifacts

        @flow
        def list_artifacts_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return list_dbt_cloud_run_artifacts(
                dbt_cloud_credentials=credentials,
                run_id=42
            )

        list_artifacts_flow()
        ```
    """  # noqa
    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.list_run_artifacts(run_id=run_id, step=step)
    except HTTPStatusError as ex:
        raise DbtCloudListRunArtifactsFailed(extract_user_message(ex)) from ex
    return response.json()["data"]


@task(
    name="Get dbt Cloud job artifact",
    description="Fetches an artifact from a completed run.",
    retries=3,
    retry_delay_seconds=10,
)
async def get_dbt_cloud_run_artifact(
    dbt_cloud_credentials: DbtCloudCredentials,
    run_id: int,
    path: str,
    step: Optional[int] = None,
) -> Union[Dict, str]:
    """
    A task to get an artifact generated for a completed run. The requested artifact
    is saved to a file in the current working directory.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        run_id: The ID of the run to list run artifacts for.
        path: The relative path to the run artifact (e.g. manifest.json, catalog.json,
            run_results.json)
        step: The index of the step in the run to query for artifacts. The
            first step in the run has the index 1. If the step parameter is
            omitted, then this method will return the artifacts compiled
            for the last step in the run.

    Returns:
        The contents of the requested manifest. Returns a `Dict` if the
            requested artifact is a JSON file and a `str` otherwise.

    Examples:
        Get an artifact of a dbt Cloud job run:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.runs import get_dbt_cloud_run_artifact

        @flow
        def get_artifact_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            return get_dbt_cloud_run_artifact(
                dbt_cloud_credentials=credentials,
                run_id=42,
                path="manifest.json"
            )

        get_artifact_flow()
        ```

        Get an artifact of a dbt Cloud job run and write it to a file:
        ```python
        import json

        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.jobs import get_dbt_cloud_run_artifact

        @flow
        def get_artifact_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            get_run_artifact_future = get_dbt_cloud_run_artifact(
                dbt_cloud_credentials=credentials,
                run_id=42,
                path="manifest.json"
            )

            with open("manifest.json", "w") as file:
                json.dump(get_run_artifact_future.result(), file)

        get_artifact_flow()
        ```
    """  # noqa

    try:
        async with dbt_cloud_credentials.get_administrative_client() as client:
            response = await client.get_run_artifact(
                run_id=run_id, path=path, step=step
            )
    except HTTPStatusError as ex:
        raise DbtCloudGetRunArtifactFailed(extract_user_message(ex)) from ex

    if path.endswith(".json"):
        artifact_contents = response.json()
    else:
        artifact_contents = response.text

    return artifact_contents
