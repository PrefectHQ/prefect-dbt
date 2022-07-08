"""Utilities for common interactions with the dbt Cloud API"""
from json import JSONDecodeError
from typing import Any, Dict, Optional

from httpx import HTTPStatusError
from prefect import task

from prefect_dbt.cloud.credentials import DbtCloudCredentials


def extract_user_message(ex: HTTPStatusError) -> Optional[str]:
    """
    Extracts user message from a error response from the dbt Cloud administrative API.

    Args:
        ex: An HTTPStatusError raised by httpx

    Returns:
        user_message from dbt Cloud administrative API response or None if a
        user_message cannot be extracted
    """
    response_payload = ex.response.json()
    status = response_payload.get("status", {})
    return status.get("user_message")


@task(
    name="Call dbt Cloud administrative API endpoint",
    description="Calls a dbt Cloud administrative API endpoint",
    retries=3,
    retry_delay_seconds=10,
)
async def call_dbt_cloud_administrative_api_endpoint(
    dbt_cloud_credentials: DbtCloudCredentials,
    path: str,
    http_method: str,
    params: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Task that calls a specified endpoint in the dbt Cloud administrative API. Use this
    task if a prebuilt one is not yet available.

    Args:
        dbt_cloud_credentials: Credentials for authenticating with dbt Cloud.
        path: The partial path for the request (e.g. /projects/). Will be appended
            onto the base URL as determined by the client configuration.
        http_method: HTTP method to call on the endpoint.
        params: Query parameters to include in the request.
        json: JSON serializable body to send in the request.

    Returns:
        The body of the response. If the body is JSON serializable, then the result of
            `json.loads` with the body as the input will be returned. Otherwise, the
            body will be returned directly.

    Example:
        List projects for an account:
        ```python
        from prefect import flow

        from prefect_dbt.cloud import DbtCloudCredentials
        from prefect_dbt.cloud.utils import call_dbt_cloud_administrative_api_endpoint

        @flow
        def get_projects_flow():
            credentials = DbtCloudCredentials(api_key="my_api_key", account_id=123456789)

            future = call_dbt_cloud_administrative_api_endpoint(
                dbt_cloud_credentials=credentials,
                path="/projects/",
                http_method="get",
            )
            return future.result()["data"]
        ```
    """  # noqa
    async with dbt_cloud_credentials.get_administrative_client() as client:
        response = await client.call_endpoint(
            http_method=http_method, path=path, params=params, json=json
        )
        try:
            return response.json()
        except JSONDecodeError:
            return response.text
