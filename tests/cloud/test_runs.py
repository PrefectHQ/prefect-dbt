import pytest
from httpx import Response

from prefect_dbt.cloud.runs import (
    DbtCloudGetRunFailed,
    DbtCloudListRunArtifactsFailed,
    get_dbt_cloud_run_info,
    list_dbt_cloud_run_artifacts,
)


class TestGetDbtCloudRunInfo:
    async def test_get_dbt_cloud_run_info(self, respx_mock, dbt_cloud_credentials):
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/12/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": {"id": 10000}}))

        response = await get_dbt_cloud_run_info.fn(
            dbt_cloud_credentials=dbt_cloud_credentials,
            run_id=12,
        )

        assert response == {"id": 10000}

    async def test_get_nonexistent_run(self, respx_mock, dbt_cloud_credentials):
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/12/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(404, json={"status": {"user_message": "Not found!"}})
        )
        with pytest.raises(DbtCloudGetRunFailed, match="Not found!"):
            await get_dbt_cloud_run_info.fn(
                dbt_cloud_credentials=dbt_cloud_credentials,
                run_id=12,
            )


class TestDbtCloudListRunArtifacts:
    async def test_list_artifacts_success(self, respx_mock, dbt_cloud_credentials):
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/12/artifacts/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": ["manifest.json"]}))

        response = await list_dbt_cloud_run_artifacts.fn(
            dbt_cloud_credentials=dbt_cloud_credentials,
            run_id=12,
        )

        assert response == ["manifest.json"]

    async def test_list_artifacts_with_step(self, respx_mock, dbt_cloud_credentials):
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/12/artifacts/?step=1",  # noqa
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": ["manifest.json"]}))

        response = await list_dbt_cloud_run_artifacts.fn(
            dbt_cloud_credentials=dbt_cloud_credentials, run_id=12, step=1
        )

        assert response == ["manifest.json"]

    async def test_list_artifacts_failure(self, respx_mock, dbt_cloud_credentials):
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/12/artifacts/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                500, json={"status": {"user_message": "This is what went wrong"}}
            )
        )
        with pytest.raises(
            DbtCloudListRunArtifactsFailed, match="This is what went wrong"
        ):
            await list_dbt_cloud_run_artifacts.fn(
                dbt_cloud_credentials=dbt_cloud_credentials,
                run_id=12,
            )
