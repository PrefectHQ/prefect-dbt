import json

import pytest
import respx
from httpx import Response
from prefect import flow

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.jobs import JobRunTriggerFailed, trigger_job_run
from prefect_dbt.cloud.models import TriggerJobRunOptions


class TestTriggerJobRun:
    @respx.mock(assert_all_called=True)
    async def test_trigger_job_with_no_options(self, respx_mock):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": {"id": 10000}}))
        response = await trigger_job_run.fn(
            dbt_cloud_credentials=DbtCloudCredentials(
                api_key="my_api_key", account_id=123456789
            ),
            job_id=1,
        )
        assert response.status_code == 200
        assert response.json() == {"data": {"id": 10000}}

    @respx.mock(assert_all_called=True)
    async def test_trigger_job_within_flow(self, respx_mock: respx.MockRouter):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": {"id": 10000}}))

        @flow
        def test_flow():
            return trigger_job_run(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key", account_id=123456789
                ),
                job_id=1,
            )

        flow_state = test_flow()
        task_state = flow_state.result()
        result = task_state.result()
        assert result.status_code == 200
        assert result.json() == {"data": {"id": 10000}}
        request_body = json.loads(respx_mock.calls.last.request.content.decode())
        assert "Triggered via Prefect in task run" in request_body["cause"]

    @respx.mock(assert_all_called=True)
    async def test_trigger_with_custom_options(self, respx_mock):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
            json={
                "cause": "Triggered via Prefect",
                "git_branch": "staging",
                "schema_override": "dbt_cloud_pr_123",
                "dbt_version_override": "0.18.0",
                "threads_override": 8,
                "target_name_override": "staging",
                "generate_docs_override": True,
                "timeout_seconds_override": 3000,
                "steps_override": [
                    "dbt seed",
                    "dbt run --fail-fast",
                    "dbt test --fail fast",
                ],
            },
        ).mock(return_value=Response(200, json={"data": {"id": 10000}}))
        response = await trigger_job_run.fn(
            dbt_cloud_credentials=DbtCloudCredentials(
                api_key="my_api_key", account_id=123456789
            ),
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
        assert response.status_code == 200
        assert response.json() == {"data": {"id": 10000}}

    @respx.mock(assert_all_called=True)
    async def test_trigger_nonexistent_job(self, respx_mock):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
            json={"cause": "Triggered via Prefect"},
        ).mock(
            return_value=Response(404, json={"status": {"user_message": "Not found!"}})
        )
        with pytest.raises(JobRunTriggerFailed, match="Not found!"):
            await trigger_job_run.fn(
                dbt_cloud_credentials=DbtCloudCredentials(
                    api_key="my_api_key", account_id=123456789
                ),
                job_id=1,
            )
