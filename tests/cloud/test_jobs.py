import json
import os

import pytest
from httpx import Response
from prefect import flow

from prefect_dbt.cloud.credentials import DbtCloudCredentials
from prefect_dbt.cloud.jobs import (
    DbtCloudGetRunFailed,
    DbtCloudJobRunCancelled,
    DbtCloudJobRunFailed,
    DbtCloudJobRunTimedOut,
    DbtCloudJobRunTriggerFailed,
    get_dbt_cloud_run_info,
    trigger_dbt_cloud_job_run,
    trigger_dbt_cloud_job_run_and_wait_for_completion,
)
from prefect_dbt.cloud.models import TriggerJobRunOptions


@pytest.fixture
def dbt_cloud_credentials():
    return DbtCloudCredentials(api_key="my_api_key", account_id=123456789)


class TestTriggerDbtCloudJobRun:
    async def test_trigger_job_with_no_options(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )

        @flow
        async def test_flow():
            return await trigger_dbt_cloud_job_run(
                dbt_cloud_credentials=dbt_cloud_credentials,
                job_id=1,
            )

        flow_state = await test_flow()
        task_state = flow_state.result()
        result = task_state.result()
        assert result == {"id": 10000, "project_id": 12345}

        request_body = json.loads(respx_mock.calls.last.request.content.decode())
        assert "Triggered via Prefect in task run" in request_body["cause"]

    async def test_trigger_with_custom_options(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
            json={
                "cause": "This is a custom cause",
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
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )

        @flow
        async def test_flow():
            return await trigger_dbt_cloud_job_run(
                dbt_cloud_credentials=dbt_cloud_credentials,
                job_id=1,
                options=TriggerJobRunOptions(
                    cause="This is a custom cause",
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

        flow_state = await test_flow()
        task_state = flow_state.result()
        result = task_state.result()
        assert result == {"id": 10000, "project_id": 12345}

    async def test_trigger_nonexistent_job(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(404, json={"status": {"user_message": "Not found!"}})
        )

        @flow
        async def test_flow():
            task_shorter_retry = trigger_dbt_cloud_job_run.with_options(
                retries=1, retry_delay_seconds=1
            )
            await task_shorter_retry(
                dbt_cloud_credentials=dbt_cloud_credentials,
                job_id=1,
            )

        with pytest.raises(DbtCloudJobRunTriggerFailed, match="Not found!"):
            flow_state = await test_flow()
            flow_state.result()


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


class TestTriggerDbtCloudJobRunAndWaitForCompletion:
    @pytest.mark.respx(assert_all_called=True)
    async def test_run_success(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/10000/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": {"id": 10000, "status": 10}}))

        flow_state = await trigger_dbt_cloud_job_run_and_wait_for_completion(
            dbt_cloud_credentials=dbt_cloud_credentials, job_id=1
        )
        assert flow_state.result() == {"id": 10000, "status": 10}

    @pytest.mark.respx(assert_all_called=True)
    async def test_run_success_with_wait(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/10000/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(200, json={"data": {"id": 10000, "status": 1}}),
                Response(200, json={"data": {"id": 10000, "status": 3}}),
                Response(200, json={"data": {"id": 10000, "status": 10}}),
            ]
        )

        flow_state = await trigger_dbt_cloud_job_run_and_wait_for_completion(
            dbt_cloud_credentials=dbt_cloud_credentials,
            job_id=1,
            poll_frequency_seconds=1,
        )
        assert flow_state.result() == {"id": 10000, "status": 10}

    @pytest.mark.respx(assert_all_called=True)
    async def test_run_failure_with_wait(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/10000/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(200, json={"data": {"id": 10000, "status": 1}}),
                Response(200, json={"data": {"id": 10000, "status": 3}}),
                Response(200, json={"data": {"id": 10000, "status": 20}}),
            ]
        )

        flow_state = await trigger_dbt_cloud_job_run_and_wait_for_completion(
            dbt_cloud_credentials=dbt_cloud_credentials,
            job_id=1,
            poll_frequency_seconds=1,
        )
        with pytest.raises(DbtCloudJobRunFailed):
            assert flow_state.result()

    @pytest.mark.respx(assert_all_called=True)
    async def test_run_cancelled_with_wait(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/10000/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(200, json={"data": {"id": 10000, "status": 1}}),
                Response(200, json={"data": {"id": 10000, "status": 3}}),
                Response(200, json={"data": {"id": 10000, "status": 30}}),
            ]
        )

        flow_state = await trigger_dbt_cloud_job_run_and_wait_for_completion(
            dbt_cloud_credentials=dbt_cloud_credentials,
            job_id=1,
            poll_frequency_seconds=1,
        )
        with pytest.raises(DbtCloudJobRunCancelled):
            assert flow_state.result()

    @pytest.mark.respx(assert_all_called=True)
    async def test_run_timed_out(self, respx_mock, dbt_cloud_credentials):
        respx_mock.post(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/jobs/1/run/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"id": 10000, "project_id": 12345}}
            )
        )
        respx_mock.get(
            "https://cloud.getdbt.com/api/v2/accounts/123456789/runs/10000/",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(200, json={"data": {"id": 10000, "status": 1}}),
                Response(200, json={"data": {"id": 10000, "status": 3}}),
                Response(200, json={"data": {"id": 10000, "status": 3}}),
                Response(200, json={"data": {"id": 10000, "status": 3}}),
            ]
        )

        flow_state = await trigger_dbt_cloud_job_run_and_wait_for_completion(
            dbt_cloud_credentials=dbt_cloud_credentials,
            job_id=1,
            poll_frequency_seconds=1,
            max_wait_seconds=3,
        )
        with pytest.raises(DbtCloudJobRunTimedOut):
            assert flow_state.result()


@pytest.fixture
def real_dbt_cloud_job_id():
    return os.environ.get("DBT_CLOUD_JOB_ID")


@pytest.fixture
def real_dbt_cloud_api_key():
    return os.environ.get("DBT_CLOUD_API_KEY")


@pytest.fixture
def real_dbt_cloud_account_id():
    return os.environ.get("DBT_CLOUD_ACCOUNT_ID")


@pytest.mark.integration
async def test_run_real_dbt_cloud_job(
    real_dbt_cloud_job_id, real_dbt_cloud_api_key, real_dbt_cloud_account_id
):
    flow_state = await trigger_dbt_cloud_job_run_and_wait_for_completion(
        dbt_cloud_credentials=DbtCloudCredentials(
            api_key=real_dbt_cloud_api_key, account_id=real_dbt_cloud_account_id
        ),
        job_id=real_dbt_cloud_job_id,
        poll_frequency_seconds=1,
    )
    assert flow_state.result().get("status") == 10
