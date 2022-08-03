# prefect-dbt

## Welcome!

`prefect-dbt` is a collection of Prefect integrations for working with dbt with your Prefect flows.

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-dbt` with `pip`:

```bash
pip install prefect-dbt
```

### Trigger a dbt Cloud job and wait for completion
```python
from prefect import flow

from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run_and_wait_for_completion

@flow
def run_dbt_job_flow():
    run_result = trigger_dbt_cloud_job_run_and_wait_for_completion(
        dbt_cloud_credentials=DbtCloudCredentials(
            api_key="my_api_key",
            account_id=123456789
        ),
        job_id=1
    )

run_dbt_job_flow()
```

### Execute a dbt CLI command
```python
from prefect import flow
from prefect_dbt.cli.commands import trigger_dbt_cli_command

@flow
def trigger_dbt_cli_command_flow() -> str:
    result = trigger_dbt_cli_command("dbt debug")
    return result # Returns the last line the in CLI output

trigger_dbt_cli_command_flow()
```

## Resources

If you encounter any bugs while using `prefect-dbt`, feel free to open an issue in the [prefect-dbt](https://github.com/PrefectHQ/prefect-dbt) repository.

If you have any questions or issues while using `prefect-dbt`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

If you need help getting started with or using dbt, please consult the [dbt documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation).

## Development

If you'd like to install a version of `prefect-dbt` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-dbt.git

cd prefect-dbt/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
