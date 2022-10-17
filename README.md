# prefect-dbt

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-dbt/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-dbt?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/prefecthq/prefect-dbt/" alt="Stars">
        <img src="https://img.shields.io/github/stars/prefecthq/prefect-dbt?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-dbt/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-dbt?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/prefecthq/prefect-dbt/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/prefecthq/prefect-dbt?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

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

Some dbt CLI profiles require additional installation; for example Databricks:

```bash
pip install dbt-databricks
```

Then, register to [view the blocks](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_dbt.commands
prefect block register -m prefect_dbt.cli
```


Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

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

### Execute a dbt CLI command without a pre-populated profiles.yml
```python
from prefect import flow
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect_snowflake.database import SnowflakeConnector

from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_dbt.cli.commands import trigger_dbt_cli_command
from prefect_dbt.cli.configs import SnowflakeTargetConfigs

@flow
def trigger_dbt_cli_command_flow():
    connector = SnowflakeConnector(
        schema="public",
        database="database",
        warehouse="warehouse",
        credentials=SnowflakeCredentials(
            user="user",
            password="password",
            account="account.region.aws",
            role="role",
        ),
    )
    target_configs = SnowflakeTargetConfigs(
        connector=connector
    )
    dbt_cli_profile = DbtCliProfile(
        name="jaffle_shop",
        target="dev",
        target_configs=target_configs,
    )
    result = trigger_dbt_cli_command(
        "dbt debug",
        overwrite_profiles=True,
        dbt_cli_profile=dbt_cli_profile
    )
    return result

trigger_dbt_cli_command_flow()
```

### Idempotent way to execute multiple dbt CLI commands without prepopulated profiles.yml
```python
from prefect import flow

from prefect_dbt.cli.credentials import DbtCliProfile
from prefect_dbt.cli.commands import trigger_dbt_cli_command

@flow
def trigger_dbt_cli_commands_flow():
    dbt_cli_profile = DbtCliProfile.load("MY_BLOCK_NAME")
    
    trigger_kwargs = dict(
        profiles_dir=".",
        overwrite_profiles=True,
        dbt_cli_profile=dbt_cli_profile,
    )
    
    trigger_dbt_cli_command(
        "dbt deps",
        **trigger_kwargs
    )
    
    result = trigger_dbt_cli_command(
        "dbt debug",
        **trigger_kwargs
    )
    
    return result
    

trigger_dbt_cli_commands_flow()
```
## Resources

If you encounter any bugs while using `prefect-dbt`, feel free to open an issue in the [prefect-dbt](https://github.com/PrefectHQ/prefect-dbt) repository.

If you have any questions or issues while using `prefect-dbt`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

If you need help getting started with or using dbt, please consult the [dbt documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation).

Feel free to ⭐️ or watch [`prefect-dbt`](https://github.com/PrefectHQ/prefect-dbt) for updates too!

## Development

If you'd like to install a version of `prefect-dbt` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-dbt.git

cd prefect-dbt/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
