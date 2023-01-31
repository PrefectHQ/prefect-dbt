# prefect-dbt

<p align="center">
    <!--- Insert a cover image here -->
    <br>
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

Visit the full docs [here](https://PrefectHQ.github.io/prefect-dbt) to see additional examples and the API reference.

`prefect-dbt` is a collection of Prefect integrations for working with dbt with your Prefect flows.

## Getting Started

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

If you need help getting started with or using dbt, please consult the [dbt documentation](https://docs.getdbt.com/docs/building-a-dbt-project/documentation).

### Installation

Install `prefect-dbt` with `pip`:

```bash
pip install prefect-dbt
```

Some dbt CLI profiles require additional installation; for example Databricks:

```bash
pip install dbt-databricks
```

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Saving dbt Cloud credentials to block

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

1. Head over to your [dbt Cloud profile](https://cloud.getdbt.com/settings/profile).
2. Login to your dbt Cloud account.
3. Scroll down to "API" or click "API Access" on the sidebar.
4. Copy the API Key.
5. Create a short script, replacing the placeholders (or do so in the UI).

```python
from prefect_dbt.cloud import DbtCloudCredentials
DbtCloudCredentials(
    api_key="API_KEY_PLACEHOLDER",
    account_id="ACCOUNT_ID_PLACEHOLDER"
).save("BLOCK_NAME_PLACEHOLDER")
```

Congrats! You can now easily load the saved block, which holds your credentials:

```python
from prefect_dbt.cloud import DbtCloudCredentials
DbtCloudCredentials.load("BLOCK_NAME_PLACEHOLDER")
```

To [view and edit the blocks](https://orion-docs.prefect.io/ui/blocks/) on Prefect UI:

```bash
prefect block register -m prefect_dbt
```

### Feedback

If you encounter any bugs while using `prefect-dbt`, feel free to open an issue in the [prefect-dbt](https://github.com/PrefectHQ/prefect-dbt) repository.

If you have any questions or issues while using `prefect-dbt`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to star or watch [`prefect-dbt`](https://github.com/PrefectHQ/prefect-dbt) for updates too!

### Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-dbt`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-dbt/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
