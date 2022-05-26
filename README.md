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

### Write and run a flow

```python
from prefect import flow
from prefect_dbt.tasks import (
    goodbye_prefect_dbt,
    hello_prefect_dbt,
)


@flow
def example_flow():
    hello_prefect_dbt()
    goodbye_prefect_dbt()

example_flow()
```

## Resources

If you encounter any bugs while using `prefect-dbt`, feel free to open an issue in the [prefect-dbt](https://github.com/PrefectHQ/prefect-dbt) repository.

If you have any questions or issues while using `prefect-dbt`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-dbt` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-dbt.git

cd prefect-dbt/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
