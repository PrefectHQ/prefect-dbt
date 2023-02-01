"""Module containing models for Postgres configs"""
from typing import Any, Dict

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from pydantic import Field

from prefect_dbt.cli.configs.base import (
    AbstractTargetConfigs,
    MissingExtrasRequireError,
)

try:
    from prefect_sqlalchemy.database import DatabaseCredentials
except ModuleNotFoundError as e:
    raise MissingExtrasRequireError("Postgres") from e


class PostgresTargetConfigs(AbstractTargetConfigs):
    """
    Target configs contain credentials and
    settings, specific to Postgres.
    To find valid keys, head to the [Postgres Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile)
    page.

    Attributes:
        credentials: The credentials to use to authenticate; if there are
            duplicate keys between credentials and TargetConfigs,
            e.g. schema, an error will be raised.

    Examples:
        Load stored PostgresTargetConfigs:
        ```python
        from prefect_dbt.cli.configs import PostgresTargetConfigs

        postgres_target_configs = PostgresTargetConfigs.load("BLOCK_NAME")
        ```

        Instantiate PostgresTargetConfigs with DatabaseCredentials.
        ```python
        from prefect_dbt.cli.configs import PostgresTargetConfigs
        from prefect_sqlalchemy import DatabaseCredentials, SyncDriver

        credentials = DatabaseCredentials(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username="prefect",
            password="prefect_password",
            database="postgres",
            host="host",
            port=8080
        )
        target_configs = PostgresTargetConfigs(credentials=credentials, schema="schema")
        ```
    """

    _block_type_name = "dbt CLI Postgres Target Configs"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/9a001902ed43a84c6c96d23b24622e19/dbt-bit_tm.png?h=250"  # noqa
    _description = "dbt CLI target configs containing credentials and settings specific to Postgres."  # noqa
    _documentation_url = "https://prefecthq.github.io/prefect-dbt/cli/configs/postgres/#prefect_dbt.cli.configs.postgres.PostgresTargetConfigs"  # noqa

    type: Literal["postgres"] = Field(
        default="postgres", description="The type of the target."
    )
    credentials: DatabaseCredentials = Field(
        default=...,
        description=(
            "The credentials to use to authenticate; if there are duplicate keys "
            "between credentials and TargetConfigs, e.g. schema, "
            "an error will be raised."
        ),
    )  # noqa

    def get_configs(self) -> Dict[str, Any]:
        """
        Returns the dbt configs specific to Postgres profile.

        Returns:
            A configs JSON.
        """
        configs_json = super().get_configs()
        invalid_keys = ["driver", "query", "url", "connect_args", "_async_supported"]
        rename_keys = {
            "database": "dbname",
            "username": "user",
            "password": "password",
            "host": "host",
            "port": "port",
        }
        # get the keys from rendered url
        for invalid_key in invalid_keys + list(rename_keys):
            configs_json.pop(invalid_key, None)

        rendered_url = self.credentials.rendered_url
        for key in rename_keys:
            renamed_key = rename_keys[key]
            configs_json[renamed_key] = getattr(rendered_url, key)
        return configs_json
