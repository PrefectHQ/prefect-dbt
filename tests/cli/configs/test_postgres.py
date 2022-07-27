from prefect_sqlalchemy.credentials import DatabaseCredentials, SyncDriver
from pydantic import SecretStr

from prefect_dbt.cli.configs import PostgresTargetConfigs


def test_postgres_target_configs_get_configs():
    credentials = DatabaseCredentials(
        driver=SyncDriver.POSTGRESQL_PSYCOPG2,
        username="prefect",
        password="prefect_password",
        database="postgres",
        host="host",
        port=8080,
    )
    configs = PostgresTargetConfigs(schema="schema", credentials=credentials)
    actual = configs.get_configs()
    expected = {
        "type": "postgres",
        "schema": "schema",
        "threads": 4,
        "dbname": "postgres",
        "user": "prefect",
        "password": "prefect_password",
        "host": "host",
        "port": 8080,
    }
    for k, v in actual.items():
        actual_v = v.get_secret_value() if isinstance(v, SecretStr) else v
        expected_v = expected[k]
        assert actual_v == expected_v
