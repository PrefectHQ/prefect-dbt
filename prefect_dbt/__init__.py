from . import _version

from .cloud import DbtCloudCredentials  # noqa
from .cli import DbtCliProfile, MissingExtrasRequireError  # noqa

try:
    from .cli.configs.snowflake import SnowflakeTargetConfigs  # noqa
except MissingExtrasRequireError:
    pass

try:
    from .cli.configs.bigquery import BigQueryTargetConfigs  # noqa
except MissingExtrasRequireError:
    pass

try:
    from .cli.configs.postgres import PostgresTargetConfigs  # noqa
except MissingExtrasRequireError:
    pass

__version__ = _version.get_versions()["version"]
