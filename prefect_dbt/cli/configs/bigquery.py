"""Module containing models for BigQuery configs"""
from typing import Any, Dict, Optional

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal

from prefect.utilities.asyncutils import sync_compatible

from prefect_dbt.cli.configs.base import MissingExtrasRequireError, TargetConfigs

try:
    from prefect_gcp.credentials import GcpCredentials
except ModuleNotFoundError as e:
    raise MissingExtrasRequireError("BigQuery") from e


class BigQueryTargetConfigs(TargetConfigs):
    """
    Target configs contain credentials and
    settings, specific to BigQuery.
    To find valid keys, head to the [BigQuery Profile](
    https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile)
    page.

    Attributes:
        credentials: The credentials to use to authenticate; if there are
            duplicate keys between credentials and TargetConfigs,
            e.g. schema, an error will be raised.

    Examples:
        Load stored BigQueryTargetConfigs:
        ```python
        from prefect_dbt.cli.configs import BigQueryTargetConfigs

        bigquery_target_configs = BigQueryTargetConfigs.load("BLOCK_NAME")
        ```

        Instantiate BigQueryTargetConfigs with service account file.
        ```python
        from prefect_dbt.cli.configs import BigQueryTargetConfigs
        from prefect_gcp.credentials import GcpCredentials

        credentials = GcpCredentials(service_account_file="~/.secrets/gcp")
        target_configs = BigQueryTargetConfigs(
            schema="schema",
            project="project",
            credentials=credentials,
        )
        ```

        Instantiate BigQueryTargetConfigs with service account info.
        ```python
        import json
        from prefect_dbt.cli.configs import BigQueryTargetConfigs
        from prefect_gcp.credentials import GcpCredentials

        credentials = GcpCredentials(
            service_account_info=json.dumps({
                "type": "service_account",
                "project_id": "project_id",
                "private_key_id": "private_key_id",
                "private_key": "private_key",
                "client_email": "client_email",
                "client_id": "client_id",
                "auth_uri": "auth_uri",
                "token_uri": "token_uri",
                "auth_provider_x509_cert_url": "auth_provider_x509_cert_url",
                "client_x509_cert_url": "client_x509_cert_url"
            })
        )
        target_configs = BigQueryTargetConfigs(
            schema="schema",
            project="project",
            credentials=credentials,
        )
        ```
    """

    _block_type_name = "dbt CLI BigQuery Target Configs"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/5zE9lxfzBHjw3tnEup4wWL/9a001902ed43a84c6c96d23b24622e19/dbt-bit_tm.png?h=250"  # noqa
    _description = "dbt CLI target configs containing credentials and settings, specific to BigQuery."  # noqa

    type: Literal["bigquery"] = "bigquery"
    project: Optional[str] = None
    credentials: GcpCredentials

    @sync_compatible
    async def get_configs(self) -> Dict[str, Any]:
        """
        Returns the dbt configs specific to BigQuery profile.

        Returns:
            A configs JSON.
        """
        # since GcpCredentials will always define a project
        self_copy = self.copy()
        if self_copy.project is not None:
            self_copy.credentials.project = None
        configs_json = self._populate_configs_json(
            {}, self_copy.__fields__, model=self_copy
        )

        if "service_account_info" in configs_json:
            configs_json["method"] = "service-account-json"
            configs_json["keyfile_json"] = configs_json.pop("service_account_info")
        elif "service_account_file" in configs_json:
            configs_json["method"] = "service-account"
            configs_json["keyfile"] = str(configs_json.pop("service_account_file"))
        else:
            configs_json["method"] = "oauth-secrets"
            # through gcloud application-default login
            google_credentials = (
                self_copy.credentials.get_credentials_from_service_account()
            )
            if hasattr(google_credentials, "token"):
                configs_json["token"] = await self_copy.credentials.get_access_token()
            else:
                for key in ("refresh_token", "client_id", "client_secret", "token_uri"):
                    configs_json[key] = getattr(google_credentials, key)

        if "project" not in configs_json:
            raise ValueError(
                "The keyword, project, must be provided in either "
                "GcpCredentials or BigQueryTargetConfigs"
            )
        return configs_json
