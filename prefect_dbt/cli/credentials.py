"""Module containing credentials for interacting with dbt CLI"""
from dataclasses import dataclass


@dataclass
class DbtCliCredentials:
    """
    Credentials class for credential use across dbt CLI tasks and flows.
    Besides profile_name and profile_target, not all of the built-in args
    have to be provided; instead, visit the [Available adapters](
    https://docs.getdbt.com/docs/available-adapters) page and
    browse the desired adapter's Profile Setup documentation
    for valid keys to pass into profile_kwargs.

    Args:
        profile_name: Profile name used for populating profiles.yml.
        profile_target: The default target your dbt project will use.
        user: The user name used to authenticate.
        password: The password used to authenticate.
        host: The host address of the database.
        port: The port to connect to the database.
        **profile_kwargs

    Examples:
        Use DbtCliCredentials instance to trigger a job run:
        ```python
        from prefect_dbt.cli import DbtCliCredentials

        credentials = DbtCliCredentials()

        async with dbt_cli_credentials.get_administrative_client() as client:
            client.trigger_job_run(job_id=1)
        ```
    """
    profile_name: str
    profile_target: str
    user: str = None,
    password: str = None,
    host: str = None,
    port: int = None,

    def __init__(
        self,
        profile_name: str,
        profile_target: str,
        user: str = None,
        password: str = None,
        host: str = None,
        port: int = None,
        **profile_kwargs
    ):
        self.profile_kwargs = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            **profile_kwargs
        }
        for key, val in self.profile_kwargs:
            if val is None:
                self.profile_kwargs.pop(key)

    def get_profile(self):
        """
        Returns the class's profile.
        """
        profile = {
            self.profile_name: {
                "outputs": {
                    self.profile_target: self.profile_kwargs
                },
                "target": self.profile_target,
            }
        }
        return profile