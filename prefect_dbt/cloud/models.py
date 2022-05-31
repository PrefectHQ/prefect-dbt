"""Module containing models used for passing data to dbt Cloud"""
from typing import List, Optional

from pydantic import BaseModel, Field


class TriggerJobRunOptions(BaseModel):
    """
    Defines options that can be defined when triggering a dbt Cloud job run.
    """

    cause: str = Field(
        default="Kicked off from Prefect",
        description="A text description of the reason for running this job.",
    )
    git_sha: Optional[str] = Field(
        default=None, description="The git sha to check out before running this job."
    )
    git_branch: Optional[str] = Field(
        default=None, description="The git branch to check out before running this job."
    )
    schema_override: Optional[str] = Field(
        default=None,
        description="Override the destination schema in the configured "
        "target for this job.",
    )
    dbt_version_override: Optional[str] = Field(
        default=None, description="Override the version of dbt used to run this job."
    )
    threads_override: Optional[int] = Field(
        default=None, description="Override the number of threads used to run this job."
    )
    target_name_override: Optional[str] = Field(
        default=None,
        description="Override the target.name context variable used when "
        "running this job",
    )
    generate_docs_override: Optional[bool] = Field(
        default=None,
        description="Override whether or not this job generates docs "
        "(true=yes, false=no).",
    )
    timeout_seconds_override: Optional[int] = Field(
        default=None, description="Override the timeout in seconds for this job."
    )
    steps_override: Optional[List[str]] = Field(
        default=None, description="Override the list of steps for this job."
    )
