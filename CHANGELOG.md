# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `trigger_dbt_cloud_job_run` task - [#16](https://github.com/PrefectHQ/prefect-dbt/pull/16)
- `get_dbt_cloud_run_info` task - [#17](https://github.com/PrefectHQ/prefect-dbt/pull/17)
- `trigger_dbt_cloud_job_run_and_wait_for_completion` flow - [#17](https://github.com/PrefectHQ/prefect-dbt/pull/17)
- `trigger_dbt_cli_command` task - [#22](https://github.com/PrefectHQ/prefect-dbt/pull/22)
- `list_dbt_cloud_run_artifacts` task - [#23](https://github.com/PrefectHQ/prefect-dbt/pull/23)
- `get_dbt_cloud_run_artifact` task - [#23](https://github.com/PrefectHQ/prefect-dbt/pull/23)
- `call_dbt_cloud_administrative_api_endpoint` task - [#25](https://github.com/PrefectHQ/prefect-dbt/pull/25)
- `SnowflakeTargetConfigs` block - [#27](https://github.com/PrefectHQ/prefect-dbt/pull/27)

### Changed
- `TargetConfigs` and `GlobalConfigs` moved to `cli/configs/base.py` and converted into a `Block` - [#27](https://github.com/PrefectHQ/prefect-dbt/pull/27)
- Updated tests to be compatible with core Prefect library (v2.0b9) and bumped required version - [#28](https://github.com/PrefectHQ/prefect-dbt/pull/28)

### Deprecated

### Removed

### Fixed

### Security

## 0.1.0

Released on ????? ?th, 20??.

### Added

- `task_name` task - [#1](https://github.com/PrefectHQ/prefect-dbt/pull/1)
