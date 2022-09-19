# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added


### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.2.1

Released on September 19th, 2022.

### Changed

- `TargetConfigs` now forbids unexpected fields; utilize the `extras` field instead - [#60](https://github.com/PrefectHQ/prefect-dbt/pull/60)

### Fixed

- Fixes `.get_configs()` method on a `TargetConfigs` instance by passing only `TargetConfigs.__fields__` - [#60](https://github.com/PrefectHQ/prefect-dbt/pull/60)

## 0.2.0

Released on August 16th, 2022.

### Changed

- Updates to `SnowflakeTargetConfigs` to accomodate breaking changes in `prefect-snowflake` - [#46](https://github.com/PrefectHQ/prefect-dbt/pull/46)

## 0.1.0

Released on August 2nd, 2022.

### Added

- `trigger_dbt_cloud_job_run` task - [#16](https://github.com/PrefectHQ/prefect-dbt/pull/16)
- `get_dbt_cloud_run_info` task - [#17](https://github.com/PrefectHQ/prefect-dbt/pull/17)
- `trigger_dbt_cloud_job_run_and_wait_for_completion` flow - [#17](https://github.com/PrefectHQ/prefect-dbt/pull/17)
- `trigger_dbt_cli_command` task - [#22](https://github.com/PrefectHQ/prefect-dbt/pull/22)
- `list_dbt_cloud_run_artifacts` task - [#23](https://github.com/PrefectHQ/prefect-dbt/pull/23)
- `get_dbt_cloud_run_artifact` task - [#23](https://github.com/PrefectHQ/prefect-dbt/pull/23)
- `call_dbt_cloud_administrative_api_endpoint` task - [#25](https://github.com/PrefectHQ/prefect-dbt/pull/25)
- `SnowflakeTargetConfigs` block - [#27](https://github.com/PrefectHQ/prefect-dbt/pull/27)
- `TargetConfigs` and `GlobalConfigs` blocks - [#27](https://github.com/PrefectHQ/prefect-dbt/pull/27)
- `BigQueryTargetConfigs` block - [#32](https://github.com/PrefectHQ/prefect-dbt/pull/32)
- `PostgresTargetConfigs` block - [#32](https://github.com/PrefectHQ/prefect-dbt/pull/32)
