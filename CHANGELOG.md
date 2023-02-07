# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added
- `DbtCloudJob` block and `trigger_wait_retry_dbt_cloud_job_run` flow - [#101](https://github.com/PrefectHQ/prefect-dbt/pull/101)

### Changed
- The minimum version of `prefect-snowflake` - [#112](https://github.com/PrefectHQ/prefect-dbt/pull/112)
- Decoupled fields of blocks from external Collections from the created dbt profile - [#112](https://github.com/PrefectHQ/prefect-dbt/pull/112)
- `DbtCliProfile` is now accepts a `Union` of `SnowflakeTargetConfigs`, `BigQueryTargetConfigs`, and `PostgresTargetConfigs` for creation on UI - [#115](https://github.com/PrefectHQ/prefect-dbt/pull/115)
- Breaking: Made `BigQueryTargetConfigs.get_configs` synchronous - [#120](https://github.com/PrefectHQ/prefect-dbt/pull/120)

### Deprecated
- `trigger_dbt_cloud_job_run_and_wait_for_completion` and `retry_dbt_cloud_job_run_subset_and_wait_for_completion` flows in favor of `DbtCloudJob` block and `trigger_wait_retry_dbt_cloud_job_run` flow - [#101](https://github.com/PrefectHQ/prefect-dbt/pull/101)

### Removed

### Fixed

- Preventing `TargetConfigs` from being dropped upon loading a `DbtCliProfile` - [#115](https://github.com/PrefectHQ/prefect-dbt/pull/115)
- The input type of `GlobalConfigs.log_format` [#118](https://github.com/PrefectHQ/prefect-dbt/pull/118)

### Security

## 0.2.7

Released on December 29th, 2022

### Added

- Added `DbtCloudMetadataClient` and `get_metadata_client` method to `DbtCloudCredentials` to enable interaction with the dbt Cloud metadata API - [#109](https://github.com/PrefectHQ/prefect-dbt/pull/109)
- Added `get_client` method to `DbtCloudCredentials` - [#109](https://github.com/PrefectHQ/prefect-dbt/pull/109)

## 0.2.6

Released on December 7th, 2022.

### Fixed

- Using the `oauth-secrets` method in `BigQueryTargetConfigs` - [#98](https://github.com/PrefectHQ/prefect-dbt/pull/98)

## 0.2.5

Released on November 16th, 2022.

### Changed

- Changed log level of dbt Cloud job run status polling from info to debug - [#95](https://github.com/PrefectHQ/prefect-dbt/pull/95)

## 0.2.4

Released on October 26th, 2022.

### Added

- `retry_dbt_cloud_job_run_subset_and_wait_for_completion` flow and `retry_*` keywords in `trigger_dbt_cloud_job_run_and_wait_for_completion` flow - [#89](https://github.com/PrefectHQ/prefect-dbt/pull/89)
- `get_dbt_cloud_job_info` task [#89](https://github.com/PrefectHQ/prefect-dbt/pull/89)

### Changed

- Allow registering all blocks at top level - [#79](https://github.com/PrefectHQ/prefect-dbt/pull/79)

## 0.2.3

Released on October 4th, 2022.

### Added

- Support [`Oauth Token-Based`](https://docs.getdbt.com/reference/warehouse-profiles/bigquery-profile#oauth-token-based) in `BigQueryTargetConfigs` - [#68](https://github.com/PrefectHQ/prefect-dbt/pull/68)

### Fixed

- `BigQueryTargetConfigs.project` now overrides `GcpCredential.project` rather than error - [#68](https://github.com/PrefectHQ/prefect-dbt/pull/68)
- `trigger_dbt_cloud_job_run_and_wait_for_completion` no longer hangs when called from a synchronous flow = [#71](https://github.com/PrefectHQ/prefect-dbt/pull/71)

## 0.2.2

Released on September 19th, 2022.

### Fixed

- `TargetConfigs` now allows unexpected fields again because block attributes were not being saved - [#64](https://github.com/PrefectHQ/prefect-dbt/pull/64)

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
