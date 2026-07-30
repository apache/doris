# dbt-doris

This is the doris adapter plugin for dbt.

## Install

```shell
git clone https://github.com/apache/doris.git
cd doris/extension/dbt-doris && pip install .
```

## Status

This adapter targets dbt Core 1.12.x on Python 3.10 or newer. It is not yet a
dbt Fusion adapter.

The Doris SQL, staging behavior, version boundaries, and configuration for
incremental models are documented in
[docs/incremental.zh-CN.md](docs/incremental.zh-CN.md).

## Configuring your profile

Example entry for profiles.yml:

```yaml
your_profile_name:
  target: dev
  outputs:
    dev:
      type: doris
      host: 127.0.0.1
      port: 9030
      username: root
      schema: dbt
```
