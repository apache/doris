# dbt-doris

This is the doris adapter plugin for dbt.

## Install

```shell
git clone https://github.com/apache/doris.git
cd doris/extension/dbt-doris && pip install .
```

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
      # optional: set Doris session variables for each connection
      # session_variables:
      #   time_zone: "Asia/Shanghai"
      #   exec_mem_limit: 8589934592
```
