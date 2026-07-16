# Hive Regression Local Environment

This document records the local Docker and regression configuration needed to run:

```bash
regression-test/suites/external_table_p0/hive/
```

## Required Thirdparty Components

Run Hive2 and Hive3 through the thirdparty launcher:

```bash
./docker/thirdparties/run-thirdparties-docker.sh -c hive2,hive3 --hive-mode refresh
```

The Hive startup script may also start `mysql` automatically. Both
`docker-compose/hive/hive-2x_settings.env` and
`docker-compose/hive/hive-3x_settings.env` default `JFS_CLUSTER_META` to:

```bash
mysql://root:123456@(127.0.0.1:3316)/juicefs_meta
```

Because of that setting, `run-thirdparties-docker.sh` treats local MySQL 5.7 as
an implicit dependency for Hive.

Each Hive version starts its own compose services:

- Hive2: `hive2-server`, `hive2-metastore`, `hadoop2-namenode`,
  `hadoop2-datanode`, `hive2-metastore-postgresql`
- Hive3: `hive3-server`, `hive3-metastore`, `hadoop3-namenode`,
  `hadoop3-datanode`, `hive3-metastore-postgresql`
- MySQL 5.7: required by the default JuiceFS metadata URI when using the local
  `127.0.0.1:3316` endpoint

The Hive p0 directory does not require these components by default:

- `pg`
- `iceberg`
- `minio`
- `hudi`
- `kerberos`
- `ranger`

`test_external_catalog_hive.groovy` has a Ranger-specific branch guarded by
`enableRangerTest`; if this option is not set to `true`, that branch is skipped.

## Current Running Containers Observed

The following relevant containers were running when checked with
`sudo docker ps`:

- Hive3 stack: `hive3-server`, `hive3-metastore`, `hadoop3-datanode`,
  `hadoop3-namenode`, `hive3-metastore-postgresql`; all healthy
- Hive2 stack: `hive2-server`, `hive2-metastore`, `hadoop2-datanode`,
  `hadoop2-namenode`, `hive2-metastore-postgresql`; all healthy
- MySQL 5.7: `mysql-doris--syt--mysql_57-1`, healthy, published as
  `127.0.0.1:3316 -> 3306`

Other Doris thirdparty containers were also running, but they are not required
for `external_table_p0/hive/`:

- PostgreSQL 14 on `5442`
- Iceberg REST, Spark, Postgres, and MinIO
- Oracle on `1521`
- ClickHouse on `8123`
- SQLServer on `1433`
- OceanBase on `2881`

The observed Hive2/Hive3 and MySQL compose projects came from another checkout
under `/mnt/disk2/suyiteng/doris`, not from this worktree. PostgreSQL and
Iceberg compose projects came from this worktree.

## Regression Configuration

The Hive p0 suite directly reads these keys from
`regression-test/conf/regression-conf.groovy`:

```groovy
enableHiveTest
externalEnvIp
hive2HmsPort
hive2HdfsPort
hive3HmsPort
hive3HdfsPort
hdfsUser
```

It also uses framework helpers backed by:

```groovy
brokerName
hdfsPasswd
hdfsFs
```

For local Hive2/Hive3 Docker with default ports, the relevant values are:

```groovy
enableHiveTest = true

hive2HmsPort = 9083
hive2HdfsPort = 8020
hive2ServerPort = 10000
hive2PgPort = 5432

hive3HmsPort = 9383
hive3HdfsPort = 8320
hive3ServerPort = 13000
hive3PgPort = 5732

hdfsUser = "doris-test"
brokerName = "broker_name"
hdfsPasswd = ""
```

`externalEnvIp` must be the address that Doris BE can use to reach the Hive
Docker services. For a fully local setup this is usually:

```groovy
externalEnvIp = "127.0.0.1"
```

If BE runs in a context where `127.0.0.1` is not the Docker host, use the host
IP selected by `run-thirdparties-docker.sh` through `IP_HOST`.

## Config Gaps Noted In regression-conf.groovy.bak

`regression-test/conf/regression-conf.groovy.bak` already contains the core Hive
keys listed above. The notable follow-ups are:

- `externalEnvIp` was set to `172.20.32.136`; verify this is reachable from BE,
  or replace it with `127.0.0.1` for local host-mode Docker.
- FE/JDBC addresses in the file used `9033`, `9022`, and `8033`; align these
  with the current Doris worktree cluster ports before running regression.
- `enableRangerTest` is not present. This is acceptable for the Hive p0 suite
  because the Ranger-specific branch is guarded and skipped unless explicitly
  enabled.

## Thirdparty Script Settings

Before starting Hive through `run-thirdparties-docker.sh`, set a unique
`CONTAINER_UID` in:

```bash
docker/thirdparties/custom_settings.env
```

The default `CONTAINER_UID="doris--"` is rejected by the startup script.

Hive baseline restore also depends on these settings:

```bash
s3Endpoint
s3BucketName
HIVE_BASELINE_VERSION
HIVE_BASELINE_TARBALL_CACHE
```

If baseline download is unavailable, place the matching tarball manually under
`HIVE_BASELINE_TARBALL_CACHE`.
