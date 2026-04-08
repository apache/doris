<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either implied.  See the License for the specific
language governing permissions and limitations
under the License.
-->

# Hive Docker Environment

This directory contains Hive2/Hive3 Docker Compose templates and bootstrap scripts used by Doris thirdparty startup.

## Components

- `hive-server`: HiveServer2 endpoint
- `hive-metastore`: Hive Metastore service
- `hive-metastore-postgresql`: metastore backend database
- `namenode` / `datanode`: HDFS services for Hive test data

## Component Segmentation

Hive startup can be understood in 3 layers:

### 1) Docker Service Layer

- Compute/SQL entry:
  - `hive-server`
- Metadata:
  - `hive-metastore`
  - `hive-metastore-postgresql`
- Storage:
  - `namenode`
  - `datanode`

### 2) Refresh Module Layer (`--hive-modules`)

- `default`: basic default-db external tables
- `multi_catalog`: multi-format and multi-path external table cases
- `partition_type`: partition type coverage cases
- `statistics`: table stats and empty-table stats cases
- `tvf`: tvf data/bootstrap cases
- `regression`: special regression datasets (serde, delimiters, etc.)
- `test`: lightweight smoke test datasets
- `preinstalled_hql`: centralized preinstalled HQL scripts (`create_preinstalled_scripts/*.hql`)
- `view`: view bootstrap (`create_view_scripts/create_view.hql`)

### 3) Bootstrap Group Layer

- `common`: shared items for hive2/hive3
- `hive2_only`: hive2-only items
- `hive3_only`: hive3-only items

By default:

- Hive2 uses: `common,hive2_only`
- Hive3 uses: `common,hive3_only`

This grouping controls which files are selected during `run.sh`/HQL refresh.

## Start/Stop

```bash
# Start Hive3
./docker/thirdparties/run-thirdparties-docker.sh -c hive3

# Start Hive2
./docker/thirdparties/run-thirdparties-docker.sh -c hive2

# Stop Hive3
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --stop
```

## Startup Modes

Use `--hive-mode` to control startup behavior:

- `fast`: reuse existing state as much as possible
- `refresh` (default): refresh only changed modules by SHA
- `rebuild`: force reset and rebuild hive state

Examples:

```bash
# Default mode (refresh)
./docker/thirdparties/run-thirdparties-docker.sh -c hive3

# Explicit refresh
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh

# Full rebuild
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode rebuild
```

## Module Refresh

Use `--hive-modules` to limit refresh scope:

- `default,multi_catalog,partition_type,statistics,tvf,regression,test,preinstalled_hql,view`
- `all` means all modules

Examples:

```bash
# Refresh only preinstalled HQL scripts
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh --hive-modules preinstalled_hql

# Refresh selected data modules
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh --hive-modules default,multi_catalog
```

## Idempotency Rules

To keep `refresh` repeatable:

- `run.sh` scripts should be idempotent
- HQL should use `DROP ... IF EXISTS` then `CREATE ...`
- avoid relying on `CREATE ... IF NOT EXISTS` for table/view recreation

## JuiceFS Metadata Backend

Hive now defaults JuiceFS metadata to PostgreSQL (Hive metastore DB), so Hive startup no longer auto-requires MySQL.

- Hive2 default (in `hive-2x_settings.env`):
  - `postgres://postgres@127.0.0.1:${PG_PORT}/juicefs_meta?sslmode=disable`
- Hive3 default (in `hive-3x_settings.env`):
  - `postgres://postgres@127.0.0.1:${PG_PORT}/juicefs_meta?sslmode=disable`

If your environment still needs MySQL metadata, override before startup:

```bash
export JFS_CLUSTER_META="mysql://root:123456@(127.0.0.1:3316)/juicefs_meta"
./docker/thirdparties/run-thirdparties-docker.sh -c hive3
```

## Logs and Debug

- Hive3 startup log: `docker/thirdparties/logs/start_hive3.log`
- Hive2 startup log: `docker/thirdparties/logs/start_hive2.log`

By default, helper scripts keep xtrace off to reduce log noise.
Enable debug trace when needed:

```bash
export HIVE_DEBUG=1
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh
```

## Common Troubleshooting

- Metastore health check fails:
  - check `hive-metastore-postgresql` is healthy
  - inspect `start_hive3.log` or `start_hive2.log`
- JuiceFS format/status fails:
  - verify `JFS_CLUSTER_META` is reachable
  - ensure target metadata database exists (startup script auto-creates for local MySQL/PostgreSQL)
- Refresh is unexpectedly slow:
  - confirm `--hive-mode refresh` is used
  - use `--hive-modules` to narrow refresh scope
