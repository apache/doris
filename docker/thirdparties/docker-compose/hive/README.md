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

Hive2/Hive3 Docker Compose templates and bootstrap scripts used by Doris thirdparty regression tests.

中文版: [README_ZH.md](README_ZH.md)

---

## Architecture

Hive startup is structured in three independent layers:

### Layer 1 — Docker Services

All services run with `network_mode: host`, so ports are bound directly on the host.

| Service | Role | Hive3 Port | Hive2 Port |
|---|---|---|---|
| `hive-server` | HiveServer2 (SQL/JDBC entry) | `13000` | `10000` |
| `hive-metastore` | Hive Metastore (HMS) | `9383` | `9083` |
| `hive-metastore-postgresql` | Metastore backend DB | `5732` | `5432` |
| `namenode` | HDFS NameNode | `8320` | `8020` |
| `datanode` | HDFS DataNode | — | — |

Container names are prefixed by `CONTAINER_UID` (set in `custom_settings.env`).
Example: `CONTAINER_UID=doris-jack-` → container name `doris-jack-hive3-server`.

### Layer 2 — Refresh Modules (`--hive-modules`)

Each module maps to a directory under `scripts/data/` or a dedicated script set.
Modules are refreshed incrementally: only modules whose content SHA changed are re-executed.

| Module | Source path | Content |
|---|---|---|
| `default` | `scripts/data/default/` | Basic external tables in the `default` database |
| `multi_catalog` | `scripts/data/multi_catalog/` | Multi-format, multi-path external table cases |
| `partition_type` | `scripts/data/partition_type/` | Partition type coverage (int, string, date, …) |
| `statistics` | `scripts/data/statistics/` | Table stats and empty-table stats cases |
| `tvf` | `scripts/data/tvf/` | TVF test data (HDFS upload) |
| `regression` | `scripts/data/regression/` | Special regression datasets (serde, delimiters, …) |
| `test` | `scripts/data/test/` | Lightweight smoke-test datasets |
| `preinstalled_hql` | `scripts/create_preinstalled_scripts/*.hql` | ~77 HQL files, executed in parallel via `xargs -P` |
| `view` | `scripts/create_view_scripts/create_view.hql` | View definitions |

### Layer 3 — Version-Specific File Selection

The startup scripts automatically choose the right file set for each Hive version:

- Hive2 runs shared files plus files listed in `bootstrap/hive2_only.*.list`
- Hive3 runs shared files plus files listed in `bootstrap/hive3_only.*.list`

This selection is an internal implementation detail; developers normally do not need to configure it manually.

---

## State: Docker Named Volumes + OSS Baseline

Hive state (HDFS data, Postgres metastore, and the module SHA tracker) lives in four Docker named volumes per version, not host bind mounts. The shared volume prefix is fixed to `doris-shared`.

| Volume | Mounted into |
|---|---|
| `doris-shared-<hive_version>-namenode` | NameNode metadata |
| `doris-shared-<hive_version>-datanode` | DataNode blocks |
| `doris-shared-<hive_version>-pgdata` | Hive Metastore Postgres data |
| `doris-shared-<hive_version>-state` | `/mnt/state` — per-module SHA files used for incremental refresh |

Lifecycle:
- `--hive-mode fast`: volumes are preserved across runs.
- `--hive-mode refresh`: volumes are reset, then restored from the published baseline tarball before module refresh.
- `--hive-mode rebuild`: volumes are removed (`docker volume rm`) and recreated empty.

### Baseline restore

The script primes volumes from a pre-built baseline tarball in two cases:

1. `--hive-mode refresh`: always reset the volumes, then restore the published baseline before reconciling changed modules.
2. `--hive-mode fast`: restore the baseline only when the volumes are empty (fresh CI host, or after manual cleanup).

Baseline restore flow:

1. Look for a cached tarball at `${HIVE_BASELINE_TARBALL_CACHE:-docker/thirdparties/docker-compose/hive/scripts/baseline}/<hive_version>-baseline-<version>.tar.gz`.
2. If not cached, download from `https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/hive_baseline/<hive_version>-baseline-<version>.tar.gz`.
3. Look for an extracted cache directory at `${HIVE_BASELINE_TARBALL_CACHE:-docker/thirdparties/docker-compose/hive/scripts/baseline}/<hive_version>-baseline-<version>/`; if missing, extract the tarball there once.
4. Restore the four volumes from the extracted cache directory in a single `alpine tar` container.
5. Bumping `HIVE_BASELINE_VERSION` changes both the cache filename and the auto-constructed OSS URL, so CI hosts fetch the newly published tarball instead of reusing an older cached artifact.

Relevant env vars:

| Variable | Default | Purpose |
|---|---|---|
| `HIVE_BASELINE_TARBALL_CACHE` | `docker/thirdparties/docker-compose/hive/scripts/baseline` in `custom_settings.env` | Local cache dir for downloaded tarballs and extracted baseline directories; cache names include `HIVE_BASELINE_VERSION` |
| `HIVE_BASELINE_VERSION` | `20260415` in `custom_settings.env` | Baseline publication key: embedded in the cache filename and the auto-constructed OSS tarball URL |

### Producing a new baseline tarball

After bootstrapping a clean Hive stack, stop the containers and run:

```bash
sudo docker compose -p "${CONTAINER_UID}hive3" \
  -f docker/thirdparties/docker-compose/hive/hive-3x.yaml down

bash docker/thirdparties/docker-compose/hive/scripts/snapshot-hive-baseline.sh \
  "${CONTAINER_UID}hive3" /tmp/hive3-baseline.tar.gz
```

Then upload the resulting tarball to OSS at `oss://<s3BucketName>/regression/datalake/pipeline_data/hive_baseline/hive3-baseline-<version>.tar.gz` (same convention for `hive2`).
To publish a new baseline, update `HIVE_BASELINE_VERSION` once in `docker/thirdparties/custom_settings.env`, produce the new tarballs, and upload them with the matching versioned filenames.

---

## Usage

### Start / Stop

```bash
# Start Hive3 (default: refresh mode)
./docker/thirdparties/run-thirdparties-docker.sh -c hive3

# Start Hive2
./docker/thirdparties/run-thirdparties-docker.sh -c hive2

# Start both
./docker/thirdparties/run-thirdparties-docker.sh -c hive2,hive3

# Stop Hive3
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --stop
```

### Startup Modes (`--hive-mode`)

| Mode | Behavior | When to use |
|---|---|---|
| `fast` | Reuse existing volumes, skip compose up if the stack is already healthy, and skip data refresh entirely | Machine reboot / Docker restart recovery when you want the previous Hive environment back as quickly as possible |
| `refresh` | Reset volumes to the published baseline, then re-run only modules/HQL files whose SHA changed *(default)* | Daily development and PR verification when case scripts or HQL changed and you want a clean baseline before reconciling your changes |
| `rebuild` | Tear down stack, wipe all volumes, and rebuild everything from scratch without baseline restore | Full local bootstrap from current scripts, typically before exporting or validating a new baseline tarball |

```bash
# Fast: reuse the existing volumes and restore the previous docker environment quickly
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode fast

# Refresh: reset to baseline and reconcile changed HQL/scripts (default)
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh

# Rebuild: clean slate from local scripts, typically before exporting a new baseline
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode rebuild
```

### Scoped Module Refresh (`--hive-modules`)

Refresh only the modules you care about:

```bash
# Re-run only changed preinstalled HQL files (parallel execution)
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
  --hive-mode refresh --hive-modules preinstalled_hql

# Refresh two specific modules
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
  --hive-mode refresh --hive-modules default,multi_catalog

# All modules (explicit)
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
  --hive-mode refresh --hive-modules all
```

Each refresh ends with a summary line showing what was actually re-executed, for example:

```text
[hive-refresh] summary refreshed_modules=2 modules=multi_catalog,preinstalled_hql
[hive-refresh] summary details=multi_catalog:run_sh=74;preinstalled_hql:files=3(create_preinstalled_scripts/run40.hql,create_preinstalled_scripts/run69.hql,create_preinstalled_scripts/run76.hql)
```

---

## Developer Guide

### Which Mode Should I Use?

- Use `fast` when the machine or Docker daemon restarted and you only need the previous Hive containers and data back without any refresh work.
- Use `refresh` for normal development. This is the safe default when you changed Hive case data, `run.sh`, or HQL files and want those changes applied on top of a clean published baseline.
- Use `rebuild` when you intentionally want to ignore the published baseline and bootstrap everything from the current repository state, usually before generating a new baseline tarball.

### Typical Workflows

- Change one or two Hive HQL files and verify them quickly:
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh --hive-modules preinstalled_hql`
- Change a small set of module data under `scripts/data/multi_catalog`:
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh --hive-modules multi_catalog`
- Restore the old environment after a host restart:
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode fast`
- Prepare to export a new baseline:
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode rebuild`

### How to Add Test Data

There are two patterns depending on where the data should live.

#### Pattern A — `run.sh` (HDFS data + DDL)

Use this when the test data files need to be uploaded to HDFS.

1. Create a directory under the appropriate module:
   ```
   scripts/data/<module>/<your_dataset>/
   ├── run.sh          # required: executed during module refresh
   └── <data files>    # csv, parquet, orc, etc.
   ```

2. Write `run.sh` to be **idempotent** (safe to run multiple times):
   ```bash
   #!/bin/bash
   set -x
   CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

   # Upload data only if not already present
   hadoop fs -mkdir -p /user/doris/preinstalled_data/your_dataset
   if [[ -z "$(hadoop fs -ls /user/doris/preinstalled_data/your_dataset 2>/dev/null)" ]]; then
       hadoop fs -put "${CUR_DIR}"/data/* /user/doris/preinstalled_data/your_dataset/
   fi

   # Create table (drop-then-create for idempotency)
   hive -e "
     DROP TABLE IF EXISTS your_table;
     CREATE EXTERNAL TABLE your_table (...)
     STORED AS PARQUET
     LOCATION '/user/doris/preinstalled_data/your_dataset';
   "
   ```

3. If the dataset is Hive2-only or Hive3-only, add the `run.sh` path to the corresponding list:
   ```
   bootstrap/hive2_only.run_sh.list
   bootstrap/hive3_only.run_sh.list
   ```

#### Pattern B — `create_preinstalled_scripts/` (HQL only)

Use this when no HDFS file upload is needed (external table pointing to pre-existing HDFS data, or managed table with inline INSERT values).

1. Add a new file `scripts/create_preinstalled_scripts/runNN.hql`:
   ```sql
   use default;

   DROP TABLE IF EXISTS `your_new_table`;
   CREATE EXTERNAL TABLE `your_new_table` (
     id INT,
     name STRING
   )
   STORED AS PARQUET
   LOCATION '/user/doris/preinstalled_data/existing_path';
   ```

2. Rules:
   - Always use `DROP TABLE IF EXISTS` before `CREATE` — never `CREATE IF NOT EXISTS` alone
   - Pick the next available `runNN` number
   - If Hive2-only or Hive3-only, add the relative path to `bootstrap/hive2_only.preinstalled_hql.list` or `bootstrap/hive3_only.preinstalled_hql.list`
   - If TPCH-related, add to `bootstrap/tpch.preinstalled_hql.list`

3. Trigger a refresh to pick it up:
   ```bash
   ./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
     --hive-mode refresh --hive-modules preinstalled_hql
   ```

---

### How to Access HiveServer2 for Debugging

All containers use `network_mode: host`, so ports are directly accessible on the host.

#### Connect via beeline (inside the container)

```bash
# Enter the hive-server container
docker exec -it ${CONTAINER_UID}hive3-server bash

# Connect via beeline (the hive shim on PATH routes here automatically)
beeline -u "jdbc:hive2://localhost:13000/default" -n root

# Or use the hive shim shorthand
hive -e "show databases;"
hive -e "show tables in default;"
hive -f /path/to/your.hql
```

#### Connect via beeline from the host

```bash
# Requires beeline on PATH locally; use the host IP
beeline -u "jdbc:hive2://127.0.0.1:13000/default" -n root
```

#### Run ad-hoc HQL from outside the container

```bash
# Execute a single query
docker exec ${CONTAINER_UID}hive3-server \
  beeline -u "jdbc:hive2://localhost:13000/default" -n root \
  -e "SELECT * FROM default.your_table LIMIT 10;"

# Run a HQL file (file must exist inside the container or on a mounted path)
docker exec ${CONTAINER_UID}hive3-server \
  hive -f /mnt/scripts/create_preinstalled_scripts/run02.hql
```

#### Inspect HDFS

```bash
# List top-level HDFS directories
docker exec ${CONTAINER_UID}hadoop3-namenode \
  hadoop fs -ls /user/doris/

# Check if a specific path exists
docker exec ${CONTAINER_UID}hadoop3-namenode \
  hadoop fs -ls /user/doris/preinstalled_data/your_dataset/
```

#### Inspect the Metastore PostgreSQL

```bash
# Connect to the metastore DB directly (port 5732 for Hive3)
psql -h 127.0.0.1 -p 5732 -U postgres -d metastore \
  -c "SELECT TBL_NAME, DB_ID FROM TBLS LIMIT 20;"
```

---

## Logs and Debug

| Log file | Content |
|---|---|
| `docker/thirdparties/logs/start_hive3.log` | Full Hive3 startup output |
| `docker/thirdparties/logs/start_hive2.log` | Full Hive2 startup output |

Enable verbose xtrace for detailed script execution:

```bash
HIVE_DEBUG=1 ./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh
```

Startup timing is printed at the end of each phase:
```
[14:02:31] [hive3] compose up done took=18s
[14:02:49] [hive3] init-hive-baseline begin
[14:03:11] [hive3] init-hive-baseline done took=22s
[14:03:11] [hive3] refresh-hive-modules begin (mode=refresh modules=all)
[14:05:44] [hive3] refresh-hive-modules done took=153s
```

---

## Troubleshooting

**Metastore health check fails**
- Verify `${CONTAINER_UID}hive3-metastore-postgresql` is healthy: `docker ps`
- Inspect the startup log: `tail -100 docker/thirdparties/logs/start_hive3.log`

**HiveServer2 not reachable**
- Check the container is running: `docker ps | grep hive3-server`
- Test the port: `nc -z 127.0.0.1 13000`
- Check HS2 logs inside the container: `docker exec ${CONTAINER_UID}hive3-server tail -50 /tmp/hive-server2.log`

**JuiceFS format/init fails**
- Verify `JFS_CLUSTER_META` is reachable (default: `mysql://root:123456@(127.0.0.1:3316)/juicefs_meta`)
- Override if needed: `export JFS_CLUSTER_META=<your_uri>`

**Refresh is unexpectedly slow**
- Check which modules are being re-run; if all, a SHA mismatch caused full refresh
- Narrow scope: `--hive-modules preinstalled_hql`
- Use timing output (see Logs section above) to identify the slow phase

**State is stale after a hard container kill**
- The state directory may have a partial write; run with `--hive-mode rebuild` to reset cleanly

**Baseline download is slow or fails**
- Verify connectivity to `https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/hive_baseline/`
- Place the tarball manually at `${HIVE_BASELINE_TARBALL_CACHE:-docker/thirdparties/docker-compose/hive/scripts/baseline}/<hive_version>-baseline-<version>.tar.gz` to skip the download
- Confirm `s3BucketName` and `s3Endpoint` are set correctly in `docker/thirdparties/custom_settings.env`

**Inspect or delete volumes manually**
```bash
# List the four volumes for a version
docker volume ls | grep "${CONTAINER_UID}hive3-"

# Remove all four (equivalent to --hive-mode rebuild's reset step)
for s in namenode datanode pgdata state; do
  docker volume rm -f "${CONTAINER_UID}hive3-${s}"
done
```
