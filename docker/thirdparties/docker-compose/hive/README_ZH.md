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

# Hive Docker 环境

Doris thirdparty 回归测试使用的 Hive2/Hive3 Docker Compose 模板与引导脚本。

英文版: [README.md](README.md)

---

## 架构

Hive 启动被拆分为三层互相独立的抽象：

### Layer 1 — Docker 服务

所有服务均使用 `network_mode: host`，端口直接暴露在宿主机上。

| 服务 | 职责 | Hive3 端口 | Hive2 端口 |
|---|---|---|---|
| `hive-server` | HiveServer2 (SQL/JDBC 入口) | `13000` | `10000` |
| `hive-metastore` | Hive Metastore (HMS) | `9383` | `9083` |
| `hive-metastore-postgresql` | Metastore 元数据库 | `5732` | `5432` |
| `namenode` | HDFS NameNode | `8320` | `8020` |
| `datanode` | HDFS DataNode | — | — |

容器名前缀由 `CONTAINER_UID`（定义在 `custom_settings.env`）指定。
例如 `CONTAINER_UID=doris-jack-` → 容器名为 `doris-jack-hive3-server`。

### Layer 2 — 刷新模块（`--hive-modules`）

每个模块对应 `scripts/data/` 下的一个目录或一组专用脚本。
模块是**增量刷新**的：只有内容 SHA 发生变化的模块才会被重新执行。

| 模块 | 源路径 | 内容 |
|---|---|---|
| `default` | `scripts/data/default/` | `default` 库中的基础外部表 |
| `multi_catalog` | `scripts/data/multi_catalog/` | 多格式、多路径的外部表用例 |
| `partition_type` | `scripts/data/partition_type/` | 各类分区类型覆盖（int、string、date 等）|
| `statistics` | `scripts/data/statistics/` | 表统计与空表统计相关用例 |
| `tvf` | `scripts/data/tvf/` | TVF 测试数据（上传到 HDFS）|
| `regression` | `scripts/data/regression/` | 特殊回归数据集（serde、分隔符等）|
| `test` | `scripts/data/test/` | 轻量级冒烟测试数据 |
| `preinstalled_hql` | `scripts/create_preinstalled_scripts/*.hql` | 约 77 个 HQL 文件，通过 `xargs -P` 并行执行 |
| `view` | `scripts/create_view_scripts/create_view.hql` | View 定义 |

### Layer 3 — 按版本自动选文件

启动脚本会按 Hive 版本自动选择正确的文件集合：

- Hive2 执行共享文件，以及 `bootstrap/hive2_only.*.list` 中列出的文件
- Hive3 执行共享文件，以及 `bootstrap/hive3_only.*.list` 中列出的文件

这是内部实现细节，开发者通常不需要手工配置。

---

## 状态存储：Docker 命名卷 + OSS Baseline

Hive 运行态（HDFS 数据、Postgres Metastore、模块 SHA 记录）存放在**每个版本 4 个 Docker 命名卷**中，不再使用宿主机 bind mount。共享卷前缀固定为 `doris-shared`。

| 卷 | 挂载位置 |
|---|---|
| `doris-shared-<hive_version>-namenode` | NameNode 元数据 |
| `doris-shared-<hive_version>-datanode` | DataNode 数据块 |
| `doris-shared-<hive_version>-pgdata` | Hive Metastore Postgres 数据 |
| `doris-shared-<hive_version>-state` | `/mnt/state` — 增量刷新用的各模块 SHA 文件 |

生命周期：
- `--hive-mode fast`：卷在多次运行间保留。
- `--hive-mode refresh`：卷会先被重置，再从已发布的 baseline tarball 恢复，然后再做模块刷新。
- `--hive-mode rebuild`：卷被删除（`docker volume rm`）后重建为空。

### Baseline 恢复

脚本会在两种情况下从预构建的 baseline tarball 恢复卷：

1. `--hive-mode refresh`：每次都先重置卷，再恢复已发布 baseline，然后按需对变化模块做 reconcile。
2. `--hive-mode fast`：仅当卷为空时（全新 CI 主机，或手动清理过后）才恢复 baseline。

恢复流程：

1. 先在 `${HIVE_BASELINE_TARBALL_CACHE:-docker/thirdparties/docker-compose/hive/scripts/baseline}/<hive_version>-baseline-<version>.tar.gz` 查找本地缓存。
2. 未命中缓存时，从 `https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/hive_baseline/<hive_version>-baseline-<version>.tar.gz` 下载。
3. 再查找 `${HIVE_BASELINE_TARBALL_CACHE:-docker/thirdparties/docker-compose/hive/scripts/baseline}/<hive_version>-baseline-<version>/` 这个解压缓存目录；如果不存在，就把 tarball 解压到这里一次。
4. 使用单个 `alpine tar` 容器把解压缓存目录恢复到 4 个卷中。
5. bump `HIVE_BASELINE_VERSION` 后，本地缓存文件名和自动拼接的 OSS URL 会同时变化，因此 CI 主机会重新下载新发布的 baseline，而不是复用旧缓存。

相关环境变量：

| 变量 | 默认值 | 作用 |
|---|---|---|
| `HIVE_BASELINE_TARBALL_CACHE` | `custom_settings.env` 中的 `docker/thirdparties/docker-compose/hive/scripts/baseline` | 下载 tarball 和解压 baseline 目录的本地缓存目录；缓存名称会带上 `HIVE_BASELINE_VERSION` |
| `HIVE_BASELINE_VERSION` | `custom_settings.env` 中的 `20260415` | baseline 发布的唯一版本变量：同时用于本地缓存文件名和自动拼接的 OSS tarball URL |

### 生成新的 baseline tarball

在一次完整 bootstrap 成功后，停止容器并运行：

```bash
sudo docker compose -p "${CONTAINER_UID}hive3" \
  -f docker/thirdparties/docker-compose/hive/hive-3x.yaml down

bash docker/thirdparties/docker-compose/hive/scripts/snapshot-hive-baseline.sh \
  "${CONTAINER_UID}hive3" /tmp/hive3-baseline.tar.gz
```

然后把得到的 tarball 上传到 `oss://<s3BucketName>/regression/datalake/pipeline_data/hive_baseline/hive3-baseline-<version>.tar.gz`（`hive2` 同理）。
发布新 baseline 时，只需要在 `docker/thirdparties/custom_settings.env` 中更新一次 `HIVE_BASELINE_VERSION`，随后按相同版本号生成并上传对应 tarball。

---

## 使用方式

### 启动 / 停止

```bash
# 启动 Hive3（默认为 refresh 模式）
./docker/thirdparties/run-thirdparties-docker.sh -c hive3

# 启动 Hive2
./docker/thirdparties/run-thirdparties-docker.sh -c hive2

# 同时启动两者
./docker/thirdparties/run-thirdparties-docker.sh -c hive2,hive3

# 停止 Hive3
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --stop
```

### 启动模式（`--hive-mode`）

| 模式 | 行为 | 适用场景 |
|---|---|---|
| `fast` | 复用已有卷；若 stack 已 healthy 则跳过 compose up；完全跳过数据刷新 | 机器重启或 Docker 重启后，想尽快把之前的 Hive 环境恢复起来 |
| `refresh` | 先把卷重置到已发布 baseline，再只重跑 SHA 发生变化的模块/HQL 文件 *(默认)* | 日常开发、PR 验证；改了 case 脚本或 HQL 后，希望先回到干净 baseline 再增量应用改动 |
| `rebuild` | 拆掉 stack，清空所有卷，不复用 baseline，从本地脚本完整重建 | 明确要忽略已发布 baseline，从当前仓库内容完整构建，通常用于准备导出新的 baseline tarball |

```bash
# fast：复用已有卷，在机器重启后快速恢复之前的 docker 环境
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode fast

# refresh：回到 baseline，并按需拾取 HQL/脚本变化（默认）
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh

# rebuild：从零开始完整重建，一般用于准备导出新的 baseline
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode rebuild
```

### 按模块限定刷新范围（`--hive-modules`）

只刷新关心的模块：

```bash
# 只重跑变化的 preinstalled HQL 文件（并行）
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
  --hive-mode refresh --hive-modules preinstalled_hql

# 刷新两个特定模块
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
  --hive-mode refresh --hive-modules default,multi_catalog

# 显式刷新所有模块
./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
  --hive-mode refresh --hive-modules all
```

每次 refresh 结束时，日志都会输出一份增量刷新摘要，说明这次实际重刷了哪些内容，例如：

```text
[hive-refresh] summary refreshed_modules=2 modules=multi_catalog,preinstalled_hql
[hive-refresh] summary details=multi_catalog:run_sh=74;preinstalled_hql:files=3(create_preinstalled_scripts/run40.hql,create_preinstalled_scripts/run69.hql,create_preinstalled_scripts/run76.hql)
```

---

## 开发者指南

### 什么时候用哪种模式？

- `fast`：机器或 Docker 服务刚重启，只想把之前的 Hive 容器和数据快速拉起来，不做任何刷新。
- `refresh`：正常开发默认用这个。改了 Hive case 数据、`run.sh`、HQL 后，用它在干净 published baseline 上增量应用改动。
- `rebuild`：刻意不使用 published baseline，而是从当前仓库状态完整 bootstrap，一般用于生成新的 baseline tarball 前的准备。

### 典型工作流

- 只改了少量 Hive HQL，想快速验证：
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh --hive-modules preinstalled_hql`
- 改了 `scripts/data/multi_catalog` 下的一小部分数据：
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh --hive-modules multi_catalog`
- 主机重启后恢复之前环境：
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode fast`
- 准备导出新的 baseline：
  `./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode rebuild`

### 如何添加测试数据

按数据存放方式，有两种模式。

#### 模式 A — `run.sh`（HDFS 数据 + DDL）

当测试数据文件需要上传到 HDFS 时使用这种模式。

1. 在合适的模块下新建目录：
   ```
   scripts/data/<module>/<your_dataset>/
   ├── run.sh          # 必需：模块刷新时被执行
   └── <data files>    # csv、parquet、orc 等
   ```

2. `run.sh` 必须是**幂等的**（反复运行不出问题）：
   ```bash
   #!/bin/bash
   set -x
   CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

   # 仅在 HDFS 上不存在时才上传
   hadoop fs -mkdir -p /user/doris/preinstalled_data/your_dataset
   if [[ -z "$(hadoop fs -ls /user/doris/preinstalled_data/your_dataset 2>/dev/null)" ]]; then
       hadoop fs -put "${CUR_DIR}"/data/* /user/doris/preinstalled_data/your_dataset/
   fi

   # 建表（drop 后再 create，保证幂等）
   hive -e "
     DROP TABLE IF EXISTS your_table;
     CREATE EXTERNAL TABLE your_table (...)
     STORED AS PARQUET
     LOCATION '/user/doris/preinstalled_data/your_dataset';
   "
   ```

3. 若仅供 Hive2 或 Hive3 使用，把 `run.sh` 的相对路径加入对应清单：
   ```
   bootstrap/hive2_only.run_sh.list
   bootstrap/hive3_only.run_sh.list
   ```

#### 模式 B — `create_preinstalled_scripts/`（仅 HQL）

适用于不需要上传 HDFS 文件的场景（指向已有 HDFS 数据的外部表，或通过 INSERT VALUES 写入内部表）。

1. 新建 `scripts/create_preinstalled_scripts/runNN.hql`：
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

2. 约定：
   - 始终先 `DROP TABLE IF EXISTS` 再 `CREATE` —— 不要只写 `CREATE IF NOT EXISTS`
   - 用下一个未占用的 `runNN` 编号
   - 仅 Hive2/Hive3 使用时，把相对路径加入 `bootstrap/hive2_only.preinstalled_hql.list` 或 `bootstrap/hive3_only.preinstalled_hql.list`
   - 若与 TPCH 相关，加入 `bootstrap/tpch.preinstalled_hql.list`

3. 触发一次刷新让它生效：
   ```bash
   ./docker/thirdparties/run-thirdparties-docker.sh -c hive3 \
     --hive-mode refresh --hive-modules preinstalled_hql
   ```

---

### 如何接入 HiveServer2 进行调试

所有容器都是 `network_mode: host`，端口在宿主机上可直接访问。

#### 容器内使用 beeline

```bash
# 进入 hive-server 容器
docker exec -it ${CONTAINER_UID}hive3-server bash

# 通过 beeline 连接（PATH 里的 hive shim 会自动走这里）
beeline -u "jdbc:hive2://localhost:13000/default" -n root

# 也可以直接用 hive 别名
hive -e "show databases;"
hive -e "show tables in default;"
hive -f /path/to/your.hql
```

#### 宿主机上使用 beeline

```bash
# 宿主机上的 beeline 已在 PATH 中；使用本地回环地址即可
beeline -u "jdbc:hive2://127.0.0.1:13000/default" -n root
```

#### 在容器外执行临时 HQL

```bash
# 执行单条查询
docker exec ${CONTAINER_UID}hive3-server \
  beeline -u "jdbc:hive2://localhost:13000/default" -n root \
  -e "SELECT * FROM default.your_table LIMIT 10;"

# 执行 HQL 文件（文件需在容器内或已挂载的路径下）
docker exec ${CONTAINER_UID}hive3-server \
  hive -f /mnt/scripts/create_preinstalled_scripts/run02.hql
```

#### 查看 HDFS

```bash
# 列出 HDFS 顶层目录
docker exec ${CONTAINER_UID}hadoop3-namenode \
  hadoop fs -ls /user/doris/

# 检查指定路径是否存在
docker exec ${CONTAINER_UID}hadoop3-namenode \
  hadoop fs -ls /user/doris/preinstalled_data/your_dataset/
```

#### 直连 Metastore PostgreSQL

```bash
# 直接连接 metastore 库（Hive3 是 5732 端口）
psql -h 127.0.0.1 -p 5732 -U postgres -d metastore \
  -c "SELECT TBL_NAME, DB_ID FROM TBLS LIMIT 20;"
```

---

## 日志与调试

| 日志文件 | 内容 |
|---|---|
| `docker/thirdparties/logs/start_hive3.log` | Hive3 完整启动日志 |
| `docker/thirdparties/logs/start_hive2.log` | Hive2 完整启动日志 |

开启详细 xtrace：

```bash
HIVE_DEBUG=1 ./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --hive-mode refresh
```

每个阶段结束时会打印耗时：
```
[14:02:31] [hive3] compose up done took=18s
[14:02:49] [hive3] init-hive-baseline begin
[14:03:11] [hive3] init-hive-baseline done took=22s
[14:03:11] [hive3] refresh-hive-modules begin (mode=refresh modules=all)
[14:05:44] [hive3] refresh-hive-modules done took=153s
```

---

## 故障排查

**Metastore 健康检查失败**
- 确认 `${CONTAINER_UID}hive3-metastore-postgresql` 已 healthy：`docker ps`
- 查看启动日志：`tail -100 docker/thirdparties/logs/start_hive3.log`

**HiveServer2 连不上**
- 检查容器是否在运行：`docker ps | grep hive3-server`
- 测试端口：`nc -z 127.0.0.1 13000`
- 查看容器内 HS2 日志：`docker exec ${CONTAINER_UID}hive3-server tail -50 /tmp/hive-server2.log`

**JuiceFS format/init 失败**
- 确认 `JFS_CLUSTER_META` 可达（默认为 `mysql://root:123456@(127.0.0.1:3316)/juicefs_meta`）
- 视需要 override：`export JFS_CLUSTER_META=<your_uri>`

**Refresh 明显变慢**
- 看是哪些模块被重跑；若全都在跑，说明 SHA 不匹配，走了完整刷新
- 收窄范围：`--hive-modules preinstalled_hql`
- 结合上面的耗时日志定位慢阶段

**容器被硬杀后状态残留**
- state 目录可能写了一半；使用 `--hive-mode rebuild` 重置干净

**Baseline 下载慢或失败**
- 确认能访问 `https://${s3BucketName}.${s3Endpoint}/regression/datalake/pipeline_data/hive_baseline/`
- 手动把 tarball 放到 `${HIVE_BASELINE_TARBALL_CACHE:-docker/thirdparties/docker-compose/hive/scripts/baseline}/<hive_version>-baseline-<version>.tar.gz` 即可跳过下载
- 确认 `docker/thirdparties/custom_settings.env` 中的 `s3BucketName` 和 `s3Endpoint` 设置正确

**手动查看或删除卷**
```bash
# 列出某个版本的 4 个卷
docker volume ls | grep "${CONTAINER_UID}hive3-"

# 删除全部 4 个（等价于 --hive-mode rebuild 的清理步骤）
for s in namenode datanode pgdata state; do
  docker volume rm -f "${CONTAINER_UID}hive3-${s}"
done
```
