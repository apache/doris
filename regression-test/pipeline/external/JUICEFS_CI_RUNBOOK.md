# JuiceFS (MySQL Meta) 在 CI 跑通实施文档

本文档用于把 Doris 的 `HMS Catalog + jfs://` 链路稳定接入 CI，并保证可自动完成初始化、回归验证和排障。

## 1. 目标与验收标准

目标：在 CI 机器上自动跑通以下链路：

1. 启动 Hive3 Metastore（带 JuiceFS Hadoop JAR）。
2. 使用 MySQL 作为 JuiceFS 元数据引擎（非 Redis）。
3. 自动完成 JuiceFS `format`（幂等）。
4. 回归用例可完成：`create catalog -> create db -> create table -> insert -> select`。

验收标准：

1. `test_jfs_hms_catalog_read` 用例通过。
2. 日志中可见 `LOCATION 'jfs://cluster/...`。
3. 不依赖人工执行 `juicefs format`。

## 2. 依赖关系与最小服务

最小依赖：

1. Doris FE/BE（包含 JuiceFS 相关代码与 JAR 加载逻辑）。
2. Hive Metastore（HMS）。
3. MySQL（JuiceFS metadata，建议复用 pipeline 的 mysql57）。
4. 本地文件 bucket 目录（例如 `/tmp/jfs-bucket`，仅用于 CI 快速验证）。

说明：

1. JuiceFS 本身不是常驻“服务进程”，核心依赖是 metadata（这里用 MySQL）+ storage backend（这里用 file bucket）。
2. 如果线上/生产改为对象存储，替换 `format --storage` 与 bucket 参数即可。

## 3. 建议纳入仓库的改动点

以下是 CI 稳定性相关的改动建议（部分已完成，部分可按需补齐）：

1. `docker/thirdparties/docker-compose/hive/scripts/hive-metastore.sh`
   - 确保将 `auxlib/*.jar` 同时拷贝到：
   - `/opt/hive/lib`
   - `/opt/hadoop-3.2.1/share/hadoop/common/lib`（若存在）
   - `/opt/hadoop/share/hadoop/common/lib`（若存在）
   - 目的：`hive --service metastore` 与 `hadoop fs` 均能识别 `io.juicefs.JuiceFileSystem`。

2. `docker/thirdparties/docker-compose/hive/hadoop-hive.env.tpl`
   - 将硬编码的 `CORE_CONF_juicefs_cluster_meta=redis://...` 改为可注入变量（例如 `${JFS_CLUSTER_META}`）。

3. `docker/thirdparties/docker-compose/hive/hadoop-hive-3x.env.tpl`
   - 将 `HIVE_SITE_CONF_juicefs_cluster_meta=redis://...` 改为 `${JFS_CLUSTER_META}`。

4. `docker/thirdparties/docker-compose/hive/hive-3x_settings.env`
   - 增加默认值（例如 `export JFS_CLUSTER_META="redis://127.0.0.1:6379/1"`），保证不影响旧链路。

5. 回归用例保持参数化（已满足）：
   - `regression-test/suites/external_table_p0/refactor_storage_param/test_jfs_hms_catalog_read.groovy`

注意：不要在仓库中写死 MySQL 账号密码，CI 使用 Secret 注入。

## 4. CI 变量设计（推荐）

建议在 CI 平台配置以下变量（Secret/普通变量分开）：

```bash
# 普通变量
JFS_VOLUME=cluster
JFS_BUCKET=/tmp/jfs-bucket
JFS_STORAGE=file
JFS_ENABLE=true

# Secret 变量（示例）
JFS_META_DSN=mysql://juicefs_user:${JFS_META_PASSWORD}@(127.0.0.1:3316)/juicefs_meta
JFS_META_PASSWORD=xxxxxx
MYSQL_ROOT_PASSWORD=123456
```

## 5. CI 执行步骤（可直接落地）

下面脚本可作为“external pipeline 的前置阶段”。
如果你希望“一条命令执行完整链路”（prepare + hive3 + 回归），可直接调用：

```bash
teamcity_build_checkoutDir=/path/to/doris \
JFS_META_DSN="mysql://juicefs_user:${JFS_META_PASSWORD}@(127.0.0.1:3316)/juicefs_meta" \
bash /path/to/doris/regression-test/pipeline/external/run_juicefs_ci.sh
```

`run_juicefs_ci.sh` 会内部调用 `prepare_juicefs_ci.sh`。
当前实现会只重建 `hive-metastore`，避免共享机器上 `hdfs datanode(9866)` 端口冲突。
`run_juicefs_ci.sh` 还支持 `JFS_REGRESSION_CONF`：

1. 本地默认：`regression-test/conf/regression-conf.groovy`
2. CI 默认：`regression-test/pipeline/external/conf/regression-conf.groovy`

可显式覆盖：

```bash
export JFS_REGRESSION_CONF=/path/to/regression-conf.groovy
```

也可以直接调用仓库脚本：

```bash
JFS_META_DSN="mysql://juicefs_user:${JFS_META_PASSWORD}@(127.0.0.1:3316)/juicefs_meta" \
JFS_VOLUME=cluster \
JFS_BUCKET=/tmp/jfs-bucket \
bash regression-test/pipeline/external/prepare_juicefs_ci.sh
```

### 5.1 准备 MySQL metadata（幂等）

```bash
set -euo pipefail

mysql -h127.0.0.1 -P3316 -uroot -p"${MYSQL_ROOT_PASSWORD}" -e "
CREATE DATABASE IF NOT EXISTS juicefs_meta;
CREATE USER IF NOT EXISTS 'juicefs_user'@'%' IDENTIFIED BY '${JFS_META_PASSWORD}';
GRANT ALL ON juicefs_meta.* TO 'juicefs_user'@'%';
FLUSH PRIVILEGES;"
```

### 5.2 准备本地 bucket 目录

```bash
sudo mkdir -p "${JFS_BUCKET}"
sudo chmod 777 "${JFS_BUCKET}"
```

### 5.3 获取 JuiceFS CLI（二选一）

方式 A（推荐）：CI 镜像预装 `juicefs`。  
方式 B（临时下载）：

```bash
curl -sSL https://d.juicefs.com/install | sh -s -- /tmp/jfs-bin
/tmp/jfs-bin/juicefs version
```

### 5.4 幂等 format（关键）

```bash
set -euo pipefail

JFS_CLI=/tmp/jfs-bin/juicefs

if ! "${JFS_CLI}" status "${JFS_META_DSN}" >/tmp/jfs.status 2>&1; then
  if grep -q "database is not formatted" /tmp/jfs.status; then
    "${JFS_CLI}" format \
      --storage "${JFS_STORAGE}" \
      --bucket "${JFS_BUCKET}" \
      "${JFS_META_DSN}" \
      "${JFS_VOLUME}"
  else
    cat /tmp/jfs.status
    exit 1
  fi
fi
```

### 5.5 启动 Hive3 并注入 JuiceFS Meta

如果模板已经支持 `${JFS_CLUSTER_META}`：

```bash
export JFS_CLUSTER_META="${JFS_META_DSN}"
bash ./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --no-load-data
```

如果模板尚未改造（短期兜底）：

```bash
bash ./docker/thirdparties/run-thirdparties-docker.sh -c hive3 --no-load-data

echo "CORE_CONF_juicefs_cluster_meta=${JFS_META_DSN}" >> docker/thirdparties/docker-compose/hive/hadoop-hive-3x.env
echo "HIVE_SITE_CONF_juicefs_cluster_meta=${JFS_META_DSN}" >> docker/thirdparties/docker-compose/hive/hadoop-hive-3x.env

sudo docker compose -p ${CONTAINER_UID}hive3 \
  -f docker/thirdparties/docker-compose/hive/hive-3x.yaml \
  --env-file docker/thirdparties/docker-compose/hive/hadoop-hive-3x.env \
  up -d --force-recreate hive-metastore
```

### 5.6 烟囱校验（HMS 容器内）

```bash
sudo docker exec ${CONTAINER_UID}hive3-metastore bash -lc \
"/opt/hadoop-3.2.1/bin/hadoop fs \
 -Dfs.jfs.impl=io.juicefs.JuiceFileSystem \
 -Djuicefs.cluster.meta='${JFS_META_DSN}' \
 -ls jfs://cluster/"
```

预期：命令返回 0（目录为空也可接受）。

### 5.7 运行回归用例

```bash
./run-regression-test.sh --run \
  -d external_table_p0/refactor_storage_param \
  -s test_jfs_hms_catalog_read \
  -conf enableJfsTest=true \
  -conf enableHiveTest=true \
  -conf externalEnvIp=127.0.0.1 \
  -conf hive2HmsPort=9383 \
  -conf jfsHiveMetastoreUris=thrift://127.0.0.1:9383 \
  -conf jfsFs=jfs://cluster \
  -conf jfsMeta="${JFS_META_DSN}"
```

## 6. 常见失败与处理

1. `Class io.juicefs.JuiceFileSystem not found`
   - 原因：JAR 只进了 Hive classpath，没进 Hadoop classpath。
   - 处理：确认 `hive-metastore.sh` 同时复制到 `/opt/hadoop*/share/hadoop/common/lib`。

2. `JuiceFS initialized failed for jfs://...`
   - 原因：元数据 DSN 不可达、未 format、volume 名不合法。
   - 处理：
   - 校验 DSN 连通性；
   - 重新执行幂等 `status/format`；
   - volume 名必须 `[A-Za-z0-9-]`，长度 3~63。

3. `name is required`（HMS create database 阶段）
   - 常见原因：volume 名不合法或 metastore 未拿到正确 `juicefs.<volume>.meta`。

4. `dial tcp ...:6379 timeout`
   - 原因：仍在用 Redis 默认配置。
   - 处理：确认 `hadoop-hive-3x.env` 最终生效值已改为 MySQL DSN，并重建 metastore。

5. `Address already in use`（datanode 9866/9864）
   - 原因：端口冲突。
   - 处理：先停冲突容器/进程，或隔离 CONTAINER_UID 与端口。

## 7. 安全与提交规范

1. 禁止提交真实密钥、密码、完整 DSN。
2. 仅提交模板与脚本逻辑；真实值走 CI Secret。
3. 不提交本地运行文件（例如 `regression-test/conf/regression-conf.groovy`、个人启动脚本）。
4. Docker 本地调试改动如果仅用于个人环境，不纳入 PR。

## 8. 最终建议

建议按“两阶段”推进：

1. 阶段 1（快速可用）：在 CI 前置脚本里做幂等 format + 覆盖 `hadoop-hive-3x.env` + 重启 metastore。
2. 阶段 2（长期稳定）：将 `redis://...` 模板改为变量注入（`${JFS_CLUSTER_META}`），彻底移除硬编码。

---

本地已验证：`catalog -> db -> table -> insert -> select` 在 `MySQL metadata + jfs://` 链路可正常通过。
