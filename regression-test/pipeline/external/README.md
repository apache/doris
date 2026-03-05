## External Pipeline (JuiceFS)

新增 `run_juicefs_ci.sh` 用于在 CI 上一键跑通 JuiceFS 回归链路：

1. 幂等初始化 JuiceFS metadata（`status`/`format`）
2. 以 `JFS_CLUSTER_META` 注入方式重建 `hive3-metastore`（不重启整套 hdfs）
3. 执行 `test_jfs_hms_catalog_read`

### 最小调用

```bash
export teamcity_build_checkoutDir=/path/to/doris
export JFS_META_DSN="mysql://juicefs_user:xxx@(127.0.0.1:3316)/juicefs_meta"

cd "${teamcity_build_checkoutDir}/regression-test/pipeline/external"
bash run_juicefs_ci.sh
```

### 可选变量

1. `JFS_VOLUME`，默认 `cluster`
2. `JFS_BUCKET`，默认 `/tmp/jfs-bucket`
3. `JFS_STORAGE`，默认 `file`
4. `JFS_HMS_PORT`，默认 `9383`
5. `JFS_RECREATE_METASTORE`，默认 `true`
6. `JFS_REGRESSION_CONF`，可手工指定回归配置文件
7. `JFS_TEST_DIR`，默认 `external_table_p0/refactor_storage_param`
8. `JFS_TEST_SUITE`，默认 `test_jfs_hms_catalog_read`

如果你要“尽量复用”现有容器，设置：

```bash
export JFS_RECREATE_METASTORE=false
```

默认行为：

1. 本地执行（未设置 `teamcity_build_checkoutDir`）：
   - 默认使用 `regression-test/conf/regression-conf.groovy`（一般是 `127.0.0.1:9030`）。
2. TeamCity 执行（设置了 `teamcity_build_checkoutDir`）：
   - 默认使用 `regression-test/pipeline/external/conf/regression-conf.groovy`。

详细说明见 [JUICEFS_CI_RUNBOOK.md](./JUICEFS_CI_RUNBOOK.md)。
