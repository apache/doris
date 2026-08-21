# Doris Iceberg 能力矩阵

## 1. 范围和结论

本文记录 Apache Doris 当前 Iceberg 连接器的能力边界，以及本仓库现有测试证据。矩阵中的“已验证”只表示对应环境和路径已经有可追溯证据，不等同于所有 Iceberg Catalog、云存储或表格式版本都保证支持。

当前环境的验证范围是：

- Doris PR worktree 的单 FE/单 BE 集群；
- Databricks Unity Catalog 的 Iceberg REST Catalog；
- Azure ADLS `abfss` 数据位置；
- Databricks OAuth 与 vended credentials；
- 2026-08-20 至 2026-08-21 的本地 ASAN 构建和本 PR 当前工作树。

当前可以确认：Doris 已能通过 Databricks REST Catalog 获取 vended Azure 凭据，并使用 Hadoop ABFS reader 读写 Iceberg 表。Catalog、namespace、表发现、snapshot time travel、基本查询，以及 `INSERT`、`INSERT OVERWRITE`、format v3 `DELETE`、`UPDATE` 和 `MERGE INTO` 均已在隔离测试表跑通。

## 2. 证据等级

| 标记 | 含义 |
| --- | --- |
| E2E | 在本文指定的 Databricks + Azure 环境中实际执行并得到结果 |
| UT | 连接器或 FE 单元测试覆盖；不代表云端 E2E 已通过 |
| REG | 仓库 regression suite 有对应场景；是否在当前环境运行需另行确认 |
| CODE | 连接器代码声明或实现了该能力，但本文没有足够运行证据 |
| E2E-FAIL | 在当前环境实际执行，但由 Doris 或远端 Catalog 明确拒绝；不能按“已支持”处理 |
| UNSUPPORTED-CURRENT | 当前 Doris/Catalog 路径已明确不支持；不代表其他 Catalog 永远不支持 |
| UNVERIFIED-PERMISSION | 当前身份或 Catalog 权限阻止了验证；不等同于功能不支持 |
| UNVERIFIED-FIXTURE | 需要专用表、凭据故障注入或其他当前环境没有的验证夹具 |
| OUT-OF-SCOPE | 不属于本轮 Azure Databricks 矩阵范围 |
| N/A | 当前连接器没有声明该通用能力，不能按支持处理 |

## 2.1 2026-08-21 Azure E2E 补充

以下结果来自本 PR worktree 的本地 ASAN FE/BE（FE query port `11030`），Catalog 为 `dbx_azure_iceberg`，数据位置为 Azure ADLS `abfss`。SQL 中的真实 OAuth token 不记录在本文。

| 验证项 | 状态 | 实测结果 |
| --- | --- | --- |
| namespace 创建 | E2E | `CREATE DATABASE IF NOT EXISTS dbx_azure_iceberg.matrix_full_20260821` 成功。 |
| table 创建 | UNVERIFIED-PERMISSION | `CREATE TABLE` 被 Databricks 返回 `Forbidden: Not authorized to make this request`；当前服务主体没有创建 Unity Catalog managed table 的权限。 |
| 基本读取 | E2E | `matrix_dml_v3_20260820` `COUNT(*)=6`，按 `id` 过滤/排序返回完整行。 |
| snapshot time travel | E2E | `$snapshots` 返回 7 个 snapshot；`FOR VERSION AS OF 8812198063549693940` 和 `FOR TIME AS OF '2026-08-20 21:45:15'` 均返回历史 `alice/bob/carol` 三行。 |
| Iceberg system tables | E2E | `$entries`、`$all_entries`、`$files`、`$data_files`、`$delete_files`、`$all_files`、`$all_data_files`、`$all_delete_files`、`$history`、`$metadata_log_entries`、`$snapshots`、`$refs`、`$manifests`、`$all_manifests`、`$partitions` 均可读。 |
| position delete / deletion vector | E2E | `$position_deletes` 返回 3 条记录，数据文件和 Puffin delete 文件均为 `abfss://`；`$delete_files` 返回 1 个 DV 文件。 |
| identity partition pruning | E2E | `WHERE p=2` 返回 2 行，`EXPLAIN VERBOSE` 为 `inputSplitNum=1`、`partition=1/3`；`p IN (1,3)` 返回 3 行。 |
| runtime-filter join | E2E | 分区表与单行 key 表 join 只返回 `p=2` 的 2 行；静态计划包含 3 个分区 scan，运行结果正确。 |
| nested projection/filter | E2E | STRUCT、ARRAY<STRUCT>、MAP<STRING,STRUCT> 子字段投影和 `info.metric >= 10` 过滤均成功；计划显示 `info.label` 的子列访问路径。 |
| Top-N | E2E / CODE | `ORDER BY age DESC LIMIT 3` 返回正确结果，计划包含 `VTOP-N` 与 `isTopMaterializeNode: true`；这是查询行为证据，不替代性能基准。 |
| branch/tag | E2E-FAIL | `CREATE BRANCH` 和 `CREATE TAG` 均被 Databricks 返回 `Branching or tagging is not allowed`；当前 Unity Catalog REST 表不能按 branch/tag 访问。 |
| view | E2E-FAIL | `CREATE VIEW ...` 被 Doris 拒绝：`External catalog ... is not allowed in CreateViewCommand`。 |
| `rewrite_manifests` | E2E | 在现有隔离表执行成功，返回 `6 0`。 |
| `remove_orphan_files` | E2E-FAIL | 当前表属性 `gc.enabled=false`，执行被拒绝：`Cannot remove orphan files: Iceberg GC is disabled`。 |

## 3. 接入与存储

| 能力 | 当前状态 | 证据和边界 |
| --- | --- | --- |
| Iceberg REST Catalog | E2E | `SHOW CATALOGS` 能看到 `dbx_azure_iceberg`；Catalog 使用 Databricks Unity Catalog REST endpoint。 |
| OAuth 访问 REST Catalog | E2E | 现有 Catalog 配置为 REST OAuth；凭据值不记录在本文。 |
| Databricks vended credentials | E2E | `iceberg.rest.vended-credentials-enabled=true`；查询 Azure 表返回真实数据。 |
| Azure ADLS `abfss` 文件读取 | E2E | 本 PR 修复的路径：vended SAS 经过 Hadoop ABFS 配置并由 `FILE_HDFS` reader 使用。 |
| vended credential 401/过期后的重认证 | CODE / UNVERIFIED-FIXTURE | `ReauthenticatingRestSessionCatalog` 有重认证实现；当前环境没有可控的过期凭据/401 故障注入夹具。 |
| 用户会话隔离 | CODE / UNVERIFIED-FIXTURE | `SUPPORTS_USER_SESSION` 仅在 `iceberg.rest.session=user` 时声明；当前环境没有第二个用户会话和可比对的缓存夹具。 |
| S3/GCS/HDFS 等其他存储后端 | REG / OUT-OF-SCOPE | 仓库存在对应 Iceberg 场景，但本轮只验证 Azure vended credentials。 |

## 4. 查询与元数据

| 能力 | 当前状态 | 代码/测试入口 | 下一步 |
| --- | --- | --- | --- |
| Catalog、namespace、table 发现 | E2E | 当前环境 `SHOW DATABASES`、`SHOW TABLES` | 保持为每次 E2E 的冒烟检查 |
| 基本投影、过滤、排序、聚合 | E2E | 当前环境 `COUNT(*)` 与 `WHERE id >= 2 ORDER BY id` | 增加结果快照和查询计划记录 |
| MVCC / snapshot time travel | E2E / CODE / REG | `SUPPORTS_MVCC_SNAPSHOT`；当前表 `$snapshots` 返回 7 个 snapshot，`FOR VERSION AS OF 8812198063549693940` 和 `FOR TIME AS OF` 均返回历史三行；`test_iceberg_time_travel.groovy` | 增加 schema evolution 后的历史读 |
| Branch / Tag 查询 | E2E-FAIL / REG | branch/tag regression suite；当前 Databricks 返回 `Branching or tagging is not allowed` | 需要支持 branching/tagging 的 Catalog 才能继续验证 Doris 语义 |
| Partition pruning | E2E / REG | 隔离表 `matrix_partition_prune_2304` 有 `p=1/2/3` 三个分区；`WHERE p=2` 的 `EXPLAIN VERBOSE` 显示 `inputSplitNum=1`、`partition=1/3`，`WHERE p IN (1,3)` 显示两个 split、`partition=2/3`，结果分别返回对应行；`test_iceberg_runtime_filter_partition_pruning*.groovy` | 增加 transform partition 和分区演进后的裁剪证据 |
| Runtime filter | E2E / REG | `matrix_partition_prune_2304` 与单行 key 表 join 时，静态计划包含 3 个 range，执行 profile 显示 `RuntimeFilterPartitionPrunedRangeNum=2`、RF input rows 5/filtered rows 2，最终只返回 `p=2` 的两行；`test_iceberg_runtime_filter_partition_pruning*.groovy` | 增加 transform partition、分区演进和 delete-aware 场景 |
| Nested column pruning | E2E / CODE / REG | `info.label`、`events.*.score` 和 `attrs.*.code` 分别把 STRUCT、ARRAY<STRUCT>、MAP<STRING,STRUCT> 裁成只含目标子字段的类型，结果正确；证据表为 `matrix_nested_*_prune_2304`；`SUPPORTS_NESTED_COLUMN_PRUNE` 和 nested schema suites | 补充 schema evolution 后的裁剪 |
| Position delete | E2E / REG / UNVERIFIED-FIXTURE | Azure v3 表的 `$position_deletes` 返回 3 条 Puffin deletion-vector 位置记录，`file_path`/`delete_file_path` 均为 `abfss://`；传统 v2 Parquet position-delete 需要兼容的远端 fixture。 |
| Equality delete | REG / UNVERIFIED-FIXTURE | `test_iceberg_equality_delete*.groovy`；当前 Azure 表只有 DV/position-delete 证据，没有可控的 equality-delete fixture。 |
| Deletion vector / row lineage | E2E / REG | format v3 表先执行 `DELETE` 产生 DV，再执行 `UPDATE`、`MERGE INTO` 和 `$position_deletes` 读取，均正确处理已有 DV；deletion-vector 和 v3 row-lineage suites | 增加多 DV、并发提交和大批量删除场景 |
| Iceberg system tables | E2E / REG | `$entries`、`$all_entries`、`$files`、`$data_files`、`$delete_files`、`$all_files`、`$all_data_files`、`$all_delete_files`、`$history`、`$metadata_log_entries`、`$snapshots`、`$refs`、`$position_deletes`、`$manifests`、`$all_manifests` 和 `$partitions` 均可读；`$files`/`$delete_files` 返回 Azure Parquet/Puffin 路径；`test_iceberg_sys_table*.groovy` | 继续补充 equality delete 和多表格式版本 |
| `SHOW CREATE TABLE/DATABASE` | E2E / CODE / REG | `SUPPORTS_SHOW_CREATE_DDL`；当前 `SHOW CREATE TABLE` 和 `SHOW CREATE DATABASE` 均成功，LOCATION/PROPERTIES 可见，OAuth token 仍被 `*XXX` 掩码；`test_iceberg_show_create.groovy` | 增加更多敏感属性组合 |
| View | E2E-FAIL / CODE / REG | `SUPPORTS_VIEW` 虽由通用连接器能力声明，但当前 Doris 明确禁止在 external catalog 执行 `CREATE VIEW`；REST view suite 也被禁用 | 需要 Doris 外部 Catalog view 语义或独立 view-capable Catalog |
| Metadata preload | CODE | `SUPPORTS_METADATA_PRELOAD` | 这是并发/锁延迟优化，不作为第一批功能 E2E |
| Top-N lazy materialization | E2E / CODE / REG | `SUPPORTS_TOPN_LAZY_MATERIALIZE`；真实 `ORDER BY age DESC LIMIT 3` 计划包含 `VTOP-N` 和 `isTopMaterializeNode: true`；TVF/Top-N suites | 仍需独立性能基准，不把单次计划当作性能结论 |

## 5. 写入、DDL 和管理操作

### 5.1 写入路径

`IcebergWritePlanProvider` 当前将写操作路由到以下 sink：

| 操作 | 当前状态 | 证据 |
| --- | --- | --- |
| `INSERT` | E2E / REG | `matrix_write_20260820` 通过 `VALUES` 写入 `id=13`、通过 `INSERT ... SELECT` 写入 `id=22`；保留的 v3 Azure 表 `matrix_dml_v3_20260820` 还验证了 `id=9001`，以及修复后用 `VALUES` 写入 `id=99001`、用 `INSERT ... SELECT` 写入 `id=99002`，按主键查询均得到完整行。 |
| `INSERT OVERWRITE` | E2E / REG | 同一隔离表 overwrite 后仅保留 `id=20/21`，结果符合替换语义。 |
| `DELETE` | E2E / REG | format v3 表删除 `id=2` 后仅剩 `id=1/3`；format v2 被远端按“delete files 需要 v3”拒绝，属于表格式前置条件。 |
| `UPDATE` | E2E / UT / REG | 新表 UPDATE 通过；已有 DELETE DV 的 `matrix_dml_v3_20260820` 更新 `id=1` 后得到 `alice_after_fix/28`。 |
| `MERGE INTO` | E2E / UT / REG | 在同一张已有 DELETE 和 UPDATE DV 的表上更新 `id=3`、插入 `id=5`，最终得到三行预期结果。 |
| `rewrite_data_files` | UNSUPPORTED-CURRENT / CODE / REG | 连接器单测明确标记为 “advertised but not yet executable”；对应 action regression 存在，但当前路径不能按支持处理。 |

第一次在已有 v3 DV 上执行 `UPDATE`/`MERGE INTO` 时，BE 的 DV reader 退化为打开空 authority 的 `hdfs://`。普通 scan range 会单独携带 `fs_name`，但 sink 侧 DV helper 只能从 Hadoop 配置读取 `fs.defaultFS`；Databricks vended SAS 配置只包含 account，不包含 container，因此此前没有该键。本 PR 从实际数据位置提取 `abfss://container@account.dfs.core.windows.net` 写入 `fs.defaultFS`，新增 FE 单元测试，并用上述 DELETE -> UPDATE -> MERGE E2E 顺序验证修复。

`$position_deletes` 的 native range 还需要显式携带 BE file type。此前 FE 没有把 Azure `abfss` 的 `FILE_HDFS` 传给 BE，`LocationPath` 默认按 S3 路由，导致 Puffin 文件报 `Invalid S3 URI`。现在 FE 按表位置和本次 vended token 解析 backend file type 写入每个 range；同一 Azure 表的 `$position_deletes` 已返回 3 条 DV 位置记录。

`$files` 的 metadata scanner 通过父优先的 JNI classloader 使用共享 preload jar 中的 Iceberg 类。此前 Azure `ADLSFileIO` 只存在于 FE 依赖，BE scanner 和 preload jar 都没有打包该实现，运行时因此报 `ClassNotFoundException`。现在两个 BE 扩展均显式打包 Iceberg Azure FileIO；类路径单元测试和本地 E2E 均通过，写入后 `$files` 仍能列出新增数据文件。

本轮保留了 `matrix_write_20260820`、`matrix_dml_v3_20260820`、`matrix_update_fresh_20260820` 和 `matrix_merge_fresh_20260820` 供复查，没有修改或清理现有 `managed_iceberg`。

### 5.2 DDL 和表演进

| 操作 | 当前状态 | 证据 |
| --- | --- | --- |
| 创建/删除 namespace、table | E2E / UNVERIFIED-PERMISSION / REG | namespace 创建成功；新 table 创建被 Databricks `Forbidden` 拒绝，因此 table create/drop 不能在当前身份下判定支持与否。 |
| Add/rename/modify column | E2E / REG | `matrix_update_fresh_20260820` 顶层 ADD、RENAME、MODIFY 成功；`matrix_nested_prune_2304` 的 `info.*` 嵌套 ADD、RENAME、MODIFY 也成功。 |
| Drop column | E2E-FAIL / REG | 顶层和嵌套 DROP 均被 Databricks 返回 `Table should have one unpartitioned spec`；当前表均有 Unity Catalog 生成的 partition spec，暂按当前远端限制记录。 |
| Partition evolution | E2E / REG | `matrix_write_20260820` 成功执行 `ADD PARTITION KEY dt`、写入 `id=99003`，`$partitions` 出现 `spec_id=1`；随后 DROP PARTITION KEY 也成功。当前只验证 identity partition。 |
| Sort order on create | UNVERIFIED-PERMISSION / CODE | `SUPPORTS_SORT_ORDER` 已声明，但创建新表被同一 Databricks 权限拒绝，暂无 Azure E2E 结果。 |
| Branch/tag create/drop | E2E-FAIL / REG | 当前 Databricks 明确拒绝 create branch/tag；因此 drop 和 branch/tag retention 也不再声称支持。 |

### 5.3 `ALTER TABLE ... EXECUTE`

当前 `IcebergExecuteActionFactory` 导出以下 10 个操作，其中 `rewrite_data_files` 是分布式执行，其余为单次调用：

```text
rollback_to_snapshot
rollback_to_timestamp
set_current_snapshot
cherrypick_snapshot
fast_forward
expire_snapshots
rewrite_data_files
publish_changes
rewrite_manifests
remove_orphan_files
```

这些操作不能统一标成已支持：`rewrite_manifests` 已在当前表成功并返回 `6 0`；`remove_orphan_files` 因 `gc.enabled=false` 被拒绝；`rewrite_data_files` 当前路径明确尚未可执行。rollback、set-current、cherrypick、fast-forward、expire 和 publish 需要专用表及可回滚权限，当前身份无法创建新表，因此保持 `CODE / REG / UNVERIFIED-FIXTURE`，不能用现有业务表冒险验证。

## 6. 明确的未声明能力

以下能力不能因为 Iceberg 支持其他功能就自动推断为支持：

| 能力 | 当前结论 |
| --- | --- |
| `SUPPORTS_PARTITION_STATS` | N/A；Iceberg connector 没有声明该 capability。 |
| `SUPPORTS_SAMPLE_ANALYZE` | N/A；Iceberg connector 没有声明，源码说明 native Iceberg 的 sample analyze 未实现。 |
| `SUPPORTS_SCAN_PARAM_OPTIONS` | N/A；当前 connector 没有声明通用 `@options` scan-param 能力。 |
| 静态凭据跨用户共享 | 不应推断支持；用户会话能力只对特定 REST 配置启用，缓存隔离必须按配置验证。 |

“N/A”表示当前通用 SPI 没有声明该能力，不表示 Iceberg 格式本身永远不能实现它。

## 7. 当前环境边界

本轮已经完成当前 Azure Databricks 环境中可执行的能力验证。本节只解释为什么少数条目无法在同一环境判定，不表示本轮验证未完成：

| 状态 | 当前边界 |
| --- | --- |
| `UNVERIFIED-PERMISSION` | 创建 Unity Catalog table 和 sort order 需要 Databricks 表创建权限；当前服务主体被 `Forbidden` 拒绝。 |
| `UNVERIFIED-FIXTURE` | vended credential 401 重认证、第二用户会话、传统 v2 position/equality delete，以及 rollback/expire 等破坏性 action 需要专用 fixture 或故障注入。 |
| `OUT-OF-SCOPE` | S3/GCS/HDFS 等非 Azure 存储后端不属于本轮矩阵。 |
| `UNSUPPORTED-CURRENT` / `E2E-FAIL` | branch/tag、external catalog view、DROP COLUMN、`rewrite_data_files` 和 GC 关闭时的 `remove_orphan_files` 已有明确拒绝结果，按当前路径不支持记录。 |

每个矩阵条目均保留了 Doris commit、Catalog 类型、云存储、执行 SQL、结果和异常（如有）；只有源码或通用 regression 证据的条目不会被写成 Azure E2E 已支持。

## 8. 主要代码和测试来源

- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java`
- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnectorMetadata.java`
- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWritePlanProvider.java`
- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergProcedureOps.java`
- `fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/action/IcebergExecuteActionFactory.java`
- `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorCapability.java`
- `regression-test/suites/external_table_p0/iceberg/`
- `regression-test/suites/external_table_p0/iceberg/dml/`
- `regression-test/suites/external_table_p0/iceberg/action/`
- `regression-test/suites/external_table_p0/iceberg/branch_tag/`
