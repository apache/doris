# Doris Iceberg 能力矩阵

## 1. 范围和结论

本文记录 Apache Doris 当前 Iceberg 连接器的能力边界，以及本仓库现有测试证据。矩阵中的“已验证”只表示对应环境和路径已经有可追溯证据，不等同于所有 Iceberg Catalog、云存储或表格式版本都保证支持。

当前环境的验证范围是：

- Doris PR worktree 的单 FE/单 BE 集群；
- Databricks Unity Catalog 的 Iceberg REST Catalog；
- Azure ADLS `abfss` 数据位置；
- Databricks OAuth 与 vended credentials；
- 2026-08-20 的本地 ASAN 构建和本 PR 当前工作树。

当前可以确认：Doris 已能通过 Databricks REST Catalog 获取 vended Azure 凭据，并使用 Hadoop ABFS reader 读写 Iceberg 表。Catalog、namespace、表发现、snapshot time travel、基本查询，以及 `INSERT`、`INSERT OVERWRITE`、format v3 `DELETE`、`UPDATE` 和 `MERGE INTO` 均已在隔离测试表跑通。

## 2. 证据等级

| 标记 | 含义 |
| --- | --- |
| E2E | 在本文指定的 Databricks + Azure 环境中实际执行并得到结果 |
| UT | 连接器或 FE 单元测试覆盖；不代表云端 E2E 已通过 |
| REG | 仓库 regression suite 有对应场景；是否在当前环境运行需另行确认 |
| CODE | 连接器代码声明或实现了该能力，但本文没有足够运行证据 |
| TODO | 下一轮需要在隔离 namespace/table 中验证 |
| N/A | 当前连接器没有声明该通用能力，不能按支持处理 |

## 3. 接入与存储

| 能力 | 当前状态 | 证据和边界 |
| --- | --- | --- |
| Iceberg REST Catalog | E2E | `SHOW CATALOGS` 能看到 `dbx_azure_iceberg`；Catalog 使用 Databricks Unity Catalog REST endpoint。 |
| OAuth 访问 REST Catalog | E2E | 现有 Catalog 配置为 REST OAuth；凭据值不记录在本文。 |
| Databricks vended credentials | E2E | `iceberg.rest.vended-credentials-enabled=true`；查询 Azure 表返回真实数据。 |
| Azure ADLS `abfss` 文件读取 | E2E | 本 PR 修复的路径：vended SAS 经过 Hadoop ABFS 配置并由 `FILE_HDFS` reader 使用。 |
| vended credential 401/过期后的重认证 | CODE / TODO | `ReauthenticatingRestSessionCatalog` 有重认证实现；尚未在当前 Azure 环境人为制造 401 并验证读写恢复。 |
| 用户会话隔离 | CODE / TODO | `SUPPORTS_USER_SESSION` 仅在 `iceberg.rest.session=user` 时声明；当前 Catalog 未按多用户会话矩阵验证。 |
| S3/GCS/HDFS 等其他存储后端 | REG / TODO | 仓库存在对应 Iceberg 场景，但不属于当前 Azure vended E2E 范围。 |

## 4. 查询与元数据

| 能力 | 当前状态 | 代码/测试入口 | 下一步 |
| --- | --- | --- | --- |
| Catalog、namespace、table 发现 | E2E | 当前环境 `SHOW DATABASES`、`SHOW TABLES` | 保持为每次 E2E 的冒烟检查 |
| 基本投影、过滤、排序、聚合 | E2E | 当前环境 `COUNT(*)` 与 `WHERE id >= 2 ORDER BY id` | 增加结果快照和查询计划记录 |
| MVCC / snapshot time travel | E2E / CODE / REG | `SUPPORTS_MVCC_SNAPSHOT`；当前表的 `$snapshots` 返回 snapshot `7725652772105110574`，`FOR VERSION AS OF` 返回历史三行；`test_iceberg_time_travel.groovy` | 增加多 snapshot、schema evolution 后的历史读 |
| Branch / Tag 查询 | CODE / REG / TODO | branch/tag regression suite | 验证 REST Catalog 对 branch/tag 的可见性和权限 |
| Partition pruning | E2E / REG | 隔离表 `matrix_partition_prune_2304` 有 `p=1/2/3` 三个分区；`WHERE p=2` 的 `EXPLAIN VERBOSE` 显示 `inputSplitNum=1`、`partition=1/3`，`WHERE p IN (1,3)` 显示两个 split、`partition=2/3`，结果分别返回对应行；`test_iceberg_runtime_filter_partition_pruning*.groovy` | 增加 transform partition 和分区演进后的裁剪证据 |
| Runtime filter | REG / TODO | `test_iceberg_runtime_filter_partition_pruning*.groovy` | 当前只验证了静态分区谓词；单独验证 join/subquery runtime filter |
| Nested column pruning | CODE / REG / TODO | `SUPPORTS_NESTED_COLUMN_PRUNE`；nested schema suites | 使用 STRUCT/ARRAY/MAP 表，检查子字段结果和扫描行为 |
| Position delete | REG / TODO | `test_iceberg_position_delete.groovy`、`test_iceberg_read_with_posdelete.groovy` | 先验证读取，再验证 Azure 上写入产生的删除文件 |
| Equality delete | REG / TODO | `test_iceberg_equality_delete*.groovy` | 验证 schema evolution 后 equality delete 仍正确 |
| Deletion vector / row lineage | E2E / REG | format v3 表先执行 `DELETE` 产生 DV，再执行 `UPDATE` 和 `MERGE INTO`，均正确读取并合并已有 DV；deletion-vector 和 v3 row-lineage suites | 增加多 DV、并发提交和大批量删除场景 |
| Iceberg system tables | E2E / REG / TODO | `$snapshots`、`$refs` 可读；`$files` 在 Azure 表上因缺少 `org.apache.iceberg.azure.adlsv2.ADLSFileIO` 失败；`test_iceberg_sys_table*.groovy` | 补齐 Azure Iceberg system-table 插件依赖后验证 files、manifests、partitions |
| `SHOW CREATE TABLE/DATABASE` | E2E / CODE / REG | `SUPPORTS_SHOW_CREATE_DDL`；当前 `SHOW CREATE TABLE` 返回 Iceberg LOCATION/PROPERTIES，未出现 OAuth token 或 SAS；`test_iceberg_show_create.groovy` | 单独验证 `SHOW CREATE DATABASE` 和敏感属性过滤 |
| View | CODE / REG / TODO | `SUPPORTS_VIEW` | Databricks REST Catalog 是否暴露 view，需要单独确认 |
| Metadata preload | CODE | `SUPPORTS_METADATA_PRELOAD` | 这是并发/锁延迟优化，不作为第一批功能 E2E |
| Top-N lazy materialization | CODE / REG / TODO | `SUPPORTS_TOPN_LAZY_MATERIALIZE`、TVF/Top-N suites | 需要真实 Top-N 查询和 profile 证据 |

## 5. 写入、DDL 和管理操作

### 5.1 写入路径

`IcebergWritePlanProvider` 当前将写操作路由到以下 sink：

| 操作 | 当前状态 | 证据 |
| --- | --- | --- |
| `INSERT` | E2E / REG | 在 `matrix_write_20260820` 通过 `VALUES` 写入 `id=13`，再通过 `INSERT ... SELECT` 写入 `id=22`，查询均得到完整行。 |
| `INSERT OVERWRITE` | E2E / REG | 同一隔离表 overwrite 后仅保留 `id=20/21`，结果符合替换语义。 |
| `DELETE` | E2E / REG | format v3 表删除 `id=2` 后仅剩 `id=1/3`；format v2 被远端按“delete files 需要 v3”拒绝，属于表格式前置条件。 |
| `UPDATE` | E2E / UT / REG | 新表 UPDATE 通过；已有 DELETE DV 的 `matrix_dml_v3_20260820` 更新 `id=1` 后得到 `alice_after_fix/28`。 |
| `MERGE INTO` | E2E / UT / REG | 在同一张已有 DELETE 和 UPDATE DV 的表上更新 `id=3`、插入 `id=5`，最终得到三行预期结果。 |
| `rewrite_data_files` | CODE / REG / TODO | 分布式 rewrite 路径；对应 action regression 已存在。 |

第一次在已有 v3 DV 上执行 `UPDATE`/`MERGE INTO` 时，BE 的 DV reader 退化为打开空 authority 的 `hdfs://`。普通 scan range 会单独携带 `fs_name`，但 sink 侧 DV helper 只能从 Hadoop 配置读取 `fs.defaultFS`；Databricks vended SAS 配置只包含 account，不包含 container，因此此前没有该键。本 PR 从实际数据位置提取 `abfss://container@account.dfs.core.windows.net` 写入 `fs.defaultFS`，新增 FE 单元测试，并用上述 DELETE -> UPDATE -> MERGE E2E 顺序验证修复。

本轮保留了 `matrix_write_20260820`、`matrix_dml_v3_20260820`、`matrix_update_fresh_20260820` 和 `matrix_merge_fresh_20260820` 供复查，没有修改或清理现有 `managed_iceberg`。

### 5.2 DDL 和表演进

| 操作 | 当前状态 | 证据 |
| --- | --- | --- |
| 创建/删除 namespace、table | CODE / REG / TODO | 连接器 metadata ops 和 external Iceberg suites 有实现；Azure Catalog 上应使用隔离 namespace。 |
| Add/drop/rename/modify column | CODE / REG / TODO | `SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE`；包含嵌套字段路径。 |
| Partition evolution | CODE / REG / TODO | `addPartitionField`、`dropPartitionField`、`replacePartitionField`；现有 partition evolution suites。 |
| Sort order on create | CODE | `SUPPORTS_SORT_ORDER`；Iceberg connector 是当前声明该 DDL 能力的连接器。 |
| Branch/tag create/drop | CODE / REG / TODO | connector metadata ops 和 branch/tag suites 有实现。 |

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

这些操作目前属于 `CODE / REG / TODO`：代码和通用 regression 已有证据，但尚未在当前 Databricks + Azure Catalog 上逐项执行。管理操作会改变远端表状态，必须使用专用测试表，并保留执行前后的 snapshot 记录。

## 6. 明确的未声明能力

以下能力不能因为 Iceberg 支持其他功能就自动推断为支持：

| 能力 | 当前结论 |
| --- | --- |
| `SUPPORTS_PARTITION_STATS` | N/A；Iceberg connector 没有声明该 capability。 |
| `SUPPORTS_SAMPLE_ANALYZE` | N/A；Iceberg connector 没有声明，源码说明 native Iceberg 的 sample analyze 未实现。 |
| `SUPPORTS_SCAN_PARAM_OPTIONS` | N/A；当前 connector 没有声明通用 `@options` scan-param 能力。 |
| 静态凭据跨用户共享 | 不应推断支持；用户会话能力只对特定 REST 配置启用，缓存隔离必须按配置验证。 |

“N/A”表示当前通用 SPI 没有声明该能力，不表示 Iceberg 格式本身永远不能实现它。

## 7. 下一轮验证顺序

为了不污染现有 Catalog，下一轮按下面顺序执行：

1. 只读：nested column、position/equality delete，以及修复 Azure 依赖后的 system tables；静态 identity partition pruning 已有 E2E 证据。
2. DDL：在独立 namespace 验证 schema evolution、partition evolution、branch/tag。
3. 写入扩展：验证分区表、多 DV、并发提交和失败重试。
4. 管理：最后验证 `EXECUTE` 操作，并在每一步保存 snapshot/metadata 结果。
5. 安全：用过期凭据和不同用户会话验证 401 重认证及缓存隔离。

每个条目至少记录：Doris commit、Catalog 类型、云存储、Iceberg format version、执行 SQL、结果、异常（如有）以及 FE/BE 日志中的关键证据。任何只通过源码或通用 regression 的条目，在 Azure E2E 完成前都保持 `CODE / REG / TODO`。

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
