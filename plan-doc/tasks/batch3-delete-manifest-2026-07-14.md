# Batch 3 原子删除清单（HEAD-verified，2026-07-14）— 待用户 review

> **状态**：删除执行清单，**待用户过目批准**后动手。HEAD = `3fdf4327f2a`。
> **核验**：确定性 inbound-ref 全量扫描 + 122-agent 对抗核验工作流（`.claude/wf-batch3-delete-verify.js`，run `wf_d1815c18-1be`：52 主源 + 68 测试逐文件分类 + gson/反射回放专项 + 完备性 critic）+ 人工独立复核最高危 6 处 + Nereids sink/scan 族 case-insensitive 补扫。
> **总判定**：READY —— 计划基本成立，本轮新增 1 处漏列耦合（`ExpressionRewrite`）；`DistributionSpecHiveTableSink*` 澄清为**保留**（原 critic 误报已纠正）。删除后 fe-core（含 test 源）应 test-compile 绿（执行时以真编译为准）。

---

## A. 删除文件（整文件删，共 109 个主源）

| 包 | 数 | 类 |
|---|---|---|
| `datasource/hive` | 29 | AcidInfo, AcidUtil, HMSCachedClient, HMSClientException, HMSDlaTable, HMSExternalCatalog, HMSExternalDatabase, HMSExternalTable, HMSSchemaCacheValue, HMSTransaction, HiveBucketUtil, HiveColumnStatistics, HiveDatabaseMetadata, HiveDlaTable, HiveExternalMetaCache, HiveMetaStoreClientHelper, HiveMetadataOps, HivePartition, HivePartitionStatistics, HivePartitionWithStatistics, HiveProperties, HiveTableMetadata, HiveTransaction, HiveTransactionMgr, HiveUtil, **HiveVersionUtil**, HudiDlaTable, IcebergDlaTable, ThriftHMSCachedClient |
| `datasource/hive/event` | 21 | AddPartitionEvent, AlterDatabaseEvent, AlterPartitionEvent, AlterTableEvent, CreateDatabaseEvent, CreateTableEvent, DropDatabaseEvent, DropPartitionEvent, DropTableEvent, EventFactory, GzipJSONMessageDeserializer, IgnoredEvent, InsertEvent, MetastoreEvent, MetastoreEventFactory, MetastoreEventType, MetastoreEventsProcessor, MetastoreNotificationException, MetastoreNotificationFetchException, MetastorePartitionEvent, MetastoreTableEvent |
| `datasource/hive/source` | 2 | HiveScanNode, HiveSplit |
| `datasource/hudi` | 9 | HudiExternalMetaCache, HudiFsViewCacheKey, HudiMetaClientCacheKey, HudiMvccSnapshot, HudiPartitionCacheKey, HudiPartitionUtils, HudiSchemaCacheKey, HudiSchemaCacheValue, HudiUtils |
| `datasource/hudi/source` | 6 | COWIncrementalRelation, EmptyIncrementalRelation, HudiScanNode, HudiSplit, IncrementalRelation, MORIncrementalRelation |
| `datasource/iceberg` | 14 | DorisTypeToIcebergType, IcebergCatalogConstants, IcebergExternalMetaCache, IcebergManifestEntryKey, IcebergMetadataOps, IcebergMvccSnapshot, IcebergPartition, IcebergPartitionInfo, IcebergSchemaCacheKey, IcebergSchemaCacheValue, IcebergSnapshot, IcebergSnapshotCacheValue, IcebergTableCacheValue, **IcebergUtils** |
| `datasource/iceberg/cache` | 2 | IcebergManifestCacheLoader, ManifestCacheValue |
| `datasource/iceberg/profile` | 1 | IcebergMetricsReporter |
| `datasource/iceberg/source` | 6 | IcebergDeleteFileFilter, IcebergHMSSource, IcebergScanNode, IcebergSource, IcebergSplit, IcebergTableQueryInfo |
| `datasource/connectivity` | 5 | AWSGlueMetaStoreBaseConnectivityTester, AbstractHiveConnectivityTester, HMSBaseConnectivityTester, HiveGlueMetaStoreConnectivityTester, HiveHMSConnectivityTester |
| `datasource/systable` | 1 | IcebergSysTable |
| `nereids/analyzer` | 1 | UnboundHiveTableSink |
| `nereids/trees/plans/logical` | 2 | LogicalHiveTableSink, LogicalHudiScan |
| `nereids/trees/plans/physical` | 2 | PhysicalHiveTableSink, PhysicalHudiScan |
| `nereids/rules/implementation` | 2 | LogicalHiveTableSinkToPhysicalHiveTableSink, LogicalHudiScanToPhysicalHudiScan |
| `nereids/trees/plans/commands/insert` | 1 | HiveInsertExecutor |
| `planner` | 1 | HiveTableSink |
| `statistics` | 1 | HMSAnalysisTask |
| `transaction` | 2 | HiveTransactionManager, TransactionManagerFactory |
| `org/apache/hadoop/hive/metastore` | 1 | HiveMetaStoreClient（fe-core 补丁版，非 hadoop 真身） |

> **注**：`connectivity`/`systable` 仅删上列 hive/glue/iceberg 专属者；两包活兄弟（S3/HDFS/Minio 探测器、`CatalogConnectivityTestCoordinator`、`NativeSysTable` 等）**保留**。旧文档误记为 `datasource/infra(6)`，实际路径为此二包。

## B. 保留文件里的耦合删改（KEEP 文件，与删除同一 commit）

1. `catalog/Env.java` — `hiveTransactionMgr` field(570)/init(867)/getter(1099-1105)/import(108)。
2. `datasource/ExternalMetaCacheMgr.java` — `new Hive/Hudi/IcebergExternalMetaCache`(292-294) + `hive()/hudi()/iceberg()`(159-172) + `ENGINE_HIVE/HUDI/ICEBERG`(62-64) + import(24-26)。
3. `nereids/glue/translator/PhysicalPlanTranslator.java` — `visitPhysicalHiveTableSink`/`visitPhysicalHudiScan` + `directoryLister` 面 + 相关 import。
4. `nereids/processor/post/ShuffleKeyPruner.java` — `visitPhysicalHiveTableSink` override + import。
5. `nereids/processor/pre/TurnOffPageCacheForInsertIntoSelect.java` — `visitLogicalHiveTableSink` override + import。
6. `nereids/properties/RequestPropertyDeriver.java` — `visitPhysicalHiveTableSink` override + import。
7. `nereids/rules/implementation/AggregateStrategies.java` — 死三元复原为 `new LogicalFileScanToPhysicalFileScan()` + import。
8. `nereids/rules/implementation/LogicalFileScanToPhysicalFileScan.java` — 去 `!(instanceof LogicalHudiScan)` 守卫 + import。
9. `nereids/rules/RuleSet.java` — 两 impl-rule import + 4 条注册。
10. `nereids/stats/StatsCalculator.java` — `visitLogicalHudiScan` override + import。
11. `nereids/trees/plans/commands/info/CreateTableInfo.java` — `validateIcebergRowLineageColumns(int)` + `IcebergUtils` import。
12. `nereids/trees/plans/visitor/RelationVisitor.java` — `visitLogical/PhysicalHudiScan` 默认方法 + 2 import。
13. `nereids/trees/plans/visitor/SinkVisitor.java` — `visitUnbound/Logical/PhysicalHiveTableSink` 默认方法 + 3 import。
14. `statistics/util/StatisticsUtil.java` — 死方法 `getHiveRowCount`/`getRowCountFromParameters`/`getTotalSizeFromHMS` + import。
15. `planner/BaseExternalTableDataSink.java` + `planner/PluginDrivenTableSink.java` — 删 `getTFileFormatType`/`getTFileCompressType`/`getBrokerAddresses` + **抽象 `supportedFileFormatTypes()`（61）** + **`PluginDrivenTableSink` 的 override（128）** + 变为未用的 import（HiveUtil/Env/FsBroker/TNetworkAddress/TFileCompressType/Collections/Collectors/List）。**两处必须同 commit 一起删**（抽象方法删了 override 才不悬空；唯一调用点在被删的 `getTFileFormatType` 内）。
16. **`nereids/rules/expression/ExpressionRewrite.java`（本轮新揪出，原计划漏列）** — 删注册 `new LogicalHiveTableSinkRewrite().build(),`(111) + 内部类 `LogicalHiveTableSinkRewrite`(504-510，其 `build()` 调依赖 `LogicalHiveTableSink` 的生成式 pattern `logicalHiveTableSink()`)。

**可选清理（孤儿枚举，留删皆可编译；随批清理，仿用户决策 #4）**：
- `nereids/trees/plans/PlanType.java` — `LOGICAL_HIVE_TABLE_SINK`(51)/`PHYSICAL_HUDI_SCAN`(113)/`PHYSICAL_HIVE_TABLE_SINK`(125)。
- `nereids/rules/RuleType.java` — `LOGICAL_HUDI_SCAN_TO_PHYSICAL_HUDI_SCAN_RULE`(554)/`LOGICAL_HIVE_TABLE_SINK_TO_PHYSICAL_HIVE_TABLE_SINK_RULE`(561)。

## C. 测试源联动（同 commit）

**整文件删（31）**：ExternalMetaCacheRouteResolverTest, GzipJSONMessageDeserializerTest, HiveAcidTest, HiveDDLAndDMLPlanTest, HiveMetaStoreCacheTest, HiveUtilTest, HmsCommitTest, HMSExternalTableTest, HMSTransactionPathTest, HiveScanNodeTest, ThriftHMSCachedClientTest, HudiExternalMetaCacheTest, HudiUtilsTest, IcebergExternalMetaCacheTest, IcebergMetadataOpsValidationTest, IcebergMetadataOpTest, IcebergPartitionInfoTest, IcebergPredicateTest, IcebergUtilsPartitionRangeTest, IcebergUtilsTest, IcebergCountPushDownTest, IcebergScanNodeTest, PathVisibleTest, HMSGlueIT, IcebergSysTableResolverTest, TestHMSCachedClient, HmsCatalogTest（@Disabled）, MetastoreEventFactoryTest, HiveTableSinkTest, HmsQueryCacheTest（@Disabled）, HMSAnalysisTaskTest。（`HiveDDLAndDMLPlanTest` 亦 @Disabled）

**改（14）**：DbsProcDirTest, ExternalCatalogTest, PluginDrivenScanNodeClassifyColumnTest, InsertOverwriteManagerTest, MTMVPartitionCheckUtilTest, PartitionCompensatorTest, StatementContextTest, OlapInsertExecutorTest, LogicalFileScanTest, ListPartitionPrunerV2Test, CacheTest, StatisticsAutoCollectorTest, StatisticsUtilTest, CommitDataSerializerTest。（多为删被删类 import/桩/DLAType 专属用例，或把 `HMSExternalTable` mock 换成保留的 `ExternalTable`/`PluginDrivenExternalTable`；`LogicalFileScanTest` 把 `IcebergUtils.ICEBERG_ROW_ID_COL` 换字面量 `"_row_id"`。逐文件细节见 workflow 结果。）

**无需改（23，误报/仅注释/仅字符串标签）**：HiveTableTest, NereidsSqlCacheManagerPluginTableTest, DefaultConnectorContextCleanupTest, ConnectorColumnConverterTest, ExternalMetaIdMgrTest, **HmsGsonCompatReplayTest**, PluginDrivenExternalCatalogDdlRoutingTest, PluginDrivenExternalTableColumnStatTest, PluginDrivenExternalTableEngineTest, PluginDrivenExternalTableTest, PluginDrivenMvccExternalTableTest, PluginDrivenScanNodeSysTableGuardTest, PluginDrivenScanNodeSysTablePinTest, PluginDrivenScanNodeTopnLazyMatTest, PluginDrivenScanNodeVerboseExplainTest, PluginDrivenSplitPartitionValuesTest, HMSIntegrationTest, BrokerPropertiesTest, SqlCacheContextPluginTableTest, CreateTableInfoEngineCatalogTest, CreateTableInfoTest, PhysicalConnectorTableSinkTest, PluginTableCacheAnalyzerTest。

## D. 序列化 / 回放安全（核验结论：LOW risk）

- `persist/gson/GsonUtils.java` 里 `"HMSExternalCatalog"`/`"HMSExternalDatabase"`/`"HMSExternalTable"` 等是 **compat 字符串标签**，早已 `registerCompatibleSubtype` 指向活类 `PluginDrivenExternalCatalog`/`PluginDrivenExternalDatabase`/`PluginDrivenMvccExternalTable`。**这些映射行必须保留**（承接老 image/edit-log 回放），删旧类不碰它。**GsonUtils 本轮零改动。**
- 无任一被删类以自身作 `registerSubtype` 活目标；无 `Class.forName`/`ServiceLoader`/按名反射命中被删类；`META-INF/services` 全指新插件。缓存/快照结构（`*SchemaCacheValue`/`*MvccSnapshot`/`HivePartition` 等）从不进 gson image，按需重建。
- `GsonUtils:505` 的 `HiveTable` 是 `catalog.HiveTable`（遗留内建表，**保留**），非删除集的 `planner.HiveTableSink`。

## E. 澄清：`DistributionSpecHiveTableSink*` = 保留（原 critic 误报纠正）

`nereids/properties/DistributionSpecHiveTableSinkHashPartitioned` 与 `DistributionSpecHiveTableSinkUnPartitioned` **名字带 HiveTableSink 但是活的通用类**：活的通用连接器写路径 `PhysicalConnectorTableSink`（iceberg/paimon/jdbc/maxcompute）在 `:204/:242` `new ...HashPartitioned()`、`:250` 返回 `SINK_RANDOM_PARTITIONED`，而 `PhysicalProperties.SINK_RANDOM_PARTITIONED`(52) 由 `...UnPartitioned.INSTANCE` 构成。**二者不删、`PhysicalProperties` 不改。** 命名与实际用途不符是既存债，非本删除任务范围（如需改名单列）。

## F. 守门（Batch 4，删后）

fe-core `test-compile` BUILD SUCCESS（含 test 源）· checkstyle 0（`UnusedImports` 必清）· `tools/check-connector-imports.sh` 净 · 回放单测 `HmsGsonCompatReplayTest`/`IcebergGsonCompatReplayTest`/`PaimonGsonCompatReplayTest` 绿 · 用户自跑翻闸 hms 全量回归删前后逐位一致。
