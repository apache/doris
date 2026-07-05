# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.1 进行中：T01（TRUNCATE SPI seam）+ T02–T04（插件写客户端）已实现+全绿+提交；下一步 = T05→T07（HiveConnectorMetadata DDL override）+ T08/T09/T10/T11**

> **本轮（P7.1 T02+T03+T04 session）做了什么** —— 提交 `fdf577e7946`，**插件写客户端一个自洽单元，全在 `fe-connector-hms`**：
> 1. **T02 写 DTO**：`HmsCreateTableRequest`（builder：全列[数据+分区]、partitionKeys 名、bucketCols/numBuckets、fileFormat、comment、properties、`defaultTextCompression`、`dorisVersion`；列默认值走 `ConnectorColumn.getDefaultValue()`）+ `HmsCreateDatabaseRequest`。仿读侧 `HmsTableInfo`/`HmsDatabaseInfo`，SPI-clean。
> 2. **T03 转换器**：`HmsTypeMapping.toHiveTypeString(ConnectorType)`（反向类型映射，等价 `dorisTypeToHiveType`；switch 于 `PrimitiveType.toString()` 名，VARCHAR→string、datetime 丢 scale、decimal p==0→9、不支持类型 throw）+ `HmsWriteConverter.toHiveTable/toHiveDatabase`（忠实移植 `HiveUtil` + `HiveProperties.setTableProperties` serde/table 属性切分；in/out/serde+压缩默认逐字照搬；`createTime` 溢出照搬）。**断耦**：`DORIS_VERSION_VALUE` 经 request 线程进（插件禁 import fe-common `Version`——import gate 禁 `org.apache.doris.common`）、text 压缩默认经 request（T06 从 session 取）、`AnalysisException`→`IllegalArgumentException`、JDK-only。
> 3. **T04 写方法**：`HmsClient` 加 5 个写方法为 **default-throw seam**（3 个读侧 fake 不破）；`ThriftHmsClient` 用现成 `execute(...)`（auth+pool）override 全部，`createTable` 从列默认值建 `SQLDefaultConstraint`→`createTableWithConstraints`，等价 legacy `ThriftHMSCachedClient`。
> 4. **验收**：fe-connector-hms compile SUCCESS + checkstyle 0（main+test）+ import-gate 净 + **28 单测绿**（反向映射 + 转换器，无 fake；client-dispatch/HiveConnectorMetadata 单测留 T10 recording-fake）。**无连接器 override 这些写方法（等 T05–T07），翻闸前行为不变。**
>
> **T05–T11 未动**（下一单元：`fe-connector-hive` 的 `HiveConnectorMetadata` DDL override + shared converter 列默认值 + config 注入 + 单测 + 守门）。T05–T07 需研读模板 `IcebergConnectorMetadata` 与 SPI base（`ConnectorSchemaOps`/`ConnectorTableOps`），故为下一自洽单元。

> **整条 catalog-SPI 主线阶段链均已合入 upstream `branch-catalog-spi`**：
> P0 #63582 · P1 #63641 · P2 trino #64096 · P3 hudi(hybrid) #64143 · P4 maxcompute #64300 · P5 paimon 迁移+翻闸 #64446 + 删 legacy #64653 · P3b kerberos→fe-kerberos #64655 · **P6 iceberg #64688（本轮收官）**。
>
> **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`，工作树干净）。P6 期间所有"未 push（[DEC-FLIP-1] 铁律）"的翻闸/删死码/属性迁移工作**已全部随 #64688 一次性合入**——**该铁律现已解除**，P7 走常规「连接器内实现 → 翻闸 → 删 legacy → squash-合入」流程（复用 P5/P6 样板）。

## #64688 做了什么（685 文件，+79738/−23744）

把原生 iceberg（元数据/scan/write/procedures/sys-tables/行级 DML）整体迁到自包含 `fe-connector-iceberg` + 翻闸（iceberg 入 `CatalogFactory.SPI_READY_TYPES`）+ 删 fe-core 原生 iceberg 子系统：
- **删除**：`IcebergExternalCatalog`/`IcebergExternalTable`/`IcebergTransaction`/`IcebergNereidsUtils`、7 catalog flavor + factory、`broker/`·`dlf/`·`fileio/`·`action/`·`rewrite/`·`helper/`、四个 DML 执行器、planner 三 sink、`IcebergTransactionManager` + fe-core Iceberg/Paimon 元存储属性簇（S7，−4914 行）等。
- **翻闸 + GSON 迁移**：旧 8 catalog 变体 + db + table 标签 `registerCompatibleSubtype`→`PluginDriven*`（保升级老集群反序列化）。
- **属性/鉴权全归插件**（S1–S10，用户 2026-07-05 架构裁定）：fe-core 不再解析任何属性；已迁连接器（iceberg+paimon）走「插件解析 → BE thrift → 回传 fe-core」。详见 memory `catalog-spi-no-property-parsing-in-fecore`。

## ⚠️ 关键遗留（P7 必须接手）= fe-core `datasource/iceberg/` 还剩 **23 个 HMS-iceberg 支撑类**

`#64688` 删的是**原生 iceberg 子系统**；但 **iceberg-on-HMS**（`type=hms` 下 `DlaType.ICEBERG` 的表）仍走 fe-core，因此以下 23 文件**故意保活**（decision D5 / Q3=B：HMS-iceberg 随 hive 一起迁）：
`IcebergUtils`、`IcebergMetadataOps`、`source/IcebergScanNode`(+`IcebergHMSSource`/`IcebergSource`/`IcebergSplit`/…)、`cache/`、`IcebergMvccSnapshot`、`IcebergSchema/Snapshot/Partition*CacheValue`、`profile/IcebergMetricsReporter`、`DorisTypeToIcebergType`、`IcebergCatalogConstants`。
→ **P7 hive 迁移把 HMS-iceberg 挪到连接器路径后，这 23 文件才能删（阶段四）**。同理 fe-core `datasource/hudi/` 与 `datasource/hive/` 也在 P7 范围。

---

# 🚀 下个 session 任务 = **执行 P7.1 实现（按 spec 末尾 P7.1-T01…T11；recon+决策已就绪，可直接写代码）**

> **权威计划**：**`tasks/P7-hive-migration.md`**——尤其**文件末尾「P7.1 逐 task 拆解」块**（本轮新建，含 recon 结论 + T01–T11 + OQ-HIVE-CFG=方案A 裁定）+ 上半 P7.1–P7.5 spec + [`connectors/hive.md`](./connectors/hive.md)。**单连接器 13 步 playbook = master plan §4。**
> **模块已就绪**：`fe-connector-hive`（"hms" 网关，只读 scan 已立、DDL override=0）+ `fe-connector-hms`（共享读元存储库，**只读、无任何写方法**）。**通用桥 = `PluginDrivenExternalCatalog`**（DDL 命令逐条 override + 内联 cache/editlog，连接器只实现纯 SPI 方法）。**模板 = `IcebergConnectorMetadata`**（最接近：HMS-backed + DLF guard + location cleanup + 插件侧 SchemaBuilder）。

## P7.1 执行要点（下个 session 开场）

1. **先读** spec 末尾「P7.1 逐 task 拆解」块 + 「P7.1 实现进度」块（recon 存档，勿再 recon）。**T01–T04 已完成**（T01 truncate SPI seam + 桥 override `c0222977ebd`；T02+T03+T04 插件写客户端 `fdf577e7946`）。**从 T05 起接**：T05(HiveConnectorMetadata DB DDL) → T06(createTable) → T07(dropTable+truncate override；rename 保持 default throw) → T08(shared converter 带列默认值) → T09(config 注入=方案A) → T10(连接器单测，recording-fake `IMetaStoreClient`) → T11(守门+交接)。**信 HEAD 控制流不信 spec 行号。** T05→T07 = 下一自洽单元（连接器 DDL override，模板 `IcebergConnectorMetadata`）。**注意**：T06 须从 `ConnectorSession` 取 text 压缩默认填 `HmsCreateTableRequest.defaultTextCompression`；T09 注入 locus 须同时把 `doris.version`（值）填 `HmsCreateTableRequest.dorisVersion`（插件已留字段，值缺时转换器省略该 tag）。
2. **本阶段范围 = 仅 DDL 写路径**：create/drop db+table + truncate。**stats/partition 写 4 法（updateTableStatistics/updatePartitionStatistics/addPartitions/dropPartition）推 P7.3**（仅 HMSTransaction 消费，本阶段加即死代码）。**rename 不实现**（hive 今天不支持，保持 SPI default throw）。**no-property-parsing**：file_format 默认/owner/bucket-gate/DLF-guard/transactional-reject 全插件侧（`ConnectorSession.getUser()`+`getCatalogProperties()`+`ConnectorColumn.defaultValue` 已够，无须扩 SPI，除 truncate seam + T08 列默认值 carrier）。
3. **OQ-HIVE-CFG 已裁定 = 方案 A**（T09）：fe-core 组装 `hms` 连接器属性处，把 `Config.hive_default_file_format`/`Config.enable_create_hive_bucket_table` 当前值注入为属性默认（仅用户未显式设时），**插件只读属性、不碰 `common.Config`**。注入 locus 待 recon。
4. **勿重议的已定决策**：**D-004** event→fe-connector-hms；**D-005** tableFormatType；**D-020** per-table SPI provider；**D-019** hudi live cutover 并入 P7；事务桥接 `PluginDrivenTransaction`。后续子阶段 OQ-* 到 recon 后再用户签字（中文讲背景+示例+推荐、不引任务代号）。
5. **纪律**：每 task/每条 fix 独立 commit + build + test + checkstyle 0（不带 -am）+ import-gate 净；**每轮完成即更新本 HANDOFF + commit**（memory `handoff-discipline-per-phase`）。P7.1 **非 live**（翻闸在 P7.5），验收靠**连接器单测**（无 Mockito、recording fake `IMetaStoreClient`），无 e2e DDL。**P7.3 ACID 写路径必须有独立集成测试作 gate（R-002 项目最大风险）**。
6. **删除排序（最硬约束，spec §跨连接器删除排序）**：`datasource/hive/` 删不掉，直到 `HudiUtils`(5 方法)/`HudiExternalMetaCache`/`HudiScanNode`(extends HiveScanNode)/`IcebergHMSSource`/`HMSAnalysisTask`/`StatisticsUtil.getIcebergColumnStats` 全 retype 到 generic（P7.4）。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**（复用 P5-T29 #64653 / P6 #64688 范式）。
- **公开 tracking issue = apache/doris#65185**（catalog-SPI 迁移 umbrella，大步骤勾选清单 + 连接器表）；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `plan-doc/reviews/P5-paimon-rereview3-*`；`.claude/` 是 memory、非仓内）。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每阶段/每条 fix = 独立 commit**；HANDOFF + 任务清单 + 设计文档单独 commit。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`**。**fe-connector-hive/hms 单独 build 注意 optional-shade 依赖**（如 HiveConf；参照 paimon `package` 而非 `test-compile` 的坑）。
- **⚠️ checkstyle 别加 `-am`**：`-am` 把 `fe-common`（大量既存 error）拖进假红 → `mvn -pl :<art> checkstyle:check`（不带 -am）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` ~590000ms 或后台跑（全模块 ~2min）。**后台 task 通知的 "exit code" 是末尾 echo/df 的、非 maven 的**——读 LOG 里 `BUILD SUCCESS`/`MAVEN_EXIT=` 行或 surefire XML（`tests=`/`failures=`），别信通知 exit。
- **⚠️ maven 经管道 `$?` 是管道尾的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`。**mutation/review 后 commit 前务必 fresh recompile**（stale `.class` 假红坑）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**HMS `HiveVersionUtil` import 命中 = 误报非违规**（`fe-connector-hms` 内 vendored 同名副本，非 fe-core）——勿改，详见 memory `catalog-spi-hms-hiveversionutil-gate-false-positive`。
- **连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField`；`anyString()` 不匹配 null）。详见 memory `catalog-spi-fe-core-test-infra`。
- **cwd 会被 harness 重置** → 一律绝对路径。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，~96% used）。**起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 起步必读

1. **本文档顶部 🎯/🚀 段** + master plan [§3.8](./00-connector-migration-master-plan.md)（P7 战略）+ [`connectors/hive.md`](./connectors/hive.md)（P7 子阶段/SPI 缺口/特殊性）+ master plan §4（13 步 playbook）。
2. **样板**：`tasks/P5-paimon-migration.md`（full-adopter + 翻闸 + 删 legacy 全流程）；`tasks/P6-iceberg-migration.md`（阶段拆分 spec 范式，P7 spec 照此建）。
3. **铁律**：fe-core 不得新增 `if(hive)` / `instanceof HMSExternal*` / 引擎名字符串判别（新 seam 走中立 SPI / `ConnectorCapability`）；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。
4. **memory（现存相关项）**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`memory-keep-only-general-or-requested`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-plugindriven-no-source-specific-code`、`catalog-spi-no-property-parsing-in-fecore`、`catalog-spi-be-java-ext-shared-classpath`、`catalog-spi-plugin-tccl-classloader-gotcha`、`catalog-spi-connector-session-tz-gotcha`、`catalog-spi-history-schema-info-lowercase-nested-names`、`catalog-spi-connector-cache-framework-caffeine-coherence`、`catalog-spi-hms-hiveversionutil-gate-false-positive`、`catalog-spi-tracking-issue`。
5. **上下文超 30% 即交接**（memory `session-handoff-at-30pct-context`）：找干净节点覆写本 HANDOFF + 通知用户开新 session。
