# TASKLIST — fe-core 数据源依赖与残留代码清理

> 配套 [`HANDOFF.md`](./HANDOFF.md) 与[分析文档](./fe-core-datasource-deps-and-code-cleanup-2026-07-21.md)。
> 勾选规则：`[ ]` 未开始 · `[~]` 进行中 · `[x]` 完成（须验证通过）。每完成一项就更新此文件 + HANDOFF 进度日志。
> **行号会漂移**，动手前按符号名/内容 grep 重新定位。批次内按序做；批次间有依赖（见每批"前置"）。

---

## Batch 0 — 起步（每个 session 开头做一次，不 commit）

- [x] **T0.1** 读 HANDOFF + 本文件 + 分析文档；对照真实代码 review。
- [x] **T0.2** 并发踩踏探测：本仓无活跃 maven/无近 90s 改动；活跃 maven 在**兄弟 worktree** `git/doris`（跑 FE 测试，不碰本树源码），仅共享 `~/.m2` → 构建保持 compile-only。
- [x] **T0.3** 建绿色基线：`test-compile -pl fe-core -am` BUILD SUCCESS。

---

## Batch 1 — 零风险依赖删除（provided / 废弃 lakesoul）

**前置**：无。**风险**：低。

- [x] **T1.1** 删 `com.dmetasoul:lakesoul-io-java`（连同 exclusions）。核实 fe-core `src/` 0 个 `com.dmetasoul`/LakeSoul 类引用（全是 Gson 兼容字符串/枚举名）。
- [x] **T1.2** 删 `org.scala-lang:scala-library`（provided）。核实 0 个 `import scala.`。
- [x] **T1.3** 删 `org.postgresql:postgresql`（provided）——**未暂缓**。实证：`JdbcResourceTest` 里 postgresql 只是字符串字面量 `"org.postgresql.Driver"`（非 `import`），且 `JdbcResource`/`ResourceMgr`/`CreateResourceInfo` 的创建/校验/回放路径零 `Class.forName`/`DriverManager`；provided 本不进生产 runtime。删除安全。
- [x] **T1.V** 验证：`test-compile -pl fe-core -am` BUILD SUCCESS。
- [x] **T1.C** commit `76e6d5fcf2d` `[chore](fe-core) drop deprecated lakesoul/scala/postgresql provided deps`。

---

## Batch 2 — iceberg/hive 死代码删除 + 注释纠错（不动依赖）

**前置**：无（与 Batch 1 独立）。**风险**：低（均为已核验死代码）。动手前逐个用 grep 复核"零调用"。

- [x] **T2.1** 删 `StatisticsUtil.getIcebergColumnStats` + `getColId` + 5 个 iceberg import。**额外**：删死方法后 `ColumnStatisticBuilder` / `java.util.Optional` 变未用（checkstyle 报），一并删。斩断 fe-core 主源码对 iceberg 库最后一处编译引用。
- [x] **T2.2** 删 iceberg 死写路径：`UnboundIcebergTableSink`（整类）、`InsertUtils` 两处 `instanceof` 分支、`InsertOverwriteTableCommand` overwrite 分支 + `setStaticPartitionToContext`、`IcebergInsertCommandContext`（整类）。**TASKLIST 漏项**：`SinkVisitor.visitUnboundIcebergTableSink`（第 4 个引用者，无 override）已一并删。删后 `grep UnboundIcebergTableSink fe/` = CLEAN。
- [x] **T2.3** 删 `HiveInsertCommandContext`（整类）。删后仅连接器 javadoc 提及。
- [x] **T2.4** 注释纠错：`kryo-shaded` "for hudi catalog" → 指向 `WorkloadSchedPolicy`（commit `0102a022341`）。`avro`/`parquet-avro` "For Iceberg" 注释随 T3.3 依赖删/换一并处理（commit `d0f6d3878d3`），避免了立即被推翻的 churn。
- [x] **T2.V** 验证：`test-compile -pl fe-core -am` BUILD SUCCESS；悬空引用 grep = CLEAN；对抗复核见 HANDOFF。
- [x] **T2.C** commit `0102a022341` `[chore](fe-core) remove dead iceberg/hive insert-sink code; fix stale pom comment`。

---

## Batch 3 — iceberg-AWS 依赖簇整体移除

**前置**：Batch 2（T2.1 已删主源码 iceberg 引用）。**风险**：中（碰测试归属 + 依赖树 + parquet 换库）。整批一起验证。

- [x] **T3.1** 迁 5 个 iceberg 测试类到 `fe-connector-iceberg`（用户拍板 **migrate**）。落在 `org.apache.doris.connector.iceberg.catalog`。**纠正原分析**：它们 0 个 Doris import、直连外部服务测 iceberg SDK，非 property 解析测试。连接器 test classpath 已备齐 iceberg-core/aws/s3-tables-catalog/junit5 + 传递 guava/hadoop → REST/Unity/Dlf/S3Tables 仅改 package；**AWSTest** 删非 iceberg 的 v1-SDK `testAWSS3` + 把唯一 v1 类字面量换成配置字符串（连接器只带 v2）。commit `24ddc8d615b`（双模块 test-compile 绿）。
- [x] **T3.2** 删依赖：`iceberg-core`、`iceberg-aws`、`glue`、`s3tables`、`s3-tables-catalog-for-iceberg`、**`aws-json-protocol`** 全删；`grep iceberg/glue/s3tables fe/fe-core/src` = 空。commit `379e4b07066`（iceberg 簇）+ `d0f6d3878d3`（aws-json-protocol）。
- [x] **T3.3** parquet：`parquet-avro`→`parquet-hadoop`+`parquet-column`（均 dependencyManagement 管版本 1.17.0），删 `avro` 显式声明，改 "For Iceberg" 注释。commit `d0f6d3878d3`。**验证方式**（先删 direct 声明再看 resolved 树，绕过 nearest-wins 掩盖）：`aws-json-protocol` 删后整树 0 命中；`avro` 仍经 `hadoop-client→hadoop-common→avro:1.12.1:compile` 留在类路径；`parquet-avro` 本就以同 1.17.0 传递带 parquet-hadoop+column，装载类不变。
- [x] **T3.V** `-am` test-compile 绿、validate gates 过；resolved dependency:tree 三项均验证运行期不缺类。
- [x] **T3.C** 三个 commit：`24ddc8d615b`（迁测试）、`379e4b07066`（删 iceberg 簇）、`d0f6d3878d3`（aws-json/avro/parquet）。

---

## Batch 4 — 待定依赖定性（先调查，再决定）

**前置**：无。**风险**：低（只调查）。产出=每项一个"删/留/迁"结论，回填分析文档 + HANDOFF。**结论：三项全 REMOVE，已执行删除并验证。**

- [x] **T4.1** `mvn dependency:tree -Dincludes=com.amazonaws:*`（须 `-am`，`${revision}` 反应堆）证 `aws-java-sdk-dynamodb`/`aws-java-sdk-logs` 是 fe-core **直接叶子、零传递消费者**；hadoop-aws 3.4.2 已随 Hadoop 3.4.0 移除 S3Guard（jar 内零 dynamodb 类）；父 pom "only for apache ranger audit" 注释过时——CloudWatch destination 类在 `ranger-plugins-audit`，不在 fe-core 类路径（fe-core 只有 `ranger-audit-core:2.8.0`，仅 File/base destination）。→ **删**。
- [x] **T4.2** `com.baidubce:bce-java-sdk`：全 `fe/**/src` + 全仓库零引用/反射/config；BOS 走 S3 兼容路径（`ObjectInfoAdapter` `case BOS`→`S3Properties`），原生 SDK 不在 BOS 路径；fe-filesystem 不声明 → 非"迁移"是"**删**"。**删除坑**：bce 是 fe-core 唯一传递带来 `validation-api` 的源，`ExternalMetaIdMgr` 装饰性 `@NotNull` 靠它编译 → 删该注解（真校验 `Preconditions.checkNotNull` 保留，全 fe 唯一一处 javax.validation 用法），守"fe-core 只减不增"，不加依赖。
- [x] **T4.3** 执行删除：fe-core 删三个直接依赖 + 修 commons-lang 注释；父 pom 连带清理孤立的 mqtt 块+属性、validation-api 块+属性、dynamodb/logs 版本锁定。4 个对抗 agent 独立反证全 `refuted=false`(high)。
- [x] **T4.V** 验证：`test-compile -pl fe-core -am` BUILD SUCCESS（gates 过）；`dependency:tree` 确认五个 jar（含孤立 mqtt/validation-api）全消失、保留的 aws-java-sdk-s3→kms/core/jmespath 完好；reactor-wide 无其他模块消费。
- [x] **T4.C** commit（见 HANDOFF 进度日志）。

---

## Batch 5 — LIVE 源特有逻辑迁移（大工程 · 独立设计 · 大概率跨多个 session）

**前置**：Batch 1–3 完成更干净。**风险**：高。**⚠️ 每子项动手前必须先核实"到底是 live 待迁移，还是已废弃/死"** —— T5.1（engine=hive）实证证明原假设"这些都是 live 未迁移特性、须走 SPI 委派"**不成立**：它已是废弃/死功能，正解是"瘦身成持久化空壳"而非 SPI 迁移（详见 T5.1 与 HANDOFF）。故后续 T5.x 别默认 SPI 委派——先判活死：真 live→连接器 SPI 委派；死/废弃→按持久化兼容义务处置（能删净则删，碰 Gson 老镜像则留空壳，仿 EsTable/EsResource）。每子项先出定性+设计再动手，遵守架构铁律（HANDOFF §2）。

> **顺序**（2026-07-21 用户调整）：iceberg 行级 DML 簇工作量最大、风险最高，**移到最后**做；先啃独立性强、边界清晰的小项（hudi TVF、ranger-hive、engine=hive、散点分支）。es 兼容桩碰持久化兼容，属长期保留候选。

- [x] **T5.1** legacy `engine=hive` 簇：`catalog/HiveTable.java`、`catalog/HMSResource.java`、`load/BrokerFileGroup.java` + `nereids/load/NereidsBrokerFileGroup.java`、`catalog/Env.java` show-create 臂。**重大纠正（侦察+对抗复核，4 条证伪全 refuted=false/high）：这簇不是"live 待 SPI 迁移"，而是已废弃/死功能** —— engine=hive 内部建表在 `InternalCatalog:1285` 直接抛错、`new HiveTable(` 仅存在于单测、broker `LOAD FROM TABLE` 唯一引擎 Spark Load 已禁用、`CREATE RESOURCE type=hms` 可建但无人消费（孤儿）；对外 Hive 早由外部 HMS 连接器承接，无"活能力"可迁（Trino 参照：核心零连接器表类、从无内部表引擎→"迁进连接器"是类别错误）。**处置=瘦身成持久化空壳（用户签字 A 方案）**：`HiveTable`/`HMSResource` 仿 `EsTable`/`EsResource` 削成 Gson 空壳（删 validate/toThrift/属性解析/4-arg ctor/getters），**保留** `GsonUtils` 两处 `registerSubtype` + `Resource.getLegacyClazz` HMS 映射（老镜像反序列化命脉，删则 `JsonParseException` → FE 启动挂）；`Resource` case HMS 建资源改抛（对齐 ES）；`Env` 两处 show-create 臂收敛成废弃注释（对齐 ODBC/ES/JDBC）；两处 broker `instanceof HiveTable` 分支塌成废弃抛错；删 `MaterializeProbeVisitor` 死残项（分析文档漏项/drift）；删旧 `HiveTableTest`，新增 `LegacyHiveMetaGsonCompatTest`（守注册+@SerializedName 标签双不变量，2/2 过）。改 `LoadCommand` 过期注释；修 CI `drop_resource.groovy`（HMS→HDFS 资源，保住 DROP RESOURCE 覆盖）。**验证**：`-am` test-compile 绿（gates 过）、守卫测试 2/2、5 维对抗 clean-room review（0 blocker/1 major 已修/1 minor 已修/2 nit）。9 文件 +41/−248。
- [x] **T5.2** `ranger-hive` 授权包（`catalog/authorizer/ranger/hive/*`，9 文件，ServiceLoader head `RangerHiveAccessControllerFactory`）——**定性=确认误判（通用授权框架，非源专有）；原地保留、不迁不删；0 代码改动（doc-only）**。原分析（B.2）标"源特有行为违规/授权迁移轴"是 **T5.1 同款过宽定性、这次误判活代码**。实证：名字里 "hive"=**Apache Ranger 内置 "hive" 服务定义模型**（库/表/列/udf）非 Doris hive 连接器——9 文件零 hive/HMS import、`checkTblPriv` 丢弃 `ctl` 不按源分支、与内表 `ranger/doris/` 共父类 `RangerAccessController`、是**唯一**外部 Ranger 授权器（iceberg/paimon/jdbc/es 皆可 wire）；**活**（回归 `test_external_catalog_hive.groovy` wire、反射可达、无停用/无 @Deprecated）。迁走须导出 ~10 个鉴权类型 + 动与 ranger-doris 共用（引用 `DorisAccessType`）的父类→**违 fe-core 只出不进铁律**，且连接器无授权 SPI 接缝、授权不在 #65185 范围。**Trino 佐证**：Ranger=引擎级单 `SystemAccessControl`、通用 catalog→表→列 统管所有连接器、无 per-connector 授权器。用户签字"保留+记录"。命名误导暂不改名（碰持久化 FQCN 需双名兼容，列独立改动）。净室对抗复核 3 证伪 agent 全 `refuted=false`/high。
- [x] **T5.3** hudi `hudi_meta` TVF —— **定性=活功能，用户拍板彻底删除（非迁移、无替代）**。决策演进见 [design 文档](./design-hudi-timeline-systable-migration.md)：先定迁 `表$timeline`→再定走 TVF/`meta_scanner` 与 `partition_values` 统一（因 hudi 时间线非可读 SDK Table，享受不到 paimon 那种零 BE 复用）→**最终用户改主意：整功能删掉，BE 死分支也一并删**。多 agent 对抗核验删除清单（无遗漏/无误删/数据读取路径零影响）：**fe-core** 删 `HudiTableValuedFunction`+`HudiMeta` 两整文件 + 4 处注册分发 + `MetadataGenerator` hudi 臂/方法 + 只服务本功能的通用 SPI 缝（`supportsMetadataTable`/`getMetadataTableRows`）+ 1 fe-core 单测；**连接器** 删 `HudiConnectorMetadata.getMetadataTableRows`、`HudiConnector.getCapabilities`、`HiveConnectorMetadata` 兄弟委派、`ConnectorCapability.SUPPORTS_METADATA_TABLE`、`ConnectorMetadata` 默认 SPI + 3 连接器单测片段；**thrift** 就地弃用（保留+`// deprecated`，对齐 iceberg/paimon）；**BE** 删 `meta_scanner.cpp` case HUDI + `_build_hudi_metadata_request` + `.h` 声明（用户"只改码不编译"）；**回归** 删 `test_hudi_meta.groovy`+`.out`，3 个数据用例的 `getCommitTimestamps` 改用 `_hoodie_commit_time`（p2 交 CI 验）。验证：FE `test-compile`（fe-core+3 连接器模块含测试编译）`BUILD SUCCESS`。
- [~] **T5.4** 分散的按源分支（净室对抗定性：5 项里仅建表校验真需动手；其余 4 项经复核为保留/不改）：
  - [x] `CreateTableInfo.java` 建表按源校验（iceberg/paimon `DISTRIBUTE BY`、iceberg 排序、hive `NOT NULL`、hive 外部分区）——**下沉到连接器**（用户签字甲方案）。见 [design 文档](./design-createtable-validation-connector-delegation.md)。连接器各在 `createTable` 开头内联校验（仿 MaxCompute），排序反向拦截改 `ConnectorCapability.SUPPORTS_SORT_ORDER` 能力位（iceberg 声明、fe-core 通用门控）。fe-core 净减（CreateTableInfo −73、PartitionTableInfo −23、CreateMTMVInfo −2）。验证：fe-core `-am` test-compile 绿；3 连接器单测（iceberg 5/paimon 2/hive 10）全绿；`CreateTableCommandTest` 15/15（移除已下沉的 hive 分区/iceberg DISTRIBUTE BY 断言）；e2e `test_hive_ddl`/`test_iceberg_create_table` 文案 byte-identical（交 CI）。
  - [x] `Coordinator.java` 按源提交数据 if-链——**保留**（通用按名分发，三分支同一序列化器无差别；定性见 HANDOFF T5.4 recon）。
  - [x] `AzureProperties.java` `isIcebergRestCatalog`——**保留**（活的、有意的临时门，native SDK OAuth2 未落地，删则放行后端不支持配置）。
  - [x] `DatasourcePrintableMap.java` maxcompute 遮蔽——**保留**（用户定；MCProperties 在 fe-common、依赖合法）。
  - [x] `InternalCatalog.java:1281` es 弃用 guard——**保留**（与 odbc/hive/jdbc 同组通用弃用守卫，删则报错退化）。
- [x] **T5.5** es 兼容桩 `catalog/EsTable.java` / `catalog/EsResource.java`——**定性=已死但必须保留（不可安全删除）；原地保留 0 源码改动 + 补 Gson 守卫测试**。净室对抗复核（3 路侦察 + 3 证伪 agent 全 `refuted=false`/high）：① ES 新建入口已堵死（`InternalCatalog:1282` 建表抛、`Resource:196` 建资源抛）→ **已死**；两类已是极薄 `@Deprecated` Gson 空壳（56/79 行、无业务逻辑，本就是「薄空壳范式」的**参考实现**）。② 全 fe 树仅 4 处 fe-core 主源引用、**零测试/零连接器引用、零 `new`**——唯一运行期消费者=Gson 反序列化老镜像。③ **不可安全删除**（编译上删得掉，运行期挂）：注册在 `RuntimeTypeAdapterFactory`（`GsonUtils:315/508` + `Resource.getLegacyClazz` ES:324），未注册 `clazz` 硬抛 `JsonParseException`（默认兜底只救「标签整个缺失」非「标签存在但未注册」）→ 含 ES 对象的老镜像启动即挂 FE；无元数据版本逃生舱（`FeMetaVersion` 只管对象内字段级、`MINIMUM_VERSION_REQUIRED` 远早于 ES 废弃）；唯一退休机制 `registerCompatibleSubtype`（外部 catalog 类走它重映射到 `PluginDriven*`）仍**保留注册**、且老内部 `EsTable`（pi/tc + 内部 schema）无结构兼容接班人。④ 惯例佐证：同批 Hive/HMS/ODBC/Spark 死数据源类**无一真删**、全留空壳；外部 catalog 类删的是**类**、注册用 remap 保留——无「注册也删」先例；Trino 核心亦无内部表引擎类。**处置**：原地保留（0 源码改动），**补齐与 Hive 那批唯一不一致处**——ES 是范式参考实现却反而没守卫测试→新增 `LegacyEsMetaGsonCompatTest`（仿 `LegacyHiveMetaGsonCompatTest`，锁「注册存活 + `@SerializedName` 标签 pi/tc/properties 不变」双不变量；对 `pi` 结构字段用反射断言标签、对 `tc`/`properties` 用老镜像字节注入）。验证：`-am test` `LegacyEsMetaGsonCompatTest` 2/2 绿、gates 过。
- [~] **T5.6** iceberg 行级 DML 簇——**判别完成（净室对抗 21 agent），处置方向待用户拍板**。见 [design 文档](./design-iceberg-rowlevel-dml-classification.md)。**重大定性修正**：原标"~15 文件 LIVE iceberg 特有、需大迁移"是**高估**——此前一次重构已把①通用框架（`RowLevelDmlTransform`/`Registry`/`Command` shell）②派发（`Delete/Update/MergeIntoCommand` 走 registry+能力位，非 instanceof）③iceberg 格式 thrift 装配（`TIcebergDeleteSink/MergeSink` 由连接器 `planWrite` 发）全部中立化/挪到连接器。`grep org.apache.iceberg fe-core/src/main`=0。约 40 单元分三档：**A 通用框架**（约一半，保留）；**B iceberg-命名但内容通用**（大头，改名/泛化即中立——sinks/impl规则/translator visitor/Explain instanceof/Bind规则/RuleSet-PlanType-RuleType-SinkVisitor/`DataPartition.IcebergPartitionField`+`DistributionSpecMerge.IcebergPartitionField`）；**C 真 iceberg 语义薄核**（约6-8单元，改名解决不了、须连接器 SPI——`IcebergRowId`/`IcebergMetadataColumn` position-delete rowid struct + `ICEBERG_EXCLUSION`/`isIcebergMergeMetaColumn` 硬编码 `ICEBERG_ROWID_COL` + Delete/Update/Merge 命令的 rowid 注入）。**核心洞察**：全部 iceberg 语义收敛到一个魔法字符串 `Column.ICEBERG_ROWID_COL`(`__DORIS_ICEBERG_ROWID_COL__`)+其 position-delete 四元组 struct，fe-core ~12 处 string-match。**Trino 对照**：引擎自持通用 MERGE，连接器只贡献 rowId ColumnHandle+RowChangeParadigm+ConnectorMergeSink；Doris 已迁后二者，唯 rowId 列身份仍硬编码 core——离 Trino 只差此一步。**改名不碰持久化**（plan 节点/PlanType/RuleType 是运行期枚举非 Gson 元数据，与 T5.1/T5.5 FQCN 问题正交；thrift 名是连接器已发的契约，Java 节点改名不强制改 thrift）。**用户签字方案甲**（Trino 式泛化 + rowid 走 SPI）。执行蓝图见 [design-iceberg-rowlevel-dml-plan-a-execution.md](./design-iceberg-rowlevel-dml-plan-a-execution.md)：**工作流2（真语义，先做）**=position-delete 合成 rowid 列/元数据列走连接器 SPI（复用**已存在**的 `reservedPassthrough` 先例——iceberg 连接器已用它把 v3 `_row_id` 连接器化；本次把 `__DORIS_ICEBERG_ROWID_COL__` 合成 struct 也搬过去，`IcebergRowId`/`IcebergMetadataColumn` 移入连接器）；**工作流1（改名，后做）**=B 档 iceberg-命名通用单元中立化（编译验证，不碰持久化，thrift 名本次不改）。顺带清 inert 死码（`DeleteCommandContext.toTFileContent`）+ 过时迁移注释。**工作流2 ✅ 已完成并提交 `3cb3d0113e4`**（用户选「删重复定义、暗号名保留」）：侦察实证 rowid 列连接器早已声明→原蓝图「新增 SPI」多余；改删 `IcebergRowId`+`IcebergMetadataColumn` 两 fe-core 重复定义，`getRowIdColumn` 走「全 schema→连接器合成写列→fail-loud」，MERGE NULL 类型取该列类型，`ICEBERG_EXCLUSION` 用内联 `$`-名常量集；`__DORIS_ICEBERG_ROWID_COL__` 线协议名保留 fe-core；净减源 +40/−194；100+ 单测绿、7 路净室对抗复核过、补 `getRowIdColumn`/accessor 单测。**工作流1（机械改名）待做**：B 档 iceberg-命名通用节点/规则/枚举/visitor 中立化（不碰 thrift/持久化），见 [execution 蓝图 §3](./design-iceberg-rowlevel-dml-plan-a-execution.md)；顺带 inert 死码/过时注释清理。

---

## 状态总览

| 批次 | 标题 | 风险 | 前置 | 状态 |
|---|---|---|---|---|
| 0 | 起步 | — | — | ✅ |
| 1 | 零风险依赖删除 | 低 | — | ✅ `76e6d5fcf2d` |
| 2 | 死代码 + 注释纠错 | 低 | — | ✅ `0102a022341`（avro 注释顺延 B3） |
| 3 | iceberg-AWS 依赖簇移除 | 中 | B2 | ✅ 迁测试 `24ddc8d615b` + 删 iceberg 簇 `379e4b07066` + aws-json/avro/parquet `d0f6d3878d3` |
| 4 | 待定依赖定性 | 低 | — | ✅ 三项全删（dynamodb/logs/bce + 孤立 mqtt/validation-api） |
| 5 | LIVE 源特有逻辑迁移/废弃清理 | 高 | B1–3 | 🔄 **进行中**：T5.1 engine=hive✅（废弃→持久化空壳）；T5.2 ranger-hive✅（**误判→原地保留**）；T5.3 hudi TVF✅（整删）；T5.4 散点分支✅（建表校验→下沉连接器；其余 4 项复核保留）；T5.5 es 桩✅（**已死但必须保留→0 源码改动 + 补 Gson 守卫测试**）；T5.6 iceberg 行级DML 🔄（**工作流2 rowid 走连接器✅ `3cb3d0113e4`；剩工作流1 机械改名**） |
