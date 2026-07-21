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
- [ ] **T5.3** hudi `hudi_meta` TVF：`tablefunction/HudiTableValuedFunction.java`、`tablefunction/MetadataGenerator.java`（`hudiMetadataResult` + `case HUDI`）、`nereids/trees/expressions/functions/table/HudiMeta.java`。
- [ ] **T5.4** 分散的按源分支：
  - `nereids/trees/plans/commands/info/CreateTableInfo.java`（paimon :788、iceberg :784、es :818/:1140 分支）
  - `qe/Coordinator.java:2634` 按源 if-链
  - `datasource/property/storage/AzureProperties.java:311` `isIcebergRestCatalog`（源码有 TODO）
  - `common/util/DatasourcePrintableMap.java:20/55` maxcompute 遮蔽 → **改字符串字面量 `"mc.secret_key"`**（见 HANDOFF §4 风险）
  - `datasource/InternalCatalog.java:1281` es 弃用 guard
- [ ] **T5.5** es 兼容桩 `catalog/EsTable.java` / `catalog/EsResource.java`——碰持久化镜像反序列化，长期保留候选（先评估能否安全去除）。
- [ ] **T5.6** iceberg 行级 DML 簇（~15 文件，**工作量最大，故排最后**）：`IcebergRowLevelDmlTransform` + `Iceberg{Delete,Merge,Update}Command` + `IcebergMetadataColumn`/`IcebergRowId` + `Logical/PhysicalIcebergDeleteSink`/`MergeSink` + 实现规则 + `PhysicalPlanTranslator` visitor + `DataPartition.IcebergPartitionField`/`DistributionSpecMerge.IcebergPartitionField` + `ExplainCommand` 分支。→ 设计 SPI 行级 DML 委派。

---

## 状态总览

| 批次 | 标题 | 风险 | 前置 | 状态 |
|---|---|---|---|---|
| 0 | 起步 | — | — | ✅ |
| 1 | 零风险依赖删除 | 低 | — | ✅ `76e6d5fcf2d` |
| 2 | 死代码 + 注释纠错 | 低 | — | ✅ `0102a022341`（avro 注释顺延 B3） |
| 3 | iceberg-AWS 依赖簇移除 | 中 | B2 | ✅ 迁测试 `24ddc8d615b` + 删 iceberg 簇 `379e4b07066` + aws-json/avro/parquet `d0f6d3878d3` |
| 4 | 待定依赖定性 | 低 | — | ✅ 三项全删（dynamodb/logs/bce + 孤立 mqtt/validation-api） |
| 5 | LIVE 源特有逻辑迁移/废弃清理 | 高 | B1–3 | 🔄 **进行中**：T5.1 engine=hive✅（废弃→持久化空壳）；T5.2 ranger-hive✅（**误判→原地保留**）；T5.3 hudi TVF / T5.4 散点分支 / T5.5 es 桩 / T5.6 iceberg 行级DML ⬜ |
