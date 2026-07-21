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
- [~] **T2.4** 注释纠错：`kryo-shaded` "for hudi catalog" → 指向 `WorkloadSchedPolicy`（**已改**）。`avro`/`parquet-avro` "For Iceberg" 注释**推迟到 Batch 3**——那两个依赖 Batch 3 会删/换（avro 删显式声明、parquet-avro→parquet-hadoop），注释随之处理，避免立即被推翻的 churn。
- [x] **T2.V** 验证：`test-compile -pl fe-core -am` BUILD SUCCESS；悬空引用 grep = CLEAN；对抗复核见 HANDOFF。
- [x] **T2.C** commit `0102a022341` `[chore](fe-core) remove dead iceberg/hive insert-sink code; fix stale pom comment`。

---

## Batch 3 — iceberg-AWS 依赖簇整体移除

**前置**：Batch 2（T2.1 已删主源码 iceberg 引用）。**风险**：中（碰测试归属 + 依赖树 + parquet 换库）。整批一起验证。

- [x] **T3.1** 迁 5 个 iceberg 测试类到 `fe-connector-iceberg`（用户拍板 **migrate**）。落在 `org.apache.doris.connector.iceberg.catalog`。**纠正原分析**：它们 0 个 Doris import、直连外部服务测 iceberg SDK，非 property 解析测试。连接器 test classpath 已备齐 iceberg-core/aws/s3-tables-catalog/junit5 + 传递 guava/hadoop → REST/Unity/Dlf/S3Tables 仅改 package；**AWSTest** 删非 iceberg 的 v1-SDK `testAWSS3` + 把唯一 v1 类字面量换成配置字符串（连接器只带 v2）。commit `24ddc8d615b`（双模块 test-compile 绿）。
- [x] **T3.2**（部分）删依赖：`iceberg-core`、`iceberg-aws`、`glue`、`s3tables`、`s3-tables-catalog-for-iceberg` 已删；`grep iceberg/glue/s3tables fe/fe-core/src` = 空。commit `379e4b07066`（`-am` test-compile 绿，gates 过）。**`aws-json-protocol` 未删**（见 T3.3-defer）。
- [~] **T3.3 / aws-json-protocol（deferred，须先验证）**：`aws-json-protocol` 摘除 + `avro` 显式声明删除 + `parquet-avro`→`parquet-hadoop` 替换 + 改 avro "For Iceberg" 注释。三者耦合、且 `dependency:tree -Dincludes` 因 nearest-wins 把 direct 声明提顶层、掩盖 kept 模块是否传递供给。做法：删声明后重跑 `-am dependency:tree`（或 `-Dverbose`）确认仍传递可得 + 运行期协议推理（json 仅 glue/s3tables/iceberg-aws；avro 由 hive-exec runtime 兜底）+ 全量 test-compile（ParquetReader 只用 `parquet.{io,column,hadoop,schema}`）。
- [x] **T3.V**（已做部分）：迁移 + iceberg 簇删除后 fe-core 与连接器均 `-am` test-compile 绿；validate gates 过。deferred 三项另行验证。
- [x] **T3.C**（已提交 2 个）：`24ddc8d615b`（迁测试）、`379e4b07066`（删 iceberg 簇）。deferred 三项待第 3 个 commit。

---

## Batch 4 — 待定依赖定性（先调查，再决定）

**前置**：无。**风险**：低（只调查）。产出=每项一个"删/留/迁"结论，回填分析文档 + HANDOFF。

- [ ] **T4.1** `mvn dependency:tree` 查 `hadoop-aws` 是否需要 `aws-java-sdk-dynamodb`（S3Guard?）/ `aws-java-sdk-logs`（CloudWatch?）。
  - 命令：`mvn -f <repo>/fe/pom.xml -pl fe-core dependency:tree -Dincludes=com.amazonaws`。
- [ ] **T4.2** `com.baidubce:bce-java-sdk`（全 `fe/**/src` 零引用、非 runtime、无注释）：确认 BOS 文件系统（fe-filesystem）是否仍需 / 是否该从 fe-core 迁走或删。
- [ ] **T4.3** 汇总结论，若确认可删则并入相应批次执行。

---

## Batch 5 — LIVE 源特有逻辑迁移（大工程 · 独立设计 · 大概率跨多个 session）

**前置**：Batch 1–3 完成更干净。**风险**：高——**这些是 live 未迁移特性，须走连接器 SPI 委派，不是删除**。每子项先出迁移设计再动手，遵守架构铁律（HANDOFF §2）。

- [ ] **T5.1** iceberg 行级 DML 簇（~15 文件）：`IcebergRowLevelDmlTransform` + `Iceberg{Delete,Merge,Update}Command` + `IcebergMetadataColumn`/`IcebergRowId` + `Logical/PhysicalIcebergDeleteSink`/`MergeSink` + 实现规则 + `PhysicalPlanTranslator` visitor + `DataPartition.IcebergPartitionField`/`DistributionSpecMerge.IcebergPartitionField` + `ExplainCommand` 分支。→ 设计 SPI 行级 DML 委派。
- [ ] **T5.2** legacy `engine=hive` 簇：`catalog/HiveTable.java`（源特有类 + 属性解析）、`catalog/HMSResource.java`、`load/BrokerFileGroup.java:198` + `nereids/load/NereidsBrokerFileGroup.java:215`、`catalog/Env.java:4480/:4847` show-create 臂。
- [ ] **T5.3** `ranger-hive` 授权包（`catalog/authorizer/ranger/hive/*`，9 文件，ServiceLoader head `RangerHiveAccessControllerFactory`）——授权迁移轴。
- [ ] **T5.4** hudi `hudi_meta` TVF：`tablefunction/HudiTableValuedFunction.java`、`tablefunction/MetadataGenerator.java`（`hudiMetadataResult` + `case HUDI`）、`nereids/trees/expressions/functions/table/HudiMeta.java`。
- [ ] **T5.5** 分散的按源分支：
  - `nereids/trees/plans/commands/info/CreateTableInfo.java`（paimon :788、iceberg :784、es :818/:1140 分支）
  - `qe/Coordinator.java:2634` 按源 if-链
  - `datasource/property/storage/AzureProperties.java:311` `isIcebergRestCatalog`（源码有 TODO）
  - `common/util/DatasourcePrintableMap.java:20/55` maxcompute 遮蔽 → **改字符串字面量 `"mc.secret_key"`**（见 HANDOFF §4 风险）
  - `datasource/InternalCatalog.java:1281` es 弃用 guard
- [ ] **T5.6** es 兼容桩 `catalog/EsTable.java` / `catalog/EsResource.java`——碰持久化镜像反序列化，放最后或长期保留（先评估能否安全去除）。

---

## 状态总览

| 批次 | 标题 | 风险 | 前置 | 状态 |
|---|---|---|---|---|
| 0 | 起步 | — | — | ✅ |
| 1 | 零风险依赖删除 | 低 | — | ✅ `76e6d5fcf2d` |
| 2 | 死代码 + 注释纠错 | 低 | — | ✅ `0102a022341`（avro 注释顺延 B3） |
| 3 | iceberg-AWS 依赖簇移除 | 中 | B2 | 🟡 大部完成（迁测试 `24ddc8d615b` + 删 iceberg 簇 `379e4b07066`）；尾巴 aws-json/avro/parquet 待验证 |
| 4 | 待定依赖定性 | 低 | — | ⬜ |
| 5 | LIVE 源特有逻辑迁移 | 高 | B1–3 | ⬜ |
