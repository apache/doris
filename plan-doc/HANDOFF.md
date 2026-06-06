# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md)（批次计划）→ [maxcompute recon](./research/p4-maxcompute-migration-recon.md) / [写 RFC §12](./tasks/designs/connector-write-spi-rfc.md)。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**实现 session ⑤ · P4 Batch A 启动**）
- **本 session 主题**：**P4-T01 连接器 DDL 完成**（Batch A，gate 关、dormant、零 live 风险）。`MaxComputeConnectorMetadata` impl SPI create/drop table+db（忠实港 legacy `MaxComputeMetadataOps`，消费 P0 `ConnectorCreateTableRequest`）+ `MCTypeMapping.toMcType` 反向类型映射。**附带修 fe-core 共享转换器 CHAR/VARCHAR 长度丢失 [DV-010]**（用户签字）+ 回归测。守门全绿。用户选「提交 T01 然后停」。
- **分支**：`catalog-spi-05`。**本场 1 commit `[P4-T01]`**（含上次 session 未提交的设计文档 + 本场 doc-sync，用户选「并入 P4-T01」）。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，勿提交）。

---

## ✅ 本 session 完成项（P4-T01，gate 关、dormant、零 live 风险）

- **P4-T01 连接器 DDL**（commit `[P4-T01]`）：`MaxComputeConnectorMetadata` impl SPI `createTable(ConnectorCreateTableRequest)` / `dropTable(handle)` / `createDatabase` / `dropDatabase`，忠实港 legacy `MaxComputeMetadataOps` 的校验（列非空/去重/identity-only 分区/hash-only bucket 1..1024/lifecycle 1..37231）+ 建 MC `TableSchema` + lifecycle/`mc.tblproperty.*`/bucketNum 抽取。**消费 P0 request 非 fe-core `CreateTableInfo`**（classloader 隔离）。连接器 `McStructureHelper` 已含全部 ODPS 原语（createTableCreator/dropTable/createDb/dropDb），无需新建。
- **`MCTypeMapping.toMcType(ConnectorType)`**：legacy `dorisTypeToMcType` 的逆；按大写 `typeName`（== `PrimitiveType.toString()`）switch（BOOLEAN..DECIMAL256 / DATE* / DATETIME* / CHAR / VARCHAR / STRING），递归 ARRAY/MAP/STRUCT，不支持类型抛 `DorisConnectorException`。
- **[DV-010] 修 fe-core 共享转换器**：`ConnectorColumnConverter.toConnectorType` 对 CHAR/VARCHAR 把 `getLength()` 写入 precision 字段（原 `getScalarPrecision()`=0 丢长度）；回归测 `ConnectorColumnConverterTest#testCharVarcharLengthPreserved`。**触碰共享 P0 代码 → 影响 live jdbc/es CHAR/VARCHAR CREATE TABLE（更正确，低风险）**。
- **守门全绿**（真实 EXIT 核验）：连接器 compile + checkstyle **0** + import-gate；fe-core `ConnectorColumnConverterTest` **9/0F0E**。
- **保真说明（R12 不静默）**：legacy 拒 auto-inc/aggregated 列校验无法表达（`ConnectorColumn` 无标志，nereids 上游已拒）→ 丢弃，次要。

---

## 🚧 下一 session = P4-T02 分区 listing（Batch A 收尾，gate 关、dormant）

**第一步 = P4-T02。** 在 `MaxComputeConnectorMetadata` impl SPI 三方法（**设计已 recon 定稿，直接写**）：
1. `listPartitionNames(handle)`：`structureHelper.getPartitions(odps, db, tbl)` → 每个 `p.getPartitionSpec().toString(false, true)`（**镜像 legacy `MaxComputeExternalCatalog:283` / `MaxComputeExternalTable:201`**）。
2. `listPartitions(handle, filter)`：每分区建 `ConnectorPartitionInfo(name, values, emptyMap)`；values 由 `PartitionSpec.keys()`/`get(k)` 抽取；**filter 忽略（返全量，镜像 legacy SHOW PARTITIONS 不裁剪）**。
3. `listPartitionValues(handle, partitionColumns)`：每分区按 `partitionColumns` 顺序取 `spec.get(col)` → `List<List<String>>`。
4. 新增 import：`ConnectorPartitionInfo` / `ConnectorExpression`（filter 参数）/ `com.aliyun.odps.Partition` / `com.aliyun.odps.PartitionSpec` / `java.util.Collections`。
5. **OQ-4 已定**：**不建**连接器自有 cache（直取 ODPS，镜像 legacy catalog `getPartitions` 直取路径；fe-core SPI meta-cache 覆盖 schema；Rule 2 不投机）。perf 回归再议。
6. 守门：连接器 compile + checkstyle + import-gate（**`-pl :fe-connector-maxcompute`**，见坑 6）。独立 commit `[P4-T02]`。

> **Batch B（写/事务，P4-T03/T04，gate 关）可与本 T02 并行/接续**。**A+B 全绿 + R-004 防御测过 → 才进 C（翻闸，唯一 live 切点）**：连接器 DDL/分区/写未达 parity 时翻闸即断 CREATE TABLE / SHOW PARTITIONS / INSERT。**别回头改 W-phase**（W1–W7 已完成）+ **别先翻闸**。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **legacy 是「删」非「搬」**：连接器**已有**读侧等价（metadata/scan/client/structure-helper/type-mapping/predicate）；`datasource/maxcompute/` 10 文件 / 3004 LOC 在 cutover（Batch D）**删除**。只 **DDL + 写/事务 + 分区** 三块功能需港入连接器。recon §2「MOVE」标签失准。
2. **写-context 注入 = T04 核心难点（OQ-2）**：W5 留**空** context；runtime `txn_id`/`write_session_id` 原经 `MCInsertExecutor.beforeExec` 注入，plugin-driven 写侧需**重建**该路径填 `TMaxComputeTableSink`（txn_id:18 / write_session_id:15 / block_id:8,9 / static_partition_spec:10）。见 [DV-009]。
3. **commit 协议红线**：`ConnectorTransaction.addCommitData` 反序列化 `TMCCommitData` 必 `TBinaryProtocol`（单点 `CommitDataSerializer`），否则 golden 红。`MCTransaction` 已有 W2 `addCommitData(byte[])` 可直接港。
4. **`getTxnById` 抛异常非返 null**（`GlobalExternalTransactionInfoMgr:30`）——复用须 guard（W3 已修；adopter 注意）。
5. **R-004**：ODPS SDK 在插件 classloader 下连通性——翻闸前（Batch C 入口门）在插件 harness 做防御测。
6. **maven 必绝对 `-f` + `-pl :artifactId`**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute ...`；勿在命令里 `cd` 子目录（破相对路径）。**`-pl` 须用冒号前缀 `:fe-connector-maxcompute`（=artifactId）**——裸名 `fe-connector-maxcompute` 被 maven 当**相对路径**（`fe/fe-connector-maxcompute`，不存在）解析 → `Could not find the selected project in the reactor`（本场踩坑；handoff 旧命令缺冒号是错的）。
7. **读真实 exit code**：命令尾 `echo "MVN_EXIT=$?" >> log`，grep `BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|Tests run:` 核；**勿信**后台 task-notification 的「exit code」（是末尾 echo 的）。
8. **checkstyle**：`CustomImportOrder`（`org.apache.doris.*` 字母序 → 第三方 → `java.*`，组间空行、组内字母序大小写敏感）；`UnusedImports`/`RedundantImport`；`LineLength` 120；test 源也扫、禁 static import。
9. （沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只扫 `*/src/main/java`、只禁 connector→fe-core 单向（fe-core→fe-connector-api 是允许方向）。

---

## 📂 关键文件锚点

```
P4 计划：  tasks/P4-maxcompute-migration.md（5 批/11 task + 验收 + 风险/OQ + 批次依赖图）
recon：    research/p4-maxcompute-migration-recon.md（§1 连接器现状 / §3 反向引用 / §5 翻闸点）
写 RFC：   tasks/designs/connector-write-spi-rfc.md §12（P4 TODO）
决策：     decisions-log.md D-023（full adopter）/ D-021（scope=C）/ D-022（写 SPI）

Batch A 靶：
  legacy DDL：fe-core/.../datasource/maxcompute/MaxComputeMetadataOps.java（createTableImpl/dropTableImpl/truncateTableImpl）
  连接器：    fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java（+ 新 ConnectorTableOps impl）
  P0 SPI 面： ConnectorTableOps.createTable(ConnectorCreateTableRequest)（default 退化）；CreateTableInfoToConnectorRequestConverter（fe-core 侧产 request）
  分区：      港 MaxComputeExternalCatalog/Table 的 getPartitions；连接器 MCConnectorClientFactory client

Batch B 靶（写/事务）：
  MCTransaction（262，已含 W2 addCommitData/block-alloc）→ 连接器 ConnectorTransaction + ConnectorWriteOps.beginTransaction
  Connector.getWritePlanProvider → planWrite 产 TMaxComputeTableSink（DataSinks.thrift:586，18 字段）

翻闸点（Batch C，recon §5 已 pin）：
  CatalogFactory:52 SPI_READY_TYPES + 删 :146 case；GsonUtils ~405/~478 registerCompatibleSubtype；
  PluginDrivenExternalTable.getEngine ~203-231 + legacyLogTypeToCatalogType ~347

守门命令（连接器模块名 = fe-connector-maxcompute）：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -Dtest=<类> -DfailIfNoTests=false test
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
```

---

## 🔴 开放问题（Batch 入口前确认）

- **OQ-1**：翻闸后 `InsertIntoTableCommand:563`/`InsertOverwriteTableCommand:320` 是否完全不再经 `MCInsertExecutor`（→ `MCInsertExecutor:64/75` cast `MCTransaction` 成死代码）？**Batch B 验**。
- **OQ-2**：write-context（overwrite/静态分区 + runtime txn_id/write_session_id）填充路径重建——**T04 核心**。
- **OQ-3**：反向引用穷举（re-grep ~19 含全部 live；category-C 注册站点 gson/enum/metacache 未穷举）——**Batch D / P4-T07 入口先完整 re-grep**。
- **OQ-4**：~~连接器是否需自有 schema/partition 缓存~~ **已定（P4-T02 设计）**：**不建**连接器自有 cache，直取 ODPS（镜像 legacy catalog `getPartitions` 直取路径；fe-core SPI meta-cache 覆盖 schema；Rule 2 不投机）。perf 回归再议。

---

## 🧠 给下一个 agent 的 meta 建议

- **P4-T01 DDL 已完成**（commit `[P4-T01]`）；**直接启 P4-T02 分区 listing**（设计已 recon 定稿，见上「下一 session」可直接写），**别回头改 W-phase / 别先翻闸**。
- **T02 与 Batch B（写/事务，T03/T04）均 gate 关、dormant、可并行/接续**；**翻闸（C）是唯一 live 切点**，前置 = A+B parity + R-004。
- **每批独立 commit**（用户定时机；本场用户选「提交 T01 然后停」）；守门 maven **`-pl :fe-connector-maxcompute`**（坑 6），读真实 BUILD/MVN_EXIT/CS_EXIT，勿信后台「exit code」通知。
- **DV-010 已修 fe-core 共享转换器**（CHAR/VARCHAR 长度，影响 live jdbc/es，已记）；P4-T01 起点的「读 legacy MaxComputeMetadataOps」已完成、其逻辑已港入连接器。
