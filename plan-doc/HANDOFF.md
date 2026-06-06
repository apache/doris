# 🤝 Session Handoff

> 滚动文档：每次 session 结束覆盖更新；历史见 `git log plan-doc/HANDOFF.md`。
> 新 session 必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → [tasks/P4](./tasks/P4-maxcompute-migration.md)（批次计划）→ [写 RFC](./tasks/designs/connector-write-spi-rfc.md)（§12 P4 TODO / 写 SPI 设计）。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-06（**P4-T02 实现 · Batch A 收尾**）
- **本 session 主题**：**P4-T02 连接器分区 listing 完成**（Batch A 收尾，gate 关、dormant、零 live 风险）。`MaxComputeConnectorMetadata` impl SPI 三方法（`listPartitionNames`/`listPartitions`/`listPartitionValues`），直取 ODPS `getPartitions`，忠实镜像 legacy SHOW PARTITIONS 路径。守门全绿。用户选「提交 T02 然后停」。
- **分支**：`catalog-spi-05`。**本场 1 commit `a03c7279eaf [P4-T02]`**（code + 3 doc-sync 文件）。未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.groovy.bak`（沿用，勿提交）。
- **Batch A（DDL + 分区）= 全完成**（T01 ✅ + T02 ✅）。

---

## ✅ 本 session 完成项（P4-T02，gate 关、dormant、零 live 风险）

- **P4-T02 连接器分区 listing**（commit `a03c7279eaf`）：`MaxComputeConnectorMetadata` impl SPI：
  1. `listPartitionNames(handle)`：`structureHelper.getPartitions(odps, db, tbl)` → 每个 `p.getPartitionSpec().toString(false, true)`（镜像 legacy `MaxComputeExternalCatalog:283` / `MaxComputeExternalTable:201`）。
  2. `listPartitions(handle, filter)`：**filter 忽略返全量**（镜像 legacy SHOW PARTITIONS 不裁剪）；每分区 `ConnectorPartitionInfo(name, values, emptyMap)`，values 由 `PartitionSpec.keys()`/`get(k)` 抽（LinkedHashMap 保序）。
  3. `listPartitionValues(handle, partitionColumns)`：每分区按入参 `partitionColumns` 列序取 `spec.get(col)` → `List<List<String>>`。
- **OQ-4 已定（resolved）**：**不建**连接器自有 cache，分区直取 ODPS（镜像 legacy catalog 直取路径；fe-core SPI meta-cache 覆盖 schema；Rule 2 不投机）。perf 回归再议。
- **保真说明（R12 不静默）**：legacy 双路径分歧 —— catalog（SHOW PARTITIONS 路径）`getPartitions` 无 emptiness guard；table（schema-cache 路径）有 `!partitionColumns.isEmpty()` guard。SPI **锚 catalog SHOW PARTITIONS 路径**故不加 guard。
- **写前核实**：javap 验 ODPS `PartitionSpec` 真实 API（`Set<String> keys()` / `String get(String)` / `toString(boolean,boolean)`，在 **odps-sdk-commons** 非 -core）。
- **守门全绿**（真实 EXIT 核验）：连接器 compile **BUILD SUCCESS/MVN_EXIT=0** + checkstyle **0/CS_EXIT=0** + import-gate **0**。
- **测试（R12 不静默）**：T02 **无新单测** —— 按 P4 计划连接器测试基线延至 **P4-T10**（JUnit5 手写替身、无 mockito）；T02 gate = compile + checkstyle + import-gate（与计划一致，非静默跳过）。

---

## 🚧 下一 session = P4-T03 写/事务 SPI（Batch B 启动，gate 关、dormant）

**第一步 = P4-T03。** ⚠️ **与 T02 不同：T03/T04 未 recon 逐行定稿** —— 先 recon 再写（**别直接 copy legacy**）。建议首步（精读，offset+limit）：
1. 读 **SPI 写接口**：`fe-connector-api` 的 `ConnectorWriteOps`（`beginTransaction`）+ `ConnectorTransaction`（`addCommitData`/`supportsWriteBlockAllocation`/`allocateWriteBlockRange`/`getUpdateCnt`/begin/finish/commit/rollback）。
2. 读 **legacy 源**：`fe-core/.../datasource/maxcompute/MCTransaction.java`（262 LOC，**已含 W2 `addCommitData(byte[])` + block-alloc**，可直接港）。
3. 读 **参考 adopter**：JDBC 连接器的 `ConnectorWriteOps`/`ConnectorTransaction` 实现（P0 已落，唯一现成样板）+ W4 `PluginDrivenTransaction`（委派 wrap 路径）。
4. 然后在连接器新建 `MaxCompute` 侧 `ConnectorTransaction` + `ConnectorWriteOps.beginTransaction`，over W4 委派。**独立 commit `[P4-T03]`**。
5. 守门：连接器 compile + checkstyle + import-gate（**`-pl :fe-connector-maxcompute`**，见坑 6）；若加测则 golden（见红线坑 3）。

> **P4-T04（写计划）紧接 T03**：`Connector.getWritePlanProvider` → `planWrite` 产 `TMaxComputeTableSink`（`DataSinks.thrift:586`，18 字段），填 W5 write-context seam（txn_id/write_session_id/static_partition_spec/block_id）。**OQ-2 = T04 核心难点**（见坑 2）。
> **Batch A 已完成** → **A+B 全绿 + R-004 防御测过 才进 C（翻闸，唯一 live 切点）**。**别回头改 W-phase / 别先翻闸**。

---

## ⚠️ 关键认知 / 坑（务必读）

1. **legacy 是「删」非「搬」**：连接器**已有**读侧等价（metadata/scan/client/structure-helper/type-mapping/predicate），**且 DDL（T01）+ 分区（T02）已港**。剩 **写/事务**（T03/T04）一块功能需港入；`datasource/maxcompute/` 10 文件/3004 LOC 在 cutover（Batch D）**删除**。
2. **写-context 注入 = T04 核心难点（OQ-2）**：W5 留**空** context；runtime `txn_id`/`write_session_id` 原经 `MCInsertExecutor.beforeExec` 注入，plugin-driven 写侧需**重建**该路径填 `TMaxComputeTableSink`（txn_id:18 / write_session_id:15 / block_id:8,9 / static_partition_spec:10）。见 [DV-009]。
3. **commit 协议红线**：`ConnectorTransaction.addCommitData` 反序列化 `TMCCommitData` 必 `TBinaryProtocol`（单点 `CommitDataSerializer`），否则 golden 红。`MCTransaction` 已有 W2 `addCommitData(byte[])` 可直接港。
4. **`getTxnById` 抛异常非返 null**（`GlobalExternalTransactionInfoMgr:30`）——复用须 guard（W3 已修；adopter 注意）。
5. **R-004**：ODPS SDK 在插件 classloader 下连通性——翻闸前（Batch C 入口门）在插件 harness 做防御测。
6. **maven 必绝对 `-f` + `-pl :artifactId`**：`mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute ...`；勿在命令里 `cd` 子目录（破相对路径）。**`-pl` 须用冒号前缀 `:fe-connector-maxcompute`（=artifactId）**——裸名被当**相对路径**解析 → `Could not find the selected project in the reactor`。
7. **读真实 exit code**：命令尾 `echo "MVN_EXIT=$?" >> log`，grep `BUILD SUCCESS|BUILD FAILURE|MVN_EXIT|Tests run:` 核；**勿信**后台 task-notification 的「exit code」（是末尾 echo 的）。
8. **checkstyle**：`CustomImportOrder`（`org.apache.doris.*` 字母序 → 第三方 → `java.*`，组间空行、组内字母序大小写敏感）；`UnusedImports`/`RedundantImport`；`LineLength` 120；test 源也扫、禁 static import。
9. （沿用）rebase 后 fe-core stale `DorisParser` → clean fe-core；import-gate 只扫 `*/src/main/java`、只禁 connector→fe-core 单向（fe-core→fe-connector-api 是允许方向）。
10. **ODPS API 在 `odps-sdk-commons` 非 `-core`**：`PartitionSpec` 等在 commons jar；javap 核 API 时认准 commons（T02 踩过）。

---

## 📂 关键文件锚点

```
P4 计划：  tasks/P4-maxcompute-migration.md（5 批/11 task；Batch A ✅，下一 Batch B）
recon：    research/p4-maxcompute-migration-recon.md（§1 连接器现状 / §3 反向引用 / §5 翻闸点）
写 RFC：   tasks/designs/connector-write-spi-rfc.md（写/事务 SPI 设计 + §12 P4 TODO）
决策：     decisions-log.md D-023（full adopter）/ D-021（scope=C）/ D-022（写 SPI A/B1/C1/D/E）

Batch B 靶（写/事务，T03/T04）：
  SPI 面：  fe-connector-api ConnectorWriteOps / ConnectorTransaction（+ JDBC 连接器现成实现作样板）
  legacy：  fe-core/.../datasource/maxcompute/MCTransaction.java（262，已含 W2 addCommitData/block-alloc）
  连接器：  fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java（+ 新 ConnectorTransaction/WriteOps impl）
  写计划：  Connector.getWritePlanProvider → planWrite 产 TMaxComputeTableSink（gensrc/thrift/DataSinks.thrift:586，18 字段）
  W 接线：  W4 PluginDrivenTransaction（委派 wrap）/ W5 opaque-sink（planWrite layer）

已完成锚点（勿重做）：
  DDL（T01）：MaxComputeConnectorMetadata createTable/dropTable/createDatabase/dropDatabase + MCTypeMapping.toMcType
  分区（T02）：MaxComputeConnectorMetadata listPartitionNames/listPartitions/listPartitionValues

翻闸点（Batch C，recon §5 已 pin）：
  CatalogFactory:52 SPI_READY_TYPES + 删 :146 case；GsonUtils ~405/~478 registerCompatibleSubtype；
  PluginDrivenExternalTable.getEngine ~203-231 + legacyLogTypeToCatalogType ~347

守门命令（连接器模块名 = fe-connector-maxcompute）：
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute -am \
    -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true -DskipTests compile   # 编译（-am 慢，建议后台）
  mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-maxcompute \
    -Dmaven.build.cache.enabled=false checkstyle:check
  bash tools/check-connector-imports.sh                           # 从 repo 根跑
  # 若加测：上面 compile 换 -Dtest=<类> -DfailIfNoTests=false test（golden 见红线坑 3）
```

---

## 🔴 开放问题（Batch 入口前确认）

- **OQ-1**：翻闸后 `InsertIntoTableCommand:563`/`InsertOverwriteTableCommand:320` 是否完全不再经 `MCInsertExecutor`（→ `MCInsertExecutor:64/75` cast `MCTransaction` 成死代码）？**Batch B 验**。
- **OQ-2**：write-context（overwrite/静态分区 + runtime txn_id/write_session_id）填充路径重建——**T04 核心**（见坑 2 / DV-009）。
- **OQ-3**：反向引用穷举（re-grep ~19 含全部 live；category-C 注册站点 gson/enum/metacache 未穷举）——**Batch D / P4-T07 入口先完整 re-grep**。
- ~~**OQ-4**：连接器是否需自有 schema/partition 缓存~~ **✅ 已定（P4-T02）**：不建自有 cache，直取 ODPS（Rule 2）。

---

## 🧠 给下一个 agent 的 meta 建议

- **Batch A（DDL T01 + 分区 T02）已全完成、已提交**；**直接启 P4-T03 写/事务 SPI（Batch B）**，**别回头改 W-phase / 别先翻闸**。
- **T03/T04 ≠ T02**：T02 是 recon 逐行定稿的直接港；**T03/T04 未逐行定稿，首步先读 SPI 写接口 + MCTransaction + JDBC 参考实现再设计**（别盲目 copy legacy；OQ-2 注入路径是真难点）。
- **写路径三红线**：① commit 载荷 `TBinaryProtocol`（坑 3）；② write-context 重建（坑 2/OQ-2）；③ `getTxnById` 抛异常须 guard（坑 4）。
- **每批独立 commit**（用户定时机；本场用户选「提交 T02 然后停」）；守门 maven **`-pl :fe-connector-maxcompute`**（坑 6），读真实 BUILD/MVN_EXIT/CS_EXIT，勿信后台「exit code」通知。
- **A+B 全绿 + R-004 防御测过 → 才进 C（翻闸，唯一 live 切点）**。
