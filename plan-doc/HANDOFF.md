# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-05
- **本 session 主题**：**P3 批 C 编码完成**——T07 三模块测试基线（hms/hive 零→有；hudi 扩）+ COW/MOR schema parity（golden-value）落地；列名 casing gap-1 **当场修**（用户签字），Hudi meta-field gap-2 推迟批 E（[DV-008]）。
- **分支**：`catalog-spi-04`（P3 工作分支，基于 `branch-catalog-spi`；P0→P1→P2→recon→批 A/B/C 之上）。工作树预期 clean（仅本地未跟踪 `.audit-scratch/`/`conf.cmy/`/`plan-doc/research/`/`regression-conf.bak`）。

---

## ✅ 本 session 完成项

| Task | 结果 | commits |
|---|---|---|
| **P3-T07** 三模块测试基线 + COW/MOR parity | ✅ 三模块 59 测全绿（hudi 33 + hms 12 + hive 14）；checkstyle 0（含 test 源）+ import-gate 通过 | `435065f`(feat) + 本 doc commit |

**净产出** = 三连接器模块测试网从 {hudi 19, hms 0, hive 0} → {hudi 33, hms 12, hive 14} + COW/MOR schema golden parity + 1 个正确性修（gap-1 列名 casing 对齐 legacy，gate 后硬化、零 live 风险、零回归）。**批 A+B+C 编码完成**。

**commit stack**（新→旧）：本 doc commit→`435065f`(T07 feat)→`04f6576`(批 B handoff)→`10b72d4`(T05)→`301fe38`(批 A handoff)→`2758cf9`(T04 doc)→`feceabb`(T04)→`517c9cf`(T03 defer)→`ac0dc7c`(T02 doc)→`95f23e9`(T02)→`9fcf21a`(recon/D-019)→`0793f03`(P2)→`2b1a3bb`(P1)→`72d6d01`(P0)。

---

## 🚧 未完成 / 待办（下一 session = 批 D）

1. **P3-T08（批 D，下一 session 第一件事）**：`tableFormatType` 分流消费**设计备忘（design-only，零代码）**。写清 `PluginDrivenExternalTable` 如何按 `ConnectorTableSchema.tableFormatType`（现 fe-core **从不消费**）把一个 `"hms"` catalog 的 per-table 路由到 HUDI/HIVE/ICEBERG。**不实现 fe-core 消费**（那是批 E）。作为 (a) 落地入口设计，可催生 D-NNN。
   - **已有素材**：`plan-doc/research/spi-multi-format-hms-catalog-analysis.md`（28KB，本地未跟踪——上一 session 起草的多格式 HMS catalog 分析；T08 设计的直接输入，**先读**，避免重复 recon）。
2. **批 E（deferred，不在 P3 hybrid 编码）**：见下「批 E backlog」。
3. （沿用）T05 regression 推迟批 E（gate 关端到端不可触达；翻闸后断言带分区谓词查询仅扫匹配分区，explain/profile 校 partition 数；precedent DV-003）。

### 批 E backlog（登记，不在 P3 编码）
- T03 schema_id/history 完整 field-id evolution（DV-006）
- T05 `listPartitions*` override（DV-007）
- T06 完整 MVCC（`HudiMvccSnapshot` + snapshot 透传 + 增量时序，DV-007）
- T04 完整 snapshot 透传 + 增量 SPI
- **T07 gap-2**：Hudi meta-field 纳入（SPI 无参 `getTableAvroSchema()` vs legacy `(true)`）真实 fixture 实证（DV-008）
- **T07 gap-1 余项**：`ThriftHmsClient` 源头防御性降字（与 hive 共享，DV-008）
- T09–T11（模型落地/gate flip/删 legacy/集群验证）；端到端/集群验证（含 COW/MOR schema vs live legacy、BE JNI parse parity）

---

## ⚠️ 关键认知 / 临时发现

### 1.【批 C 已用，批 D/E 仍需】parity 可行性 = golden-value（无跨模块编译路径）
- `fe-core` 只依赖 `fe-connector-api` + `fe-connector-spi`，**不依赖**具体 `-hudi`/`-hms`/`-hive` 模块；连接器模块不依赖 fe-core。import-gate（`tools/check-connector-imports.sh`）**只扫 `*/src/main/java`、只禁 connector→fe-core 单向**（test 目录豁免，但无编译路径仍使跨模块 parity 不可行）。
- → 任何 legacy↔SPI parity 用 **golden 值**（注 legacy `file:line`）。测试栈 **JUnit5 only，无 mockito**，替身手写（`FakeHmsClient` 先例：`tableExists`/`getTable`/`listPartitionNames`/`getPartitions` 按需实现，其余 throw）。checkstyle **含 test 源**（`fe/pom.xml:162` `includeTestSourceDirectory=true`）、**禁 static import**（用 `Assertions.assertX`），且 **test 阶段不跑 checkstyle**，需单独 `mvn -pl <module> checkstyle:check`。

### 2.【批 C 关键结论】COW/MOR schema = type-agnostic
- legacy `HMSExternalTable.initHudiSchema` 与 SPI `HudiConnectorMetadata.getTableSchema`→`avroSchemaToColumns` 都从**同一 avro schema** 推导列表，**零表型分支**。COW/MOR 区别**只在 scan planning**（`HudiScanNode.canUseNativeReader` / `HudiScanPlanProvider.planScan:92`：COW=base files native、MOR=merged slices + delta logs JNI）。→ schema parity 是 avro→column 的纯函数；表型只影响 `detectHudiTableType` + split 收集。

### 3.【批 D 直接相关】tableFormatType 模型（T08 设计目标）
- hudi **无独立 catalog**：寄生 `HMSExternalTable.dlaType=HUDI`（D-005）。SPI 侧 `ConnectorTableSchema.tableFormatType`（`getTableSchema` 设 `"HUDI"`）是区分符，但 **fe-core 从不消费**。
- T08 = 设计 `PluginDrivenExternalTable` 如何按 tableFormatType per-table 路由到 HUDI/HIVE/ICEBERG（**design-only，不动 fe-core live 路径**）。legacy 对照：`HMSExternalTable.dlaType` + `initSchema` 分流（`initHudiSchema`/`initIcebergSchema`/`initHiveSchema`）。素材 `research/spi-multi-format-hms-catalog-analysis.md`。

### 4.（沿用）SPI 分区裁剪链路 + Hive parity 基准（T05）
- `PluginDrivenScanNode.applyFilter`→`currentHandle`→`getSplits`→`HudiScanPlanProvider.resolvePartitions` 读 `getPrunedPartitionPaths()`。Hudi `applyFilter` 镜像 `HiveConnectorMetadata.applyFilter`（7 步 + 7 helper duplicate，hudi 仅依赖 fe-connector-hms）。

### 5.（沿用）BE Hudi JNI column_types/names/delta 契约（T02）
- `THudiFileDesc.{delta_logs,column_names,column_types}` thrift `list<string>`；**BE 自做 join**：names `,` / types **`#`** / delta `,`（`hudi_jni_reader.cpp:52-54`）。FE 传 typed list、类型串用 Hive 串（`HudiTypeMapping.toHiveTypeString`，非 `getTypeName()`）。

### 6.（沿用）批 E 去向 + 沿用坑
- T03（DV-006）/ T04 完整 snapshot+增量 / T06 完整 MVCC / T05 list* / T07 gap-2 + ThriftHmsClient 降字：见批 E backlog。
- rebase 后 fe-core `target/generated-sources/.../DorisParser.java` 残留 → cannot find symbol：**clean fe-core**（非 fe-sql-parser），别当代码 bug 查。
- `PhysicalPlanTranslator` 里 hudi **之外**的连接器 `instanceof` 分支待各自 P 阶段迁完再删，**本场只动 hudi**。
- 用户向文档在 doris-website 仓（DV-004）。

---

## 🎯 下一个 session 第一件事（P3 批 D / T08）

```
1. 自检：
   git branch --show-current → catalog-spi-04
   git log --oneline -6 → <doc>(批 C handoff) 435065f(T07 feat) 04f6576(批 B handoff) 10b72d4(T05) 301fe38 2758cf9
   git status → clean（除 .audit-scratch/ conf.cmy/ plan-doc/research/ regression-conf.bak）
   Read plan-doc/tasks/P3-hudi-migration.md（批 D = T08）+ 本文件关键认知 3

2. 启动【批 D / T08】（design-only，不动 fe-core；零代码）：
   先读 plan-doc/research/spi-multi-format-hms-catalog-analysis.md（已有 28KB 分析，直接输入）。
   recon 锚点：
     fe-core 不消费处: PluginDrivenExternalTable（getEngine/schema 路径）、ConnectorTableSchema.tableFormatType（getTableSchema 设但无消费方）
     legacy 模型:      HMSExternalTable.dlaType（HIVE/HUDI/ICEBERG）+ initSchema 分流（initHudiSchema/initIcebergSchema/initHiveSchema）
     decision:         D-005（DLA 用 tableFormatType）
   产出 = 设计备忘 plan-doc/tasks/designs/P3-T08-tableformat-dispatch-design.md（不动 fe-core live 路径），可催生 D-NNN。

3. 文档同步（playbook §5.1 五步）：tasks/P3 状态 + phase log + PROGRESS §三/四 + connectors/hudi（+ 新 D-NNN 如有）。

4. commit：`[doc](connector) [P3-T08] ...`；PR base = `apache/doris:branch-catalog-spi`（base 已对齐）。
```

---

## 📂 P3 关键文件锚点

```
T02（已修）:  HudiTypeMapping.toHiveTypeString / HudiScanRange（typed list）/ BE hudi_jni_reader.cpp:52-54
T03（批 E）:  ExternalUtil.initSchemaInfo / BE table_schema_change_helper.h:219-267 / HudiColumnHandle（无 field id）
T04（已修）:  PhysicalPlanTranslator.visitPhysicalHudiScan SPI 分支（两守卫）
T05（已修）:  HudiConnectorMetadata.applyFilter（7 步 + 7 helper）/ HudiPartitionPruningTest（FakeHmsClient 先例）
T06（决策）:  ConnectorMetadata MVCC 三 default / 无 override（opt-out）
T07（已修）:  HudiConnectorMetadata.avroSchemaToColumns（顶层降字 + package-private static）
              测试: hudi HudiTypeMappingTest/HudiSchemaParityTest/HudiTableTypeTest；hms HmsTypeMappingTest；hive HiveFileFormatTest/HiveConnectorMetadataPartitionPruningTest
              legacy 锚: HMSExternalTable.initHudiSchema:734-758 / HudiUtils.convertAvroToHiveType / fromAvroHudiTypeToDorisType
              设计: designs/P3-T07-test-baseline-design.md
T08（批 D 下一步）: PluginDrivenExternalTable / ConnectorTableSchema.tableFormatType / HMSExternalTable.dlaType+initSchema 分流 / D-005
              素材: plan-doc/research/spi-multi-format-hms-catalog-analysis.md（未跟踪）
gate:         CatalogFactory.java:52（SPI_READY_TYPES，不含 hms/hudi——别动）
设计备忘:     plan-doc/tasks/designs/P3-T02-*.md / T04 / T05 / T06 / T07
scratch:      .audit-scratch/p3-t0X-*.workflow.js（本地 workflow 脚本，未跟踪）
```

---

## 🧠 给下一个 agent 的 meta 建议

- **批 D 是 design-only**（T08）：**不动 fe-core live 路径、不实现 tableFormatType 消费（批 E）、不碰 `SPI_READY_TYPES` / legacy / 非 hudi 连接器**。产出是设计备忘 + 可能的 D-NNN。
- **先读 `research/spi-multi-format-hms-catalog-analysis.md`**（已有 28KB 分析，避免重复 recon）；recon 可用 workflow（参 `.audit-scratch/p3-t0X-recon.workflow.js` 模板）。
- 偏差先记 `deviations-log.md` 再改文档；架构/可行性 fork 先问用户（本场 T07 casing/baseline 两 fork 已签字 → DV-008）。
- Maven：cwd=`fe/`；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；**checkstyle 单独跑**（含 test 源）；**禁 static import**（用 `Assertions.assertX`）。
