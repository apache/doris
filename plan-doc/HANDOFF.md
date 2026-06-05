# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-05
- **本 session 主题**：**P3 批 D 完成（T08，design-only）**——`tableFormatType` 分流消费设计备忘 + **[D-020]**（用户签字 M2=方案 B per-table SPI provider）。**P3 hybrid in-scope（批 A–D）全部完成**；剩批 E（live cutover）并入 P7。
- **分支**：`catalog-spi-04`（P3 工作分支，基于 `branch-catalog-spi`）。工作树预期 clean（仅本地未跟踪 `.audit-scratch/`/`conf.cmy/`/`regression-conf.bak`；**`plan-doc/research/` 本 session 已纳入 git 跟踪**）。

---

## ✅ 本 session 完成项

| Task | 结果 | commits |
|---|---|---|
| **P3-T08** tableFormatType 分流消费设计备忘 | ✅ design-only（零代码）；产出设计备忘 + [D-020]（M2=方案 B）；核心拆解 M1⊥M2 | 本 doc commit |

**净产出** = 设计备忘 `designs/P3-T08-tableformat-dispatch-design.md` + 决策 D-020 + 把上 session 的 recon 研究文件纳入跟踪。**P3 hybrid 全部 in-scope（批 A–D）完成**：2 正确性修（T02/T05）+ 2 fail-loud/决策（T04/T06）+ 测试网零→59 测（T07）+ 模型 dispatch 设计（T08/D-020）。

**commit stack**（新→旧）：本 doc commit→`76586b2`(批 C handoff)→`435065f`(T07 feat)→`04f6576`(批 B handoff)→`10b72d4`(T05)→`301fe38`(批 A handoff)→`2758cf9`(T04 doc)→`feceabb`(T04)→`517c9cf`(T03 defer)→`ac0dc7c`(T02 doc)→`95f23e9`(T02)→`9fcf21a`(recon/D-019)→`0793f03`(P2)→`2b1a3bb`(P1)→`72d6d01`(P0)。

---

## 🚧 未完成 / 待办（下一 session：三选一，待用户定）

**P3 hybrid in-scope（批 A–D）已全部完成。** 没有"批 D 之后的批"——批 E 是 deferred、并入 P7。下一 session 是一个**分叉点**：

1. **开 P3 PR**（推荐先确认）：把 `catalog-spi-04` 的批 A–D 工作开 PR，base = `apache/doris:branch-catalog-spi`。**git/push/PR 只在用户明确要求时做**（本 session 未开）。P3 无独立"开 PR"task（不同于 P2-T13），需用户拍板是否现在开、是否等批 E。
2. **批 E 并入 P7**（不在 P3 编码）：live cutover——见下「批 E backlog」。属 hive/HMS migration（P7 或专门子阶段），不在 P3 PR 内。
3. **启 P4**（maxcompute）：若 P3 告一段落，按 master plan 进下一连接器。

> ⚠️ 三选项**都不应**在 P3 分支内碰 `SPI_READY_TYPES` / fe-core 消费实现 / legacy `datasource/hudi/` / 非 hudi 连接器——皆批 E。

### 批 E backlog（登记，不在 P3 编码；T08/D-020 已为其出设计）
- **M1**（T08 设计）：fe-core `PluginDrivenExternalTable` 消费 `tableFormatType`——`PluginDrivenSchemaCacheValue` 缓存格式 + `getEngine/getEngineTableTypeName` per-table 化（opaque 串、热路径不读）。
- **M2**（T08/D-020 设计）：新增 default `ConnectorMetadata.getScanPlanProvider(handle)` + fe-core `PluginDrivenScanNode.getSplits` 优先 per-table 回落 per-catalog + hms 网关按 `handle.getTableType()` 委派。
- T03 schema_id/history 完整 field-id evolution（DV-006）
- T05 `listPartitions*` override（DV-007）；T06 完整 MVCC（DV-007）；T04 完整 snapshot 透传 + 增量 SPI
- **T07 gap-2**：Hudi meta-field 纳入（`getTableAvroSchema()` 无参 vs legacy `(true)`）真实 fixture 实证（DV-008）；gap-1 余项 `ThriftHmsClient` 源头防御降字（DV-008）
- T09–T11（模型落地/gate flip/删 legacy/集群验证）；Iceberg-on-hms 经 SPI 依赖 **P6** 补 `IcebergScanPlanProvider`（M3）；探测共享化消 drift（M5，P7）
- 端到端/集群验证（COW/MOR schema vs live legacy、BE JNI parse parity、混合多格式 catalog）

---

## ⚠️ 关键认知 / 临时发现

### 1.【T08/D-020 新结论】keystone gap = M1（身份消费）⊥ M2（scan 路由），可分离
- `tableFormatType` **产而不用**：`HiveConnectorMetadata.getTableSchema` 设了它，但 `PluginDrivenExternalTable.initSchema:79-109` **只读 `getColumns()`**、丢 `getTableFormatType()`（本 session firsthand 核读确认）。第二缺口：`getEngine:195-215`/`getEngineTableTypeName:217-231` switch **catalog type** 非 per-table format。
- **M1**（fe-core 读格式做 per-table 引擎名/身份，**opaque 串、热路径不读**）在 A/B/C **三方案通用**；**M2**（单 hms connector 产 Hudi/Iceberg scan plan）才是 A/B/C 分歧处。→ keystone 可控化。
- **M2 = 方案 B**（[D-020]，用户签字）：新增向后兼容 default `ConnectorMetadata.getScanPlanProvider(handle)`（默认 null→回落 per-catalog `Connector.getScanPlanProvider()`），fe-core `PluginDrivenScanNode.getSplits` 优先 per-table、回落 per-catalog。前提：`ConnectorScanPlanProvider.planScan:62-66` 入参已带 per-table handle（本 session 核实）。**A 备选**（连接器内 router，零 SPI churn）；**C 否决**（fe-core 长格式分派，违瘦 fe-core）。
- **D-020 细化 D-005**（非推翻）：tableFormatType 区分符沿用；D-005 的"fe-core→PhysicalXxxScan"措辞早于 P1 scan-node 统一，由 per-table provider seam 取代。**批 E 实现别按 D-005 旧措辞做 PhysicalXxxScan**。

### 2.【批 C 已用，批 E 仍需】parity 可行性 = golden-value（无跨模块编译路径）
- `fe-core` 只依赖 `fe-connector-api` + `fe-connector-spi`，**不依赖**具体 `-hudi`/`-hms`/`-hive` 模块；连接器模块不依赖 fe-core。import-gate（`tools/check-connector-imports.sh`）**只扫 `*/src/main/java`、只禁 connector→fe-core 单向**（test 豁免，但无编译路径仍使跨模块 parity 不可行）。
- → legacy↔SPI parity 用 **golden 值**（注 legacy `file:line`）。测试栈 **JUnit5 only，无 mockito**，替身手写（`FakeHmsClient` 先例）。checkstyle **含 test 源**（`fe/pom.xml:162`）、**禁 static import**（用 `Assertions.assertX`）、**test 阶段不跑 checkstyle** → 单独 `mvn -pl <module> checkstyle:check`。

### 3.【批 C 关键结论】COW/MOR schema = type-agnostic
- legacy `HMSExternalTable.initHudiSchema` 与 SPI `HudiConnectorMetadata.getTableSchema`→`avroSchemaToColumns` 都从**同一 avro schema** 推导列表，**零表型分支**。COW/MOR 区别**只在 scan planning**（`HudiScanPlanProvider.planScan:92`：COW=base files native、MOR=merged slices + delta logs JNI）。→ schema parity 是 avro→column 纯函数；表型只影响 `detectHudiTableType` + split 收集。

### 4.（沿用）SPI 分区裁剪链路 + Hive parity 基准（T05）
- `PluginDrivenScanNode.applyFilter`→`currentHandle`→`getSplits`→`HudiScanPlanProvider.resolvePartitions` 读 `getPrunedPartitionPaths()`。Hudi `applyFilter` 镜像 `HiveConnectorMetadata.applyFilter`（7 步 + 7 helper duplicate，hudi 仅依赖 fe-connector-hms）。

### 5.（沿用）BE Hudi JNI column_types/names/delta 契约（T02）
- `THudiFileDesc.{delta_logs,column_names,column_types}` thrift `list<string>`；**BE 自做 join**：names `,` / types **`#`** / delta `,`（`hudi_jni_reader.cpp:52-54`）。FE 传 typed list、类型串用 Hive 串（`HudiTypeMapping.toHiveTypeString`，非 `getTypeName()`）。

### 6.（沿用）批 E 去向 + 沿用坑
- rebase 后 fe-core `target/generated-sources/.../DorisParser.java` 残留 → cannot find symbol：**clean fe-core**（非 fe-sql-parser），别当代码 bug 查。
- `PhysicalPlanTranslator` 里 hudi **之外**的连接器 `instanceof` 分支待各自 P 阶段迁完再删，**本场只动 hudi**。
- 用户向文档在 doris-website 仓（DV-004）。
- connectors/hudi.md 的 §关联「偏差：（暂无）」是 pre-existing 陈旧（实际 DV-005..008 相关），本场未顺手改（surgical）；下次清 kanban 时一并修。

---

## 🎯 下一个 session 第一件事

```
1. 自检：
   git branch --show-current → catalog-spi-04
   git log --oneline -6 → <本 doc>(T08/D-020) 76586b2(批 C handoff) 435065f(T07 feat) 04f6576 10b72d4 301fe38
   git status → clean（除 .audit-scratch/ conf.cmy/ regression-conf.bak；research/ 现已跟踪）
   Read PROGRESS.md §一/§三 + 本文件关键认知 1（M1⊥M2 + D-020）

2. 与用户确认分叉（本文件「未完成/待办」三选一）：
   (1) 开 P3 PR（base apache/doris:branch-catalog-spi）——需用户明确要求才 push/开 PR
   (2) 批 E 并入 P7（live cutover；T08/D-020 已出 M1+M2 设计）
   (3) 启 P4（maxcompute）
   → P3 内不碰 SPI_READY_TYPES / fe-core 消费实现 / legacy / 非 hudi 连接器（皆批 E）

3. 若走 (2) 批 E：实现序见本文件「批 E backlog」M1→M2→M4→翻闸；
   设计直接读 designs/P3-T08-tableformat-dispatch-design.md（M1+M2 + Implementation Plan + Open）。
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
              设计: designs/P3-T07-test-baseline-design.md
T08（本场，设计）: 设计 designs/P3-T08-tableformat-dispatch-design.md；决策 D-020
   keystone:   PluginDrivenExternalTable.initSchema:79-109（只读 columns）/ getEngine:195-215 / getEngineTableTypeName:217-231（switch catalog type）
   M2 seam:    ConnectorMetadata:37-44（加 default getScanPlanProvider(handle)）/ Connector.getScanPlanProvider:40-42（per-catalog 回落）
               ConnectorScanPlanProvider.planScan:62-66（入参带 handle）/ PluginDrivenScanNode.getSplits（~356-378，fe-core 改动点，批 E）
   载体:       ConnectorTableSchema.getTableFormatType:58-60
   素材:       plan-doc/research/spi-multi-format-hms-catalog-analysis.md（本场已跟踪）
gate:         CatalogFactory.java:52（SPI_READY_TYPES，不含 hms/hudi——别动）
设计备忘:     plan-doc/tasks/designs/P3-T02-*.md / T04 / T05 / T06 / T07 / T08
scratch:      .audit-scratch/p3-t0X-*.workflow.js（本地 workflow 脚本，未跟踪）
```

---

## 🧠 给下一个 agent 的 meta 建议

- **P3 hybrid 收尾**：批 A–D 已全部 in-scope 完成。下一步是**分叉决策**（PR / 批 E→P7 / P4），**先问用户**，别默认开 PR 或自动进 P4。
- **批 E 实现按 T08 设计走**（M1⊥M2，M2=方案 B），**别按 D-005 旧"PhysicalXxxScan"措辞**（已被 D-020 supersede）。新 default 方法保持 D-009（不破签名）。
- 偏差先记 `deviations-log.md` 再改文档；架构/可行性 fork 先问用户（本场 M2 方案 B 已签字 → D-020）。
- Maven：cwd=`fe/`；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；**checkstyle 单独跑**（含 test 源）；**禁 static import**。
```
