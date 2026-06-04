# 🤝 Session Handoff

> 这是**滚动文档**：每次 session 结束时覆盖更新；历史通过 `git log plan-doc/HANDOFF.md` 查看。
> 新 session 开始时必读：[PROGRESS.md](./PROGRESS.md) → 本文件 → 对应 task 文件。
> 协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)

---

## 📅 最后一次 handoff

- **日期 / 时间**：2026-06-05
- **本 session 主题**：**P3 批 A 编码完成**——T02（column_types 双 bug）✅ + T04（time-travel/增量 fail-loud）✅ 落地；T03（schema_id/history）经 code-grounded recon 证非 model-agnostic SPI 修复，**推迟批 E**（[DV-006]，用户签字）。
- **分支**：`catalog-spi-04`（P3 工作分支，基于 `branch-catalog-spi`；P0→P1→P2→recon(D-019) 之上）。工作树 clean（仅本地未跟踪 `.audit-scratch/`/`conf.cmy/` 等）。

---

## ✅ 本 session 完成项

| Task | 结果 | commits |
|---|---|---|
| **P3-T02** column_types 双 bug | ✅ 修复 + 模块首批 11 单测全绿 + 3-lens 对抗 review 零确认缺陷 | `95f23e9`(code) `ac0dc7c`(doc) |
| **P3-T03** schema_id/history_schema_info | 🟡 **推迟批 E**（[DV-006]）——recon 证：连接器缺 field-id/InternalSchema/type→thrift；裸基线 BE 走 `ConstNode`(大小写敏感) 弱于现状 `by_parquet_name` = 净回归 | `517c9cf`(DV-006+doc) |
| **P3-T04** time-travel/增量 fail-loud | ✅ `visitPhysicalHudiScan` SPI 分支抛 `AnalysisException`；fe-core 编译+checkstyle 0 | `feceabb`(code) `2758cf9`(doc) |

**commit stack**（新→旧）：`2758cf9`(T04 doc)→`feceabb`(T04)→`517c9cf`(T03 defer)→`ac0dc7c`(T02 doc)→`95f23e9`(T02)→`9fcf21a`(recon/D-019)→`0793f03`(P2 #64096)→`2b1a3bb`(P1 #63641)→`72d6d01`(P0 #63582)。

**批 A 净产出** = 2 个正确性修复（gate 后硬化，零 live 风险，零回归）+ 1 个 code-grounded 推迟决策。每 task 走 design 备忘（`tasks/designs/P3-T0X-*.md`）→ impl → build → commit → 5 步文档同步。

---

## 🚧 未完成 / 待办（下一 session = 批 B）

1. **P3-T05（批 B，下一 session 第一件事）**：`listPartitions/listPartitionNames/listPartitionValues` override + 真实 `applyFilter` EQ/IN 分区裁剪。现状：Hudi `applyFilter` 列**全部**分区不裁剪（`HudiScanPlanProvider.resolvePartitions` 用 `handle.getPrunedPartitionPaths()`，但没人 set 它 → 永远 null → 列全部）；Hive 已做 EQ/IN。**对标** `HiveConnectorMetadata.applyFilter`。
2. **P3-T06（批 B）**：MVCC/snapshot SPI。`HudiConnectorMetadata` 未 override `beginQuerySnapshot/getSnapshotAt/getSnapshotById`（默认 `Optional.empty()`=unsupported）。与 T04 关联：大概率**显式 unsupported**（与 T04 fail-loud 一致）即可，完整 MVCC 入批 E。
3. **批 C**（T07）：三模块（hms/hive/hudi）测试基线 + COW/MOR parity 测试（SPI `HudiConnectorMetadata` schema/partition vs legacy `HiveMetaStoreClientHelper.getHudiTableSchema`）。注：T07 parity 也该覆盖 T02 的 column_names/types 列集合+顺序 vs legacy。
4. **批 D**（T08）：`tableFormatType` 分流消费设计备忘（design-only，不动 fe-core）。
5. **批 E（deferred，不在 P3 hybrid 编码）**：T03（schema_id/history 完整 field-id evolution）、T04 的完整 snapshot 透传+增量 SPI+MVCC、T09–T11（模型落地/gate flip/删 legacy/集群验证）。并入 hive/HMS migration。
6. **T04 单测推迟批 E**（dormant 分支不可 exercise；regression 断言 `FOR TIME AS OF`/增量→报错；precedent DV-003）。

---

## ⚠️ 关键认知 / 临时发现

### 1.【批 A 已用】BE Hudi JNI column_types/names/delta 契约（T02 已修，code-grounded 两点确认）
- `THudiFileDesc.{delta_logs,column_names,column_types}` 是 thrift `list<string>`；**BE 自做 join**：names `,` / types **`#`** / delta `,`（`be/src/format/table/hudi_jni_reader.cpp:52-54`），Java `HadoopHudiJniScanner.java` split 同（types `#`@:113 / names `,`@:212 / delta `,`@:106）。types 用 `#` 正因类型串含逗号（`decimal(10,2)`/`struct<...>`）。**FE 永远传 typed list、不 join/split**；类型串用 Hive 串（`HudiTypeMapping.toHiveTypeString` 复刻 legacy `HudiUtils.convertAvroToHiveType`），**不是** Doris `getTypeName()`。

### 2.【批 E 必读】T03 schema_id/history 为何不是 SPI-surface 可修（DV-006，code-grounded）
- BE `table_schema_change_helper.h:219-267`：`history_schema_info` **unset** → `by_parquet_name`/`by_orc_name`（鲁棒名匹配，处理大小写/缺列）= **现状**；`current==file` schema_id → **`ConstNode`**(`:92-121`，**identity-by-name，大小写敏感**)；id 不同 → `by_table_field_id`（唯一 field-id evolution 路径）。
- **裸基线 `current==file==-1`→ConstNode 弱于现状名匹配 = 净回归**。faithful 需：`HudiColumnHandle` 加 field id（现无）+ Hudi `InternalSchema` 版本（`getCommitInstantInternalSchema`）+ 连接器内 type→`TColumnType` thrift（legacy 在 fe-core `ExternalUtil`，import-gate 禁复用）。legacy hudi baseline = `ExternalUtil.initSchemaInfo(params,-1L,table.getColumns())`（`HudiScanNode:240`）+ native split per-commit `InternalSchema.schemaId()`。**「Paimon/ES 已 override hook 设 schema」前提失真**——其 override 为 predicate/docvalue。
- **结论**：批 E 与 hive/HMS migration 一并建机制。

### 3.【批 E 必读】T04 fail-loud 位置 + 完整实现去向
- 守卫在 `PhysicalPlanTranslator.visitPhysicalHudiScan` SPI 分支（`:828-841` 附近）——**唯一**同时可见 `getTableSnapshot()` + `getIncrementalRelation()` 处（SPI surface 拿不到 incremental；`IncrementalRelation` 是 fe-core 概念，4 个 `*IncrementalRelation` 仍在 fe-core）。完整：snapshot 透传到 `HudiScanPlanProvider.planScan`（现永远用 `timeline.lastInstant()`@`:103`）+ 增量 SPI 表示 + MVCC（T06）。

### 4.（沿用）rebase 后 fe-core 编译坑：stale `DorisParser`
- rebase 拉入 nereids 语法拆分后，`fe-core/target/generated-sources/.../DorisParser.java` 残留导致 cannot find symbol。**修法：clean fe-core**（不是 fe-sql-parser），别当代码 bug 查。

### 5.（沿用）import gate + P1 fallback + docs-next
- `tools/check-connector-imports.sh`：fe-core 不能 import `org.apache.doris.connector.*`；连接器模块不能 import fe-core `org.apache.doris.(catalog|common|datasource|qe|analysis|nereids|planner)`。
- `PhysicalPlanTranslator` 里 hudi **之外**的连接器 `instanceof` 分支待各自 P 阶段迁完再删，**本场只动了 hudi 的 SPI 分支**，别乱碰其他。
- 用户向文档在 doris-website 仓（DV-004），本仓只有 `docs/`。

---

## 🎯 下一个 session 第一件事（P3 批 B / T05）

```
1. 自检：
   git branch --show-current → catalog-spi-04
   git log --oneline -6 → 2758cf9(T04 doc) feceabb(T04) 517c9cf(T03 defer) ac0dc7c(T02 doc) 95f23e9(T02) 9fcf21a(recon)
   git status → clean（除 .audit-scratch/ conf.cmy/ regression-conf.bak）
   Read plan-doc/tasks/P3-hudi-migration.md（批 B = T05/T06）+ 本文件关键认知

2. 启动【批 B / T05】（仍 behind 关闭的 gate，零 live 风险）：listPartitions override + 真实 applyFilter EQ/IN 裁剪。
   recon 锚点：
     fe-connector-hudi: HudiScanPlanProvider.resolvePartitions(:284-312)（用 handle.getPrunedPartitionPaths，永远 null）
                        HudiConnectorMetadata（看有无 applyFilter；listPartitions* 默认空）
                        HudiTableHandle（prunedPartitionPaths 字段 + toBuilder）
     对标 fe-connector-hive: HiveConnectorMetadata.applyFilter（真 EQ/IN 分区裁剪——recon DV-005 已确认 Hive 做、Hudi 不做）
     SPI: ConnectorMetadata.applyFilter / listPartitions* 签名（fe-connector-api）
   先 recon（可用 workflow，参 .audit-scratch/p3-t03-recon.workflow.js 模板）→ 设计备忘 → 实现 → 单测 → 守门。

3. 守门 / 构建（每 task）：
   mvn -pl fe-connector/fe-connector-hudi -am test -Dmaven.build.cache.enabled=false -DfailIfNoTests=false
   （cwd=fe/）；checkstyle 0 + import-gate 通过。**注意 Doris checkstyle 禁 static import**（用 `Assertions.assertX`，非 `import static`）。
   若改 fe-core：mvn -pl fe-core -am compile（大；rebase 后失败先 clean fe-core）。

4. 文档同步（每 task，playbook §5.1 五步）：tasks/P3 状态+phase log + PROGRESS §一/三/四 + connectors/hudi（+ 新 DV/D 如有）。

5. commit：`[feat|doc](connector) [P3-Tnn] ...`；PR base = `apache/doris:branch-catalog-spi`（base 已对齐）。
```

---

## 📂 P3 关键文件锚点

```
T02（已修）:  fe-connector-hudi/HudiTypeMapping.java（toHiveTypeString）
              HudiScanPlanProvider.java:118-120（用 toHiveTypeString）
              HudiScanRange.java（typed list 字段 + populateRangeParams 直接 set）
              BE: hudi_jni_reader.cpp:52-54 / be-java-extensions/.../HadoopHudiJniScanner.java:106/113/212
T03（批 E）:  ExternalUtil.initSchemaInfo（fe-core:86-92）/ HudiScanNode:240,288-324（per-split schema_id）
              BE: table_schema_change_helper.h:219-267（ConstNode vs by_parquet_name vs by_table_field_id）
              HudiColumnHandle（无 field id）/ ConnectorScanPlanProvider.populateScanLevelParams（hook，:162-165）
T04（已修）:  PhysicalPlanTranslator.visitPhysicalHudiScan SPI 分支（:828-841 附近，两守卫）
T05（批 B 下一步）: HudiScanPlanProvider.resolvePartitions / HudiConnectorMetadata / HudiTableHandle.prunedPartitionPaths
              对标 HiveConnectorMetadata.applyFilter
gate:         CatalogFactory.java:52（SPI_READY_TYPES，不含 hms/hudi——别动）
设计备忘:     plan-doc/tasks/designs/P3-T02-*.md / P3-T04-*.md
scratch:      .audit-scratch/p3-t02-review.workflow.js / p3-t03-recon.workflow.js（本地 workflow 脚本，未跟踪）
```

---

## 🧠 给下一个 agent 的 meta 建议

- **批 B 仍是 model-agnostic SPI 硬化**（gate 关，零 live 风险）；不要碰 `SPI_READY_TYPES`、不删 legacy、不动 hudi 之外的连接器 `instanceof` 分支。
- **T05 先确认 SPI applyFilter 链路**：`ConnectorMetadata.applyFilter` 回传裁剪后的 handle（带 prunedPartitionPaths）→ `HudiScanPlanProvider.resolvePartitions` 已会消费它。对标 Hive 的真实 EQ/IN 裁剪。
- **T06 大概率显式 unsupported**（与 T04 一致），别在批 B 做完整 MVCC（入批 E）。
- 偏差先记 `deviations-log.md`（本场记了 DV-006）再改文档；架构/可行性 fork 先问用户（本场 T03 已问并签字）。
- Maven：cwd=`fe/`；`-pl <module> -am`；`-Dmaven.build.cache.enabled=false`；测试 `-DfailIfNoTests=false`；**checkstyle 禁 static import**。
```
