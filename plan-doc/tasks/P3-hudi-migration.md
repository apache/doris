# P3 — hudi 迁移

> 阶段总览见 [00-master-plan §3.4](../00-connector-migration-master-plan.md)。
> 协作规范见 [AGENT-PLAYBOOK.md](../AGENT-PLAYBOOK.md)。
> 连接器看板：[connectors/hudi.md](../connectors/hudi.md)。
> 关键前情：[DV-005](../deviations-log.md)（依赖假设更正）、[D-019](../decisions-log.md)（hybrid 策略）、[HANDOFF 关键认知 1 / 1b](../HANDOFF.md)。

---

## 元信息

- **状态**：🚧 进行中（批 0 ✅；批 A：T02 ✅，T03 推迟批 E（[DV-006]），T04 待启动）
- **启动日期**：2026-06-04
- **目标完成**：—（hybrid 范围，估时按批 A–C 约 1–1.5 周；批 D 设计 0.5 周；批 E deferred 不计入 P3）
- **实际完成**：—
- **阻塞**：无（P0 ✅ / P1 ✅ / P2 ✅ 已合入 #64096）
- **阻塞下游**：批 E（live cutover）与 P7 hive/HMS migration 合并；P3 批 A–D 不阻塞任何下游
- **主 owner**：@me
- **分支**：从 `branch-catalog-spi` 切（建议 `catalog-spi-04` / `catalog-spi-p3-hudi`）；PR base = `apache/doris:branch-catalog-spi`

---

## 策略：hybrid（D-019）

两轮 code-grounded recon（+ 对抗验证）的结论（详见 [DV-005](../deviations-log.md) / HANDOFF 关键认知 1+1b）：

- HMS-over-SPI **读码已存在但 dormant**（`fe-connector-hms` 客户端库 + `HiveConnectorMetadata`(type `"hms"`) + `HudiConnectorMetadata`(type `"hudi"`)，gate 关闭、零 live caller）。
- scan/split **plumbing 正确**：单 `PluginDrivenScanNode` 能混合 COW-native + MOR-JNI（per-range format，BE 每 range 建 reader），与 legacy `HudiScanNode` 结构等价 —— **混合格式不是问题**。
- **真正阻塞 = catalog 模型错配 + gate**（架构级）；另有一批**与模型无关**的 SPI-surface 正确性缺口。

**hybrid = 现在做 (b)，推迟 (a)**：

- **(b) 现在做（批 A–D，全部 behind 关闭的 gate，零 live-path 风险）**：把 dormant 的 hudi 连接器**硬化到正确性 parity** + 补 metadata 缺口 + 建**测试基线** + 出**模型 dispatch 设计**。这些都与最终选哪种模型无关，且无论如何都要做。
- **(a) 推迟（批 E，登记不编码）**：fe-core 消费 `tableFormatType` 的 per-table 分流、gate flip（`SPI_READY_TYPES` 加 hms/hudi）、live 路径 cutover、删 legacy `datasource/hudi/`、完整增量/time-travel、集群/runtime 验证 —— 并入一个 **properly-scoped hive/HMS migration**（P7 或专门子阶段），避免把 P7 范围与 live 重度 HMS 路径风险压进 P3。

> ⚠️ **P3（hybrid）不交付用户可见行为变化**：hudi 查询仍走 legacy 路径（gate 不翻）。P3 的产出是**连接器硬化 + 测试网 + 设计**，为后续 live cutover 扫清正确性障碍。批 A–C 的验证是**单测 + 设计级**；端到端/集群验证随批 E cutover 一起做（recon 的 open questions 见关联）。

---

## 验收标准

- [x] **批 A / T02**：`column_types` 双 bug 修复（发完整 Hive 类型串 + 弃逗号 join/split）✅（`95f23e9`）
- [ ] **批 A / T04**：time-travel / 增量读 **fail-loud**（不静默返最新 / 不静默全扫）
- [~] **批 A→E / T03**：native split `schema_id` + `params.history_schema_info` 填充 —— **推迟批 E（[DV-006]）**，非 model-agnostic SPI 修复（连接器缺 field-id/InternalSchema/type→thrift；裸基线净回归）
- [ ] **批 B**：`listPartitions*` override + 真实 `applyFilter` 约束裁剪；MVCC/snapshot SPI 实现或显式 unsupported
- [ ] **批 C**：fe-connector-hms/hive/hudi 测试基线（当前零测试）；**parity 测试**——SPI `HudiConnectorMetadata` schema/partition 输出 vs legacy `HiveMetaStoreClientHelper.getHudiTableSchema`，COW & MOR 各一
- [ ] **批 D**：`tableFormatType` 分流消费设计备忘（design-only，**不动 fe-core live 路径**）
- [ ] 全程 fe-connector 编译 + checkstyle 0 + import-gate 通过；新增单测全绿
- [ ] gate 保持关闭（`SPI_READY_TYPES` 不含 hms/hudi）；legacy `datasource/hudi/` 不删（批 A–D 内）
- [ ] 批 E 各项作为 deferred 明确登记，不在 P3 PR 内编码
- [ ] 同步看板 + PROGRESS + connectors/hudi

---

## 任务清单

> ID 永不复用。批次：批 0=recon/决策；批 A=scan 正确性；批 B=metadata 补全；批 C=测试；批 D=模型设计；批 E=deferred（登记）。

| ID | 任务 | 批次 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P3-T01 | 两轮 code-grounded recon + hybrid 决策（D-019）+ 本 task 文件 | 批 0 | @me | ✅ | — | 2026-06-04 | 2026-06-04 | recon #1（元数据）+ #2（scan/split）均含对抗验证；DV-005 记依赖更正；D-019 定 hybrid。锚点见 HANDOFF「P3 关键文件锚点」 |
| P3-T02 | `column_types` 双 bug 修复 + 单测 | 批 A | @me | ✅ | `95f23e9` | 2026-06-04 | 2026-06-04 | (a) `HudiScanPlanProvider` 弃 `ConnectorType.getTypeName()`（丢精度/scale/子类型），改发完整 Hive 类型串（对标 legacy `HudiUtils.convertAvroToHiveType`，如 `decimal(10,2)`/`struct<...>`）；(b) `HudiScanRange` 停止 column_names/column_types/delta_logs 的逗号 join/split（含逗号的类型串会被打碎），改 typed list 端到端。**先读 BE `hudi_jni_reader.cpp` 确认 JNI scanner 期望的精确串格式**（names `,` / types `#`），再改。命中含 decimal/复杂列的 MOR-with-logs JNI split |
| P3-T03 | native split `schema_id` + `history_schema_info` 填充 + 单测 | ~~批 A~~→**批 E** | TBD | 🟡 推迟 | — | — | — | **[DV-006] 推迟批 E**：recon 实证非 model-agnostic SPI-surface 修复——连接器缺 field-id（`HudiColumnHandle` 无）/ Hudi `InternalSchema` 版本 / type→`TColumnType` thrift；「Paimon/ES 已 override」前提失真（其 override 为 predicate/docvalue，**不设** schema 元数据）；裸 `current==file==-1`→BE `ConstNode`(identity-by-name,大小写敏感) **弱于**当前 `by_parquet_name` 名匹配 → **净回归**。faithful field-id evolution parity 需批 E 一次性建机制。批 A 保持现状名匹配（零回归） |
| P3-T04 | time-travel + 增量读 fail-loud 守卫 + 单测 | 批 A | @me | ⏳ | — | — | — | 当前 `HudiScanPlanProvider` 永远用 `timeline.lastInstant`、`HudiTableHandle` 无 snapshot 字段 → `FOR TIME AS OF` 静默返最新；增量读无 SPI 表示。本 task 仅做**显式报错**（不静默），完整实现入批 E。透传 snapshot 的完整 wiring 也可在此起步 |
| P3-T05 | `listPartitions/listPartitionNames/listPartitionValues` override + 真实 `applyFilter` 裁剪 + 单测 | 批 B | @me | ⏳ | — | — | — | 现 Hudi `applyFilter` 列**全部**分区不做约束裁剪（Hive 已做 EQ/IN）；SPI partition 方法默认空。补真实裁剪 + override 分区方法 |
| P3-T06 | MVCC/snapshot SPI（实现或显式 unsupported） | 批 B | @me | ⏳ | — | — | — | `HudiConnectorMetadata` 未 override `beginQuerySnapshot/getSnapshotAt/getSnapshotById`（默认 `Optional.empty()`）。与 T04 关联：要么接 `HudiMvccSnapshot` 语义，要么显式 unsupported |
| P3-T07 | 三模块测试基线 + parity 测试 | 批 C | @me | ⏳ | — | — | — | fe-connector-hms/hive/hudi 当前**零测试**。补单测 + **parity**：SPI `HudiConnectorMetadata` schema/partition 输出 vs legacy `HiveMetaStoreClientHelper.getHudiTableSchema`（fe-core），COW & MOR 各一。Rule 9：测意图（type-string 编码、schema_id、裁剪正确） |
| P3-T08 | `tableFormatType` 分流消费设计备忘（design-only） | 批 D | @me | ⏳ | — | — | — | 写清 `PluginDrivenExternalTable` 如何按 `ConnectorTableSchema.tableFormatType`（现 fe-core 从不消费）把一个 `"hms"` catalog 的 per-table 路由到 HUDI/HIVE/ICEBERG。**不实现 fe-core 消费**（那是批 E）。作为 (a) 落地入口设计，可催生 D-NNN |
| P3-T09 | [deferred] fe-core 消费 `tableFormatType` + hudi 表产出为 `PluginDrivenExternalTable` | 批 E | TBD | ⏳ | — | — | — | **不在 P3 hybrid 编码范围**；并入 hive/HMS migration（D-019）。catalog 模型落地 |
| P3-T10 | [deferred] gate flip（`SPI_READY_TYPES` 加 hms/hudi）+ live cutover + 删 legacy `datasource/hudi/` | 批 E | TBD | ⏳ | — | — | — | **不在 P3 hybrid 编码范围**。15 文件 ~2403 LOC + `HudiDlaTable`(在 hive/)，live caller 仅 7 个 fe-core 文件。cutover 经验证后再删 |
| P3-T11 | [deferred] 集群/runtime 验证 + 完整增量/time-travel + image 兼容 | 批 E | TBD | ⏳ | — | — | — | **不在 P3 hybrid 编码范围**。混合格式 MOR regression、BE JNI parse parity、name-match 精确性、image 反序列化兼容（R-001） |

**状态图例**：⏳ pending / 🚧 in_progress / ✅ done / ❌ blocked / 🚫 deleted

---

## 阶段日志（倒序）

### 2026-06-05（批 A 续：T03 推迟决策）
- **P3-T03 🟡 推迟批 E**（[DV-006]，用户签字 AskUserQuestion「Defer T03 to batch E」）：T03 启动前 4-reader code-grounded recon + 主线核读 BE `table_schema_change_helper.h:219-267` 揭示——schema_id/history **不是** 批 A 可做的 model-agnostic SPI-surface 修复：
  - **连接器缺料**：`HudiColumnHandle` 无 field id；SPI 无 Hudi `InternalSchema` 版本跟踪；连接器模块无 type→`TColumnType` thrift 转换（legacy 在 fe-core `ExternalUtil`，import-gate 禁复用）。
  - **「Paimon/ES 已 override hook」前提失真**：二者 override `populateScanLevelParams` 为 predicate/docvalue，**不设** schema 元数据（无 SPI 先例）。
  - **裸基线净回归**：仅设 `current==file==-1` → BE 走 `ConstNode`（identity-by-name，大小写敏感），**弱于**当前 unset→`by_parquet_name`（鲁棒名匹配，处理大小写/缺列）。faithful field-id evolution parity 需批 E 与 hive/HMS migration 一次性建机制。
  - **批 A 动作**：不发 schema 元数据，保持现状名匹配（**零回归**），不 ship 裸 ConstNode。→ 直接进 **T04**。

### 2026-06-04（批 A 启动）
- **P3-T02 ✅**（commit `95f23e9`，feat）：修 hudi JNI `column_types` 双 bug。
  - **(a)** `HudiScanPlanProvider` 原用 `HudiTypeMapping.fromAvroSchema(..).getTypeName()` 发 **Doris** 裸类型名（`DECIMALV3`/`STRUCT`，丢精度/scale/子类型）；BE Hudi JNI scanner 期望 **Hive 类型串**。新增 `HudiTypeMapping.toHiveTypeString`（忠实复刻 legacy `HudiUtils.convertAvroToHiveType`，import-gate 禁止直接复用 fe-core）。`fromAvroSchema`（→Doris ConnectorType，服务 schema 上报）不动；删 dead `unwrapNullable`。
  - **(b)** `HudiScanRange` 原把 column_names/types/delta_logs 逗号 join 再 split，打碎含逗号的 Hive 类型串（`decimal(10,2)`/`struct<a:int,b:string>`）并使 names↔types 错位。改为 typed `List<String>` 字段直接设 thrift `list<string>`；BE（`hudi_jni_reader.cpp`）自做 join（names `,` / types `#` / delta `,`），与 Java `HadoopHudiJniScanner` split 契约一致（两点 code-grounded 对抗确认）。
  - **测试**：建模块**首批**测试（`HudiTypeMappingTest` 9 + `HudiScanRangeTest` 2 = 11 全绿）。断言旧码会失败的行为（Rule 9）：decimal 精度、struct/array/map 逗号存活、union unwrap、不支持类型 fail-loud、typed-list 对齐 + native 降级。
  - **守门**：fe-connector-hudi 编译 + checkstyle 0 + import-gate 通过；BUILD SUCCESS。**3 路对抗 review（parity / BE-contract / style+test）零确认缺陷**。
  - 设计备忘：[`designs/P3-T02-column-types-design.md`](./designs/P3-T02-column-types-design.md)。gate 保持关闭，零 fe-core/BE/thrift 改动。

### 2026-06-04（批 0）
- **批 0 完成**：两轮 recon（#1 元数据路径就绪 / #2 scan-split 路径，均 8/7-agent code-grounded workflow + 对抗验证）。结论改写原计划依赖假设 → 记 **DV-005**；用户定 **hybrid** 策略 → 记 **D-019**；建本 task 文件。
- 关键结论：HMS-over-SPI 读码 dormant、scan plumbing 正确（混合格式非问题）、真阻塞=模型错配+gate；批 A–D 与模型无关，先做。

---

## 关联

- Master plan 章节：[§3.4 P3 hudi](../00-connector-migration-master-plan.md)、[§3.8 P7 hive+HMS](../00-connector-migration-master-plan.md)（批 E 并入处）
- RFC 章节：tableFormatType / DLA 模型（D-005 相关）
- 决策：[D-005](../decisions-log.md)（DLA 用 tableFormatType）、[D-019](../decisions-log.md)（hybrid 策略）、D-002（PluginDrivenScanNode extends FileQueryScanNode）
- 偏差：[DV-005](../deviations-log.md)（依赖假设更正 + scan 侧 parity gap）
- 风险：R-001（image 兼容，批 E）
- 连接器：[connectors/hudi.md](../connectors/hudi.md)

---

## 当前阻塞项

无。批 A 可立即启动（gate 关闭，零 live-path 风险）。批 E 待 hive/HMS migration 排期。
