# Connector: `hudi`

---

## 概况

| 项 | 值 |
|---|---|
| **catalog type 名** | （依附 hms；通过 `tableFormatType=HUDI` 区分，见 D-005）|
| **fe-connector 模块** | `fe/fe-connector/fe-connector-hudi/` |
| **fe-core 旧路径** | `fe/fe-core/src/main/java/org/apache/doris/datasource/hudi/` |
| **共享依赖** | `fe-connector-hms`（通过 HMS 拿元数据） |
| **计划迁移阶段** | **P3** |
| **当前状态** | ⏸ 未启动 |
| **完成度** | 20% |
| **主 owner** | TBD |

---

## 迁移 Playbook 进度

| 步骤 | 状态 | 备注 |
|---|---|---|
| 1 | 🟡 | fe-core 9 个顶层类（cache key、schema cache、MvccSnapshot、partition utils、HudiUtils）+ `source/` 6 个（含 4 个 incremental relation）|
| 2 | 🟡 | fe-connector 9 个文件：Provider/Metadata/ScanPlanProvider/ScanRange/TableHandle/...|
| 3 | ✅ | 反向 instanceof：0 处（hudi 寄生在 Hive 上，没有独立 `HudiExternalCatalog`）|
| 4 | 🟡 | ConnectorMetadata 骨架完成；incremental query 路径未补 |
| 5 | ⏳ | |
| 6 | ✅ | META-INF/services 已注册 |
| 7 | ⏳ | `SPI_READY_TYPES` 未加（hudi 不能独立创建 catalog）|
| 8-9 | 🚫 | hudi 无独立 catalog；走 D-005 的 `tableFormatType` 模型 |
| 10 | ⏳ | 替换 `visitPhysicalHudiScan` 中 `HMSExternalTable.dlaType=HUDI` 检查 |
| 11 | ⏳ | 删 `HudiScanNode`，由 `PluginDrivenScanNode` + `HudiScanPlanProvider` 承接 |
| 12 | ⏳ | 0 个测试 |
| 13 | ⏳ | 删 `datasource/hudi/` |

---

## SPI 实现完成度

| 扩展点 | 是否需要 | 实现状态 | 备注 |
|---|---|---|---|
| E1 CreateTableRequest | ❌ | n/a | hudi 不支持 CREATE TABLE |
| E2 Procedures | 🟡 | hudi 有 `archive_log` 等 procedure | 后续可考虑 |
| E3 MetaInvalidator | 🟡 | 通过 HMS event 同步 | 复用 `fe-connector-hms` 的 invalidator |
| E4 Transactions | 🟡 | hudi 有 timeline | 暂用 no-op |
| E5 MvccSnapshot | ✅ 需要 | 🟡 批 B 决策 keep default opt-out（T06/DV-007）；完整 `HudiMvccSnapshot` → 批 E | 全体连接器无 override，T04 已 fail-loud time-travel；incremental query 时序入批 E |
| E6 VendedCredentials | ❌ | n/a | |
| E7 SysTables | ❌ | n/a | |
| E8 ColumnStatistics | 🟡 | hudi 有 column stats | 后续 |
| E9 Delete/Merge sink | ❌ | hudi 写路径不在本计划范围 | 与 BE 强耦合 |
| E10 listPartitions | ✅ 需要 | 🟡 批 B：applyFilter EQ/IN 裁剪 ✅（T05 `10b72d4`，镜像 Hive）；`listPartitions*` override → 批 E（DV-007，零 live caller）| 分区裁剪经 applyFilter→prunedPartitionPaths→resolvePartitions 链路 |

---

## 已知特殊性（**重要**）

- **没有独立的 `HudiExternalCatalog`**！hudi 表通过 `HMSExternalTable.dlaType=HUDI` 暴露，本质上是寄生在 Hive 连接器上。
- D-005 决定：用 `ConnectorTableSchema.tableFormatType=HUDI` 显式建模，由 HMS connector 探测后填充。
- 4 个 `HoodieIncremental*Relation` 类是和 hudi-spark 库交互——必须在 fe-connector 模块内（classpath 隔离）。
- P3 实质上要做的是：
  1. 把 `HudiUtils` / `HudiSchemaCacheKey/Value` / `HudiMvccSnapshot` / `HudiPartitionProcessor` 搬到 `fe-connector-hudi`。
  2. 把 `HudiScanNode` 删除，由 `PluginDrivenScanNode` + 增强后的 `HudiScanPlanProvider`（已存在）承接 incremental relation 逻辑。
  3. 改造 `PhysicalHudiScan` 让它走 SPI 路径。
- **P3 启动前必须 P5 paimon 或 P7 hive 进入到至少完成 hms metadata 路径**，否则 hudi 拿不到底层 HMS 表元数据。**这是依赖序的隐藏约束**——见 master plan §3.4 第一段。
- **⚠️ 2026-06-04 recon 更正（[DV-005](../deviations-log.md)）**：上一条「隐藏依赖」与代码不符。HMS-over-SPI 读路径（`fe-connector-hms` 客户端库 + `HiveConnectorMetadata`(type `"hms"`) + `HudiConnectorMetadata`(type `"hudi"`) + `ConnectorTableSchema.tableFormatType` 区分符）**早已存在但 dormant**（`CatalogFactory.SPI_READY_TYPES` 不含 hms/hudi，零 live caller）。**真正阻塞是 catalog 模型错配**：现存连接器是独立 `"hudi"` catalog type，而 Doris 真实模型是 hudi 寄生在 `"hms"` catalog 内、以 `DLAType.HUDI` 暴露，且 fe-core 不消费 `tableFormatType`。P3 改为：先 recon scan/split 路径 + 写 catalog 模型决策备忘（a/b；c 否决）→ 用户签字 → 编码。详见 [HANDOFF](../HANDOFF.md) 关键认知 1。

---

## 关联

- 阶段 task：P3（待启动时建）
- 决策：D-005（DLA 模型方案 A）
- 偏差：（暂无）
- 风险：（暂无独立的）

---

## 进度日志

### 2026-06-05（批 B）
- **P3-T05 ✅**（批 B，commit `10b72d4`）：`HudiConnectorMetadata.applyFilter` 真实 EQ/IN 分区裁剪。原占位实现列**全部** HMS 分区不裁剪、且无条件设 `prunedPartitionPaths`（静默把分区来源从 Hudi-metadata 切到 HMS）；重写为忠实镜像 `HiveConnectorMetadata`（抽取 partition 列 EQ/IN 谓词→列候选→裁剪→仅有效果时回传 pruned handle，否则 `Optional.empty()` 回落 Hudi-metadata listing）。保留 `List<String>` 路径表示 + `-1` 上限；7 helper duplicate from Hive（仅依赖 fe-connector-hms）。`HudiPartitionPruningTest` 8 测全绿；gate 保持关闭。`listPartitions*` override 推迟批 E（[DV-007](../deviations-log.md)：零 live caller、Hive 不 override）。设计 [`../tasks/designs/P3-T05-partition-pruning-design.md`](../tasks/designs/P3-T05-partition-pruning-design.md)。
- **P3-T06 ✅**（批 B 决策，零代码，[DV-007](../deviations-log.md)，用户签字）：MVCC/snapshot SPI 保持 default `Optional.empty()` opt-out，不新增抛异常 override（破 SPI opt-out 约定、全体连接器无 override、无 production caller=死代码、T04 已 fail-loud time-travel）。完整 MVCC 入批 E。设计 [`../tasks/designs/P3-T06-mvcc-design.md`](../tasks/designs/P3-T06-mvcc-design.md)。

### 2026-06-05（批 A）
- **P3-T04 ✅**（批 A，commit `feceabb`）：`visitPhysicalHudiScan` SPI 分支 fail-loud——`FOR TIME/VERSION AS OF`（曾静默返最新）与增量读（曾静默全扫）抛 `AnalysisException`。dormant 分支零 live 风险；单测推迟批 E。**批 A 编码完成**（T02+T04 落地，T03→批 E）。
- **P3-T03 🟡 推迟批 E**（[DV-006](../deviations-log.md)，用户签字）：schema_id/history_schema_info 非批 A 可做的 SPI-surface 修复——`HudiColumnHandle` 无 field id、SPI 无 Hudi `InternalSchema` 版本、连接器无 type→`TColumnType` thrift；裸 `current==file==-1`→BE `ConstNode`(大小写敏感) 弱于现状 `by_parquet_name` 名匹配（净回归）。批 A 保持现状名匹配（零回归，common 无 evolution 可用；改名/evolution 退化非崩溃），faithful parity 入批 E。

### 2026-06-04
- **P3-T02 ✅**（批 A，commit `95f23e9`）：修 JNI scanner `column_types` 双 bug——(a) 发完整 Hive 类型串（新 `HudiTypeMapping.toHiveTypeString` 复刻 legacy `HudiUtils.convertAvroToHiveType`），不再用 `getTypeName()` 丢精度/子类型；(b) `HudiScanRange` typed list 端到端，弃逗号 join/split（曾打碎 `decimal(10,2)`/`struct<...>`），BE 自做 join（types `#`）。建模块首批测试 11 个全绿；gate 保持关闭。设计见 [`../tasks/designs/P3-T02-column-types-design.md`](../tasks/designs/P3-T02-column-types-design.md)。
- P3 启动 recon（8-agent code-grounded workflow + 对抗验证）。结论（[DV-005](../deviations-log.md)）：HMS-over-SPI 读码已存在但 **dormant**（gate 未开、零 live caller）；**真阻塞=catalog 模型错配**（独立 `"hudi"` type vs 寄生 `"hms"` 的 `DLAType.HUDI`，fe-core 不消费 `tableFormatType`）+ 增量读无 SPI 表示（P1-T04 gap）+ 三模块零测试。P3 待 catalog 模型决策（a/b；c 否决）签字后开工。关键文件锚点见 HANDOFF。

### 2026-05-24
- 跟踪文件建立。50% 实现已就位，但 P3 依赖 hms-connector 路径先打通（D-005 模型）。
