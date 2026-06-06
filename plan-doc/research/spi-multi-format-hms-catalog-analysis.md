# 独立调研：SPI 体系下「单 HMS catalog 同时访问 Hive / Iceberg / Hudi」的现状分析

> **性质**：独立调研快照（read-only），不修改任何现有文档。结论仅引用、不改写 [PROGRESS](../PROGRESS.md) / [tasks/P3](../tasks/P3-hudi-migration.md) / [DV-005](../deviations-log.md) / [D-019](../decisions-log.md)。
> **方法**：6-reader code-grounded recon workflow（legacy-model / spi-catalog-gate / connector-providers / format-dispatch / scan-split-path / module-deps-reuse）+ 主线核读。所有结论带 `file:line` 锚点。
> **调研日期**：2026-06-05　**分支**：`catalog-spi-04`（基于 `branch-catalog-spi`）
> **范围**：只回答「legacy 单 `hms` catalog 同时暴露 Hive+Iceberg+Hudi」这一能力在 SPI 体系下的现状、依赖、复用、调用关系、阶段、缺口、后续步骤。**未做任何代码改动。**

---

## 0. TL;DR（一句话结论）

**当前 SPI 体系「尚未」端到端支持单个 `hms` catalog 同时访问 Hive/Iceberg/Hudi。** 三件事已就位：①连接器模块齐全（hive 注册 `"hms"` 类型、hudi 注册 `"hudi"`、iceberg 注册 `"iceberg"`）；②**per-table 格式探测已忠实复刻 legacy**（`HiveTableFormatDetector` 与 `HMSExternalTable.makeSureInitialized` 同序同集）；③探测结果已写入 `HiveTableHandle.tableType` + `ConnectorTableSchema.tableFormatType`。

但**三处关键链路断裂**，使其仍不可用：

1. **`tableFormatType` 产而不用**——fe-core `PluginDrivenExternalTable.initSchema()` 拿到 `ConnectorTableSchema` 后**只读 columns、从不读 `getTableFormatType()`**（`PluginDrivenExternalTable.java:~93`/`~210`），per-table 格式信号在 fe-core 边界被丢弃。
2. **scan 派发按连接器硬编码、非按表格式**——`HiveScanPlanProvider.planScan` 对所有表都发 `HiveScanRange` 且 `tableFormatType="hive"`（`HiveScanRange.java:120-122,195`），**从不读 `handle.getTableType()`**；hms 里的 Hudi/Iceberg 表会被当成 Hive 误扫。
3. **一个 `Connector` 只有一个 `ScanPlanProvider`（per-catalog 非 per-format）**——`Connector.getScanPlanProvider()` 默认返 null、`HiveConnector` 恒返 `HiveScanPlanProvider`；没有按 `HiveTableType` 选 `HudiScanPlanProvider`/`IcebergScanPlanProvider` 的 router。

加之 **gate 关闭**（`SPI_READY_TYPES={jdbc,es,trino-connector}`，不含 hms/hudi/iceberg，`CatalogFactory.java:52`），**整个 HMS 家族当前一律走 legacy `HMSExternalCatalog`**——SPI 路径是 dormant 的。

> 这与项目既有判断 [DV-005](../deviations-log.md)（真阻塞=catalog 模型错配 + fe-core 不消费 `tableFormatType`）、[D-019](../decisions-log.md)（hybrid：先硬化连接器、推迟模型落地到 P7/批 E）**完全吻合**——本调研在代码层面进一步坐实了"缺什么"。

---

## 1. Legacy 模型（目标行为：SPI 必须复刻它）

单个 `HMSExternalCatalog`（type=`"hms"`）同时暴露 Hive/Iceberg/Hudi 表，靠**per-table 一次性格式探测 + 多态分派**：

| 环节 | 位置 | 行为 |
|---|---|---|
| catalog | `HMSExternalCatalog.java:52-106` | 单实例，无 per-format 子类；所有表走 `HMSExternalTable` |
| 格式枚举 | `HMSExternalTable.java:208-210` | `DLAType { UNKNOWN, HIVE, HUDI, ICEBERG }` |
| **一次性探测** | `HMSExternalTable.java:250-307` | `makeSureInitialized()` 顺序：①Iceberg（`table_type=ICEBERG` 参数）→ ②Hudi（input format 或 `flink.connector=hudi`）→ ③Hive（支持的 input format）；设 `dlaType` + 建多态 `dlaTable`（`IcebergDlaTable`/`HudiDlaTable`/`HiveDlaTable`）|
| schema 分派 | `HMSExternalTable.java:384-408` | `getFullSchema()` switch(dlaType)：HUDI→`HudiDlaTable.getHudiSchemaCacheValue()`、ICEBERG→`IcebergUtils.getIcebergSchema()`、else→Hive |
| cache 引擎分派 | `HMSExternalTable.java:226-240` | `getMetaCacheEngine()` switch：`Hive/Hudi/IcebergExternalMetaCache.ENGINE` |
| **scan 分派** | `PhysicalPlanTranslator.java:~724-770` | `visitPhysicalFileScan` switch(`getDlaType()`)：ICEBERG→`IcebergScanNode`、HIVE→`HiveScanNode`、HUDI→抛异常（须走 `visitPhysicalHudiScan`→`HudiScanNode`，`:819`）|
| 多态基类 | `HMSDlaTable.java:36-87` | 抽象基类定义 per-format 的 partition/snapshot/MTMV 操作；3 个实现 `Hive/Hudi/IcebergDlaTable` |

**要点**：legacy 的"同时多格式"靠 **`DLAType` 这个 per-table tag + 处处 switch(dlaType)**。SPI 必须复刻这个 per-table tag 的产生 **与** 消费（switch）。当前**只复刻了产生，没复刻消费**（见 §4.3 / §6）。

---

## 2. 模块全景与依赖图

### 2.1 依赖图（已 code/pom 确认，箭头 = "依赖")

```
                         fe-thrift (provided)
                              ▲
                       fe-connector-api ──────────────┐ (SPI 接口: Connector / ConnectorMetadata /
                              ▲                        │  ConnectorTableSchema / handle / pushdown / scan)
                       fe-extension-spi                │
                              ▲                        │
                       fe-connector-spi ───────────────┤ (ConnectorProvider / ConnectorContext)
                         ▲         ▲                    │
        ┌────────────────┘         └───────────┐       │
 fe-connector-hms              fe-connector-iceberg     │
 (共享 HMS Thrift 客户端库,       (type="iceberg";        │
  非 plugin: HmsClient /          iceberg-core/-aws,     │
  ThriftHmsClient / HmsTableInfo  hadoop; **不依赖 hms,   │
  / HmsPartitionInfo /            也不依赖 api!**)        │
  HmsTypeMapping;                 + iceberg-api          │
  hive-catalog-shade,             + iceberg-backend-     │
  hadoop, commons-pool2)            {rest,hms,glue,dlf,  │
   ▲           ▲                     hadoop,s3tables}     │
   │           │                                         │
fe-connector- fe-connector-                              │
  hive          hudi                                     │
 (type="hms")  (type="hudi";                             │
               + hudi-common,                            │
               hudi-hadoop-mr)                           │
   │           │            │                            │
   └───────────┴────────────┴──── 均依赖 fe-connector-api/-spi ┘

 fe-core ── 仅依赖 ──> fe-connector-api + fe-connector-spi
   （拥有 CatalogFactory / ConnectorFactory / ConnectorPluginManager /
     PluginDrivenExternalCatalog·Database·Table / PluginDrivenScanNode）
   **绝不依赖任何 fe-connector 实现模块**（hms/hive/hudi/iceberg/jdbc/es...）
```

### 2.2 依赖边清单（pom 实证）

| 模块 | 依赖 | 锚点 |
|---|---|---|
| `fe-connector-api` | `fe-thrift`(provided) | `fe-connector-api/pom.xml:45-51` |
| `fe-connector-spi` | `fe-connector-api` + `fe-extension-spi` | `fe-connector-spi/pom.xml:42-52` |
| `fe-connector-hms` | `fe-connector-spi` + `hive-catalog-shade` + `hadoop-common` + `commons-pool2`（**库，非 plugin**）| `fe-connector-hms/pom.xml:43-96` |
| `fe-connector-hive` | `fe-connector-spi` + `fe-connector-api`(provided) + `fe-thrift`(provided) + **`fe-connector-hms`** | `fe-connector-hive/pom.xml:43-82` |
| `fe-connector-hudi` | `fe-connector-spi` + `fe-connector-api`(provided) + `fe-thrift`(provided) + **`fe-connector-hms`** + `hudi-common` + `hudi-hadoop-mr` | `fe-connector-hudi/pom.xml:44-96` |
| `fe-connector-iceberg` | `fe-connector-spi` + `iceberg-core` + `iceberg-aws` + `hadoop-common`（**无 hms，且 pom 未见 api**）| `fe-connector-iceberg/pom.xml:43-81` |
| `fe-core` | `fe-connector-api` + `fe-connector-spi`（仅此）| 由 import-gate 保证 |

**关键结构观察**：
- **依赖脊柱**：`api ← spi ← hms ← {hive, hudi}`；`iceberg` 走另一条线（Iceberg SDK，**不复用 hms**）。
- **Hudi 不依赖 Hive**（pom 确认）——这印证了 P3-T05 里"分区裁剪 helper 只能从 Hive 复刻、不能跨模块共享"的判断。
- **单向隔离**：`tools/check-connector-imports.sh:30-60` 禁止连接器 import fe-core `{catalog,common,datasource,qe,analysis,nereids,planner}`；fe-core 只 import `connector.api.*`/`connector.spi.*`。这是整个 SPI 解耦的护栏。

### 2.3 各连接器完成度（LOC / 阶段，recon 实测）

| 连接器 | LOC / 类数 | 注册 type | 实现范围 | 缺 |
|---|---|---|---|---|
| **fe-connector-hms** | 1461 / 9 | —（共享库）| HMS Thrift 客户端 + 类型映射 | n/a |
| **fe-connector-hive** | 2010 / 12 | `"hms"` | metadata + scan + 分区裁剪 + **格式探测** | 非-Hive 格式的 scan 派发 |
| **fe-connector-hudi** | 1854 / 11 | `"hudi"` | metadata + COW/MOR scan（T02/T04/T05 已硬化）| 接入 `hms` catalog 的 per-table 路径；MVCC/增量(批 E) |
| **fe-connector-iceberg** | 596 / 6 | `"iceberg"` | **metadata-only**（list/getTableHandle/getTableSchema，用 Iceberg SDK）| **无 `ScanPlanProvider`**；pom 未依赖 `fe-connector-api` |

---

## 3. SPI 接口契约 & 调用关系（call chains）

### 3.1 关键 SPI 类型

- `Connector`（`fe-connector-api/Connector.java:34-121`）：`getMetadata(session)` + `getScanPlanProvider()`（默认返 null，`:40-42`，**per-catalog 一个**）。
- `ConnectorMetadata`（`ConnectorMetadata.java:37-44`）= `ConnectorSchemaOps + ConnectorTableOps + ConnectorPushdownOps + ConnectorStatisticsOps + ConnectorWriteOps + ConnectorIdentifierOps + Closeable`。
- `ConnectorProvider`（`fe-connector-spi/.../ConnectorProvider.java:40-98`）：`getType()` / `supports(type,props)`（默认 `type.equalsIgnoreCase(getType())`）/ `create(props,ctx)` / `apiVersion()`。
- `ConnectorTableSchema`（`fe-connector-api/.../ConnectorTableSchema.java:29-92`）：`tableName + columns + **tableFormatType(String)** + properties`。**承载 per-table 格式信号的载体**。
- `ConnectorScanPlanProvider`（`.../scan/ConnectorScanPlanProvider.java:38-196`）：`planScan(session, handle, columns, filter, limit) → List<ConnectorScanRange>`。
- `ConnectorScanRange.getTableFormatType()`（`.../scan/ConnectorScanRange.java:96-98`）：默认 `"plugin_driven"`，各连接器 override（Hive→`"hive"`、Hudi→`"hudi"`）。

### 3.2 catalog 创建链路

```
CREATE CATALOG
 → CatalogFactory.createCatalog()  (CatalogFactory.java:71-184)
   → if (SPI_READY_TYPES.contains(type))        // 当前仅 jdbc/es/trino-connector
       ConnectorFactory.createConnector(type, props, ctx)
        → ConnectorPluginManager.createConnector()  (:126-144)
           → 遍历 providers, 第一个 provider.supports(type,props)==true
           → provider.create(props, ctx) → Connector
       → new PluginDrivenExternalCatalog(..., connector)
     else  // hms/iceberg/paimon/hudi/max_compute 走这里
       new HMSExternalCatalog(...) / IcebergExternalCatalogFactory.createCatalog(...) ...   ← legacy
 → (FE 重启/反序列化) PluginDrivenExternalCatalog.initLocalObjectsImpl()  (:87-145)
       → 用带 auth 的 DefaultConnectorContext 重新 createConnector（连接器生命周期 = 2 次创建）
```

### 3.3 元数据 / schema 链路（per-table 格式在此"产生"）

```
PluginDrivenExternalTable.initSchema()  (PluginDrivenExternalTable.java:79-109)
 → metadata.getTableHandle(session, db, tbl)
     → [Hive] HiveConnectorMetadata.getTableHandle()  (:105-131)
         → HiveTableFormatDetector.detect(HmsTableInfo)  (:77-100)  ← 产生 HiveTableType{HIVE|HUDI|ICEBERG|UNKNOWN}
         → new HiveTableHandle(..., tableType)            ← 格式写入 handle
 → metadata.getTableSchema(handle)
     → [Hive] HiveConnectorMetadata.getTableSchema()  (:134-154)
         → detectFormatType(tableInfo)  (:282-294)  ← 产生 tableFormatType 字符串("HIVE_*"/"HUDI"/"ICEBERG")
         → return new ConnectorTableSchema(name, cols, **formatType**, props)
 → ❗ initSchema 只迭代 tableSchema.getColumns()，**从不读 getTableFormatType()** ← 信号在此丢弃（见 §6 缺口①）
```

### 3.4 scan / split 链路（per-table 格式在此"本应被消费"却未）

```
PhysicalPlanTranslator.visitPhysicalFileScan()  (:~735-740)
 → if (table instanceof PluginDrivenExternalTable)  → PluginDrivenScanNode   // 先于 HMSExternalTable 匹配
PluginDrivenScanNode.getSplits()  (:356-378)
 → connector.getScanPlanProvider()        // per-catalog 一个；Hive 恒返 HiveScanPlanProvider
 → scanProvider.planScan(session, currentHandle, cols, filter, limit)
     → [Hive] HiveScanPlanProvider.planScan()  (:95-132)
         → resolvePartitions(handle) → listAndSplitFiles(...)
         → 每 split: HiveScanRange{ ..., tableFormatType="hive" }  (:268-296)  ← ❗ 不读 handle.getTableType()
     → [Hudi] HudiScanPlanProvider.planScan()  (:85-162)   // 但 hms catalog 不会路由到它
         → HoodieTableMetaClient → resolvePartitions → COW/MOR split → HudiScanRange{tableFormatType="hudi"}
 → PluginDrivenScanNode.setScanParams()  (:381-395)
     → TTableFormatFileDesc.setTableFormatType(scanRange.getTableFormatType())  ← BE 按此 string 选 reader
```

> **断点可视化**：格式信号有两条独立通道——(1) `ConnectorTableSchema.tableFormatType`（metadata 阶段产生，**fe-core 不消费**）；(2) `ConnectorScanRange.getTableFormatType()`（scan 阶段产生，**被消费但 per-connector 硬编码**）。两条都无法让"一个 hms catalog 把某张表当 Hudi/Iceberg 扫"。

---

## 4. per-table 格式探测：已就位的部分

SPI 侧 `HiveTableFormatDetector.detect(HmsTableInfo)`（`fe-connector-hive/.../HiveTableFormatDetector.java:77-100`）**逐条镜像** legacy `HMSExternalTable.supportedIcebergTable/supportedHoodieTable/supportedHiveTable`：

```
(1) params["table_type"] == "ICEBERG"                         → ICEBERG
(2) params["flink.connector"] == "hudi"
    || inputFormat ∈ {HoodieParquetInputFormat, HoodieParquetRealtimeInputFormat, ...}  → HUDI
(3) inputFormat ∈ {MapredParquetInputFormat, OrcInputFormat, TextInputFormat, ...}      → HIVE
(4) else                                                       → UNKNOWN
```

- 同序、同集，与 fe-core 检测**不漂移**（两套各一份，recon 已比对一致；潜在 drift 风险见 §8）。
- 结果落两处：`HiveTableHandle.tableType`（handle，`HiveTableHandle.java:~41`）+ `ConnectorTableSchema.tableFormatType`（schema，`HiveConnectorMetadata.java:153`）。

**所以"探测"这一步已具备 legacy 的全部能力**——问题在下游"消费/路由"。

---

## 5. 复用地图（哪些可复用）

| 可复用资产 | 位置 | 谁在用 / 可被谁用 |
|---|---|---|
| **HMS Thrift 客户端**（`HmsClient`/`ThriftHmsClient`，池化）| `fe-connector-hms` | hive + hudi 已复用；iceberg-HMS-backend **未**复用（用 Iceberg SDK）|
| `HmsTableInfo` / `HmsPartitionInfo` DTO | `fe-connector-hms` | hive + hudi |
| `HmsTypeMapping`（HMS→ConnectorType）| `fe-connector-hms` | hive + hudi |
| **`HiveTableFormatDetector`**（per-table 格式探测）| `fe-connector-hive` | 仅 hive 内部；**应抽到共享层**供 router 复用 |
| `ConnectorScanRange.populateRangeParams()` 钩子 | `fe-connector-api` | 各连接器写 per-split BE thrift（Hive ACID、Hudi JNI）|
| **`PluginDrivenScanNode`** 通用 split/pushdown/limit/projection | `fe-core` | 任何新 provider 插入 `getScanPlanProvider()` 即免费获得 |
| `ConnectorMetadata.applyFilter()` 下推钩子 | `fe-connector-api` | Hive/Hudi 分区裁剪；Iceberg/Hudi 可 override 加格式特定下推 |
| 分区裁剪 helper（extractPartitionPredicates 等）| hive ↔ hudi **重复**（P3-T05 登记）| 待 P7 consolidate |
| `ConnectorFactory.createConnector()` null-fallback | `fe-core` | legacy↔SPI 共存的 feature-flag（`SPI_READY_TYPES`）|

---

## 6. 关键缺口（为什么还不能端到端）

> 按"阻断性"排序。①②③是机制断点，④⑤是模块缺失，⑥是开关。

**① `tableFormatType` 产而不用（keystone gap）**
`HiveConnectorMetadata` 正确地 per-table 设置了 `ConnectorTableSchema.tableFormatType`，但 `PluginDrivenExternalTable.initSchema()`（`:79-109`）**只读 columns、从不读 `getTableFormatType()`**。legacy 的 `DLAType` tag 在 SPI 里有"产生"无"消费"。→ fe-core 无从得知一张 SPI 表是 Hive/Hudi/Iceberg，也就无法把它路由到对的 scan/cache 路径。

**② scan 派发 per-connector 硬编码、非 per-table**
`HiveScanPlanProvider.planScan` 对所有表恒发 `tableFormatType="hive"`（`HiveScanRange.java:120-122,195`），**从不读 `handle.getTableType()`**（`HiveScanPlanProvider` 取 inputFormat/serde 但不分支格式）。→ hms 里的 Hudi/Iceberg 表会被 BE 当 Hive 文件误扫。

**③ 一个 `Connector` 只有一个 `ScanPlanProvider`**
`Connector.getScanPlanProvider()` 是 per-catalog（`Connector.java:40`），`HiveConnector` 恒返 `HiveScanPlanProvider`（`HiveConnector.java:60-62`）。没有"按 `HiveTableType` 选 `HudiScanPlanProvider`/`IcebergScanPlanProvider`"的 router/strategy。

**④ Iceberg SPI 仅 metadata、无 scan provider**
`IcebergConnectorMetadata` 仅 167 LOC（list/getTableHandle/getTableSchema，用 Iceberg SDK）；`IcebergConnector` **无 `getScanPlanProvider()` override → 返 null**（`IcebergConnector.java`，scan 仍在 fe-core `IcebergScanNode`）。且 `fe-connector-iceberg` pom **未依赖 `fe-connector-api`**（仅 spi），是接入 scan SPI 前必须补的结构缺口。

**⑤ Hudi 的 metadata/scan 未接入 `hms` catalog 的 per-table 路径**
`HudiConnectorProvider` 注册的是**独立** `"hudi"` 类型（面向专用 Hudi catalog），不在 `hms` catalog 的 per-table 分派内。hms 里探测为 HUDI 的表，目前 SPI 无法把它交给 `HudiConnectorMetadata`/`HudiScanPlanProvider`。

**⑥ gate 关闭**
`SPI_READY_TYPES={jdbc,es,trino-connector}`（`CatalogFactory.java:52`）不含 hms/hudi/iceberg → 整个 HMS 家族走 legacy `HMSExternalCatalog`。即便①–⑤补齐，也需翻闸 + legacy 兼容/cutover + image 反序列化兼容（R-001）。

**附：测试缺口**　多格式分派零测试（`HMSExternalTableTest` 仅测 view）；三连接器模块 parity 测试为 P3 批 C 待补项。

---

## 7. 当前阶段定位

> 引用 [PROGRESS.md](../PROGRESS.md)（不改写）：

- **P0 SPI 基座 ✅ / P1 scan-node 收口 ✅ / P2 trino-connector ✅**（已合入 `branch-catalog-spi`）。
- **P3 hudi（hybrid，D-019）进行中**：批 A（T02 column_types、T04 time-travel/增量 fail-loud）+ 批 B（T05 分区裁剪、T06 MVCC keep-defaults）**编码完成**，**gate 仍关**；批 C（三模块测试 + COW/MOR parity）、批 D（T08 `tableFormatType` 分流消费**设计**）待启动；批 E（模型落地/翻闸/删 legacy/集群验证）deferred 并入 P7。
- **P4 maxcompute / P5 paimon / P6 iceberg / P7 hive(+HMS) / P8 收尾**：未启动。
- **Iceberg / Hive 连接器**：iceberg=metadata-only（596 LOC），hive=metadata+scan+探测（2010 LOC），**均 dormant**（gate 关）。
- **真阻塞（[DV-005](../deviations-log.md)）= catalog 模型错配 + fe-core 不消费 `tableFormatType`**——本调研在 §6 逐条坐实。

**一句话定位**：底座（SPI 接口、PluginDriven* 框架、HMS 共享库、per-table 探测、各连接器骨架）已就位且各连接器在 gate 后逐个硬化；**但"单 hms catalog 多格式分派"这条主干尚未接通，且其设计（批 D / T08）尚未落笔、落地在 P7/批 E**。

---

## 8. 还缺哪些模块 / 机制

| # | 缺失项 | 类型 | 落点（项目计划）|
|---|---|---|---|
| M1 | **fe-core 消费 `tableFormatType`**：`PluginDrivenExternalTable`（或一个 table 工厂）读 `ConnectorTableSchema.tableFormatType`，驱动 per-table 的 scan 路径 + cache 引擎选择 | fe-core 机制 | **批 D 设计(T08) → 批 E/P7 实现** |
| M2 | **per-table scan-provider router**：让单个 `hms` 连接器按 `HiveTableType` 选 Hive/Hudi/Iceberg 的 scan 规划（见下"3 选项"）| SPI/连接器机制 | 批 D 设计 → P7 |
| M3 | **`IcebergScanPlanProvider`** + `fe-connector-iceberg` 依赖 `fe-connector-api` | iceberg 模块 | **P6** |
| M4 | **Hudi metadata/scan 接入 hms catalog**（hms 探测为 HUDI 的表交给 Hudi 路径）| 连接器组合 | 批 E/P7 |
| M5 | **格式探测共享化**：把 `HiveTableFormatDetector` 抽到共享层，消除 fe-core / SPI 两份 drift 风险 | 复用重构 | P7 |
| M6 | **gate flip** `SPI_READY_TYPES += hms` + legacy `HMSExternalCatalog` cutover/兼容 + image 反序列化兼容（R-001）| 翻闸/迁移 | 批 E/P7 |
| M7 | **多格式分派测试网**（parity：SPI 输出 vs legacy；混合 Hive/Hudi/Iceberg catalog 端到端）| 测试 | P3 批 C + P7 |
| M8 | Paimon / MaxCompute 的 ConnectorProvider（与本问题相关性低，但同属 HMS 家族外的并行迁移）| 连接器 | P4 / P5 |

### M2 的关键未决设计决策（"3 选项"，recon 浮现，**项目尚未拍板**）

单个 `hms` catalog 如何把 per-table 路由到 Hive/Hudi/Iceberg？三条路线（互斥）：

- **(A) 连接器内 router**：`HiveConnector`（type=`"hms"`）作为网关，`getScanPlanProvider()` 返回一个按 `handle.getTableType()` 选子 provider 的 router；metadata 侧同理 `HiveConnectorMetadata` 委托 `Hudi/IcebergConnectorMetadata`。**优点**：贴合"hive 模块已注册 `hms`"现状、单 catalog 单 connector 不变。**缺点**：把 hive 连接器变成三格式聚合体，模块边界变重。
- **(B) SPI 改为 per-table 选 provider**：把 `getScanPlanProvider()` 从 `Connector`（per-catalog）下移到 `ConnectorMetadata.getScanPlanProvider(handle)`（per-table，按 handle 类型）。**优点**：最干净的 per-table 语义。**缺点**：改 SPI 接口，影响所有连接器。
- **(C) fe-core 发现期分派**：fe-core 读 `tableFormatType`，在建表时产出 format-specific 的表对象（最接近 legacy `DLAType`→多态 `DlaTable`）。**优点**：与 legacy 心智一致、改动集中在 fe-core。**缺点**：fe-core 需重新长出 per-format 分派（部分回到 legacy 形态），与"瘦 fe-core"目标张力。

> 这正是 [D-019](../decisions-log.md) 把 (a)模型落地推迟到 P7/批 E 的核心待决项；[tasks/P3 T08](../tasks/P3-hudi-migration.md)（批 D，design-only）是其设计入口。本调研建议把"M1+M2 的 (A/B/C) 选型"作为 T08 设计备忘的核心命题。

---

## 9. 后续开发步骤（建议 roadmap）

> 与既有阶段计划（P3 hybrid → P6 iceberg → P7 hive/HMS）和 D-019/DV-005 对齐；不替代项目计划，作为"打通单 hms 多格式"的依赖序梳理。

```
[已完成] SPI 基座(P0) · scan-node 收口(P1) · trino 迁移(P2) · hudi 连接器硬化(P3 批A+B)
   │
   ├─[P3 批C]  三模块测试基线 + COW/MOR parity（SPI 输出 vs legacy）           ← 正在路上
   │
   ├─[P3 批D / T08]  ★keystone 设计★：`tableFormatType` 分流消费 + M2(A/B/C)选型   ← 设计 only
   │                 产出 D-NNN（模型决策），明确 M1+M2 的接口形态
   │
   ├─[P6]  Iceberg scan SPI：补 IcebergScanPlanProvider + iceberg 依赖 api(M3)     ← 让 iceberg 可走 SPI
   │
   └─[P7 / 批E]  模型落地（live cutover）：
        1. M1  fe-core 消费 tableFormatType（PluginDrivenExternalTable / table 工厂）
        2. M2  落地选定的 router 方案（A/B/C 之一）
        3. M4  hms catalog 内 per-table 把 HUDI→Hudi 路径、ICEBERG→Iceberg 路径
        4. M5  抽共享格式探测，消 drift
        5. M6  SPI_READY_TYPES += hms 翻闸 + legacy HMSExternalCatalog cutover + image 兼容(R-001)
        6. 删 legacy datasource/{hive,hudi,iceberg}/ + 清反向 instanceof
        7. M7  混合 Hive/Hudi/Iceberg catalog 端到端/集群验证
```

**最短关键路径**（让单 hms catalog 多格式"先能跑通"）：**T08 设计(M1+M2 选型) → M1 fe-core 消费 tableFormatType → M2 router → M4 hms 内 Hudi 路径 →（Iceberg 需 M3 先行）→ M6 翻闸**。其中 **M1+M2 是真正的 keystone**：没有它，per-table 探测的成果无法兑现。

---

## 10. 开放问题（留给 T08 设计 / 后续决策）

1. **M2 选型**：(A) 连接器内 router / (B) `ConnectorMetadata.getScanPlanProvider(handle)` per-table / (C) fe-core 发现期分派——哪条？（§8）
2. **Iceberg 归属**：hms 里的 Iceberg 表是由 hms 连接器委托 `IcebergConnectorMetadata`，还是 fe-core 仍回落 legacy `IcebergScanNode`？Iceberg 不依赖 hms（用 SDK），跨界委托如何拼装？
3. **Hudi time-travel/增量**：`planScan` 只读最新快照（`HudiScanPlanProvider.java:100-108`），`visitPhysicalHudiScan` 对 `AS OF`/增量已 fail-loud（P3-T04）。snapshot/timestamp 如何经 SPI 传入 `planScan`？（批 E）
4. **连接器生命周期**：catalog 创建期 + `initLocalObjectsImpl` 期各创建一次 connector（`PluginDrivenExternalCatalog.java:87-145`）——首个是否被丢弃？`HmsClient` 是否重复建（池泄漏风险）？
5. **`tableFormatType` 去留**：它是面向未来 per-table 分派的前瞻字段（应被 M1 消费），不是技术债——T08 须明确其消费契约。
6. **fe-core ↔ SPI 探测 drift**：`HMSExternalTable.makeSureInitialized` 与 `HiveTableFormatDetector` 两份逻辑，长期是否抽共享（M5）以防漂移？

---

## 附录 A：核心 file:line 锚点索引

**Legacy 模型**
- `fe-core/.../datasource/hive/HMSExternalCatalog.java:52-106`
- `fe-core/.../datasource/hive/HMSExternalTable.java:208-210`(DLAType) `:250-307`(探测) `:226-240`(cache 分派) `:384-408`(schema 分派)
- `fe-core/.../datasource/hive/{Hive,Hudi,Iceberg}DlaTable.java` / `HMSDlaTable.java:36-87`
- `fe-core/.../nereids/glue/translator/PhysicalPlanTranslator.java:~724-770`(scan 分派) `:819`(visitPhysicalHudiScan)

**SPI catalog / gate / 框架**
- `fe-core/.../datasource/CatalogFactory.java:52`(SPI_READY_TYPES) `:71-184`(createCatalog)
- `fe-core/.../connector/ConnectorFactory.java:53-75` / `ConnectorPluginManager.java:74-144`
- `fe-core/.../datasource/PluginDrivenExternalCatalog.java:57-145` / `PluginDrivenExternalTable.java:79-109`(❗不读 tableFormatType) / `PluginDrivenScanNode.java:85-100,356-395`

**SPI 接口**
- `fe-connector-api/.../Connector.java:34-121` / `ConnectorMetadata.java:37-44` / `ConnectorTableSchema.java:29-92`
- `fe-connector-api/.../scan/ConnectorScanPlanProvider.java:38-196` / `ConnectorScanRange.java:96-98`
- `fe-connector-spi/.../ConnectorProvider.java:40-98`

**连接器实现**
- hive: `HiveConnectorProvider.java:32-43`(type="hms") / `HiveConnector.java:54-73` / `HiveConnectorMetadata.java:105-131,134-154,282-294,193-234` / `HiveTableFormatDetector.java:77-100` / `HiveTableType.java:28-41` / `HiveTableHandle.java:35-196` / `HiveScanPlanProvider.java:95-132` / `HiveScanRange.java:120-122,195`
- hudi: `HudiConnectorProvider.java:36`(type="hudi") / `HudiConnector.java:46-110` / `HudiConnectorMetadata.java:69-214` / `HudiScanPlanProvider.java:85-162` / `HudiScanRange.java:140-142`
- iceberg: `IcebergConnectorProvider.java:34-45`(type="iceberg") / `IcebergConnector.java:51-150`(无 scan provider) / `IcebergConnectorMetadata.java:57-167`(metadata-only)
- hms: `fe-connector-hms/.../HmsClient.java:40-78`

**护栏 / 项目文档**
- `tools/check-connector-imports.sh:30-60`
- `plan-doc/deviations-log.md`(DV-005) / `plan-doc/decisions-log.md`(D-019) / `plan-doc/tasks/P3-hudi-migration.md`(T08 批 D)

---

## 附录 B：调研方法与可信度

- 6 个 read-only `Explore` agent 并行（areas：legacy-model / spi-catalog-gate / connector-providers / format-dispatch / scan-split-path / module-deps-reuse），合计读 ~286 次工具调用、~469K token；结论经 6 reader 交叉印证（§0 三断点、gate、依赖图均多 reader 一致）。
- **可信度高的结论**：`tableFormatType` 产而不用、scan 硬编码 `"hive"`、Iceberg 无 ScanPlanProvider、依赖图、type 注册（hms/hudi/iceberg）、gate 内容——多 reader + 主线核读一致。
- **行号为近似锚点**：个别文件不同 reader 报的行段略有出入（如 `PhysicalPlanTranslator` ~724-795），已取交集并标"~"。落地修改前应按附录 A 重新精确定位。
- **本调研未运行构建/测试**（纯静态阅读）；未改动任何代码或现有文档。
```
