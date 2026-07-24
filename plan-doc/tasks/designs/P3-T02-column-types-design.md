# P3-T02 设计 — `column_types` 双 bug 修复

> 批 A / scan 正确性 · behind 关闭的 gate（`SPI_READY_TYPES` 不含 hms/hudi）· 零 live-path 风险
> 关联：[tasks/P3](../P3-hudi-migration.md) · [HANDOFF 关键认知 1b HIGH ②](../../HANDOFF.md) · [DV-005](../../deviations-log.md)
> 状态：设计完成（code-grounded，BE↔Java 契约两点对抗确认）

---

## Problem

MOR-with-logs 的 JNI split 在 SPI 路径下，传给 BE Hudi JNI scanner 的 `hudi_column_types`（以及与之配对的 `hudi_column_names`）**被破坏**，命中**任何含 decimal / 复杂类型（array/map/struct）列**的表会读出错列 / 类型，或 names↔types 长度错位。

两个独立 bug 叠加：

- **(a) 类型系统错 + 丢精度**：`HudiScanPlanProvider.planScan`（`HudiScanPlanProvider.java:118-120`）用 `HudiTypeMapping.fromAvroSchema(...).getTypeName()` 生成 column_types。`fromAvroSchema` 产出的是 **Doris** `ConnectorType`（`DECIMALV3`/`DATETIMEV2`/`STRING`...），`.getTypeName()` 只取**裸类型名**，丢失 precision/scale/子类型。而 BE Hudi JNI scanner 期望的是 **Hive 类型串**（`decimal(10,2)`/`struct<a:int,b:string>`/`timestamp`/`date`...）——是另一套类型系统。
- **(b) 逗号 join→split 打碎类型串**：`HudiScanRange` 把 `columnNames`/`columnTypes`/`deltaLogs` 三个 `List<String>` 用 `String.join(",")` 压成单串存进 `properties` map（`HudiScanRange.java:89/92/95`），再在 `populateRangeParams` 里 `split(",")` 还原（`HudiScanRange.java:194/199/204`）。Hive 类型串**本身含逗号**（`decimal(10,2)`、`struct<a:int,b:string>`、`map<string,int>`）→ 一个类型被打碎成多个 list 元素，column_types 长度膨胀、与 column_names 错位。

---

## Root Cause

### BE ↔ Java JNI scanner 的精确契约（已对抗确认，两处独立代码）

`THudiFileDesc.{delta_logs, column_names, column_types}` 是 thrift **`list<string>`**（`gensrc/thrift/PlanNodes.thrift:396-398`）。**join 由 BE 做，不是 FE 做**：

| 字段 | BE cpp join（`be/src/format/table/hudi_jni_reader.cpp`）| Java scanner split（`fe/be-java-extensions/hadoop-hudi-scanner/.../HadoopHudiJniScanner.java`）|
|---|---|---|
| `delta_file_paths` | `join(delta_logs, ",")` (:52) | `.split(",")` (:106) |
| `hudi_column_names` | `join(column_names, ",")` (:53) | `.split(",")` (:212) |
| `hudi_column_types` | `join(column_types, **"#"**)` (:54) | `.split(**"#"**)` (:113) |

**关键**：column_types 用 `#` 分隔，正是**因为**类型串里含逗号（`decimal(10,2)`/`struct<...>`）；names 是标识符（无逗号）用 `,` 安全。所以 FE 的正确做法是：**把每个列类型作为一个完整 Hive 类型串、整体作为 list 的一个元素塞进 `THudiFileDesc.column_types`，绝不在 FE 端 join/split**。BE join `#`、Java split `#`，类型串里的逗号天然保留。

### legacy 参照（确认设计方向）

- legacy `HudiScanNode.java:299-301` 直接 `fileDesc.setDeltaLogs(...)/setColumnNames(...)/setColumnTypes(...)` 设 thrift list —— **不 join**。
- legacy column_types 来源：`HMSExternalTable.java:752` `colTypes.add(HudiUtils.convertAvroToHiveType(hudiAvroField.schema()))` → 完整 Hive 类型串。`HudiUtils.convertAvroToHiveType`（`HudiUtils.java:68-135`）是 canonical Avro→Hive-type-string 转换器。

### 当前 SPI bug 的连锁后果

`columnTypes = [..., "decimal(10,2)", "struct<a:int,b:string>"]`
→ 构造器 `String.join(",")` → `"...,decimal(10,2),struct<a:int,b:string>"`
→ `populateRangeParams` `split(",")` → `[..., "decimal(10", "2)", "struct<a:int", "b:string>"]`（元素数膨胀 + 串被打碎）
→ BE `join("#")` → `...#decimal(10#2)#struct<a:int#b:string>` → Java `split("#")` 得到无意义的类型片段 → JNI reader 解析失败 / 错列。
叠加 bug (a)：即便不打碎，`getTypeName()` 也只给 `DECIMALV3`（无 `(10,2)`）/`STRUCT`（无子字段）——BE 无法用。

---

## Design

三处改动，全部在 `fe-connector-hudi` 模块内（**不动 fe-core，不动 BE，不动 thrift**），gate 保持关闭。

### 改动 1 — `HudiTypeMapping.java`：新增 Avro→Hive 类型串方法

新增 `public static String toHiveTypeString(Schema schema)`，**逐行 mirror** legacy `HudiUtils.convertAvroToHiveType`（fe-core，import gate 禁止直接复用，故在连接器模块内复刻）。语义完全对齐，包括：
- `decimal(p,s)`、`array<...>`、`struct<name:type,...>`、`map<string,...>`、`timestamp`/`date`、primitives；
- UNION 单一非空类型递归 unwrap；
- 不支持类型（TimeMillis/TimeMicros/多类型 union/空 record）**抛 `IllegalArgumentException`**（与 legacy 一致，fail-loud）。

保留现有 `fromAvroSchema`（Avro→Doris `ConnectorType`）**不动**——它服务 schema 上报（`HudiConnectorMetadata.java:240`），是正确的、另一条路径。

### 改动 2 — `HudiScanPlanProvider.java`：用 Hive 类型串生成 column_types

`planScan` 中 column_types 生成（:118-120）：
```java
// before
columnTypes = avroSchema.getFields().stream()
        .map(f -> HudiTypeMapping.fromAvroSchema(unwrapNullable(f.schema())).getTypeName())
        .collect(Collectors.toList());
// after
columnTypes = avroSchema.getFields().stream()
        .map(f -> HudiTypeMapping.toHiveTypeString(f.schema()))
        .collect(Collectors.toList());
```
（`toHiveTypeString` 自己处理 union unwrap，直接传 `f.schema()`，对齐 legacy `convertAvroToHiveType(field.schema())`。）若此改动使私有 `unwrapNullable`（:350-359）变为未引用，则一并删除。

### 改动 3 — `HudiScanRange.java`：三个 list 字段端到端 typed，弃逗号 join/split

- 新增三个 `List<String>` 字段：`deltaLogs`、`columnNames`、`columnTypes`（不可变副本）。
- 构造器：**移除** `props.put("hudi.delta_logs"/"hudi.column_names"/"hudi.column_types", String.join(",", ...))`（:88-96 三处），改为赋值字段。标量 JNI 字段（instant_time/serde/input_format/base_path/data_file_path/data_file_length）**保持原样存 props map**（无逗号问题，最小改动）。
- `populateRangeParams`：
  - JNI 降级判定的 delta-logs 空检查（:161-162）改用 `deltaLogs` 字段（`deltaLogs == null || deltaLogs.isEmpty()`）。
  - 直接 `fileDesc.setDeltaLogs(deltaLogs)/setColumnNames(columnNames)/setColumnTypes(columnTypes)`（保留 null/empty 守卫），**不再 split**。

> `getProperties()` 仅被 `HudiScanRange.populateRangeParams` 自身消费（`PluginDrivenScanNode.java:392` 调 `populateRangeParams`，且 `HudiScanRange` override 了该方法、不走 `ConnectorScanRange` 默认 generic 路径）——故三个 list key 从 map 移除对外部零影响。

---

## Implementation Plan

1. `HudiTypeMapping.java`：加 `toHiveTypeString(Schema)` + 递归 helper（mirror legacy）。
2. `HudiScanPlanProvider.java`：改 :118-120；若 `unwrapNullable` 变 dead 则删。
3. `HudiScanRange.java`：加 3 字段、改构造器、改 `populateRangeParams`。
4. 新增单测（见下）。
5. 构建守门：`mvn -pl fe-connector/fe-connector-hudi -am test -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`（cwd=fe/）；checkstyle 0；import-gate 通过。

---

## Risk Analysis

- **回归面**：gate 关闭，hudi 查询仍走 legacy 路径；SPI `HudiScanRange`/`HudiScanPlanProvider` **零 live caller**（trino-connector 之外的 SPI 表才会触达，hudi 未翻闸）。改动纯属硬化 dormant 代码，**零 live-path 风险**。
- **契约风险**：BE↔Java 的 `#`/`,` 分隔已两点确认；不改 BE/thrift，契约不变。
- **parity 残留（不在 T02 范围，登记）**：
  - column_names/types 的**列集合与顺序**是否与 legacy（meta 列 `_hoodie_*`、partition 列）完全一致 → 由 **T07 parity 测试**校验。
  - schema 解析失败时 `planScan` try/catch 吞成空 list（:121-126）的 fail-loud 问题 → **T04** 处理，本 task 不动控制流。
  - `toHiveTypeString` 对不支持类型抛异常，会被上述 try/catch 吞 → 同属 T04 范畴。
- **simplicity（CLAUDE.md R2/R3）**：仅 3 文件、新增一个 mirror 方法 + 字段化三个 list；标量保持原样，最小 diff。

---

## Test Plan

### Unit Tests（本 task 交付；建立 fe-connector-hudi 首个测试）

`HudiTypeMappingTest`（验证 Avro→Hive 串编码意图，Rule 9）：
- `decimal` logical type → `"decimal(10,2)"`（**精度/scale 不丢** —— 直击 bug a）。
- record → `"struct<a:int,b:string>"`（**含逗号的复杂类型** —— 串里必须保留逗号）。
- `array<int>`、`map<string,bigint>`、嵌套 `array<struct<...>>`。
- primitives：int/bigint/boolean/float/double/string/date/timestamp。
- nullable union（`["null", T]`）→ unwrap 为 T。
- 不支持类型（TimeMillis）→ 抛 `IllegalArgumentException`（fail-loud parity）。

`HudiScanRangeTest`（验证 typed-list 端到端不被打碎，Rule 9 —— 直击 bug b）：
- 构造 range：`columnNames=["x","y","z"]`、`columnTypes=["int","decimal(10,2)","struct<a:int,b:string>"]`、`deltaLogs=["s3://.../.log.1","s3://.../.log.2"]`、含 delta logs（→ JNI）。
- 调 `populateRangeParams`，断言 `THudiFileDesc.getColumnTypes()` **恰为 3 个元素且逐元素未变**（不是被逗号打碎成 5 个），`getColumnNames()` 3 元素，`getDeltaLogs()` 2 元素，names↔types **长度一致**。
- 断言 `decimal(10,2)`、`struct<a:int,b:string>` 作为**单个 list 元素**完整保留（编码"类型串里的逗号必须存活到 BE，否则 JNI scanner split('#') 读错类型"这一 WHY）。
- 降级用例：无 delta logs 的 `.parquet` data file → format 降为 `FORMAT_PARQUET`、不设 JNI 列字段（验证 :160-176 降级逻辑用 field 后仍正确）。

### E2E Tests

**不适用 / 推迟批 E**：gate 关闭，hudi 查询不走 SPI 路径，无法在 regression-test 中端到端触达本代码（需 batch E 翻闸 + Hudi 集群）。按 [tasks/P3 §策略](../P3-hudi-migration.md) 批 A–C 验证为「单测 + 设计级」，端到端随批 E cutover 做。**显式登记，不静默跳过（CLAUDE.md R12）**。
