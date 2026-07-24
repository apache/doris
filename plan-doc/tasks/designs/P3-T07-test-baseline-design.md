# P3-T07 设计 — 三模块测试基线 + COW/MOR schema parity（golden-value）

> 关联：[tasks/P3-hudi-migration.md](../P3-hudi-migration.md)（批 C / T07）、[HANDOFF 关键认知 2/3](../../HANDOFF.md)。
> recon：5-agent code-grounded workflow（`p3-t07-recon`，2026-06-05），结论见下。
> 用户签字（AskUserQuestion，2026-06-05）：① **casing 当场修**；② baseline **focused intent-driven set**。

---

## Problem

批 C 目标：为三个连接器模块建**测试基线** + 证 SPI hudi schema 输出与 legacy parity。

- `fe-connector-hms` / `fe-connector-hive`：**当前零测试**；`fe-connector-hudi`：已有 3 测试类（`HudiTypeMappingTest` / `HudiScanRangeTest` / `HudiPartitionPruningTest`）。
- parity 要求（T07 验收 + HANDOFF）：SPI `HudiConnectorMetadata` schema / column-type 输出 vs legacy `HiveMetaStoreClientHelper.getHudiTableSchema` + `HMSExternalTable.initHudiSchema`，**COW & MOR 各一**；覆盖 column_names / types **列集合 + 顺序 + casing**。Rule 9：测意图。

---

## Recon 结论（决定整个设计）

### 1. parity 可行性 = **golden-value（唯一可行）**
- import-gate（`tools/check-connector-imports.sh`）只扫 `*/src/main/java` 且只禁 connector→fe-core 单向；**test 目录豁免**。但真正约束是 **Maven 依赖图**：`fe-core` 只依赖 `fe-connector-api`/`-spi`，**不依赖** 具体 `-hudi`/`-hms`/`-hive` 模块；连接器模块不依赖 fe-core。→ **无任何编译路径能同时见 legacy `HudiUtils` 与 SPI `HudiTypeMapping`**。直接跨模块 parity 断言不可行（除非新增依赖 = 架构倒退）。
- → parity 用 **golden 值**：把 legacy `HudiUtils.convertAvroToHiveType` / `fromAvroHudiTypeToDorisType` / `initHudiSchema` 的契约读出，作为带 `file:line` 注释的 golden 常量，断言 SPI 复现。与 T02 `HudiTypeMappingTest` golden 断言 `toHiveTypeString` 一脉相承。
- 测试栈：**JUnit 5 only，无 mockito/jmockit**；替身全手写（`FakeHmsClient` 先例）。checkstyle **禁 static import**。

### 2. COW/MOR schema = **type-agnostic**（关键简化）
- legacy（`HMSExternalTable.initHudiSchema:734-758`）与 SPI（`HudiConnectorMetadata.getTableSchema → avroSchemaToColumns:262`）都从**同一份 Hudi avro schema** 推导列表/名/类型，**零** COW/MOR 分支。COW/MOR 区别只在 **scan planning**（split 收集 + reader 格式：`HudiScanNode.canUseNativeReader` / `HudiScanPlanProvider.planScan:92`）。
- → "COW & MOR 各一" **不需两份真实 Hudi 表 fixture**，降解为：
  - (a) **一份纯 avro→column 变换**（真实 parity 面：名/序/类型/Hive 串/nullable），golden 对标 legacy 契约 —— COW≡MOR 恒等；
  - (b) **`detectHudiTableType` 分类**（`HudiConnectorMetadata:301`，COW vs MOR vs UNKNOWN）—— metadata SPI 中唯一区分表型处，经 `getTableHandle` + `FakeHmsClient` 喂 `HmsTableInfo` 测。

### 3. legacy↔SPI 类型映射 = **逐类型一致**（recon 矩阵）
- `HudiTypeMapping.toHiveTypeString` ≡ `HudiUtils.convertAvroToHiveType`；`HudiTypeMapping.fromAvroSchema` ≡ `HudiUtils.fromAvroHudiTypeToDorisType`，**每个 avro 类型** Hive 串 + Doris 类型均一致。例外见下两 gap。

---

## 两处 parity gap

### gap-1（confirmed）column 名 casing —— **当场修（用户签字）**
- SPI `avroSchemaToColumns:270` 用 `field.name()` **原样**；legacy 在 `HMSExternalTable:745` `toLowerCase(Locale.ROOT)`（**仅顶层列名**；嵌套 struct 字段名 legacy/SPI 均不降，保持一致）。
- **修法**：`avroSchemaToColumns` 顶层列名改 `field.name().toLowerCase(Locale.ROOT)`，镜像 legacy:745。**仅顶层**，不动 `HudiTypeMapping` 嵌套 struct 名（两侧本就一致）。
- **安全性**（已核）：`ThriftHmsClient.convertFieldSchemas:303-304` 用 `fs.getName()` 不防御性降字，但 **Hive Metastore 自身存小写标识符** → HMS 来源的 partition key 名到手即小写。降 avro 路径列名 → 与小写 HMS partition key **对齐**（改善 `getColumnHandles:142` 对 mixed-case Hudi 表的匹配），**无回归**。
- **明确缩界（Rule 12 不静默）**：`ThriftHmsClient` 源头的防御性降字（与 hive 模块共享）**不在 T07 改** —— 触碰 hive 行为属 P7/批 E。本场只对齐 hudi 的 metaclient-avro schema 路径。

### gap-2（open）Hudi meta-field 纳入 —— **登记 DV，推迟批 E**
- legacy `getHudiTableSchema:852` 调 `getTableAvroSchema(true)`；SPI `getSchemaFromMetaClient:235` 调无参 `getTableAvroSchema()`。`true` 很可能强制纳入 `_hoodie_*` meta 列；无参默认随 Hudi 版本/表配置（`populateMetaFields`）变。可能改变**列集合**。
- 无真实 metaclient 不可单测判定（recon 未能从库源解析），且属 T03 同族（schema-evolution/field-id 已推批 E）。→ 记 **DV-008**，批 E 用真实 fixture 实证。本场 parity 测**不依赖该差异**（测纯 avro→column 变换，不经 metaclient）。

---

## Design（focused intent-driven set）

### 模块 hudi（task 3）

**改动（main）**：
1. `HudiConnectorMetadata.avroSchemaToColumns` 顶层列名 `toLowerCase(Locale.ROOT)`（gap-1 修），加 `import java.util.Locale`。
2. `avroSchemaToColumns` 由 `private` 改 **package-private `static`**（仅可见性/static 化，**零行为变更**）——使测试可直接喂手造 avro record schema 断言完整列变换（名/序/类型/nullable）。`getSchemaFromMetaClient:236` 的静态调用不变。

**测试**：
- **扩 `HudiTypeMappingTest`**：补 `fromAvroSchema`→`ConnectorType` golden（**当前零覆盖**）——primitives / DATEV2 / DATETIMEV2(3/6) / DECIMALV3(p,s) / array / map / struct / nullable-unwrap / ENUM→STRING / multi-union→UNSUPPORTED。对标 legacy `fromAvroHudiTypeToDorisType`。
- **新 `HudiSchemaParityTest`**（纯 avro，无 HMS）：核心 parity 交付。
  - `avroSchemaToColumns(代表性 record)` → 断言 **小写列名 + 原序 + 每列 ConnectorType + nullable**（golden 注 legacy `initHudiSchema`/`HudiUtils` file:line）。
  - 同 schema 每列 `toHiveTypeString` = golden Hive 串（= legacy `colTypes`）。
  - **casing pin**：mixed-case avro 字段（如 `Amount`）→ 列名 `amount`（gap-1 修的回归网）。
  - javadoc 写明 **COW≡MOR schema 恒等**（变换是 schema 的纯函数，不取表型）。
- **新 `HudiTableTypeTest`**（`FakeHmsClient`）：`detectHudiTableType` 经 `getTableHandle` → COW（`HoodieParquetInputFormat` / `spark.sql.sources.provider=hudi`）、MOR（`...RealtimeInputFormat` / `realtime`）、UNKNOWN。= "COW & MOR 各一" 分类面。

### 模块 hms（task 4）

- **新 `HmsTypeMappingTest`**（最高价值——`HmsTypeMapping` 是 hms+hive **共享** 的 Hive-类型串解析器，零测试，真解析逻辑）：primitives（boolean/tinyint/smallint/int/bigint/float/double/string/date/timestamp/binary）、`char(N)`/`varchar(N)`（含无长度默认）、`decimal`/`decimal(p)`/`decimal(p,s)`（默认精度）、`array<>`/`map<,>`/`struct<:,:>`/嵌套、Options（timeScale / mapBinaryToVarbinary / mapTimestampTz）、`timestamp with local time zone`、unsupported→UNSUPPORTED、大小写不敏感。golden 值对标解析逻辑。
- **缩界**：DTO 不可变/getter/config 常量等 ~60 低意图测**不做**（Rule 2/9），记为 backlog。

### 模块 hive（task 4）

- **新 `HiveConnectorMetadataPartitionPruningTest`**：镜像 `HudiPartitionPruningTest` 测 `HiveConnectorMetadata.applyFilter`（T05 裁剪逻辑的 Hive 原型）。手写 `FakeHmsClient`。pin EQ/IN 裁剪、非分区谓词忽略、全/零匹配、unpartitioned。**javadoc 注明与 hudi 的结构性重复**（consolidation 信号，P7 处理）。
- **新 `HiveFileFormatTest`**：`HiveFileFormat.fromInputFormat`/`fromSerDeLib`/`detect`/`isSplittable`——纯逻辑（drive BE reader 选择）：parquet/orc/text/json 大小写子串匹配、SerDe 回退、inputFormat 优先、splittable。
- **缩界**：`HiveTableFormatDetector`/`HiveTextProperties`/`HiveColumnHandle`/`HiveScanRange` 等记 backlog（择期或 P7）。

---

## Implementation Plan

1. hudi：改 `avroSchemaToColumns`（降字 + package-private static）→ 扩 `HudiTypeMappingTest` → 新 `HudiSchemaParityTest` + `HudiTableTypeTest` → `mvn -pl ...-hudi -am test` 绿。
2. hms：新 `HmsTypeMappingTest` →（先读 `HmsTypeMapping.java` 定 golden）→ test 绿。
3. hive：新 `HiveConnectorMetadataPartitionPruningTest` + `HiveFileFormatTest` →（先读 `HiveConnectorMetadata.applyFilter` + `HiveFileFormat` + 各 handle builder）→ test 绿。
4. 守门（每模块）：`checkstyle:check`（单独跑）+ `bash tools/check-connector-imports.sh`。
5. 文档同步（playbook §5.1 五步）+ DV-008 + commit。

---

## Risk Analysis

- **gate 保持关闭**（`SPI_READY_TYPES` 不动）；零 fe-core/BE/thrift 改动；唯一 main 改动 = hudi `avroSchemaToColumns`（降字 + 可见性），dormant、零 live 风险。
- casing 修：已核 HMS 来源小写 → 无回归；缩界明确（不动 `ThriftHmsClient`/hive）。
- golden 漂移：每 golden 注 legacy `file:line`，legacy 变则人工核（无跨模块编译耦合，这是 golden 法的固有代价，已登记）。
- gap-2 不在本场测面，避免基于未判定语义写脆测。

---

## Test Plan

### Unit Tests（本 task 交付）
- hudi：`HudiTypeMappingTest`(+fromAvroSchema) / `HudiSchemaParityTest` / `HudiTableTypeTest`。
- hms：`HmsTypeMappingTest`。
- hive：`HiveConnectorMetadataPartitionPruningTest` / `HiveFileFormatTest`。
- 三模块编译 + checkstyle 0 + import-gate 通过；全绿。

### E2E / parity-vs-live
**推迟批 E**（gate 关，端到端不可触达；precedent DV-003）：批 E 翻闸后用真实 COW/MOR fixture 实证 (a) schema 列集合/类型/casing vs legacy，(b) gap-2 meta-field 纳入，(c) BE name-match 精确性。**显式登记，不静默跳过（R12）**。

---

## Decisions / Deviations

- **DV-008**（本场新增）：SPI hudi `getTableAvroSchema()` 无参 vs legacy `(true)` 的 meta-field 纳入差异，推迟批 E 实证（同 T03 族）。
- casing gap-1：**当场修**（用户签字），仅 hudi metaclient-avro 路径顶层列名；`ThriftHmsClient` 源头防御降字推 P7/批 E。
- baseline：**focused**（用户签字）；DTO/config/detector/textprops 等记 backlog。
