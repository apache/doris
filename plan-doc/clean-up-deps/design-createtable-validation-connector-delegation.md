# 设计：把建表语句的按源校验下沉到连接器

> 配套 [`HANDOFF.md`](./HANDOFF.md) / [`TASKLIST.md`](./TASKLIST.md)。本文覆盖"散点按源分支"清理里**唯一真正活的、数据源专属**的一处——`CreateTableInfo` 建表校验中对 iceberg / paimon / hive 的专属规则。
> 创建：2026-07-21 · 分支：`catalog-spi-review-17`
> 决策来源：用户签字（① 排序反向拦截走**连接器能力位**；② hive 分区校验**本轮一并搬进连接器**）。

---

## 1. 背景与目标

`fe/fe-core` 应当对具体数据源无感知；各数据源已迁到独立连接器插件（`fe-connector-*`）。经净室对抗侦察，`CreateTableInfo.validate()` / `analyzeEngine()` 里仍有一簇**活的、按源硬编码**的建表 DDL 校验（iceberg/paimon 不支持 `DISTRIBUTE BY`、iceberg 排序列、hive `NOT NULL` 列、hive 外部分区语义）。这些规则本应由**各连接器自己声明**——对齐 Trino：连接器专属的 CREATE TABLE 校验放在连接器（`ConnectorMetadata.beginCreateTable` + 连接器声明的表属性/能力），而非引擎的分析器。

**目标**：把这些按源 DDL 校验从 fe-core 下沉到 iceberg / paimon / hive 连接器；fe-core 侧只减不增（除新增一个**连接器无关**的能力位枚举成员）。

**为什么这条路可行（关键前置事实，均已核验）**：
- 送进连接器的建表请求 `ConnectorCreateTableRequest` **已携带全部所需字段**：每列 `nullable/isKey/type`（`ConnectorColumn`）、`bucketSpec`（= `DISTRIBUTE BY` 存在性）、`sortOrder`（= `ORDER BY` 写序子句）、`partitionSpec`（`LIST/RANGE/IDENTITY/TRANSFORM` + 字段 + `hasExplicitPartitionValues`）。→ 下沉**无须给 fe-core 加新管道**。
- **已有范式**：`MaxComputeConnectorMetadata.createTable` 在方法开头调 `validateColumns(...)`（`fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java:377`），抛 `DorisConnectorException`（unchecked），由 `PluginDrivenExternalCatalog.createTable` 的 `catch (DorisConnectorException) → DdlException` 透出文案。照抄即可。
- 会话变量**已线程**到连接器：`ConnectorSessionBuilder.extractSessionProperties` 走 `VariableMgr.toMap(ctx.getSessionVariable())` 全量导出 → 连接器可 `session.getProperty("allow_partition_column_nullable", Boolean.class)`（默认 `true`）。
- 连接器能力位机制**已存在且 fe-core 已在用**：`ConnectorCapability` 枚举 + `connector.getCapabilities().contains(...)`（`PluginDrivenExternalCatalog` 已用 `SUPPORTS_VIEW`/`SUPPORTS_USER_SESSION` 门控）。
- **hms→hive 路由已澄清（定性核验）**：对 hive-metastore 目录建表**永远**建普通 hive 表，绝不会经此路径建 iceberg-on-HMS / hudi-on-HMS 表（引擎被 `checkEngineWithCatalog` 锁死成 `hive` + 每目录单连接器 + 兄弟分流不在建表路径，三重保险）。→ 把 hive 校验搬进 hive 连接器**不会误伤**其它表格式。

---

## 2. 采用的接缝与范式

**接缝**：`ConnectorTableOps.createTable(ConnectorSession, ConnectorCreateTableRequest)`（`ConnectorMetadata extends ConnectorTableOps`；默认实现降级抛"CREATE TABLE not supported"）。各连接器**在自己 `createTable` 方法开头内联校验**，失败抛 `DorisConnectorException`。**不新增**通用 `validateCreateTable` SPI 钩子（对齐 MaxCompute 唯一范式，最小 SPI 面）。

**时机说明（可接受的放宽）**：fe-core 原在**分析期**（`CreateTableInfo.validate`）抛 `AnalysisException`；下沉后在**执行期**（连接器 `createTable`）抛 `DorisConnectorException`，稍晚，且命中 `IF NOT EXISTS` 已存在表时短路跳过——与 MaxCompute `validateColumns` 现状一致，已是既定放宽。

---

## 3. 逐项设计

### 3.1 paimon —— 拒绝 `DISTRIBUTE BY`

- **搬出（fe-core）**：`CreateTableInfo.validate()` 的 paimon `else if`（`engineName==PAIMON && distribution != null` → 抛错）。⚠️ 这是 iceberg/paimon 的 `if / else-if` 链，**只删 paimon 这一臂**，iceberg 臂由 3.2 处理。
- **搬入（连接器）**：`PaimonConnectorMetadata.createTable`（`fe-connector-paimon`）**第一句**（在 `PaimonSchemaBuilder.build` 之前、`executeAuthenticated` try 之外——try 的 catch 会把异常重写成 "Failed to create Paimon table ..."，会破坏文案）：`if (request.getBucketSpec() != null) throw new DorisConnectorException(<原文案>)`。
- **文案**（字节保持）：`Paimon doesn't support 'DISTRIBUTE BY', and you can use 'bucket(num, column)' in 'PARTITIONED BY'.`
- **必要性**：`PaimonSchemaBuilder` **故意忽略** `bucketSpec`（源码注释在案）；若只删 fe-core 而不加连接器拒绝，`DISTRIBUTE BY` 会**静默成功**（回退）。
- **信号保真**：`request.getBucketSpec()!=null ⇔ info.getDistribution()!=null ⇔ 用户写了 DISTRIBUTE BY`（自动默认分布仅 OLAP 引擎触发，不会污染 paimon）。

### 3.2 iceberg —— 拒绝 `DISTRIBUTE BY` + 排序列校验

- **搬出（fe-core）**：
  - iceberg `if` 臂（`engineName==ICEBERG && distribution != null` → 抛错）。
  - `validateIcebergSortOrder(columnMap)` 整个方法体（列存在 / 非 metric 类型 / 无重复）。
- **搬入（连接器）**：`IcebergConnectorMetadata.createTable`（`fe-connector-iceberg`，现有 override，行首已有 `rejectReservedRowLineageColumns`）**顶部**新增 `validateCreateTable(request)`（在 `IcebergSchemaBuilder.buildSchema` 之前）：
  1. `if (request.getBucketSpec() != null) throw ...`（拒绝 DISTRIBUTE BY）。
  2. 排序列校验：遍历 `request.getSortOrder()`，对每个 `ConnectorSortField.getColumnName()` 大小写不敏感地在 `request.getColumns()` 里查存在性（不存在 → 抛）、查 metric-only 类型（`HLL`/`BITMAP`/`QUANTILE_STATE` 类型名枚举 → 抛）、查重复（→ 抛）。
- **文案**（字节保持，均有 e2e 断言子串）：
  - `Iceberg doesn't support 'DISTRIBUTE BY', and you can use 'bucket(num, column)' in 'PARTITIONED BY'.`
  - `Sort order column '<col>' does not exist in table`（e2e 断言 `does not exist in table`）
  - `Sort order column '<col>' has unsupported type: <type>`
  - `Duplicate sort order column: <col>`（e2e 断言 `Duplicate sort order column`）
- **注意**：今天 iceberg 连接器**信任** fe-core（`buildSortOrder` 只映射不校验）；下沉后必须**真正**对请求列校验。排序校验须在 `buildSchema` 之前，否则 metric 类型列会先在 schema 构建时因类型映射失败报另一条错（文案漂移）。大小写不敏感与现有 `resolveColumnName` 行为一致。

### 3.3 排序反向拦截（"只有 iceberg 支持排序"）→ 连接器能力位

- **搬出（fe-core）**：`validate()` 中 `if (sortOrderFields present) { if (!engineName.equalsIgnoreCase(ICEBERG)) throw "Only Iceberg catalog supports sort order..."; validateIcebergSortOrder(...); }` 的**反向拦截臂**（正向 `validateIcebergSortOrder` 已在 3.2 搬走）。
- **新增能力位**：`ConnectorCapability.SUPPORTS_SORT_ORDER`；`IcebergConnector.getCapabilities()` 声明它（其它连接器不声明）。
- **通用门控（连接器无关，不点名数据源）**：把反向拦截改写成能力驱动的**通用**校验——若建表带 `sortOrder` 且目标不支持排序 → 拒绝。文案改为通用措辞（如 `Sort order (ORDER BY) is not supported for engine '<engine>'`），不点名 iceberg。
  - **放置与全引擎奇偶保真**（实现已采**首选**）：现状的反向拦截不仅覆盖 SPI 引擎，也覆盖 legacy 内部目录引擎（mysql/broker/es）。**已落地**：在 `CreateTableInfo.validate()`（分析期）按目标目录的连接器能力位做**通用**门控——`catalog instanceof PluginDrivenExternalCatalog && catalog.getConnector().getCapabilities().contains(SUPPORTS_SORT_ORDER)`；不满足即抛 `Sort order (ORDER BY) is not supported for engine: <engine>`。SPI iceberg 有能力→放行（列级校验由 iceberg 连接器 `createTable` 做）；SPI 其它→拒绝；legacy 内部目录引擎（非 PluginDriven）→拒绝，奇偶保真。仅当建表带 `sortOrder` 时才触发 `getConnector()`（罕见，仅 iceberg 带 ORDER BY 的建表会在分析期 init 连接器，与随后执行期 init 相差毫秒，可接受），不点名数据源。

### 3.4 hive —— 拒绝 `NOT NULL` 列

- **搬出（fe-core）**：`validate()` 中 `for (columnDef) if (!columnDef.isNullable() && engineName==HIVE) throw ...`。
- **搬入（连接器）**：`HiveConnectorMetadata.createTable`（`fe-connector-hive`，现有 override）顶部新增 `validateColumns(request)`：遍历 `request.getColumns()`，`if (!col.isNullable()) throw new DorisConnectorException("hive catalog doesn't support column with 'NOT NULL'.")`。
- **文案**（字节保持）：`hive catalog doesn't support column with 'NOT NULL'.`（连接器**硬编码字面量 `hive`**，勿从变量派生——保 e2e 断言字节一致）。
- 请求 `ConnectorColumn.isNullable()` 由转换器从 `d.isNullable()` 填，完整复现。

### 3.5 hive —— 外部分区校验

- **搬出（fe-core）**：`validate()` 中 `if (engineName==HIVE) partitionTableInfo.validatePartitionInfo(engineName, columns, columnMap, properties, ctx, false, true)`（isExternal=true 那次调用）。
- **可达子集分析**（对 hive 外部建表 `PARTITIONED BY LIST(col…)`——identity 列、无显式分区值、非 RANGE）：`validatePartitionInfo` 中真正可达的校验是——
  1. 分区键必须存在于列 → `partition key %s is not exists`
  2. 每个分区列（hive 外部：列均为 key、MoW=false、String 允许）：浮点类型 → `Floating point type column can not be partition column`；复杂类型 → `Complex type column can't be partition column: <type>`；可空且会话开关 OFF → `The partition column must be NOT NULL with allow_partition_column_nullable OFF`
  3. 重复分区列 → `Duplicated partition column <col>`
  4. hive 专属：全列做分区 → `Cannot set all columns as partitioning columns.`；分区字段须在 schema 末尾 → `The partition field must be at the end of the schema.`；顺序一致 → `The order of partition fields in the schema must be consistent with the order defined in \`PARTITIONED BY LIST()\``
- **不可达/已在别处拦截（无须搬，但设计须给出证明）**：`validateAutoPartitionExpression`（耦合 nereids 函数注册表，**不可**搬连接器）对 hive identity `LIST` 分区是 no-op（`UnboundSlot` 分支仅对 auto-RANGE 抛错）；`partitionDefs`（显式分区值）与 `RANGE` 已由 hive 连接器现有校验拒绝（`Only support 'LIST' partition type in hive catalog.` / `Partition values expressions is not supported in hive catalog.`）。
- **搬入（连接器）**：`HiveConnectorMetadata.createTable` 顶部新增 `validatePartition(request, session)`，基于 `request.getColumns()` + `request.getPartitionSpec().getFields()` 复现上述 1–4：
  - 类型判定用 `ConnectorType.getTypeName()` 枚举（浮点：`FLOAT`/`DOUBLE`；复杂：`ARRAY`/`MAP`/`STRUCT`）。
  - 可空校验读会话变量：`Boolean allow = session.getProperty("allow_partition_column_nullable", Boolean.class); if ((allow==null?true:allow)==false && col.isNullable()) throw ...`（默认 true）。
  - hive 连接器现有代码已从 `partitionSpec.getFields()` 收集 `partitionColNames` 并拒绝 RANGE/显式值——新校验紧随其后，勿重复。
- **文案**：8 条均字节保持（连接器硬编码，e2e 对齐连接器）。

---

## 4. 架构铁律核对（HANDOFF §2）

- **A（fe-core 只减不增）**：满足。校验从 fe-core 迁出→连接器；请求/会话/能力机制均已存在，无新增 fe-core 管道。唯一"增"是 `ConnectorCapability` 枚举加 `SUPPORTS_SORT_ORDER`（**连接器无关** SPI 声明，非源特有逻辑）+ 排序反向拦截改写成能力驱动的**通用**门控（去掉 iceberg 点名，非新增源特有逻辑）。
- **B（禁就近搬迁进 fe-core util）**：满足，无逻辑挪进 fe-core。
- **C（fe-core 不解析属性）**：满足，这些是 DDL 子句校验（分布/排序/分区/可空），非 storage/meta 属性字符串解析。
- **D（活的源特有逻辑走 SPI 委派）**：本设计正是委派而非删除。
- **E（通用按名 dispatch 允许）**：`analyzeEngine` 的多引擎并列 allow-list、`checkEngineName`、`pluginCatalogTypeToEngine` 是通用 dispatch，**保留不动**；es 死分支（`validate` 的 must-have-columns 豁免）另按死代码处理或随本簇顺带（见 §6）。

---

## 5. 测试与验证计划

- **连接器单测**（每个连接器仿 `MaxComputeValidateColumnsTest`：直呼 `validateXxx(request)`，断言文案子串，且编码"为什么"——Rule 9）：
  - paimon：`DISTRIBUTE BY` 拒绝。
  - iceberg：`DISTRIBUTE BY` 拒绝 + 排序列（不存在/类型/重复）三态。
  - hive：`NOT NULL` 拒绝 + 分区（存在/浮点/复杂/重复/全列/末尾/顺序/可空）各态。
- **e2e 回归**（已有断言须继续绿；文案按项目惯例**对齐连接器**而非连接器复刻旧措辞，见 memory `prefer-align-test-to-connector-message`）：`test_hive_ddl.groovy`（NOT NULL/分区）、`test_iceberg_partition_evolution_ddl.groovy` 及 iceberg 排序 e2e、`test_paimon_catalog.groovy`。iceberg-on-HMS 网关新增能力须补 e2e（memory `hms-iceberg-delegation-needs-e2e`）——本簇不新增 iceberg-on-HMS 能力，故不涉及；但 hive 建表校验搬迁后跑一遍 hms 目录建表回归。
- **编译门禁**：`test-compile -pl fe-core -am` + 三连接器模块 `test-compile` BUILD SUCCESS；validate 阶段 gates（`check-fecore-metadata-funnel.sh` / `check-connector-imports` / `check-authz-cache-sharding.sh`）不受影响。
- **fe-core 净室复核**：删后 `grep` 确认 `CreateTableInfo` 内 iceberg/paimon/hive 专属校验清零（保留通用 allow-list）。

---

## 6. 范围与顺序

**本轮范围**：§3.1–§3.5 五项 + §3.3 能力位。
**顺带**：删 §3.2/§3.3 后 fe-core 若有变未用的 import/helper（如 `validateIcebergSortOrder`、metric 判定）一并删，过 checkstyle。
**明确不含（另项/已完成）**：es 死分支（`validate` must-have-columns 豁免——已核验结构不可达死代码，可作纯死代码清理，规模极小，可随本轮尾部一并删或单列）；`analyzeEngine` 的 es reject-distribution（`ENGINE=elasticsearch` 显式可达但 es 建表最终被 `InternalCatalog` 弃用守卫拒绝，属 es 弃用簇，非本簇）。

**建议顺序**（每项独立可验、独立 commit）：paimon（最简）→ hive NOT NULL → iceberg DISTRIBUTE BY+排序 → 排序能力位反向拦截 → hive 分区（最重）。

---

## 7. 风险与规避

- **R1 全引擎奇偶（排序反向拦截）**：见 §3.3——首选 fe-core 通用能力门控保 legacy 引擎奇偶；若过早 init 连接器有副作用则退次选并单独评估 mysql/broker。实现期定稿并在 commit 说明。
- **R2 文案漂移 vs CI**：凡连接器文案与旧 fe-core 仅措辞差异 → **改测试对齐连接器**，勿在连接器写 type-aware 代码复刻旧措辞（memory `prefer-align-test-to-connector-message`）。字节保持项（本设计大多）则须一字不差。
- **R3 iceberg 排序须先于 buildSchema**：否则 metric 类型/不存在列先在 schema 构建炸出别的错。校验置 `createTable` 顶部。
- **R4 paimon/hive 校验须在 auth-try 之外**：try 的 catch 会重写文案。
- **R5 hive 分区可达子集证明**：搬迁只复现 identity-LIST 可达子集；须在实现/复核中证明 `validateAutoPartitionExpression` 与 `partitionDefs` 分支对 hive 外部建表确不可达（已由现有 RANGE/显式值拒绝 + UnboundSlot no-op 支撑）。
- **R6 metric 类型枚举保真**：fe-core 用 `DataType.isOnlyMetricType()`；连接器按类型名枚举（HLL/BITMAP/QUANTILE_STATE）。这些类型在 iceberg 表本就罕见/更早被拒，属低概率；若不可达则对齐测试。
- **R7 时机放宽**：分析期→执行期抛错（§2），与 MaxCompute 一致，可接受。

---

## 8. 变更清单（文件级 TODO）

**fe-core（减）**
- [ ] `nereids/.../info/CreateTableInfo.java`：删 iceberg/paimon `DISTRIBUTE BY` 两臂、`validateIcebergSortOrder` 方法体、排序反向拦截臂改写成通用能力门控、hive `NOT NULL` 循环分支、hive 分区 `validatePartitionInfo(isExternal=true)` 调用臂；顺带删由此变未用的 import/helper。
- [ ] `nereids/.../info/PartitionTableInfo.java`：若 `validatePartitionInfo` 的 hive 专属块（§3.5 第 4 项）在删除 hive 调用后变死，一并清（谨慎：该方法 OLAP 路径仍用，仅删 hive 专属块与仅服务 hive 的形参路径）。
- [ ] 排序能力门控落点（§3.3 首选 `CreateTableInfo`/建表入口；次选 `PluginDrivenExternalCatalog.createTable`）。

**SPI（连接器无关，增一枚举成员）**
- [ ] `fe-connector-api/.../ConnectorCapability.java`：加 `SUPPORTS_SORT_ORDER`。

**连接器（增校验，承接源特有逻辑）**
- [ ] `fe-connector-iceberg/.../IcebergConnector.java`：`getCapabilities()` 声明 `SUPPORTS_SORT_ORDER`。
- [ ] `fe-connector-iceberg/.../IcebergConnectorMetadata.java`：`createTable` 顶部加 `validateCreateTable(request)`（DISTRIBUTE BY + 排序三态）。
- [ ] `fe-connector-paimon/.../PaimonConnectorMetadata.java`：`createTable` 首句加 DISTRIBUTE BY 拒绝（auth-try 之外）。
- [ ] `fe-connector-hive/.../HiveConnectorMetadata.java`：`createTable` 顶部加 `validateColumns`（NOT NULL）+ `validatePartition`（§3.5 1–4，读会话变量）。

**测试（增）**
- [ ] 三连接器各加 `*ValidateColumnsTest` 风格单测。
- [ ] 校验既有 e2e 绿，必要处对齐连接器文案。

**验证**
- [ ] `test-compile -pl fe-core -am` + 三连接器模块 test-compile BUILD SUCCESS；gates 过；净室复核 fe-core 专属校验清零。
