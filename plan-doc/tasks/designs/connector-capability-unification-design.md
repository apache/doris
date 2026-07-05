# 设计：统一 Connector 能力声明（Trino 式「seam 即声明」）

- **日期**：2026-06-29
- **分支**：catalog-spi-10-iceberg
- **状态**：设计已与用户逐节确认（整体通过）；待落实现计划（writing-plans）
- **北极星**：Trino「capability = 实现 SPI seam；引擎查 seam，不查平行 flag」
- **前置分析**：本设计基于上一轮的能力双轨制分析（`ConnectorWriteOps.supportsXXX()` vs `ConnectorCapability` 枚举的重复与三处不一致）

---

## 1. 背景与问题

当前一个 connector 的「能力」散落在**三套并存机制**里，且彼此不自洽：

| 机制 | 载体 | 例子 |
|---|---|---|
| A. 静态枚举集 | `Connector.getCapabilities()` → `Set<ConnectorCapability>` | `SUPPORTS_INSERT`、`SUPPORTS_PARALLEL_WRITE`、`SUPPORTS_VIEW` |
| B. 写能力布尔方法 | `ConnectorWriteOps`（`ConnectorMetadata` 继承） | `supportsInsert()`、`supportsDelete()`、`supportsInsertOverwrite()` |
| C. 真实准入判断 | fe-core 翻译器 | `getWritePlanProvider() != null` |

**上一轮调研实证的三处不一致**（证据见前置分析）：

1. **`ConnectorCapability` 24 个值中 12 个是死值**（无人声明、无人读）：`SUPPORTS_FILTER/PROJECTION/LIMIT_PUSHDOWN`、`SUPPORTS_PARTITION_PRUNING`、`SUPPORTS_DELETE`、`SUPPORTS_UPDATE`、`SUPPORTS_MERGE`、`SUPPORTS_CREATE_TABLE`、`SUPPORTS_STATISTICS`、`SUPPORTS_METASTORE_EVENTS`、`SUPPORTS_VENDED_CREDENTIALS`、`SUPPORTS_ACID_TRANSACTIONS`。另 2 个被声明但无人读：`SUPPORTS_INSERT`（仅 JDBC 声明）、`SUPPORTS_TIME_TRAVEL`（iceberg/paimon 声明，真实门是 `SUPPORTS_MVCC_SNAPSHOT`）。
2. **`supportsInsert()` 无任何产品消费者**——仅单测断言其自身返回值（同义反复测试，违反 Rule 9）。真实 INSERT 闸是 `getWritePlanProvider() != null`（`PhysicalPlanTranslator.visitPhysicalConnectorTableSink`）。
3. **同一事实多处声明且都不生效**：iceberg 支持 INSERT 却**既没声明 `SUPPORTS_INSERT` 也没 `supportsInsert()`**，全靠 provider 存在；JDBC 同时声明 `SUPPORTS_INSERT`（枚举）+ `supportsInsert()=true`（方法），两者皆无人读。

**用户在设计评审中追加的核心诉求**：声明能力的「处数」要统一。当前 INSERT 要改两处（声明枚举 + 实现方法），而 FILTER_PUSHDOWN 只改一处（实现 `ConnectorPushdownOps` 方法）——同样是加功能，处数不一致，仍不统一。

---

## 2. 目标 / 非目标

**目标**
- G1：一个能力**只在一处声明**，且该处 = 实现它的 SPI seam。INSERT 与 FILTER_PUSHDOWN 在「加能力改几处」上对称。
- G2：所有真实逻辑（尤其写逻辑）**真正根据能力接口判断**，不再用 `getWritePlanProvider() != null` 这类粗粒度旁路。
- G3：保留的每个声明都有**真实产品消费者**，删除死声明与同义反复测试。
- G4：写能力**粒度化**——能区分 INSERT / OVERWRITE / DELETE / MERGE / REWRITE，而非「能不能写」一刀切。

**非目标**
- N1：不动读侧 `getScanPlanProvider() != null`——读侧不存在重复/矛盾问题。
- N2：pushdown 维持 `ConnectorPushdownOps` 方法驱动，不进枚举。
- N3：不做与本目标无关的重构。

---

## 3. 设计原则（Trino 派生）

> **一个能力只在一处声明——实现它的 seam。「不支持」就是该 seam 的默认行为（硬能力 `throw NOT_SUPPORTED`；优化能力返回 `null` / `Optional.empty()` / 空集）。引擎靠调用 / 查询 seam 得知支持与否，从不查平行 flag。能力枚举只作为「没有任何 seam 能表达的静态规划开关」的逃生舱。**

### Trino 实证（依据）

- `core/trino-spi/.../ConnectorCapabilities.java`：整个枚举**只有 2 个值**（`NOT_NULL_COLUMN_CONSTRAINT`、`MATERIALIZED_VIEW_GRACE_PERIOD`），仅在 `AddColumnTask` / `CreateTableTask` / `CreateMaterializedViewTask` 分析期被 `getCapabilities().contains(...)` 读。
- `ConnectorMetadata.java`：INSERT / CREATE TABLE / ADD COLUMN / beginMerge 等**硬能力**全是「覆写默认方法」，默认 `throw new TrinoException(NOT_SUPPORTED, "This connector does not support ...")`（如 `beginInsert` :830-832）。
- `applyFilter` / `applyLimit` / `applyProjection` / `applyDelete` 等**优化能力**默认 `Optional.empty()`。
- 分布 / 排序不是 flag——`getInsertLayout()` 返回 `Optional<ConnectorTableLayout>`（携 partitioning + sort），引擎据返回值决定写分布。

**关键洞察**：Trino 解决「处数不一致」靠的是**砍掉 flag 层**，让声明=实现；不是到处加 flag。

---

## 4. R1 落地设计

Doris 的写 seam 已经半 Trino 化：`ConnectorWritePlanProvider.planWrite(handle)` + 描述符方法（`getWriteSortColumns` / `getWritePartitioning` 已用「`null`/list = 否/是」习惯）。R1 是**收口而非重写**：补齐粒度化能力查询、删除 flag/方法重复、把残留 sink-trait flag 从枚举挪到 provider。

### §A 写能力 = 写 provider 这一个 seam（粒度化，连接器自负拒绝）

在 `ConnectorWritePlanProvider`（`fe-connector-api/.../write/ConnectorWritePlanProvider.java`）上补充，**全部带默认，连接器只覆写它真支持的**：

```java
public interface ConnectorWritePlanProvider {
    ConnectorSinkPlan planWrite(ConnectorSession s, ConnectorWriteHandle h);   // 已有

    /** 单一来源：本 provider 支持哪些写操作。默认仅 INSERT（有 provider 至少能 append）。 */
    default Set<WriteOperation> supportedOperations(ConnectorSession s, ConnectorTableHandle t) {
        return EnumSet.of(WriteOperation.INSERT);
    }

    /** 是否支持写命名 branch（INSERT@branch）。branch 是与 op 正交的修饰，故独立查询。默认否。 */
    default boolean supportsWriteBranch(ConnectorSession s, ConnectorTableHandle t) {
        return false;
    }

    // 把残留的 sink-trait flag 从 ConnectorCapability 挪到 provider，与已有 sort/partition 描述符并列：
    default boolean requiresParallelWrite(ConnectorSession s, ConnectorTableHandle t) { return false; }
    default boolean requiresFullSchemaWriteOrder(ConnectorSession s, ConnectorTableHandle t) { return false; }
    default boolean requiresPartitionLocalSort(ConnectorSession s, ConnectorTableHandle t) { return false; }

    // 已有，保持不变：appendExplainInfo / getWriteSortColumns / getWritePartitioning / getSyntheticWriteColumns
}
```

在 `Connector`（`fe-connector-api/.../Connector.java`）上加 **null-safe 委派**，使引擎**永不直接判 `provider != null`**：

```java
/** 引擎读这个，而非 getWritePlanProvider()!=null。无写 provider → 空集 → 一切写被拒。 */
default Set<WriteOperation> supportedWriteOperations(ConnectorSession s, ConnectorTableHandle t) {
    ConnectorWritePlanProvider p = getWritePlanProvider();
    return p == null ? EnumSet.noneOf(WriteOperation.class) : p.supportedOperations(s, t);
}
default boolean supportsWriteBranch(ConnectorSession s, ConnectorTableHandle t) {
    ConnectorWritePlanProvider p = getWritePlanProvider();
    return p != null && p.supportsWriteBranch(s, t);
}
```

**`ConnectorWriteOps` 瘦身**（`fe-connector-api/.../ConnectorWriteOps.java`）：
- **删除** 5 个布尔方法：`supportsInsert` / `supportsInsertOverwrite` / `supportsDelete` / `supportsMerge` / `supportsWriteBranch`。
- **保留** `validateRowLevelDmlMode(session, handle, op)`——T3「按表属性校验 + 连接器自起报错」，是写能力的**唯一动态部分**（iceberg copy-on-write / merge-on-read 读表属性），与 Trino「op 受支持但此表的 mode 禁止」同形。
- **保留** `beginTransaction(session)`——事务工厂，非能力旗标。

#### 为什么保留 `supportedOperations()` 查询，而非纯 Trino「call-and-throw」

Doris 在**分析期**就要 fail-loud（如 OVERWRITE 落到只支持 INSERT 的连接器、`@branch` 落到不支持 branch 的连接器），早于建 sink。`supportedOperations()` 与 `planWrite` **同在 provider 一个类**：单一来源、不跨模块、契约测试保证两者不漂移（§E）。这是对「早准入」的务实让步，已与用户确认采用。

> **考虑过但不采用的变体**：纯 Trino call-and-throw（`planWrite` 对不支持的 op 默认 `throw NOT_SUPPORTED`，准入=调它接异常）。代价是 OVERWRITE/branch 的拒绝从分析期挪到建 sink 期（仍是执行前、仍 fail-loud，但时机变化）。保留为未来可切换的纯化方向。

### §B 残留 `ConnectorCapability` 枚举 = 静态规划开关（Trino 逃生舱层）

**保留 7 个，全部已有活产品消费者**（消费点见 §C）：

| 枚举值 | 为何留作枚举（无自然 per-op seam） |
|---|---|
| `SUPPORTS_MVCC_SNAPSHOT` | 结构性：`PluginDrivenExternalDatabase` 据此在 load 期选 MVCC 表子类 |
| `SUPPORTS_VIEW` | 规划开关：是否把 `listViewNames` 并入 SHOW TABLES、`isView()` 是否问连接器 |
| `SUPPORTS_SHOW_CREATE_DDL` | **安全开关**：properties 是否可在 SHOW CREATE 渲染（凭据泄漏门）——与 Trino `NOT_NULL_COLUMN_CONSTRAINT` 同类的语义事实 |
| `SUPPORTS_PARTITION_STATS` | SHOW PARTITIONS 渲染 5 列还是 1 列 |
| `SUPPORTS_COLUMN_AUTO_ANALYZE` | 后台 auto-analyze 框架准入开关 |
| `SUPPORTS_TOPN_LAZY_MATERIALIZE` | 优化器 Top-N 惰性物化探针开关 |
| `SUPPORTS_PASSTHROUGH_QUERY` | `query()` TVF 准入开关 |

**删除清单**：
- 12 个死值（§1 列出）。
- `SUPPORTS_INSERT` → 由 provider 的 `supportedOperations` 派生。
- `SUPPORTS_TIME_TRAVEL` → 由 `SUPPORTS_MVCC_SNAPSHOT` 派生（**删前与 P6.6 翻闸计划核对**，确认不是「留给翻闸后接线」的占位）。
- `SUPPORTS_PARALLEL_WRITE` / `SINK_REQUIRE_FULL_SCHEMA_ORDER` / `SINK_REQUIRE_PARTITION_LOCAL_SORT` → 挪到 provider 描述符（§A）。

> 这正是 Trino 留 2 个枚举值的同一理由——Doris 因 plugin-driven 表在规划期消费的静态开关多些，故保留 7 个。每个都是单一来源（无平行的 `supportsView()` 方法之类），且已有活消费者。

### §C 消费侧改写（落实 G2：真实逻辑查能力接口）

| 准入点（file·方法） | 旧判据 | 新判据 |
|---|---|---|
| `PhysicalPlanTranslator.visitPhysicalConnectorTableSink`（INSERT，约 :732） | `getWritePlanProvider()==null` 拒「does not support INSERT」 | `!connector.supportedWriteOperations(s,t).contains(INSERT)` 拒 |
| `PhysicalPlanTranslator.buildPluginRowLevelDmlSink`（行级 DML，约 :684） | `getWritePlanProvider()==null` 拒「row-level DML」 | `!(ops.contains(DELETE)\|\|ops.contains(MERGE))` 拒 |
| `InsertOverwriteTableCommand.pluginConnectorSupportsInsertOverwrite`（:341） | `supportsInsertOverwrite()` | `supportedWriteOperations(...).contains(OVERWRITE)` |
| `IcebergNereidsUtils`（:166）/ `IcebergRowLevelDmlTransform`（:100） | `supportsDelete()\|\|supportsMerge()` | `supportedWriteOperations(...)` 含 DELETE 或 MERGE |
| `InsertIntoTableCommand.connectorSupportsWriteBranch`（:814）/ `InsertOverwriteTableCommand`（:357） | `supportsWriteBranch()` | `connector.supportsWriteBranch(...)` |
| `PluginDrivenExternalTable.supportsParallelWrite`（:112 区）/ `requiresFullSchemaWriteOrder`（:202 区）/ `requirePartitionLocalSortOnWrite`（:187 区） | `getCapabilities().contains(PARALLEL_WRITE/SINK_*)` | 读 provider 的 `requiresParallelWrite/requiresFullSchemaWriteOrder/requiresPartitionLocalSort`（经 null-safe 包装） |
| `validateRowLevelDmlMode` 调用（`IcebergRowLevelDmlTransform` :144） | 不变 | 不变（T3，在 DELETE/MERGE 准入通过**之后**调） |

> 行号为本轮取证时的提示值；实现时以控制流/方法名为准（行号可能漂移）。

### §D 一致性验收（G1 的兑现）

给新连接器加任意能力，都只改**一处 = 实现它的 seam**：

| 加能力 | 改哪里 | 处数 |
|---|---|---|
| FILTER_PUSHDOWN | 实现 `ConnectorPushdownOps.applyFilter` | 1 |
| INSERT | 实现 `getWritePlanProvider()` 的 `planWrite`（`supportedOperations` 默认已含 INSERT） | 1（provider 内） |
| +OVERWRITE / DELETE / MERGE / REWRITE | provider 内 `planWrite` 加分支 + `supportedOperations` 加值 | 1（同一 provider 类） |
| 写 branch | provider 覆写 `supportsWriteBranch` | 1（provider 内） |
| parallel/full-schema/local-sort | provider 覆写对应 `requiresXxx` | 1（provider 内） |
| VIEW / MVCC 等规划开关 | 声明枚举值 + 实现对应 ops 方法 | 仍 2，但属逃生舱层、与 Trino 同源、数量已最小化 |

**INSERT 与 FILTER_PUSHDOWN 彻底对称**——都在各自 provider/ops 内实现，无跨模块 flag。

### §E 注册期契约校验

新增 `ConnectorContractValidator`，在连接器实例化 / 建 catalog 时调用（`ConnectorPluginManager` 的 `ServiceLoader<ConnectorProvider>` 装配后，`:75` 一带），**fail-loud**；并作为对六连接器参数化的契约测试：

1. `supportedOperations()` 里每个 op 都被 `planWrite` 真正处理（不抛 NOT_SUPPORTED）——保证「声明=实现」不漂移。
2. `supportsWriteBranch() == true` ⇒ `INSERT ∈ supportedOperations()`。
3. `requiresPartitionLocalSort() == true` ⇒ `requiresParallelWrite() == true ∧ requiresFullSchemaWriteOrder() == true`（把旧 `ConnectorCapability` doc 注释里的口头约定升级为强校验）。

---

## 5. 六连接器迁移后声明矩阵

| 连接器 | 残留枚举 capabilities | provider `supportedOperations` | provider 其它（branch / sink-trait） | T3 方法 |
|---|---|---|---|---|
| **ES** | ∅（只读） | —（无 write provider → 空集） | — | — |
| **Trino** | ∅（只读） | — | — | — |
| **JDBC** | `PASSTHROUGH_QUERY` | `{INSERT}`（默认，无需覆写） | — | — |
| **MaxCompute** | ∅ | `{INSERT, OVERWRITE}` | `requiresParallelWrite`、`requiresFullSchemaWriteOrder`、`requiresPartitionLocalSort` = true | — |
| **Paimon** | `MVCC_SNAPSHOT`、`PARTITION_STATS`、`COLUMN_AUTO_ANALYZE`、`SHOW_CREATE_DDL` | —（写路径未接） | — | — |
| **Iceberg** | `MVCC_SNAPSHOT`、`COLUMN_AUTO_ANALYZE`、`TOPN_LAZY_MATERIALIZE`、`SHOW_CREATE_DDL`、`VIEW` | `{INSERT, OVERWRITE, DELETE, MERGE, REWRITE}` | `supportsWriteBranch`=true、`requiresFullSchemaWriteOrder`=true、`requiresParallelWrite`=true | `validateRowLevelDmlMode` |

**关键修复**：iceberg 今天既没声明 `SUPPORTS_INSERT` 也没 `supportsInsert()`，全靠 provider 存在能写。新模型下其写 provider 的 `supportedOperations()` 必须列出所支持的 op（不列 INSERT 就被 §C 准入拒），把这条隐性事实显式化；§E 契约 #1 在建 catalog 时即逼出/校验它。

> **`REWRITE` 的准入**：`REWRITE`（`ALTER TABLE ... EXECUTE rewrite_data_files`）经 procedure / rewrite 执行路径（`ConnectorProcedureOps`），不走 §C 的 sink-翻译准入点，故未列入 §C 表。它列进 `supportedOperations()` 仅为「写操作能力」的单一来源完整——procedure 路径如需准入判断亦可读同一集合，避免再开第二处声明。

---

## 6. 测试策略（Rule 9：测意图非字面值）

- **删除同义反复测试**：`assertTrue(metadata.supportsInsert())`（`FakeConnectorPluginTest:163`、`EsScanPlanProviderTest:149`、`JdbcDorisConnectorTest:158`）及 `SUPPORTS_TIME_TRAVEL` 两处自我断言（`IcebergConnectorTest:110`、`PaimonConnectorMetadataMvccTest:1162`）。
- **行为门测试**：声明 INSERT 的 fake 连接器能过 `visitPhysicalConnectorTableSink` 准入；不声明的被拒且报错文案正确——业务逻辑回退时此测试会红。
- **契约测试**：§E 三条不变式对六连接器参数化跑（声明缺实现 / branch 缺 INSERT / local-sort 缺依附 都要 fail）。
- **粒度准入测试**：只声明 `{INSERT}` 的连接器，OVERWRITE/DELETE 被分别拒，且文案区分操作。

---

## 7. 迁移顺序与翻闸安全

1. SPI 加性改动：`ConnectorWritePlanProvider` 补 `supportedOperations`/`supportsWriteBranch`/`requiresXxx`；`Connector` 补 null-safe 委派；新增 `ConnectorContractValidator`。
2. 改 §C 全部准入点读新接口；`PluginDrivenExternalTable` 三个 sink-trait helper 改读 provider。
3. 各连接器 provider 声明 `supportedOperations` 等（iceberg/maxcompute/jdbc）。
4. **删除**：`ConnectorWriteOps` 5 个布尔方法 + 死枚举值 + 挪走的 3 个 sink-trait 枚举值 + `SUPPORTS_INSERT`/`SUPPORTS_TIME_TRAVEL` + 同义反复测试。

**翻闸安全**：iceberg 翻闸前 inert——`visitPhysicalConnectorTableSink` / `buildPluginRowLevelDmlSink` 等 generic 准入点只对 `PluginDrivenExternalTable`（翻闸后）生效，pre-cutover iceberg 走 legacy sink，故 §C 改写不影响 legacy 路径。JDBC/MaxCompute/Paimon 已在 SPI_READY_TYPES，其准入即时生效——迁移后声明须与实现一致（§E 契约保证）。

> ⚠️ 与 P6.6 翻闸 blocker 解耦：本设计是 P6.6 之外的跨切面统一，**不应**与翻闸 push 捆绑，避免扩大翻闸验证面。建议作为独立任务序列（writing-plans 拆分）在翻闸稳定后落地；删 `SUPPORTS_TIME_TRAVEL` 前须与翻闸计划核对（§B）。

---

## 8. 决策记录

| # | 决策 | 选择 | 备注 |
|---|---|---|---|
| D1 | 能力分层模型 | 三层：能力 / 行为提示 / 校验 | 用户选定 |
| D2 | 三层落到类型系统 | 初选「双枚举+方法」，经 Trino 调研后**修订为 R1** | 见 D3 |
| D3 | 统一方向 | **R1：一律「实现 seam」（Trino 式）** | 推翻 D2 的「给写能力加 flag」；改为 seam 即声明、粒度化 |
| D4 | 早准入机制 | `supportedOperations()` 查询（非纯 call-and-throw） | 保留分析期 fail-loud；纯化变体存档 |
| D5 | 读侧对称 / pushdown 入枚举 | 都不做 | 非目标 N1/N2 |

---

## 9. 风险

- **R-1（翻闸耦合）**：删 `SUPPORTS_TIME_TRAVEL` 等若与 P6.6 翻闸后接线计划冲突 → 缓解：删前核对，§B/§7 已标注。
- **R-2（行号漂移）**：§C 行号为取证提示值 → 缓解：实现以方法名 + 控制流为准（遵 HANDOFF「信控制流不信注释」）。
- **R-3（default `{INSERT}` 过宽）**：有写 provider 但实际不支持纯 INSERT 的连接器（暂无此例）会被默认误纳 → 缓解：§E 契约 #1 校验 `planWrite` 真处理 INSERT，否则注册期 fail。
