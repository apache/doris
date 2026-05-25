# P1 — scan-node 收口 + 重复清理

> 阶段总览见 [00-master-plan §3.2](../00-connector-migration-master-plan.md)。
> 协作规范见 [AGENT-PLAYBOOK.md](../AGENT-PLAYBOOK.md)。

---

## 元信息

- **状态**：🚧 启动中
- **启动日期**：2026-05-25
- **目标完成**：2026-06-01（1 周）
- **实际完成**：—
- **阻塞**：无（P0 已合入 `branch-catalog-spi`，PR #63582 / `c6f056fa5bd`）
- **阻塞下游**：P2-P7 所有连接器迁移依赖 P1 把 scan-node 收口 + 旧路径删除
- **主 owner**：@me
- **分支**：`catalog-spi-02`（基于 `upstream-apache/branch-catalog-spi`）

---

## 阶段目标

承接 P0 的 SPI baseline，做两件事：

1. **删旧**：清理 fe-core 中已经被 SPI 实现覆盖、但还没删的 legacy 代码（JDBC 旧 client、Paimon/MaxCompute 重复 converter）。
2. **收口**：把 `PhysicalPlanTranslator.visitPhysicalFileScan` 的 7+ 个 `instanceof XExternalTable` 分支统一到 `PluginDrivenExternalTable` 路径（迁移期可保留老分支兜底）；让 `LogicalFileScan.computeOutput` 通过 SPI 而非 instanceof 拿 metadata 列。

完成后：

- `PhysicalPlanTranslator` 不再 `import` 任何具体 `*ExternalTable` 类（除迁移期 fallback）。
- 后续每个连接器迁移（P3-P7）只需删掉对应 fallback 分支，不需要触碰 scan-node 主干。

---

## 验收标准

从 master plan §3.2 同步：

- [ ] 13 个 `datasource/jdbc/client/Jdbc*Client.java` + `JdbcFieldSchema.java` 全部删除（约 2730 LOC）
- [ ] fe-core 重复的 `PaimonPredicateConverter` + `McStructureHelper` 处理完毕（见 T2 决议）
- [ ] `PhysicalPlanTranslator.visitPhysicalFileScan` 优先走 `PluginDrivenExternalTable` 分支
- [ ] `visitPhysicalHudiScan` 通过 `PluginDrivenScanNode` 处理增量场景
- [ ] `LogicalFileScan.computeOutput` 不再 `instanceof IcebergExternalTable / HMSExternalTable`
- [ ] `PhysicalPlanTranslator` 不再 `import` 任何具体 `*ExternalTable` 类（除迁移期 fallback）
- [ ] fe-core 全编译 + checkstyle 0
- [ ] PR CI 全绿（JDBC + ES regression-test 仍通过；其他连接器走 fallback 通过）

---

## 任务清单

> ID 永不复用。批次方案 2026-05-25 用户已确认：批 A=T3+T4+T5、批 B=T1、T2 推迟 P4/P5。

| ID | 任务 | 批次 | Owner | 状态 | PR | 启动 | 完成 | 备注 |
|---|---|---|---|---|---|---|---|---|
| P1-T01 | 删除 13 个 `Jdbc*Client.java` + `JdbcFieldSchema.java` | **批 B** | @me | ⏳ | — | — | — | 先解耦 3 个外部 caller（PostgresResourceValidator / StreamingJobUtils / CdcStreamTableValuedFunction）+ 处理 3 个测试 |
| P1-T02 | 重复 `PaimonPredicateConverter` + `McStructureHelper` 处理 | **🚫 推迟到 P4/P5** | — | 🚫 | — | — | — | 2026-05-25 用户决议（Q2）：fe-core caller 本身是 P4/P5 要删的 legacy；本阶段不动 |
| P1-T03 | `PhysicalPlanTranslator.visitPhysicalFileScan` 收口（**保留 fallback**） | **批 A** | @me | ✅ | TBD | 2026-05-25 | 2026-05-25 | `PluginDrivenExternalTable` 分支提到 if-else 链最前；7 个老分支原地保留作 P3-P7 迁移期 fallback |
| P1-T04 | `visitPhysicalHudiScan` 委托给 `PluginDrivenScanNode` | **批 A** | @me | ✅ | TBD | 2026-05-25 | 2026-05-25 | 新分支前置；`scanParams` + `tableSnapshot` 经 `FileQueryScanNode` setters 透传；`incrementalRelation` 待 P3 Hudi 迁移时 SPI 扩展（TODO 注释已落） |
| P1-T05 | `LogicalFileScan.computeOutput` 改走 SPI | **批 A** | @me | ✅ | TBD | 2026-05-25 | 2026-05-25 | 新增 `computePluginDrivenOutput()`（与 `computeIcebergOutput` 同 shape，用 `getFullSchema` + virtualColumns）；`supportPruneNestedColumn` 加 `PluginDrivenExternalTable → false` 显式分支（无新 SPI capability 时保守默认）；`IcebergExternalTable` 路径原地保留 |

**状态图例**：⏳ pending / 🚧 in_progress / ✅ done / ❌ blocked / 🚫 deleted

---

## 阶段日志（倒序）

### 2026-05-25（白天 ③）— 批 A 编码完成（T3 + T4 + T5）

实施了三处 SPI 收口（保留迁移期 fallback）：

- **T3** — `PhysicalPlanTranslator.visitPhysicalFileScan`：把现有 `if (table instanceof PluginDrivenExternalTable)` 分支提到 if-else 链最前；7 个连接器特定分支（HMS/Iceberg/Paimon/Trino/MaxCompute/LakeSoul/RemoteDoris）原地保留作 P3-P7 迁移期 fallback。
- **T4** — `PhysicalPlanTranslator.visitPhysicalHudiScan`：在 method 顶部新增 `PluginDrivenExternalTable` 分支，路由到 `PluginDrivenScanNode.create(...)`，通过 `FileQueryScanNode` setters 透传 `tableSnapshot` / `scanParams`。`hudiScan.getIncrementalRelation()` 增量场景被记为 P3 Hudi SPI 扩展的 TODO（注释已落）。HMS + DLAType.HUDI 路径保留。本分支今日不可达（PhysicalHudiScan 目前只为 HMSExternalTable 创建），P3 Hudi 迁移时激活。
- **T5** — `LogicalFileScan`：
  - `computeOutput()`：新增 `PluginDrivenExternalTable` 分支，调新增 helper `computePluginDrivenOutput()`，用 `getFullSchema() + virtualColumns`（与 `computeIcebergOutput` 同 shape）。JDBC/ES 当前无 hidden cols 也无 virtualColumns，行为等价。Iceberg 分支原地保留。
  - `supportPruneNestedColumn()`：新增 `PluginDrivenExternalTable → return false` 显式分支。语义无变化（fall-through 也是 false），但显式声明 SPI 默认；未来加 `ConnectorCapability` 时改这里。
  - 新增 import：`org.apache.doris.datasource.PluginDrivenExternalTable`。

**编译 / Checkstyle**：`mvn -pl fe-core -am compile` BUILD SUCCESS；`mvn -pl fe-core checkstyle:check` 0 violations。

**测试范围**：三处变更对 JDBC/ES（当前唯一已迁 SPI 连接器）行为等价（fullSchema == baseSchema 且无 virtualColumns；supportPruneNestedColumn 原本就 false）。集成层信号依赖 PR CI 上的 JDBC + ES regression-test（P0 已基线 PASS）。本地单测层未新增——三处都是路由 reorder + 显式声明，难以在不引入 PluginDrivenExternalTable mock 的前提下意义单测；待 PR review 决定是否补。

### 2026-05-25（白天 ②）— 批次方案确认

用户回复 3 个决策点（HANDOFF Q1/Q2/Q3）：

- **Q1 → A → B → C**：先做 T3+T4+T5 scan-node 收口（批 A），再删 legacy JDBC client（批 B），T2 推迟到 P4/P5
- **Q2 → 推迟 T2**：fe-core PaimonPredicateConverter + McStructureHelper 留到 P4/P5 caller 删除时一并干掉；P1 不动
- **Q3 → 保留 fallback**：T3 仅把 `PluginDrivenExternalTable` 分支提到最前；老 instanceof 链原地保留，每个连接器在 P3-P7 迁移完成时删对应分支

任务表的"批次"列已同步更新；T2 状态翻 🚫（推迟标记）。

### 2026-05-25（白天）— 阶段启动 + recon

- 新建分支 `catalog-spi-02` 基于 `upstream-apache/branch-catalog-spi`（PR #63582 已合入 `c6f056fa5bd`）
- Recon 5 个子任务，输出代码侧 facts：
  - **T1**：13 个 `Jdbc*Client.java`（合计 ~2730 LOC）+ `JdbcFieldSchema.java`（129 LOC）。fe-core 内 3 个外部 caller 必须先解耦：`PostgresResourceValidator.java`、`StreamingJobUtils.java`、`CdcStreamTableValuedFunction.java`。3 个测试需删或迁
  - **T2**：fe-core 有 `datasource/paimon/source/PaimonPredicateConverter.java`（201 LOC）和 `datasource/maxcompute/McStructureHelper.java`（298 LOC）。fe-connector 侧的对应类是 canonical 版本。fe-core caller：`PaimonScanNode`、`MaxComputeExternalCatalog`、`MaxComputeMetadataOps` 自身就是 legacy，P4/P5 会删
  - **T3**：`PhysicalPlanTranslator.visitPhysicalFileScan` lines 726-797（72 LOC），含 8 个 instanceof 分支（HMSExternalTable + 嵌套 DLAType 路由；Iceberg / Paimon / Trino / MaxCompute / LakeSoul / RemoteDoris / PluginDrivenExternalTable）。`PluginDrivenScanNode.create(...)` 和 `PluginDrivenExternalTable` 已存在
  - **T4**：`visitPhysicalHudiScan` lines 821-841（21 LOC），目前断言 HMSExternalTable + DLAType.HUDI，构造 HudiScanNode 时传 `getScanParams()` + `getIncrementalRelation()` 支持增量
  - **T5**：`LogicalFileScan.computeOutput` lines 201-212（12 LOC），instanceof IcebergExternalTable 时走 `computeIcebergOutput()` 加 v3 row-lineage 虚拟列。`supportPruneNestedColumn()` 也用了 3 个 instanceof（lines 236-238）
  - **Bonus**：`nereids/` 目录下还有 ~62 处 `instanceof.*ExternalTable`；P1 范围只覆盖 PhysicalPlanTranslator + LogicalFileScan，其余 50+ 处在 P3-P7 各连接器迁移时随主任务清理
- 批次方案待用户确认（见 HANDOFF）

---

## 关联

- Master plan 章节：[§3.2 P1 阶段](../00-connector-migration-master-plan.md)
- RFC 章节：n/a（P1 是 SPI 消费方收口，不涉及 SPI 设计修改）
- 决策：—
- 偏差：—
- 风险：R-008（文档脱节）、R-001（image 兼容回归——T3/T4/T5 收口须不影响序列化路径）
- 连接器：jdbc（T1）、paimon（T2）、maxcompute（T2）；T3-T5 是平台层

---

## 当前阻塞项

无。批次方案已确认；新 session 直接开始批 A 编码。
