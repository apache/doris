# FIX-OVERWRITE-GATE (P0-1) — 对抗 review 轮次记录

> issue: NG-1 (F42/F47) — INSERT OVERWRITE 整条被 `allowInsertOverwrite` 网关挡死（翻闸后表为 `PluginDrivenExternalTable`）。
> 设计: `plan-doc/tasks/designs/P4-T06e-FIX-OVERWRITE-GATE-design.md`
> 流程: 每轮记结论防跨轮矛盾；最多 5 轮。

---

## Round 1（2026-06-07）— verdict: **needs-revision**（推翻设计「bare instanceof 可接受」的 deferral）

**fix（round-1）**: `InsertOverwriteTableCommand.allowInsertOverwrite` else 分支追加 `|| targetTable instanceof PluginDrivenExternalTable` + import。UT `InsertOverwriteTableCommandTest`（positive+negative）。编译+UT 2/2 过；mutation 自证（去 arm+import → positive test 红 `expected:<true> but was:<false>`，negative 仍绿）。

**review 机制**: clean-room workflow `w5ke8sjaq`（13 agents）— Phase A 2 lens 只读码 → Phase B 每 finding 2 票对抗 refute → Phase C 解禁先验交叉核对。raw 5 → 存活 4（+1 borderline）。

**存活 findings**:
| # | sev | cat | 标题 | refute | 处置判定 |
|---|---|---|---|---|---|
| 1 | **major** | regression | bare instanceof 纳入 JDBC → `JdbcConnectorMetadata.getWriteConfig` 不透传 overwrite → JDBC INSERT OVERWRITE **静默退化为 plain INSERT（丢数据）** | 2/2 real | **必须修**（见下分析） |
| 2 | major | regression | 纳入 ES/Trino（`supportsInsert()=false`）→ 不在网关拒、改在 `PhysicalPlanTranslator:686-698` 抛**泛化** "does not support INSERT"（非 "OVERWRITE not supported"） | 2/2 real | 修（窄化网关后自动 fail-loud 于网关，消息清晰） |
| 3 | minor/nit | correctness/style | 拒绝消息过期："only support OLAP/Remote OLAP and HMS/ICEBERG"（漏 MaxCompute/plugin 类型）→ 误导 | 2/2 | round-2 顺手更正 |
| 4 | minor | test-quality | negative test `mock(TableIf.class)` 是 tautology（任何 instanceof 都 false）→ 只能抓"放宽为无条件 true"突变，抓不到具体 arm 删除 | 1/2（borderline） | round-2 强化：加"PluginDriven 但非 overwrite-capable（JDBC-backed）应被拒"用例 |

**Phase C 交叉核对**: matchesDesignIntent=true（改动逐字符合设计）；contradictsHistory=false（与 FIX-PART-GATES 决策① 不矛盾——此处 instanceof 已类型限定、下游统一，无需窄化；Batch-D 红线满足——本 fix 加 arm，legacy MC arm 与新 arm 共存）；testVacuousRisk=true（positive test 非空、negative test 弱但够其声明范围）。**doc-sync 缺口**: task-list 仍 "6/6"、无 FIX-OVERWRITE-GATE 行、改动未 commit。

**关键裁决（Rule 7 + Rule 12）**: Phase C 把 findings #1/#2 归"设计已知 deferral / out-of-scope"。**本轮推翻该 deferral**：
- 事实核验（against code）: `supportsInsert()` 存在（`ConnectorWriteOps:47` 默认 false；JDBC+MC override true；ES/Trino 继承 false）；**无** overwrite-specific capability；MaxCompute `MaxComputeWritePlanProvider:167` `builder.overwrite(true)` → **真支持 overwrite**；JDBC `getWriteConfig`（`JdbcConnectorMetadata:289+`）**不透传 overwrite** → 真静默丢。
- 修前 JDBC overwrite = 在网关被**大声拒**（不在 allow-list）；修后（bare instanceof）= 通过网关 → 静默 plain INSERT。**=本 fix 引入的新静默丢数据路径**，即便底层 getWriteConfig gap 预先存在（此前不可达）。**Rule 12 不允许静默错误** → 必须窄化谓词，不能 ship bare instanceof。
- ES/Trino 非数据 bug（已 fail-loud），但窄化谓词后顺带获得网关层清晰错误（消除"半接 dispatch"味）。

**round-2 计划（待用户定谓词窄化方案后执行）**: ① 窄化 `allowInsertOverwrite` 的 PluginDriven 分支（方案 A/B/C 见下，已 surface 用户）；② 更正拒绝消息（#3）；③ 强化 negative test（#4，加 JDBC-backed PluginDriven 应被拒用例，直接守门窄化谓词）。然后重跑 review。

**谓词窄化方案（surface 用户，2026-06-07）**:
- **A（推荐）**: SPI 加 `supportsInsertOverwrite()`（`ConnectorWriteOps` 默认 false，MaxCompute override true），网关 `instanceof PluginDrivenExternalTable && <connector supportsInsertOverwrite>`。通用/SPI 对齐/未来连接器可 opt-in；JDBC/ES/Trino 在网关清晰拒（fail-loud）；MC 恢复 parity。涉 fe-connector-api + fe-connector-maxcompute + fe-core（各小改）。
- **B**: fe-core only，网关 `instanceof PluginDrivenExternalTable && "max_compute".equals(catalogType)`。最小、不动连接器、精确 legacy parity，但在通用 dispatch 点硬编码 "max_compute"（反 SPI）。
- **C（不推荐）**: 保 bare instanceof + 登记 JDBC 静默丢 + ES/Trino 泛化错为 known deviation + 另开 ticket。违 Rule 12（新静默丢数据）。

**用户决策（2026-06-07）= Option A（SPI capability）。**

---

## Round 2（2026-06-07）— fix: SPI capability `supportsInsertOverwrite()`；verdict: **CONVERGED（code sound）**

**fix（round-2，3 模块）**:
1. `ConnectorWriteOps.java` 加 `default boolean supportsInsertOverwrite() { return false; }`（supportsInsert 后）。默认 false → 支持 plain INSERT 但不支持 overwrite 的连接器（jdbc/es/trino）在网关被**大声拒**，不静默退化。
2. `MaxComputeConnectorMetadata.java` `@Override supportsInsertOverwrite()=true`（MaxComputeWritePlanProvider:167 `builder.overwrite(true)` 真支持）。
3. `InsertOverwriteTableCommand.java`: 网关 PluginDriven 分支窄化为 `instanceof PluginDrivenExternalTable && pluginConnectorSupportsInsertOverwrite(...)`；helper 经 `catalog.getConnector().getMetadata(catalog.buildConnectorSession()).supportsInsertOverwrite()`（镜像 PhysicalPlanTranslator:657-686 访问式）；+import PluginDrivenExternalCatalog；拒绝消息更正（不再误导）。
4. test 强化（解 round-1 #4）：3 用例 —— (a) overwrite-capable PluginDriven→放行；(b) **非 overwrite-capable PluginDriven（jdbc-like，capability=false）→拒**（回归守门）；(c) `mock(TableIf)`→拒。

**编译+UT**: 3 模块编译 BUILD SUCCESS，UT 3/3 过（MVN_EXIT=0）。
**mutation（Rule 9）**: 还原为 round-1 bare instanceof（去 `&&` clause+helper+import，干净编译）→ **唯 (b) 红**（`expected:<false> but was:<true>` = JDBC 静默退化场景），(a)(c) 仍绿。证 capability 网关必要、(b) 真守门。

**round-1 findings 关闭情况（待 round-2 review 确认）**: #1 JDBC 静默丢→jdbc 现于网关 fail-loud（capability=false）✓；#2 ES/Trino→现于网关 fail-loud 清晰消息✓；#3 误导消息→已更正✓；#4 tautology→已加 (b) 非空守门✓。pre-existing JDBC `getWriteConfig` overwrite gap 留另开 ticket（overwrite 现不可达，无 live 回归）。

**round-2 review 裁决（clean-room workflow `wo81wbi7x`，3 agents）**: **rawFindings=0 / survivors=0**（code-only 2 lens 零发现）。Phase C 交叉核对：`round1FindingsClosed=true`（逐条 against code 确认上述 4 项关闭）、`matchesDesignIntent=true`、`testVacuousRisk=false`（(b) pin capability 语义、suite 对相关突变真能 fail）、`contradictsHistory=false`（与 FIX-PART-GATES 决策① 一致——本处谓词既类型限定又 capability-gated，是决策① 认可的"勿过宽"方向；Batch-D 红线满足——本 fix 加 PluginDriven arm 紧随 legacy MC arm，删 legacy 后覆盖不丢）。verdict=`minor-issues` **仅**由 doc-sync/commit 收尾项驱动（非代码缺陷）。

**结论：P0-1 代码 CONVERGED（2 轮）**。收尾：commit（code+test+design+review-rounds+task-list）；doc-sync（HANDOFF :26 stale 的 round-1 描述更正、decisions-log 登记新 SPI capability `supportsInsertOverwrite` + Option A 决策）作为 doc-sync WIP（这些文件本就有 prior-session 未提交改动，不混入本 issue commit）。
**scope reminder（非缺陷，设计已述）**: 本 fix 只开 FE 入口网关；live INSERT OVERWRITE 正确性 + NG-2/NG-4（动态分区 local-sort）+ NG-3（静态分区 bind）须 live e2e（CI 跳）。绿网关+绿 UT ≠ e2e overwrite 工作。
