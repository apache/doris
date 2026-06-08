# FIX-CAST-PUSHDOWN 设计（F9 / READ-C6）

> 严重度：🔴 **major / correctness — 静默数据丢失回归**（review 原误判为「known-degradation / 已登记」，本复查推翻）。
> 用户拍板（2026-06-08）：**Fix（MaxCompute override `supportsCastPredicatePushdown=false`）+ 顺手深查受影响类型对**。
> 来源：`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` F9（confirms 3/3）/ `P4-cutover-review-findings.md` READ-C6。
> 对抗核验：workflow `wzoa6dkvw`（establish + 3 skeptic refute，**0/3 refuted、verdict=real-unregistered-regression**）。

## Problem

查询 MaxCompute 外表时，`WHERE` 含**隐式类型转换**（implicit CAST）的谓词会被**剥壳下推到 ODPS**，
导致**静默少返回行**（错误结果、无报错）。例：STRING 列 `code` 存 `"5"/"05"/" 5"`（数值皆 5）：

```sql
SELECT * FROM mc_tbl WHERE CAST(code AS INT) = 5;
```
- 正确（= legacy）：3 行全返回；cutover：**只返 `"5"`，`"05"/" 5"` 静默丢失**。

## Root Cause（已核源）

1. fe-core 共享 converter 无条件剥 CAST：`ExprToConnectorExpressionConverter.java:108`
   （`else if (expr instanceof CastExpr) return convert(expr.getChild(0));`）→ `CAST(code AS INT)=5` 变 `code=5`。
2. `PluginDrivenScanNode.buildRemainingFilter:779` 仅当 `!supportsCastPredicatePushdown` 才剔除含 CAST 的 conjunct；
   **MaxCompute 不 override，继承 `ConnectorPushdownOps:72` 默认 `true`** → 剥壳后的谓词**不被剔除**、流入 `planScan`。
3. 连接器侧 `MaxComputePredicateConverter.formatLiteralValue:219-222` 按**列**的 ODPS 类型 quote literal
   → STRING 列得到源端过滤 `code = "5"`；`MaxComputeScanPlanProvider:309 withFilterPredicate` 推入 read session。
4. ODPS 在**读取时**过滤掉 `"05"/" 5"`（源端 under-match）→ 这些行**从未读回**。
5. BE 仍保留原 conjunct 复算（MC 不 override `applyFilter`，`convertPredicate:330` `result` 空、conjunct 不清；
   MC 无 conjunct tracking、`pruneConjunctsFromNodeProperties` 早退）——**但 BE 复算只能把 ODPS 返回的超集再过滤*下*，
   无法找回源端已丢的行**。BE backstop 仅救 over-match、不救 under-match。

**为何 legacy 无此问题（→ 这是回归）**：legacy `MaxComputeScanNode.convertSlotRefToColumnName:477` 对非-`SlotRef`
操作数（即 `CastExpr`）**抛 `AnalysisException`** → `convertPredicate:308-313` try/catch **吞掉、丢弃该谓词**（不下推）
→ ODPS 返回全集、BE 复算正确。cutover 比 legacy **严格更紧** → 静默丢行。

## Design

**最小连接器局部修复 = MaxCompute override `supportsCastPredicatePushdown(session) → false`**（镜像 `JdbcConnectorMetadata:222`
的能力门 + `ConnectorPushdownOps:64-70` doc 明示的「coercion 规则不同的连接器应置 false」处方）。
激活**既有** strip 路径（`PluginDrivenScanNode:779-787`）：含 CAST 的 conjunct 在下推前被剔除、保留 BE-only，
ODPS 返回全集、BE 复算正确——**恢复 legacy parity、消除数据丢失**。**无新代码路径。**

## 受影响类型对深查（用户要求；fix 为全覆盖，本节为动机/测试文档）

> 关键：本 fix **剔除所有含 CAST 的 conjunct**（`containsCastExpr` 查整树），故**不需精确枚举即安全**——
> 任何 Doris CAST 语义 ≠ ODPS 隐式 coercion 的对都被一网打尽。下列为代表性 under-match 风险对：

| 谓词形 | Doris 语义 | cutover 推下的 ODPS 源过滤 | under-match 风险 |
|---|---|---|---|
| STRING 列 vs 数值字面量（`CAST(s AS INT)=5`、`s IN (1,2)`） | 数值相等（`"05"`=5） | `s = "5"`（按列 STRING quote） | **高**（确认）：丢 `"05"/" 5"/"+5"/"5.0"` |
| 数值列 vs 字符串字面量（`CAST(n AS STRING)='5'`） | 字符串相等 | `n = 5`（按列数值） | 中：ODPS 数值比较 vs Doris 串比较，边界/前导零差异 |
| DATE/DATETIME vs STRING（`CAST(d AS STRING)='2024-01-01'`） | 串格式相等 | 按列 DATE quote，格式/时区 coercion 差 | 中：格式串差异致丢行 |
| DECIMAL/精度（`CAST(dec AS INT)=5`、float↔decimal） | 截断/舍入后比较 | 按列精度比较 | 中：精度/舍入语义差 |
| CHAR 定长 padding（`CAST(c AS ...)`） | trim/pad 语义 | 按列 CHAR 比较 | 低-中 |

各对的**确切** under-match 取决于 ODPS 运行时 coercion（代码层不可完全枚举），但 fix 对全部 CAST 谓词一律剔除下推，
故覆盖完整，无需逐一证实。**等值/`IN` 最清晰；范围比较（`>/</>=/<=`）同理**（剥壳后边界 coercion 差亦 under-match）。

## Risk Analysis

- **性能（可接受、= legacy parity）**：CAST 谓词不再下推 ODPS → 该谓词不再窄化源端扫描、多读些行交 BE 复算。
  与 legacy 行为一致（legacy 本就丢弃 CAST 谓词下推）。correctness > 这点丢失的下推优化。
- **limit-opt 交互（更保守、安全）**：含 CAST 的分区等值谓词不再进 pushed filter → `shouldUseLimitOptimization`
  的 `checkOnlyPartitionEquality` 对其判不资格 → limit-opt 更保守触发（少触发非误触发，无正确性损失）。
- **分区裁剪不受影响**：Nereids `PruneFileScanPartition` 用原始 Doris Expr 独立算 `SelectedPartitions`，
  不经 `supportsCastPredicatePushdown`、不经 connector converter → 裁剪照常。
- **其余连接器零影响**：仅 MaxCompute override；jdbc(session-gated true)/es/hive/paimon/hudi/trino 不变。
- **无 SPI 变更**：`supportsCastPredicatePushdown` 已是 SPI 既有方法、strip 路径已存在。

## Test Plan

### Unit（offline）
- `MaxComputeConnectorMetadataCapabilityTest` 加 `maxComputeDisablesCastPredicatePushdown`：
  `new MaxComputeConnectorMetadata(null,null,"proj","ep","quota",emptyMap()).supportsCastPredicatePushdown(null)` == **false**
  （getter 不碰实例字段，offline；mirror 既有 `maxComputeDeclaresSupportsCreateDatabase` + JDBC `JdbcConnectorMetadataTest:106`）。
  **WHY**：flip 回 true（或删 override 回默认 true）→ 重新打开 CAST 下推 → 数据丢失回归。mutation：override `false→true` 该测变红。
- buildRemainingFilter 的 strip-when-false 行为是 fe-core 共享逻辑，已被既有路径（JDBC false 分支）覆盖；
  其对 MC 节点的端到端 wiring 受同类 harness 缺位限制（同 [DV-015]），由 live e2e 守（见下）。

### E2E（CI-skip，真值闸）
- live ODPS：STRING 列存 `"5"/"05"/" 5"`，`SELECT ... WHERE CAST(code AS INT)=5` 返回**全部** 3 行（修前只 1 行）；
  EXPLAIN 证 CAST 谓词不在下推 filter、留 BE。归 DV（CAST-pushdown 数据丢失修复真值闸）。

## Implementation Plan
1. `MaxComputeConnectorMetadata` 加 `@Override supportsCastPredicatePushdown(session)→false`（带 WHY 注释引 F9/legacy parity）。
2. `MaxComputeConnectorMetadataCapabilityTest` 加测 + mutation。
3. 守门：连接器 compile BUILD SUCCESS、UT、checkstyle 0、import-gate 净、mutation（false→true 变红）。
4. impl-review workflow 收敛。
5. 独立 commit（fix）+ commit（hash 回填）；D-036 + 必要时 DV；**更正 review F9 定级**（known-degr→regression）+ task-list/HANDOFF。

## impl-review（clean-room，workflow `wj2h0120n`，2 lens + verify）—— 收敛 1 shouldFix

**GO-WITH-EDITS：1 shouldFix（2/2 confirmed）+ 3 rejected，已折入**：
- **F9-LIMITOPT-1（shouldFix）**：`supportsCastPredicatePushdown=false` 在 fe-core 剥 CAST conjunct → 连接器收到**空 filter** →
  当 `enable_mc_limit_split_optimization=ON` 且 query 唯一谓词是 CAST（`WHERE CAST(nonpart)=5 LIMIT 10`）时，
  `MaxComputeScanPlanProvider.shouldUseLimitOptimization` 的 `!filter.isPresent()→true` 分支触发 → row-offset 读首 N 行**无谓词** →
  BE 复算 CAST 于首 N 行 → **under-return**。legacy `checkOnlyPartitionEqualityPredicate` 读**原始** conjuncts、CAST child 非 SlotRef→false→limit-opt 关→正确。
  **故仅 override 会把 bug 从「pushdown 丢行」移成「limit-opt 丢行」（仅 limit-opt ON 时，默认 OFF）。**
- **修（折入本 commit，连接器无关、更通用）**：fe-core `getSplits` 在 `filteredToOriginalIndex != null`（CAST conjunct 被剥）时
  **抑制 source-side LIMIT 下推**（抽纯静态 `effectiveSourceLimit(limit, stripped)→stripped?-1:limit`）。limit-opt 需 `limit>0`，
  传 -1 即不触发；BE 仍应用 LIMIT。原则普适（剥了 BE-only 谓词就不能让 source 先 LIMIT），`startSplit` 批路径已恒传 -1（DEC-1）故只改 `getSplits`。
- **守门补**：fe-core LimitStripTest 2/2 + BatchMode 9/9、mutation 2/2 向红（drop-suppression / always-suppress）。
- **out-of-scope 跟进（Rule 12 surface）**：JDBC 若 session 关 cast-pushdown 且经 `applyLimit` 推 limit，理论同类 under-return；
  但 MaxCompute 不 override `applyLimit`（no-op），F9 的 getSplits limit-param 抑制对 MaxCompute 完整；JDBC `applyLimit` 路径**非本修范围**（pre-existing、非 MC），登记备查。

## 关联
- 决策 [D-036]（待）；复查证据 workflow `wzoa6dkvw`
- 复审 [F9 / READ-C6](../../reviews/P4-maxcompute-full-rereview-2026-06-07.md)；区别于 [DV-016]（CAST-unwrap 仅 limit-opt 资格、非丢行）
- 参照 `JdbcConnectorMetadata:222`（同能力门）、`ConnectorPushdownOps:64-74`（SPI doc 处方）
