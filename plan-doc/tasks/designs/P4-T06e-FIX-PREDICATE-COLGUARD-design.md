# [P4-T06e] FIX-PREDICATE-COLGUARD (GAP2) — design

> 来源：Batch-D 红线扩充对抗复审 workflow `wbw4xszrg`（GAP2，Tier 2，minor，**多半不可达**）。
> 关联：legacy 对照 `MaxComputeScanNode.convertExprToOdpsPredicate`（未知列→`throw AnalysisException`）+ 其 caller loop（per-predicate `catch (Exception)`→丢该谓词）；新路 `MaxComputePredicateConverter.formatLiteralValue`（odpsType==null→**静默引号化下推非法谓词**）。

## Problem

翻闸后，谓词引用**表中不存在的列**时，新路把字面量**静默引号化并下推一条非法谓词到 ODPS**，而非像 legacy 那样丢弃该谓词。下推 `unknowncol == "v"` 给 ODPS 结果未定义（ODPS 可能报错，或更糟——按其语义返回错误行集 → 静默错结果）。

链（已核码，2026-06-08）：
- `MaxComputePredicateConverter.formatLiteralValue:210-213`：
  ```java
  OdpsType odpsType = columnTypeMap.get(columnName);
  if (odpsType == null) {
      return " \"" + rawValue + "\" ";   // ← 静默引号化，下推非法谓词
  }
  ```
- 调用链：`convertFilter`(`MaxComputeScanPlanProvider:273-298`) → `converter.convert(filter.get())`(`:297`) → `doConvert` → `convertComparison:141` / `convertIn:177` → `formatLiteralValue(columnName, ...)`。
- `columnTypeMap` 由 `convertFilter:280-285` 从 ODPS 表 schema 的**数据列 + 分区列**构建。

## Root Cause（已核码确认）

| # | 位置 | 现状 | legacy parity |
|---|---|---|---|
| 1 | `MaxComputePredicateConverter.formatLiteralValue:211-213` | `if (odpsType == null) return " \"" + rawValue + "\" ";`（静默引号化→下推非法谓词） | legacy `MaxComputeScanNode:~420/~484`：`if (!getColumnNameToOdpsColumn().containsKey(columnName)) throw new AnalysisException("Column ... not found ...")` → caller `:309-310` catch → **丢该谓词**（不下推） |

**守卫反转**：legacy 用 `containsKey` 守卫、未知列**抛**→丢谓词；新路在 `get()==null` 时**反向**地静默接受、构非法串。本 issue = 把该 null 分支从「静默引号化」改为「抛」，使其经 `convert()` 的既有 catch 降级为 `Predicate.NO_PREDICATE`（= 不下推该过滤 = 丢谓词），恢复 legacy「不下推非法谓词」不变式。

**为何 CI 没抓 / 为何多半不可达**：`columnTypeMap` 覆盖表全部数据列+分区列；nereids/SPI 下达的 bound 谓词只引用已绑定的真实表列 → `get()` 实务上永不返 null。此守卫是**防御性**（defense-in-depth）；触发条件需一条 bound 谓词引用 schema 外的列名（理论上不应发生）。低优、`minor`。

## Blast radius

- 改动集中在连接器 `MaxComputePredicateConverter.formatLiteralValue` **一处分支**（一条 `return` → 一条 `throw`）。**无 SPI 变更、无 fe-core 改动**。
- `convert()` 的既有顶层 `catch (Exception)`（`:91-96`）已把 `formatLiteralValue` 现有的 3 处 `throw`（非列引用 `:198`、非字面量 `:204`、不可下推类型 `:260`）统一降级为 `NO_PREDICATE`；本修新增的 throw 复用同一通道，**与方法既有契约一致**（Rule 3 surgical / Rule 11 conformance）。
- import-gate 净（不新增任何 import；`UnsupportedOperationException` 为 `java.lang`）。

## Design

**Shape：连接器局部，无 SPI / 无 fe-core 变更。**

`MaxComputePredicateConverter.formatLiteralValue:211-213`：

```java
OdpsType odpsType = columnTypeMap.get(columnName);
if (odpsType == null) {
    throw new UnsupportedOperationException(
            "Cannot push down predicate on unknown column: " + columnName);
}
```

- 抛 `UnsupportedOperationException`（非 legacy 的 `AnalysisException`）：① 连接器禁 import fe-core（`AnalysisException` 在 fe-core，import-gate 禁）；② 与**同方法**既有 3 处守卫一致（均 `UnsupportedOperationException`，Rule 11）；③ `convert()` 的 catch 是 `catch (Exception)`，任何异常皆降级，类型不影响行为。
- 行为结果：未知列谓词 → throw → `convert()` catch → `NO_PREDICATE` → 该过滤不下推、BE 兜底复算 → **结果正确**（= legacy「丢谓词」的本质不变式）。

### 与 legacy 的粒度差异（如实登记，Rule 12）

legacy 的 try-catch 在 **per-doris-predicate** 粒度（`MaxComputeScanNode:309-310`），故未知列只丢**那一条**谓词、其余照常下推；新路 `convert()` 在**整个 filter 表达式**粒度（`MaxComputeScanPlanProvider:297` 一次性 convert 整树），故触发时**整树**降 `NO_PREDICATE`（全部谓词丢下推）。

- 此粒度差异**非本 fix 引入**：是 SPI converter 设计 + G0（datetime CST 降级、不可下推类型）既有属性，对**所有** `formatLiteralValue` throw 一致成立。
- **correctness-safe**：无论丢一条还是整树，丢的谓词均由 BE 复算 → 结果恒正确；差异仅在**下推程度**（perf）。
- 既然守卫**多半不可达**，触发时的 perf 退化不构成实际风险；不为此重构 converter 的 catch 粒度（Rule 2 不投机 / Rule 3 surgical）。若未来证明可达且 perf 重要，再单独提 per-conjunct 降级 issue。

## Risk Analysis

- **over-rejection（误丢真谓词）**：仅当 `columnTypeMap.get(columnName)==null` 即列不在表 schema 时触发；真实 bound 谓词只引真列 → 不会误丢。✅
- **行为回归**：修前「静默下推非法谓词」是 bug（错结果或 ODPS 报错）；修后「降级 NO_PREDICATE」是 legacy parity 且 correctness-safe。无回归，纯修正。✅
- **import-gate / SPI**：零新增 import、零 SPI 变更。✅

## Test Plan

### Unit Tests（`MaxComputePredicateConverterTest`，连接器模块）

新增针对未知列守卫的用例（Rule 9 — 钉「不下推非法谓词」不变式）：

1. **未知列比较谓词 → NO_PREDICATE**：构 `columnTypeMap` 只含已知列（如 `id`→BIGINT），对**未在 map 中**的列名（如 `ghost`）构 `ConnectorComparison(ghost == 5)`，断言 `convert(...) == Predicate.NO_PREDICATE`（修前：返回含 `ghost == "5"` 的 `RawPredicate`，断言其**非** NO_PREDICATE → 修前红 / 修后绿）。
2. **未知列 IN 谓词 → NO_PREDICATE**：同上，`ConnectorIn(ghost IN (1,2))` → 断言 NO_PREDICATE。
3. **已知列谓词不受影响（回归护栏）**：已知列 `id == 5` 仍正常下推为 `RawPredicate("id == 5")`（确认本修未误伤正常路径）。

> mutation 验证：把 fix 后的 `throw` 临时改回 `return " \"" + rawValue + "\" ";` → 用例 1/2 应变红（钉死守卫真在起作用）；还原。

### E2E / live（真实 ODPS，CI 跳，登记 DV）

本守卫多半不可达，无自然 live 触发路径；不新增 e2e suite。在 deviations/decisions 标注：未知列谓词下推已与 legacy 对齐（不下推非法谓词），真值由 UT + mutation 保证；live 层无回归面（正常查询不触发该分支）。

## 实现清单

1. `MaxComputePredicateConverter.java:211-213`：`return` → `throw UnsupportedOperationException`。
2. `MaxComputePredicateConverterTest`：+3 用例（未知列 comparison / IN → NO_PREDICATE；已知列回归护栏）。
3. 守门：编译（`-pl :fe-connector-maxcompute`）+ UT + checkstyle + import-gate + mutation（向红→还原）。
4. 单 Agent 对抗 impl-review。
5. 独立 `[P4-T06e]` commit + hash 回填 + tracker 更新（`task-list-batchD-redline-gaps.md` G2 行）。
