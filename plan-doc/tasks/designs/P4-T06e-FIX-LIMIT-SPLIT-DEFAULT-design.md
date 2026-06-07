# P4-T06e FIX-LIMIT-SPLIT-DEFAULT — Design

> Issue: P3-9 / NG-5 / F11 (major, read, regression). Source:
> `plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §NG-5.
> Also closes the two related minors F2 / F12 (the `checkOnlyPartitionEquality` stub).
> 用户定夺：**Fix（恢复三重闸）**（2026-06-08）。

## Problem

After cutover, MaxCompute's LIMIT-split optimization semantics are **reversed** vs legacy:

`MaxComputeScanPlanProvider.planScan` (`:199-202`):
```java
boolean onlyPartitionEquality = filter.isPresent()
        && checkOnlyPartitionEquality(filter.get(), partitionColumnNames); // STUB: always false
boolean useLimitOpt = limit > 0 && (onlyPartitionEquality || !filter.isPresent());
```
Because `checkOnlyPartitionEquality` is hard-stubbed to `false`, this reduces to
`useLimitOpt = limit > 0 && !filter.isPresent()`. Two regressions:

1. **Session var ignored.** The gate never reads `enable_mc_limit_split_optimization`
   (`SessionVariable.java:2891/2908`, registered `@VarAttr`, **default false**). So any
   no-filter `SELECT … LIMIT n` is **always** compressed into a single row-offset split —
   the opposite of legacy's default-OFF. For large `n` this serializes a read that legacy
   parallelized (perf regression); it also silently overrides a user who set the var false.
2. **Partition-equality path dead.** With the stub at `false`, a `LIMIT n` query whose
   filter is purely partition-column equality never gets the optimization even when the
   user explicitly enabled the var.

Legacy three-gate (`MaxComputeScanNode.java:735-737`), default OFF:
```java
if (sessionVariable.enableMcLimitSplitOptimization   // (1) session var, default false
        && onlyPartitionEqualityPredicate            // (2) all conjuncts are partcol = lit / IN (lits)
        && hasLimit()) {                             // (3) limit > 0
```
with `checkOnlyPartitionEqualityPredicate()` (`:334-375`): empty conjuncts → true; else every
conjunct must be `BinaryPredicate EQ` (`SlotRef(partcol) = LiteralExpr`) or non-NOT `InPredicate`
(`SlotRef(partcol) IN (LiteralExpr…)`); anything else → false.

## Root Cause

The connector port kept the *shape* of the legacy gate but dropped gate (1) entirely (the
session var was never threaded to the connector) and left gate (2) as a `return false` stub.
What the legacy gate did with `sessionVariable` + Doris `Expr conjuncts`, the connector must
now do with `ConnectorSession.getSessionProperties()` + the `ConnectorExpression filter`.

## Design

**Connector-local. No SPI change.** Both inputs are already available at `planScan`:

- **Gate (1) — session var.** `ConnectorSession.getSessionProperties()` is populated for live
  scans: `PluginDrivenExternalCatalog.buildConnectorSession()` → `ConnectorSessionBuilder.from(ctx)`
  → `extractSessionProperties` → `VariableMgr.toMap(sessionVariable)`, which includes every
  `@VarAttr`, so `"enable_mc_limit_split_optimization"` → `"true"/"false"` is readable. (Same
  pattern the JDBC connector already uses for `jdbc_clickhouse_query_final`,
  `enable_odbc_transcation`, etc. — the connector hardcodes the var-name string; it must not
  depend on fe-core's `SessionVariable` constant, per import-gate.)
- **Gate (2) — only-partition-equality.** The `filter` passed to `planScan` is
  `buildRemainingFilter()` → `ExprToConnectorExpressionConverter.convertConjuncts(...)`:
  - empty conjuncts → `Optional.empty()` (handled by the `!filter.isPresent()` arm).
  - 1 conjunct → the bare converted node.
  - N conjuncts → a **flat** `ConnectorAnd` (count preserved; `convertConjuncts` never drops a
    conjunct — unknown types become `ConnectorFunctionCall`).
  - MaxCompute uses the **default** `supportsCastPredicatePushdown = true`, so `buildRemainingFilter`
    takes the `else` branch and passes the **full** conjunct set (no whole-conjunct drops). Thus
    `checkOnlyPartitionEquality(filter)` faithfully sees all conjuncts — equivalent to legacy
    walking `conjuncts`.

**Two pure static helpers (mirror the `toPartitionSpecs` test-as-pure-static convention):**

```java
private static final String ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION =
        "enable_mc_limit_split_optimization";

/** Gate (1): read the session var (default false). Map-typed for direct unit testing. */
static boolean isLimitOptEnabled(Map<String, String> sessionProperties) {
    return Boolean.parseBoolean(
            sessionProperties.getOrDefault(ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION, "false"));
}

/** Composite eligibility: gate(1) && gate(3) && gate(2). Pure → directly unit testable. */
static boolean shouldUseLimitOptimization(boolean limitOptEnabled, long limit,
        Optional<ConnectorExpression> filter, Set<String> partitionColumnNames) {
    if (!limitOptEnabled || limit <= 0) {
        return false;
    }
    if (!filter.isPresent()) {           // no predicate → every row qualifies
        return true;
    }
    return checkOnlyPartitionEquality(filter.get(), partitionColumnNames);
}
```

**Real `checkOnlyPartitionEquality` (replaces the stub; make `static` package-private):**

```java
static boolean checkOnlyPartitionEquality(ConnectorExpression expr,
        Set<String> partitionColumnNames) {
    if (expr instanceof ConnectorAnd) {
        for (ConnectorExpression conjunct : ((ConnectorAnd) expr).getConjuncts()) {
            if (!isPartitionEqualityLeaf(conjunct, partitionColumnNames)) {
                return false;
            }
        }
        return true;
    }
    return isPartitionEqualityLeaf(expr, partitionColumnNames);
}

private static boolean isPartitionEqualityLeaf(ConnectorExpression expr,
        Set<String> partitionColumnNames) {
    // partcol = literal   (mirror legacy: col on the LEFT, literal on the RIGHT, EQ only)
    if (expr instanceof ConnectorComparison) {
        ConnectorComparison cmp = (ConnectorComparison) expr;
        if (cmp.getOperator() != ConnectorComparison.Operator.EQ) {
            return false;
        }
        return isPartitionColumnRef(cmp.getLeft(), partitionColumnNames)
                && cmp.getRight() instanceof ConnectorLiteral;
    }
    // partcol IN (literal, …)   (not NOT-IN; all elements literal)
    if (expr instanceof ConnectorIn) {
        ConnectorIn in = (ConnectorIn) expr;
        if (in.isNegated() || !isPartitionColumnRef(in.getValue(), partitionColumnNames)) {
            return false;
        }
        for (ConnectorExpression item : in.getInList()) {
            if (!(item instanceof ConnectorLiteral)) {
                return false;
            }
        }
        return true;
    }
    return false;
}

private static boolean isPartitionColumnRef(ConnectorExpression expr,
        Set<String> partitionColumnNames) {
    return expr instanceof ConnectorColumnRef
            && partitionColumnNames.contains(((ConnectorColumnRef) expr).getColumnName());
}
```

**Wire into `planScan` (`:199-202`):**
```java
boolean limitOptEnabled = isLimitOptEnabled(session.getSessionProperties());
boolean useLimitOpt = shouldUseLimitOptimization(
        limitOptEnabled, limit, filter, partitionColumnNames);
```

Net gate: `enableVar && limit>0 && (noFilter || onlyPartitionEquality)` — byte-faithful to
legacy's `enableMcLimitSplitOptimization && onlyPartitionEqualityPredicate && hasLimit()`
(legacy's `onlyPartitionEqualityPredicate` is `true` for empty conjuncts, matching `noFilter`).

## Implementation Plan

1. `MaxComputeScanPlanProvider.java`:
   - add `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION` constant + `isLimitOptEnabled` + `shouldUseLimitOptimization` (static) + real `checkOnlyPartitionEquality` (static, package-private) + `isPartitionEqualityLeaf` / `isPartitionColumnRef` (private static).
   - replace the `:199-202` block with the two-line wiring above.
   - imports: `ConnectorAnd`, `ConnectorComparison`, `ConnectorIn`, `ConnectorColumnRef`, `ConnectorLiteral` (all `org.apache.doris.connector.api.pushdown.*`); `java.util.Map` already imported.
2. No other prod files (no SPI, no fe-core).

## Risk Analysis

- **Blast radius:** single connector method; only MaxCompute reaches it. Default var stays
  **false** → default behavior reverts to legacy (no limit-opt unless explicitly enabled),
  which is the conservative direction. Zero impact on other connectors.
- **Correctness:** limit-opt now fires only when (var on) AND (no filter OR pure partition
  equality). In both predicate cases every row in the (pruned) partitions qualifies, so reading
  the first `min(limit,total)` rows by offset is correct (LIMIT w/o ORDER BY is order-free).
- **Known divergence (minor, note as DV):** `convert(CastExpr)` unwraps the cast **in every
  position**, so `CAST(partcol AS T) = lit`, `partcol = CAST(lit AS T)`, and
  `partcol IN (CAST(lit AS T), …)` all reach the connector with the cast stripped and pass
  gate (2); legacy's `checkOnlyPartitionEqualityPredicate` saw the raw `CastExpr` child (failing
  its `instanceof SlotRef` / `instanceof LiteralExpr` checks) and returned false. Cutover
  therefore enables the opt on a slightly **broader** set — but it is still pure-partition and
  correctness-safe (the converted `filterPredicate` is still passed to the read session as a
  backstop on both the standard and limit-opt paths — `MaxComputeScanPlanProvider :191,:208,:353`
  — and partition pruning is computed identically via Nereids `SelectedPartitions`; LIMIT w/o
  ORDER BY is order-free). Opt-in only (var default OFF). Register in deviations-log.
  (Validated by design-review workflow `w17wzd0el`, correctness-lostrows + legacy-parity lenses.)
- **Interaction:** orthogonal to FIX-PRUNE-PUSHDOWN (P1-4). `requiredPartitions` continues to
  flow to both the limit-opt and standard read-session paths unchanged.

## Test Plan

### Unit Tests (`MaxComputeScanPlanProviderTest`, connector module — no fe-core/Mockito)

`checkOnlyPartitionEquality` / `shouldUseLimitOptimization` / `isLimitOptEnabled` are pure
static; exercise directly. `partitionColumnNames = {"pt","region"}`.

1. `isLimitOptEnabled`: empty map → false; `{k="true"}` → true; `{k="false"}` → false. (kills default-literal + parse mutations). **Build the map with the literal key `"enable_mc_limit_split_optimization"`, NOT the prod constant** — matches the JDBC test convention (`JdbcConnectorMetadataTest`) so a prod-side typo in the constant value is caught (review `w17wzd0el` test-mutation nit).
2. `shouldUseLimitOptimization` gate (1): `limitOptEnabled=false` → false even with limit>0 & no filter. (kills dropping the `!limitOptEnabled` guard)
3. gate (3): `limitOptEnabled=true, limit=0` → false. (kills `limit<=0` guard)
4. no-filter arm: `enabled, limit=10, Optional.empty()` → true.
5. partition equality single: `pt = 1` → `checkOnlyPartitionEquality` true → eligible.
6. partition IN: `region IN ('cn','us')` → true.
7. `ConnectorAnd` all partition eq: `pt=1 AND region='cn'` → true.
8. mixed: `pt=1 AND data_col=5` → false (data_col not partition). (kills the AND short-circuit)
9. data-col equality: `data_col = 5` → false.
10. non-EQ on partition col: `pt > 1` → false. (kills the `op==EQ` guard)
11. NOT IN on partition col: `pt NOT IN (1,2)` → false. (kills the `!negated` guard)
12. IN with non-literal element on partition col → false.
13. literal-on-left `1 = pt` → false (mirror legacy col-on-left only). (kills swapping left/right)
14. **partcol = partcol** `pt = region` (col on BOTH sides) → false. Reaches the RHS check (left is a valid partition col-ref) and fails on `right instanceof ConnectorLiteral`. (kills dropping `&& getRight() instanceof ConnectorLiteral` — review `w17wzd0el` shouldFix: without this, `pt = region` / `pt = func(...)` would be wrongly eligible, mirroring legacy `MaxComputeScanNode:346` requiring `child(1) instanceof LiteralExpr`)

### Mutation (cp-backup the prod file; per HANDOFF operational notes)

- `isLimitOptEnabled` default `"false"`→`"true"` → test 1 (empty map) red.
- drop `!limitOptEnabled` in `shouldUseLimitOptimization` → test 2 red.
- drop `limit <= 0` → test 3 red.
- `op == EQ` → `!=` / remove → test 10 red.
- `!negated` removal → test 11 red.
- AND-loop `return false`→`continue`/`true` → test 8 red.
- drop `&& getRight() instanceof ConnectorLiteral` → test 14 red.

**Coverage gap (inherent, acknowledge — review `w17wzd0el` test-mutation nit):** the two replaced
wiring lines in `planScan` (`isLimitOptEnabled(session.getSessionProperties())` +
`shouldUseLimitOptimization(...)` receiving the live `filter`/`partitionColumnNames`) cannot be
unit-tested in the connector module — `planScan` needs a live `com.aliyun.odps.Table` and there is
no fe-core/Mockito. The pure helpers are fully covered; the integration seam is guarded only by the
CI-skipped live E2E below (record as the DV truth-gate, same posture as P1-4 DV-015).

### E2E (CI-skipped; live ODPS truth-gate — record as DV, not run here)

`regression-test` p2 `test_max_compute_limit_*` (or extend an existing MC suite):
- var OFF (default): `SELECT * FROM mc_t LIMIT 1000000` → EXPLAIN/profile shows multi-split
  parallel scan (no row-offset single split).
- var ON + partition-eq filter + LIMIT → single row-offset split.
- var ON + no filter + LIMIT → single row-offset split.

## Doc-sync (with commit)

- `task-list-P4-rereview.md`: P3-9 row → DONE (+ round summary); RESUME → P3-10.
- `deviations-log.md`: DV — CAST-unwrap broadens limit-opt eligibility (opt-in, safe);
  note F2/F12 closed by the real `checkOnlyPartitionEquality`.
- `decisions-log.md`: D — limit-opt restored as connector-local three-gate via
  `getSessionProperties()` (no SPI change).
- review-rounds file: `plan-doc/reviews/P4-T06e-FIX-LIMIT-SPLIT-DEFAULT-review-rounds.md`.

## Outcome ✅ DONE (commit `<hash>`)

Implemented as designed (1 prod file `MaxComputeScanPlanProvider` + tests; no SPI/fe-core change).
Design-validation workflow `w17wzd0el` 0 mustFix; impl-review workflow `walkff1vf` 1 mustFix
(IN-value guard lacked a killing test — added `testInValueDataColumnIneligible` + mutation G;
**no prod change**, the code was already correct). Guards: build SUCCESS, **UT 26/26**, checkstyle 0,
import-gate clean, mutation 8/8 killed + final green. Also closes minors **F2/F12**. Divergences
(CAST-unwrap, nested-AND, LIMIT-0 path, wiring-unit-test gap) recorded in **DV-016**; decision **D-032**.
