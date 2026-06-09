# P4-T06e FIX-LIMIT-SPLIT-DEFAULT — Review Rounds

> Issue P3-9 / NG-5 / F11 (major, read, regression). Also closes minors F2 / F12.
> Design: `plan-doc/tasks/designs/P4-T06e-FIX-LIMIT-SPLIT-DEFAULT-design.md`.
> Flow: design → design-validation workflow → implement → guards (compile+UT+checkstyle+import-gate+mutation)
> → adversarial impl-review workflow → converge → commit.

## Decision recap

Cutover (`MaxComputeScanPlanProvider.planScan:199-202`) ignored the `enable_mc_limit_split_optimization`
session var (default false) and hard-stubbed `checkOnlyPartitionEquality`→false, so
`useLimitOpt = limit>0 && !filter.isPresent()` — limit-opt fired by default for any no-filter LIMIT
(opposite of legacy default-OFF) and never for the partition-equality path. **User decision (2026-06-08):
Fix — restore the legacy default-OFF three-gate**, connector-local, no SPI change.

## Round 0 — Design validation (workflow `w17wzd0el`, 4 adversarial lenses, clean-room)

Lenses: legacy-parity / correctness-lostrows / channel-feasibility / test-mutation. **0 mustFix.**
Verdict: "safe to implement as written".

- **legacy-parity** — 2 nits isReal=false (both unreachable: `limit==0` is rewritten to
  `LogicalEmptyRelation` by Nereids `EliminateLimit` and never reaches planScan, so `limit>0` ≡
  legacy `hasLimit()`; a single conjunct is never a CompoundPredicate(AND) because scan-node
  conjuncts are split via `ExpressionUtils.extractConjunctionToSet`). 1 nit isReal=true: the
  CAST-unwrap divergence is broader than the doc's single example (covers right-side literal CAST
  and IN-element CAST too) — **doc-only**, folded into the design DV note + DV-016.
- **correctness-lostrows** — 1 nit isReal=true (the CAST-unwrap), verified **NOT** a lost-rows
  defect: partition pruning is computed identically via Nereids `SelectedPartitions`, and the
  converted `filterPredicate` is still passed to the read session on both the standard and
  limit-opt paths (`MaxComputeScanPlanProvider:191,:208,:353`) as a backstop. Worst case is the opt
  firing on a slightly broader (still pure-partition) set, opt-in (var default OFF).
- **channel-feasibility** — 0 findings. Confirmed: the `@VarAttr` is in `VariableMgr.toMap` under
  the exact lowercase key; booleans serialize "true"/"false" (Boolean.parseBoolean-safe); the only
  scan-node-creation path dereferences `ConnectContext.get()` unconditionally so the live scan
  session always carries real session properties (errs OFF if ever absent); the
  hardcoded-string + `getOrDefault(...,"false")` + parseBoolean pattern is byte-identical to the JDBC
  connector convention.
- **test-mutation** — 1 **shouldFix** isReal=true: the RHS-literal guard
  (`cmp.getRight() instanceof ConnectorLiteral`) had no killing test — a mutant accepting
  `pt = region` (partcol=partcol) would survive. **Folded in: added test
  `testPartitionColumnEqualsPartitionColumnIneligible` + mutation C.** Plus 2 nits isReal=true:
  hardcode the literal var-key string in tests (not the prod constant) — done (`VAR_KEY`); and the
  planScan wiring is untestable in-module (no fe-core/Mockito) — acknowledged, E2E is the sole
  guard (recorded as DV-016, same posture as DV-015).

### Design-validation actions folded in (pre-implementation)
- Added unit test for `pt = region` (RHS-literal guard) + mutation C.
- Tests build the session-property map with the literal `"enable_mc_limit_split_optimization"`.
- Broadened the CAST-unwrap DV note (all cast positions) in the design + DV-016.
- Acknowledged the planScan-wiring coverage gap (E2E-only) in the design Test Plan + DV-016.

## Guards (post-implementation)

- **Build:** `:fe-connector-maxcompute -am` BUILD SUCCESS (no SPI/fe-core change → only the
  connector module touched).
- **UT:** `MaxComputeScanPlanProviderTest` 21/21 (3 pre-existing `toPartitionSpecs` + 18 new),
  0 failures/errors/skips.
- **checkstyle:** `:fe-connector-maxcompute checkstyle:check` 0 violations.
- **import-gate:** `tools/check-connector-imports.sh` clean (the hardcoded var-name string keeps
  the connector free of any fe-core `SessionVariable` dependency).
- **mutation:** see table below.

Each mutation: `cp`-restore the prod file, apply one change, `-am test -DfailIfNoTests=false`,
confirm red, restore. (The `-am` reactor needs `-DfailIfNoTests=false` or upstream `fe-thrift`
aborts with "No tests were executed!" — an early harness miss that was caught and fixed.)

| # | mutation | killing test(s) | result |
|---|---|---|---|
| A | `isLimitOptEnabled` default `"false"`→`"true"` | `testLimitOptDisabledWhenVarAbsent` | Failures: 1 ✓ |
| B | comparison `getOperator() == EQ` → `!= EQ` | `testSinglePartitionEqualityEligible` + 3 others | Failures: 4 ✓ |
| C | drop `&& getRight() instanceof ConnectorLiteral` (→ `true`) | `testPartitionColumnEqualsPartitionColumnIneligible` | Failures: 1 ✓ |
| D | AND-loop guard: drop the `!` in `!isPartitionEqualityLeaf(conjunct,…)` | `testAndOfPartitionEqualitiesEligible` | Failures: 1 ✓ |
| E | drop `in.isNegated() ||` (NOT-IN no longer rejected) | `testNotInOnPartitionIneligible` | Failures: 1 ✓ |
| F1 | `shouldUseLimitOptimization`: `!limitOptEnabled` → `false` | `testGateClosedWhenVarDisabled` | Failures: 1 ✓ |
| F2 | `limit <= 0` → `limit < 0` | `testGateClosedWhenNoLimit` | Failures: 1 ✓ |
| G | drop IN-value guard `\|\| !isPartitionColumnRef(in.getValue(),…)` (added in Round 1) | `testInValueDataColumnIneligible` | Failures: 1 ✓ |

Final green confirm (26 tests after Round 1): `Tests run: 26, Failures: 0, Errors: 0, Skipped: 0`.

(Note: the first D variant `if (false)` left `conjunct` unused → checkstyle-bound-to-validate
went red before tests, an ambiguous "empty" capture; re-run with the negation-drop D above gives an
unambiguous test failure — the AND short-circuit is genuinely guarded.)

## Round 1 — Impl review (workflow `walkff1vf`, 4 lenses, clean-room) → converged

Lenses: correctness-vs-legacy / regression-other-paths / test-quality / edge-cases.
**1 mustFix (resolved this round) + benign nits.** Verdict after fix: converged, ready to commit.

- **correctness-vs-legacy** — 0 mustFix. Independently traced the helper bodies against legacy
  `checkOnlyPartitionEqualityPredicate` + gate; confirmed byte-faithful on every reachable shape
  (incl. EQ_FOR_NULL rejected as a distinct operator). 1 nit isReal=true: `LIMIT 0` takes a
  different *path* than legacy (`limit<=0` vs legacy `hasLimit()`=`limit>-1`) but is
  correctness-equivalent (both yield 0 rows) and unreachable (Nereids folds `LIMIT 0` to EmptySet).
- **regression-other-paths** — 0 mustFix. Verified: standard read-session path byte-unchanged;
  `requiredPartitions` flows into BOTH paths (FIX-PRUNE-PUSHDOWN preserved); no SPI change (all 3
  `planScan` signatures unchanged, zero external callers of the new helpers); session read NPE-safe
  per `getSessionProperties()` never-null contract. 2 nits isReal=false (the contract-guaranteed
  null-map; a nested-AND-as-single-conjunct broadening that is safe like the CAST DV). This lens
  also executed the suite: 21/21 at the time.
- **test-quality** — **1 mustFix isReal=true (RESOLVED):** every `ConnectorIn` test used a
  *partition* column as the IN value, so a mutant dropping `!isPartitionColumnRef(in.getValue(),…)`
  (line 469) survived the suite — a real correctness invariant with legacy parity
  (`MaxComputeScanNode:358-364`); a regressed guard would silently under-read on
  `data_col IN (...) LIMIT n` with the var ON. **Fix: added `testInValueDataColumnIneligible`
  (`data_col IN ('a','b')` → false); mutation G confirms it now goes red (Failures: 1).** Other
  named concerns (no-filter→true arm, IN all-literal loop, Comparison-side col-ref check) verified
  genuinely covered. 1 nit isReal=true: planScan gate(1) wiring is unit-untested (E2E-only) — the
  acknowledged DV-016 posture, not a false-confidence claim.
- **edge-cases** — 0 mustFix. Probed EQ_FOR_NULL / nested AND-OR-NOT-Between-IsNull-Like-FunctionCall
  conjuncts / both-literal / empty-IN / case-sensitivity / null; all handled correctly & conservatively.
  2 nits isReal=true (correctly-handled-but-untested edge cases + empty-IN returning true [legacy-
  parity-faithful, unreachable, backstopped]).

### Round 1 actions folded in
- **mustFix:** added `testInValueDataColumnIneligible` + mutation G (confirmed kill).
- **edge-case hardening (nits):** added `testEqForNullOnPartitionIneligible`,
  `testBothLiteralsComparisonIneligible`, `testAndContainingNonLeafConjunctIneligible`,
  `testEmptyInListMatchesLegacyEligible` (pins the deliberate legacy-parity empty-IN behavior).
  Suite 21 → 26, all green; checkstyle 0.
- **doc-only:** the `LIMIT 0` path nit + the nested-AND broadening recorded in DV-016 alongside the
  CAST-unwrap divergence.

**No prod change in Round 1** — the implementation was already correct; the only change was test
coverage (the mustFix was a missing test, not a code defect).
