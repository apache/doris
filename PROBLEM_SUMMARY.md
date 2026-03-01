# Problem Summary

## Background

The materialized view (MV) rewrite in Nereids optimizer previously had limitations when handling OR conditions in predicates. The predicate compensation logic would fail if:
1. MV contains OR predicates (e.g., `WHERE ds >= '202531' OR ds = '202529'`)
2. Query matches only one branch of MV's OR predicate (e.g., `WHERE ds = '202529'`)

The existing logic required exact predicate matching or full containment, which was too strict for OR conditions.

## Problem

1. **OR Condition Support Missing**: The residual predicate compensation did not support OR branch matching. If an MV had `(A OR B)` and the query had `A`, the system would reject the rewrite opportunity even though the MV could be used with appropriate compensation.

2. **Uncovered Predicate Handling**: When equivalence or range compensation stages found predicates that MV cannot cover, these were simply rejected. The system did not attempt to match these uncovered predicates against MV's residual predicates (which might contain OR conditions that could partially match).

## Solution

Implemented support for OR branch matching in residual predicate compensation:

1. **Uncovered Predicate Collection**: Modified `compensateEquivalence` and `compensateRangePredicate` to collect uncovered predicates (instead of failing immediately) into output parameters `uncoveredEquals` and `uncoveredRanges`.

2. **Residual Compensation with Extra Predicates**: Enhanced `compensateResidualPredicate` to accept `extraQueryResiduals` parameter, which includes uncovered predicates from earlier compensation stages.

3. **OR Branch Matching**: Implemented branch-level matching logic that:
   - Extracts disjunction branches from both MV and query residual predicates
   - Allows partial intersection: if MV has `(A OR B)` and query has `A`, considers it a match
   - Generates compensation: `NOT(B)` to exclude MV's extra branches, and includes query's extra branches directly

## Example

**MV Definition**: `SELECT * FROM T1 WHERE ds >= '202531' OR ds = '202529'`
**Query**: `SELECT * FROM T1 WHERE ds = '202529'`

**Behavior**:
1. Range compensation stage finds `ds = '202529'` is not covered by MV's range predicates, collects it as `uncoveredRanges`
2. Residual compensation receives `uncoveredRanges` as `extraQueryResiduals`
3. OR branch matching: MV residual `(ds >= '202531' OR ds = '202529')` matches query `ds = '202529'` via branch intersection
4. Compensation generated: `NOT(ds >= '202531')` to exclude the extra MV branch

## Code Changes

- `Predicates.compensateEquivalence`: Added `uncoveredEquals` output parameter
- `Predicates.compensateRangePredicate`: Added `uncoveredRanges` output parameter  
- `Predicates.compensateResidualPredicate`: Added `extraQueryResiduals` parameter, implemented OR branch matching via `coverResidualSets` and `coverSingleResidual`
- `AbstractMaterializedViewRule.predicatesCompensate`: Collects uncovered predicates and passes them to residual compensation

## Benefits

1. **Increased MV Utilization**: More queries can utilize MVs with OR predicates, improving query performance
2. **Flexible Matching**: Supports partial matching of OR conditions instead of requiring exact containment
3. **Backward Compatible**: Changes are additive and do not break existing functionality
