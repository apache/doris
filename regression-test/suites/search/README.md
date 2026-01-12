# Search Function NULL Semantics Tests

This directory contains regression tests for search function NULL value handling fixes.

## Background

Fixed two major issues:
1. **OR Query Inconsistency**: search() vs match functions returned different results
2. **NOT Query Inconsistency**: Internal NOT vs external NOT returned different results

## Test Files

- `test_search_null_semantics.groovy`: Basic NULL semantics tests
- `test_search_vs_match_consistency.groovy`: search() vs match() consistency tests
- `test_search_null_regression.groovy`: Specific regression tests for original bugs
- `test_search_boundary_cases.groovy`: Edge cases and boundary conditions

## Expected Behavior

After the fix:
- OR queries: `search('A or B')` consistent with `A match ... or B match ...`
- NOT queries: `search('not A')` consistent with `not search('A')`
- Proper SQL three-value logic for NULL handling

## Running Tests

```bash
cd regression-test
./run-regression-test.sh --suite search
```