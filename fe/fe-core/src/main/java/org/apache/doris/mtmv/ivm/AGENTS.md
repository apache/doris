# AGENTS.md — IVM (Incremental View Maintenance)

## Recommended Test Suite

It is strongly recommended to run the following tests before committing IVM changes.

### Regression Tests

Run all IVM regression suites under `regression-test/suites/mtmv_p0/` (all suites whose name contains `ivm`):

```bash
./run-regression-test.sh -d mtmv_p0 -s test_ivm_basic_mtmv,test_ivm_agg_mtmv,test_ivm_dup_keys_mtmv,test_ivm_complete_refresh_rowid
```

Note: both `-d` and `-s` accept comma-separated values to run multiple suites in one invocation. New IVM suites are added over time — always check `regression-test/suites/mtmv_p0/` for the current set of `test_ivm_*` files.

### FE Unit Tests

Run all IVM-related FE unit tests. The relevant test files are located in:

- `fe/fe-core/src/test/java/org/apache/doris/mtmv/ivm/` — IVM core tests
- `fe/fe-core/src/test/java/org/apache/doris/nereids/rules/rewrite/IvmNormalizeMtmvTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/CreateMTMVCommandTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/catalog/ShowCreateMTMVTest.java`

New test classes are added over time — always check the directories above for the current set.

## Important: Binlog/Stream Not Ready

The binlog/stream-based incremental data capture is **not yet implemented**. During IVM incremental refresh, the base table delta is **mocked** — it reads the full base table data as if it were the incremental change. This means:

- `IvmStreamRef` and `StreamType` are placeholder structures
- The delta executor currently treats the entire base table scan as the delta input
- Real incremental behavior (capturing only inserted/deleted rows since last refresh) will require binlog/stream integration in the future
- All current regression and unit tests operate under this mock-full-scan assumption

## Backward Compatibility

IVM is not publicly available until July 2026. Before that date, there is no need to maintain backward compatibility with existing IVM materialized views. Breaking changes to IVM metadata, DDL format, or internal storage layout are acceptable without migration support.
