# FIX-3 Summary — "Duplicated named partition: p_NULL"

## Problem
CI 973411 `test_paimon_mtmv:247`: CREATE MV partitioned by `region` over paimon `null_partition` failed with
`Duplicated named partition: p_NULL`.

## Root Cause
`null_partition` has genuine NULL, string `'null'`, string `'NULL'`, `'bj'`. The branch's IS-NULL-prune fix
marks a genuine-null partition `isNull=true` → `PartitionInfo.toPartitionValue` renders it as the bare keyword
`NULL` → MTMV name `p_NULL`, colliding with the literal string `'NULL'` partition (also `p_NULL`). Master kept
`isNull=false` so genuine-null got a distinct name. SPI regression.

## Fix
`ListPartitionItem.toPartitionKeyDesc` (both overloads): a new `toDisplayPartitionValues` helper substitutes the
key's `originHiveKeys` sentinel string (e.g. `__HIVE_DEFAULT_PARTITION__`) as the DISPLAY value for a
genuine-null partition, keeping `isNull=true`. The literal stays a `NullLiteral` (IS NULL prune unaffected); only
the rendered name changes → genuine-null becomes `p_HIVEDEFAULTPARTITION` (distinct), no collision. Connector-
agnostic; MTMV-only blast radius (OLAP partitions have empty originHiveKeys → no-op).

## Tests
New `ListPartitionItemTest`: (1) genuine-null vs string-'NULL' produce distinct names AND the null key still
resolves to a NullLiteral (RED reproduced `expected: not equal but was: <p_NULL>`); (2) OLAP null partition
unchanged (no-op guard). 2/2 GREEN; MTMVPartitionUtilTest 10/10 (no regression); fe-core checkstyle clean.

## Result
Fixed (offline UT reproduces + verifies). Real gate: docker enablePaimonTest=true rerun of test_paimon_mtmv;
also re-verify test_paimon_runtime_filter_partition_pruning (IS NULL prune) stays GREEN.
