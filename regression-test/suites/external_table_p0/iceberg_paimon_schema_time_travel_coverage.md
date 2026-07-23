# Iceberg / Paimon Schema Evolution × Time Travel P0 Coverage

## Scope

This document maps the P0 regression suites that validate Doris reads after
Iceberg or Paimon schema evolution. The matrix intentionally combines schema
changes with snapshot, tag, branch, time travel, delete/upsert and reader
variants instead of testing these dimensions in isolation.

The tests use explicit old/new field projections and negative bindings in
addition to row-shape assertions. This prevents a rename with unchanged values
from passing accidentally.

## Schema operations

| ID | Operation | Iceberg | Paimon | P0 contract |
| --- | --- | --- | --- | --- |
| S01 | Add one nullable top-level field | Positive | Positive | Old refs reject the new field; new refs backfill NULL |
| S02 | Add multiple top-level fields | Positive | Positive | Order, types and NULL backfill are snapshot-local |
| S03 | Reorder / FIRST / AFTER | Positive | Positive | Field IDs remain stable across old and new refs |
| S04 | Rename top-level field | Positive | Positive | Old and new names bind only in their own schemas |
| S05 | Case-only or mixed-case rename | Positive | Positive | Case handling does not hide schema selection errors |
| S06 | Drop top-level field | Positive | Positive | Old refs retain the field; current refs reject it |
| S07 | Drop and re-add the same name | Positive | Positive | Old and new field IDs never share values |
| S08 | Compatible type promotion | Positive | Positive | Historical and current types remain correct |
| S09 | Add STRUCT child | Positive | Positive | Nested field is visible only after its schema version |
| S10 | Rename STRUCT child | Positive | Positive | Nested old/new paths have snapshot-local binding |
| S11 | Drop STRUCT child | Positive | Positive | Historical nested projections remain readable |
| S12 | Drop and re-add STRUCT child | Positive | Positive | Nested field IDs never leak values |
| S13 | Promote/reorder STRUCT child | Positive | Positive | Nested predicate and projection use the right type |
| S14 | MAP value STRUCT evolution | Positive | Positive | `element_at(...).field` uses the selected schema |
| S15 | ARRAY element STRUCT evolution | Positive | Positive | `array[i].field` uses the selected schema |
| S16 | Partition-column mutation | Positive/restricted | Negative format contract | Unsupported mutations are rejected atomically |
| S17 | Partition evolution | Positive | Format-constrained contract | Data and payload evolution preserve partition reads |
| S18 | PK-table non-key evolution | N/A | Positive | Upsert/delete/DV remain correct through evolution |
| S19 | Comment/default/nullability | Positive and negative | Positive and negative | Metadata-only changes and rejections are atomic |
| S20 | Narrowing, map-key, key/partition mutations | Negative | Negative | No schema, snapshot or cached state is polluted |

## Historical references and actions

| ID | Reference/action | Iceberg | Paimon | Coverage |
| --- | --- | --- | --- | --- |
| T00 | Latest/current | Yes | Yes | Current schema, explicit projection, predicate, aggregate |
| T01/T02 | Pre/post numeric snapshot | Yes | Yes | Old/new field binding and row visibility |
| T03 | Timestamp string | Yes | Yes | Resolves the expected pre-change schema |
| T04 | Epoch millis | Stable rejection | Yes | Iceberg syntax rejection; Paimon positive read |
| T05/T06/T07 | Pre/post tag forms | Yes | Yes | `FOR VERSION AS OF` and `@tag` |
| T08 | Pre-change branch | Negative product contract | Yes | Branch schema/data are compared with the format oracle |
| T09 | Independent branch evolution | Negative product contract | Negative product contract | Failure is isolated without crashing the shared BE |
| T10 | Expired snapshot retained by tag | Existing lifecycle + matrix | Yes | Retained tag never falls back to latest |
| T11 | Missing snapshot/tag | Yes | Yes | Stable error, never latest fallback |
| T12/T13 | Dual historical relations | Negative product contract | Negative product contract | Join, reverse join, UNION, nested UNION, CTE, subquery |
| T14 | Incremental read across change | N/A | Yes | JNI/CPP-supported paths use the end schema |
| T15 | Rollback to snapshot/timestamp | Yes | N/A | Data rolls back while Iceberg current schema semantics remain correct |
| T16 | Cherry-pick / fast-forward | Yes | Paimon fast-forward oracle | Current, tag and branch state are verified |

## Delete and execution dimensions

| Dimension | Iceberg | Paimon |
| --- | --- | --- |
| Position delete | v2, Parquet and ORC, before/after evolution | N/A |
| Equality delete | Rename, promotion, drop/re-add, old/new snapshots | N/A |
| Deletion vector | v3, Parquet and ORC, before/after evolution | PK-table DV path |
| Row operations | Delete visibility around every checkpoint | Upsert, delete and compaction |
| Readers | File scanner V1/V2 | JNI/native/CPP-supported paths |
| Cache | REST cache on/off | Filesystem metadata cache on/off |
| Catalog smoke | REST full matrix and JDBC rename/time-travel | Filesystem full matrix and JDBC rename/time-travel |

HMS and DLF are credential/environment variants rather than additional schema
semantics. Their existing catalog suites remain responsible for connectivity;
the deterministic P0 schema matrix uses REST/filesystem, and JDBC gets an
explicit rename plus old snapshot/tag smoke path.

## Suite map

| Suite | Responsibility |
| --- | --- |
| `iceberg/test_iceberg_schema_time_travel_matrix.groovy` | S01-S17 × T00-T13, nested evolution, partition evolution, cache/scanner variants |
| `iceberg/test_iceberg_schema_dual_relation_matrix.groovy` | Dual-snapshot join/UNION/CTE/subquery negative contracts |
| `iceberg/test_iceberg_schema_equality_delete_time_travel.groovy` | Equality delete × rename/promotion/drop-re-add |
| `iceberg/test_iceberg_schema_position_dv_time_travel.groovy` | Position delete and DV × top-level/nested evolution, Parquet/ORC |
| `iceberg/test_iceberg_schema_ref_actions_matrix.groovy` | Rollback, cherry-pick, fast-forward and branch action semantics |
| `iceberg/test_iceberg_schema_metadata_atomicity_matrix.groovy` | Comment/default/nullability/narrowing/map-key atomicity |
| `paimon/test_paimon_schema_time_travel_matrix.groovy` | S01-S18 × T00-T14, PK upsert/delete/DV, cache/readers |
| `paimon/test_paimon_schema_dual_relation_matrix.groovy` | Dual-snapshot join/UNION/CTE/subquery negative contracts |
| `paimon/test_paimon_schema_branch_partition_matrix.groovy` | Independent branch evolution, fast-forward and partition restrictions |
| `paimon/test_paimon_schema_metadata_atomicity_matrix.groovy` | Comment/default/nullability/narrowing atomicity |
| `iceberg/test_iceberg_jdbc_catalog.groovy` | JDBC catalog rename × numeric snapshot/tag smoke |
| `paimon/test_paimon_jdbc_catalog.groovy` | JDBC catalog rename × numeric snapshot/tag smoke |

Each Groovy file contains `Scenario` comments identifying the matrix cell under
test. Jira keys are deliberately absent from Groovy source. Product issues link
back to the exact suite, scenario and file location from Jira instead.

## Product contracts discovered by the matrix

| Issue | Observed contract | Negative regression location |
| --- | --- | --- |
| DORIS-27425 | Iceberg branch can use latest schema instead of branch schema | `test_iceberg_schema_time_travel_matrix.groovy`, `test_iceberg_schema_ref_actions_matrix.groovy` |
| DORIS-27427 | Iceberg dual historical relations can share the wrong schema | `test_iceberg_schema_dual_relation_matrix.groovy` |
| DORIS-27428 | Paimon dual historical relations can share the wrong schema | `test_paimon_schema_dual_relation_matrix.groovy` |
| DORIS-27433 | Paimon branch schema init fails; a post-fast-forward scan can abort BE | `test_paimon_schema_branch_partition_matrix.groovy` |
| DORIS-27434 | Doris `DESC` omits Iceberg field comments | `test_iceberg_schema_metadata_atomicity_matrix.groovy` |

## Verification record

- Source baseline: `8a1bf788e29` from `upstream/master`.
- Full FE/BE build passed with
  `DORIS_THIRDPARTY=/mnt/disk3/gabriel/Workspace/doris-master-thirdparty`.
- The ten deterministic REST/filesystem suites above ran concurrently:
  10 suites, 0 failed, 0 fatal, 0 skipped.
- Log:
  `output/regression-test/log/doris-regression-test.20260723.180450.log`.
- The two JDBC catalog smoke suites ran concurrently with PostgreSQL, MySQL,
  local JDBC drivers and Docker: 2 suites, 0 failed, 0 fatal, 0 skipped.
- JDBC log:
  `output/regression-test/log/doris-regression-test.20260723.180753.log`.
- Regression framework compile and `git diff --check` are required before the
  branch is pushed.

The requested schema-change × historical-operation correctness matrix has no
unimplemented P0 cell. Unsupported format operations and currently incorrect
Doris behavior are represented by stable negative regression contracts rather
than being marked as missing.
