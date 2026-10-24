<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg Write P0 Coverage Matrix

## Scope and criteria

This document covers the correctness, compatibility and failure atomicity of Doris writes to Iceberg tables. The matrix checks individual capabilities as well as the interactions among schema change, partition evolution, snapshots/tags/branches, row-level delete/update/merge, table models, partitioning and bucketing, data types and NULL semantics.

After the last write, every positive suite compares the logical result of the same table in Doris and Spark. Suites that involve transform metadata also compare physical partition values, and suites that involve historical references additionally check that snapshots, tags and branches stay isolated.

Coverage status:

- Covered (verified): a P0 suite asserts a deterministic result for the scenario, and it has been verified on a multi-BE cluster.
- Expected rejection: Doris explicitly does not support the operation; a P0 suite checks the error message and failure atomicity.
- Covered (isolated negative): a regression reproduces a known product issue. It runs isolated from the default P0 run, so that it does not kill a shared BE or commit unreadable files.

## Risks

| ID | Risk | Source | Impact | Priority |
| --- | --- | --- | --- | --- |
| R01 | After a schema change the writer still writes by old column positions or old field IDs, silently misplacing columns | White box: mapping of Iceberg field IDs to Doris slots | Data correctness | P0 |
| R02 | After partition evolution new files land in an old spec, partition values are computed wrongly, or filtering across specs misses rows | Black box + white box: multiple coexisting partition specs | Write and query correctness | P0 |
| R03 | Strings, numerics, date/time, decimal, boolean and NULL behave inconsistently as the source of identity/bucket/truncate/time transforms | Black box: types and boundary inputs | Partition routing, pruning | P0 |
| R04 | After schema/partition evolution a snapshot, tag or branch binds the wrong schema or data version | Black box: historical reads and references | Time travel correctness | P0 |
| R05 | MOR delete/update/merge across old and new specs produces wrong delete files; COW still publishes a snapshot after being rejected | White box: row-level DML commit | Data loss, failure atomicity | P0 |
| R06 | Duplicate, Unique MOW, Unique MOR and Aggregate source table semantics change in INSERT SELECT | Black box: different Doris table models | Cross-table write correctness | P0 |
| R07 | RANGE/LIST/unpartitioned source tables and HASH/RANDOM/AUTO buckets produce duplicate or lost rows on multiple BEs | Black box + white box: distributed exchange and sink writer | Distributed write correctness | P0 |
| R08 | Primitive, ARRAY, MAP, STRUCT and nested NULL values are written wrongly before or after a schema change | Black box: complex types and NULL | Data correctness, compatibility | P0 |
| R09 | Writing NULL to an Iceberg required column does not fail, and partial data or an empty snapshot is committed | White box: required check and commit | Constraints, failure atomicity | P0 |
| R10 | INSERT OVERWRITE on the evolved current spec, a branch or a NULL partition deletes other partitions by mistake | Black box: overwrite | Data loss | P0 |
| R11 | A single BE passes but concurrent sinks on multiple BEs conflict on file names, commits or partitions | White box: parallel writers and a single commit | Distributed stability | P0 |
| R12 | A nullable STRING, or a Nullable block produced by DML, makes the BE FATAL in the truncate transform | White box: column type constraints of the partition transformer | Cluster availability | P0 |
| R13 | When several MERGE source rows match one target row, the cardinality check is skipped and duplicate data is committed | Black box + white box: MERGE cardinality and commit | Data correctness | P0 |
| R14 | A branch write pollutes main, or a failed tag write or unsupported branch row-level DML still publishes a snapshot | Black box: reference write boundaries | Historical references, failure atomicity | P0 |
| R15 | Overwriting the current spec does not clean up delete files of old specs, or a failed overwrite leaves a partial snapshot | White box: overwrite commit and delete files | Data loss, failure atomicity | P0 |
| R16 | CTAS behaves inconsistently for complex types, NULL, partition transforms, file formats and failure cleanup | Black box: DDL and writer committed together | Schema, file format, atomicity | P0 |
| R17 | Combining sort order, distribution mode, multiple file flushes and concurrent commits causes misordering, lost rows or duplicate commits | White box: exchange, sort writer, optimistic commit | Distributed correctness, stability | P0 |
| R18 | STRING identity/bucket/truncate computes wrong physical partition values for empty strings, Chinese text, emoji, combining characters and NULL | Black box: UTF-8 transform metadata | Partition routing, pruning | P0 |

## Combined coverage

| Dimension | Scenario | Status | P0 suite |
| --- | --- | --- | --- |
| Basic writes | Parquet/ORC, primitive/complex types, INSERT/OVERWRITE | Covered | `test_iceberg_write_insert`, `test_iceberg_insert_overwrite` |
| Partition transforms | identity, bucket, truncate, year/month/day/hour | Covered | `test_iceberg_write_transform_partitions`, `test_iceberg_static_partition_overwrite` |
| Schema + partition evolution | Continued writes and filtering after add/rename/drop/type promotion and ADD/REPLACE/DROP partition field | Covered (verified) | `test_iceberg_write_evolution_refs` |
| Complex type evolution | ARRAY/MAP/STRUCT promotion, new STRUCT fields, old files coexisting with new writes | Covered (verified) | `test_iceberg_write_complex_evolution` |
| Historical versions | Snapshots, tags and branches before and after evolution; independent writes and overwrites on a branch | Covered (verified) | `test_iceberg_write_evolution_refs` |
| MOR | DELETE/UPDATE/MERGE after partition evolution, checking the current version, delete files and historical versions | Covered (verified) | `test_iceberg_write_dml_modes_evolution` |
| COW | DELETE/UPDATE/MERGE after partition evolution are rejected, with data and snapshot count unchanged | Expected rejection | `test_iceberg_write_dml_modes_evolution` |
| Doris source table models | Duplicate, Unique MOW, Unique MOR, Aggregate | Covered (verified) | `test_iceberg_write_source_models` |
| Doris source partitioning | Unpartitioned, RANGE, LIST | Covered (verified) | `test_iceberg_write_source_models` |
| Doris source buckets | Fixed HASH buckets, RANDOM buckets, HASH AUTO buckets | Covered (verified) | `test_iceberg_write_source_models` |
| Partition source types | bucket and applicable transforms on STRING/INT/BIGINT/DATE/DATETIME/DECIMAL; BOOLEAN identity and invalid bucket | Covered (verified) | `test_iceberg_write_partition_types_null` |
| NULL partitions | identity NULL, numeric/decimal bucket and truncate NULL, time transform NULL, NULL in multi-column combinations | Covered (verified) | `test_iceberg_write_partition_types_null` |
| Nullable STRING truncate | INSERT of a nullable STRING through the truncate transform, and writes of the Nullable projection produced by UPDATE | Covered (regular regression) | `test_iceberg_write_nullable_truncate_negative` |
| Full MERGE semantics | Conditional MATCHED, DELETE/UPDATE, multiple conditional NOT MATCHED, NULL-safe and plain NULL keys | Covered (verified) | `test_iceberg_write_merge_semantics` |
| MERGE cardinality | Several source rows matching one target row must fail the whole statement and publish no snapshot | Covered (regular regression) | `test_iceberg_write_merge_duplicate_source_negative` |
| MERGE + STRING truncate | A required truncate source column written through the MERGE nullable projection | Covered (regular regression) | `test_iceberg_write_merge_truncate_negative` |
| Branch/tag write boundaries | Branch INSERT/OVERWRITE isolation; tag writes and branch DELETE/UPDATE/MERGE explicitly rejected | Covered (verified) | `test_iceberg_write_branch_dml_boundary` |
| Nullable data | Top-level NULL, NULL ARRAY elements, NULL MAP values, NULL STRUCT children | Covered and extended | `test_iceberg_write_insert`, `test_iceberg_write_complex_evolution` |
| Required columns and schema change | Valid writes to required columns, NULL written to nullable columns, rejection of adding a required column and of nullable→required | Covered (verified) | `test_iceberg_write_nullability_atomicity` |
| NULL written to required columns | NULL written by VALUES and by mixed batches of a distributed INSERT SELECT | Covered (isolated negative) | `test_iceberg_write_required_null_values_negative`, `test_iceberg_write_required_null_select_negative` |
| Overwrite | Current spec, static partitions, branches, empty input, consecutive partition evolutions, NULL current partition | Covered and extended | `test_iceberg_static_partition_overwrite`, `test_iceberg_write_evolution_refs`, `test_iceberg_write_overwrite_evolution` |
| Overwrite + delete files | Overwrite after MOR DELETE/UPDATE/MERGE, with delete files from before and after evolution coexisting with historical tags | Covered (verified) | `test_iceberg_write_overwrite_delete_files` |
| Overwrite failure atomicity | Distributed strict cast failure on main/branch, snapshots/files/data unchanged, retry after the fix | Covered (verified) | `test_iceberg_write_overwrite_atomicity` |
| STRING physical transforms | UTF-8 boundary values for identity, nullable bucket and required truncate, and transform width evolution | Covered (verified) | `test_iceberg_write_string_transform_metadata` |
| CTAS | Complex types, nested NULL, identity+bucket, ORC compression, cleanup after a failed CTAS | Covered (verified) | `test_iceberg_write_ctas_format_boundary` |
| File format boundaries | Positive Parquet/ORC writes; writes to Avro tables explicitly rejected, with snapshots and files unchanged | Covered (positive + expected rejection) | `test_iceberg_write_ctas_format_boundary` |
| Sort and distribution properties | Multi-column sort order, NULL ordering, none/hash/range distribution, forced multi-file flush | Covered (verified) | `test_iceberg_write_order_distribution_properties` |
| Concurrent writes | Serializability invariant of conflicting MERGEs on the same row, non-conflicting distributed appends | Covered (verified) | `test_iceberg_write_concurrent_merge_invariants` |
| Distributed execution | Multi-bucket source tables, multi-partition Iceberg sinks, multiple BE writers, no catalog/database shared between suites | Covered (verified) | All suites added by this work |
| Spark cross-check | Fifteen positive suites compare row by row after the last write; the STRING transform suite also compares physical partition metadata before and after evolution | Covered (verified) | The fifteen positive suites; physical metadata in `test_iceberg_write_string_transform_metadata` |

## Design of the added cases

| Case | Goal | Risks covered | Test dimensions | Preconditions | Workload | Expected result |
| --- | --- | --- | --- | --- | --- | --- |
| W01 | Verify writes, filtering and historical references after schema and partition spec evolve together | R01, R02, R04, R10 | Function, correctness, compatibility | Iceberg REST catalog | Several batches of Doris writes before and after evolution, creating snapshots/tags/branches, and overwriting a branch | Current, historical and branch versions each return deterministic data; filtering across specs misses no rows |
| W02 | Verify that complex type field IDs stay correct after evolution | R01, R08 | Function, correctness | Iceberg v2 | ARRAY/MAP value promotion, STRUCT child promotion/add, writing old and new rows with nested NULL | Old values are readable under the new schema, new values do not cross fields, nested NULL is preserved |
| W03 | Verify the interaction of MOR/COW with partition evolution, NULL partitions and time travel | R02, R04, R05 | Function, correctness, failure | Iceberg v2 MOR/COW | Delete/update/merge on MOR; the same operations on COW | MOR current and historical versions agree; COW is explicitly rejected with no new snapshot |
| W04 | Verify different Doris table models, partitioning and buckets as the source of Iceberg writes | R06, R07, R11 | Correctness, compatibility | Multi-BE Doris | INSERT SELECT from four table models, three partitioning schemes and HASH/RANDOM/AUTO buckets | Results keep each source table's semantics, with no duplicate or lost rows |
| W05 | Verify partition/bucket transforms on different types and NULL | R02, R03, R11 | Function, correctness, boundaries | Iceberg v2 | Multi-column combinations of identity/bucket/truncate/time transforms, including NULL | Data agrees with the `$partitions` statistics; NULL rows can be filtered and written again |
| W06 | Verify required/nullable schema changes and valid writes | R09, R11 | Failure, correctness | Iceberg required columns | Reject adding a required column without a default and nullable→required; run valid VALUES/INSERT SELECT writes | A failed schema change creates no snapshot; valid writes agree with Spark |
| W07 | Verify rejection of NULL in required columns and statement atomicity | R09, R11 | Isolated negative, correctness | Isolated Iceberg database | VALUES writing NULL; INSERT SELECT from a multi-bucket source table mixing valid and NULL rows | Before the fix it wrongly commits and produces unreadable files; after the fix the whole statement is rejected before the snapshot is published |
| W08 | Verify handling of the Nullable block in STRING truncate | R03, R05, R12 | Regular regression, stability | Iceberg v2 | Nullable STRING INSERT; UPDATE producing a Nullable block after partition evolution | Writes succeed and keep NULL partition semantics, and the BE does not FATAL |
| W09 | Verify MERGE conditional actions, multiple NOT MATCHED clauses and NULL key semantics | R02, R03, R05 | Function, correctness | Iceberg v2 MOR | Moving rows between identity/bucket partitions, deletes, inserts, NULL-safe and plain equality matches | Each source row picks exactly one action; Spark and Doris agree |
| W10 | Verify the cardinality constraint of several MERGE source rows matching one target | R13 | Regular regression, atomicity | Iceberg v2 MOR | Two source rows updating one target row at the same time | The whole statement is rejected with no new snapshot or files |
| W11 | Verify the write capability boundaries of branches and tags | R04, R14 | Function, failure, atomicity | Branch and tag created | Branch INSERT/OVERWRITE; branch row-level DML and tag writes | Branch is isolated from main; unsupported operations are explicitly rejected and references do not change |
| W12 | Verify overwrite and historical references after repeated partition evolution | R02, R04, R10, R15 | Function, correctness | Iceberg v2 | Dynamic overwrite after ADD/REPLACE/DROP of identity, bucket, truncate and day/hour fields | Only partitions matched by the current spec are replaced; tags/branches and old specs stay readable |
| W13 | Verify the interaction of delete files with overwrite and evolution | R02, R05, R15 | Correctness, compatibility | Iceberg v2 MOR | DELETE/UPDATE/MERGE producing delete files, then overwrites on old and new specs | Replacement rows are not hidden by old delete files; historical tags are unaffected |
| W14 | Verify atomicity of failed overwrites on main/branch and of retries | R09, R10, R15 | Failure, atomicity | Multi-BE Doris | Check data, files and snapshots after a distributed strict cast failure, then retry after the fix | The failure commits nothing; the retry produces exactly one snapshot with no duplicates |
| W15 | Verify the real physical partition values of STRING transforms | R03, R18 | Boundaries, correctness | Iceberg v2 | Empty strings, ASCII, Chinese text, emoji, combining characters and NULL buckets, then replacing the bucket/truncate width | Row results and the physical values in `$partitions` both agree with Spark |
| W16 | Verify CTAS, complex types, formats and failure cleanup | R08, R09, R16 | Function, failure, compatibility | Internal multi-bucket source table | CTAS to a partitioned ORC table; strict cast failure; writing to an Avro table | ORC agrees with Spark; a failure leaves no table or snapshot; Avro is explicitly rejected |
| W17 | Verify sort order, distribution mode and multi-file flush | R03, R11, R17 | Correctness, stability | Multi-BE Doris | NULL sort keys, multi-column ascending/descending order, none/hash/range, low target file size | The plan contains the declared sort; the total row count across files is correct; the three distribution modes agree |
| W18 | Verify commit invariants of concurrent MERGE and append | R11, R13, R17 | Concurrency, atomicity | Multi-BE Doris | A readiness barrier makes two independent sessions ready to dispatch at the same time; updates of the same row and non-conflicting appends | Commits on the same row are serializable with cardinality one; only Iceberg validation/commit conflicts are accepted; non-conflicting writes are neither lost nor duplicated |
| W19 | Verify type safety of the MERGE source projection entering the truncate transform | R12, R18 | Regular regression, stability | Iceberg v2 MOR | Matched updates and unmatched inserts on a required STRING truncate column | MERGE accepts non-NULL values in the nullable physical projection and produces correct physical partitions |

## P0 coverage check

Each of R01-R18 maps to at least one P0 regression. After the last successful write, all fifteen positive suites cross-check the logical result of the same table between Spark and Doris; transforms, rollover, CTAS properties and failure atomicity additionally have physical metadata or state oracles. W08, W10 and W19 run as regular regressions; W07, which may still write unreadable required-null files, keeps its isolation switch so that it does not damage a shared cluster.

This matrix has no uncovered items. COW row-level DML, branch row-level DML, tag writes and Avro writes are explicit current capability boundaries, each pinned by an expected-rejection case that fixes the error semantics and failure atomicity; only the known required-null defect keeps using an isolated negative regression.
