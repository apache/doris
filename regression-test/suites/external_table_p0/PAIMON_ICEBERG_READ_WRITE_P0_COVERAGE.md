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

# Paimon / Iceberg Read and Write P0 Coverage

## Scope

This matrix compares the supported Doris surface with the format capabilities documented by
[Apache Paimon](https://paimon.apache.org/docs/1.0/), its
[ecosystem matrix](https://paimon.apache.org/docs/master/ecosystem/), and Iceberg's
[write](https://iceberg.apache.org/docs/latest/spark-writes/) and
[evolution](https://iceberg.apache.org/docs/latest/evolution/) documentation. A format feature is a Doris P0 contract
only when Doris supports it. Unsupported write or format boundaries require deterministic negative
coverage and no metadata or data mutation.

Doris currently documents Paimon data access as read-only. Master also supports Paimon catalog,
database and table metadata creation/deletion, but it has no Paimon data sink. Iceberg supports read,
DDL, INSERT, INSERT OVERWRITE, CTAS and, for compatible V2/V3 tables, DELETE, UPDATE and MERGE INTO.

## Audit result

The master inventory before this change contained 41 Paimon suites and 145 Iceberg suites, including
35 Iceberg write suites and four dedicated Iceberg DML suites. Iceberg P0 is complete for the Doris-
supported read/write surface: its existing matrices cover file-format versions, delete encodings,
evolution, time travel, DDL, distributed writes, row-level DML, failure atomicity and unsupported
boundaries.

Paimon P0 was incomplete. No suite selected a `merge-engine`, and write rejection was not checked
across all DML shapes with pre/post data and snapshot invariants. PM01-PM03 below close those gaps.
PM04 now runs as an active negative regression: failed Paimon CTAS is rejected without leaving target
metadata, while `IF NOT EXISTS` remains a no-op for an existing table. Streaming writes, overwrite,
delete/update and merge listed as unsupported by the
Paimon ecosystem matrix are boundary tests rather than positive Doris P0 contracts.

## Risks

| ID | Risk | Source | Impact | Priority |
| --- | --- | --- | --- | --- |
| R01 | Append and primary-key tables are planned with the wrong merge semantics | Black box: Paimon table models | Silent wrong results | P0 |
| R02 | Deduplicate, partial-update, aggregation and first-row produce the same result for duplicate keys | White box: LSM sorted-run merge | Silent wrong results | P0 |
| R03 | Fixed and dynamic bucket tables expose duplicate or stale cross-partition keys | Paimon data distribution | Silent wrong results | P0 |
| R04 | Automatic and forced-JNI routing disagree on Paimon merged rows, or native raw-file reads disagree where conversion is supported | Doris split routing | Query correctness | P0 |
| R05 | A rejected Paimon write creates a snapshot, changes data or leaves a CTAS table | Doris read-only boundary | Data or metadata mutation | P0 |
| R06 | Iceberg schema or partition evolution binds old field/spec IDs during current or historical reads | Iceberg evolution | Silent wrong results | P0 |
| R07 | Position/equality deletes or V3 deletion vectors are applied to the wrong file or snapshot | Iceberg row-level deletes | Deleted data visible or live data lost | P0 |
| R08 | Iceberg writes lose rows, route them to the wrong transform, or publish partial failed commits | Doris distributed Iceberg sink | Data loss or corruption | P0 |
| R09 | Snapshot, tag or branch reads/writes leak schema or data across references | Both formats | Historical data corruption | P0 |
| R10 | Unsupported format, DML mode or CTAS mutates state before rejection | Capability boundary | Partial commits or orphan metadata | P0 |

## Feature matrix

| Format | Capability | P0 status | Main suites |
| --- | --- | --- | --- |
| Paimon | Append table, partitioned table, primitive and nested types | Covered | `test_paimon_catalog`, `test_paimon_partition_table`, `test_paimon_full_schema_change` |
| Paimon | Primary-key deduplicate, partial-update, aggregation and first-row | Covered | `test_paimon_merge_engine_matrix` |
| Paimon | Fixed bucket, dynamic bucket and cross-partition update | Covered | `test_paimon_merge_engine_matrix`, `test_paimon_partition_pk_delete_refs` |
| Paimon | Parquet/ORC and mixed-format reads; JNI/native parity | Covered | `test_paimon_merge_engine_matrix`, `paimon_tb_mix_format`, `test_paimon_cpp_reader` |
| Paimon | Snapshot/timestamp/tag/branch and incremental modes | Covered | `paimon_time_travel`, `paimon_incr_read`, `test_paimon_schema_time_travel_matrix` |
| Paimon | Schema evolution, partition-key restrictions and historical schema binding | Covered | `test_paimon_schema_time_travel_matrix`, `test_paimon_partition_mutation_atomicity` |
| Paimon | Deletion vectors, upsert/delete visibility and data/system tables | Covered | `test_paimon_deletion_vector`, `paimon_data_system_table`, `paimon_system_table` |
| Paimon | Catalog/database/table create and drop | Covered | `test_create_paimon_table` |
| Paimon | Doris data write-back | Negative boundary covered | `test_paimon_write_boundary` |
| Paimon | Failed CTAS metadata atomicity | Active negative regression | `test_paimon_ctas_atomicity_negative` |
| Iceberg | V1/V2/V3, Parquet/ORC, position/equality deletes and deletion vectors | Covered | `test_iceberg_position_delete`, `test_iceberg_equality_delete`, `test_iceberg_deletion_vector` |
| Iceberg | Schema, partition and sort-order evolution | Covered | `test_iceberg_schema_time_travel_matrix`, `test_iceberg_partition_evolution_format_scanner`, `iceberg_schema_change_ddl` |
| Iceberg | Snapshot/timestamp/tag/branch reads and reference actions | Covered | `test_iceberg_time_travel`, `iceberg_query_tag_branch`, `test_iceberg_schema_ref_actions_matrix` |
| Iceberg | INSERT, INSERT OVERWRITE, static/hybrid partition and CTAS | Covered | `write/test_iceberg_write_insert`, `write/test_iceberg_write_overwrite_evolution`, `write/test_iceberg_write_ctas_format_boundary` |
| Iceberg | DELETE, UPDATE and MERGE in supported MOR modes | Covered | `dml/test_iceberg_update_delete_advanced`, `dml/test_iceberg_merge_into_advanced`, `write/test_iceberg_write_dml_modes_evolution` |
| Iceberg | Distributed/concurrent commits, failure atomicity and Spark interoperability | Covered | `write/test_iceberg_write_concurrent_merge_invariants`, `write/test_iceberg_write_overwrite_atomicity` |
| Iceberg | System tables, views, caches and catalog variants | Covered | `test_iceberg_sys_table`, `test_iceberg_view_query_p0`, `test_iceberg_table_cache`, catalog-specific suites |
| Iceberg | COW row DML, tag writes and unsupported file writes | Negative boundary covered | `write/test_iceberg_write_dml_modes_evolution`, `write/test_iceberg_write_branch_dml_boundary`, `write/test_iceberg_write_ctas_format_boundary` |

Detailed schema/time-travel and Iceberg write combinations are maintained in
`iceberg_paimon_schema_time_travel_coverage.md` and `iceberg/write/ICEBERG_WRITE_P0_COVERAGE.md`.

## Added test design

| Case | Goal | Risks | Dimension | Preconditions | Load | Expected |
| --- | --- | --- | --- | --- | --- | --- |
| PM01 | Distinguish all four primary-key merge engines | R01, R02, R04 | Functional, correctness, compatibility | Paimon Parquet/ORC tables | Duplicate keys across several commits | Each engine returns its documented merged row under automatic and forced-JNI routing |
| PM02 | Validate dynamic-bucket cross-partition deduplication | R03, R04 | Correctness | Primary key excludes partition key, bucket=-1 | Move one key between partitions | Exactly one current row remains in the new partition |
| PM03 | Preserve the Paimon read-only boundary | R05, R10 | Negative, atomicity | Existing Paimon PK table | VALUES, SELECT, OVERWRITE, UPDATE, DELETE, MERGE | Every statement fails before a snapshot or data change |
| PM04 | Reject or roll back Paimon CTAS atomically | R05, R10 | Negative, atomicity | Missing and existing Paimon target tables | CREATE TABLE AS SELECT, including IF NOT EXISTS | Unsupported CTAS leaves no target; an existing target makes IF NOT EXISTS a successful no-op |

Every P0 risk maps to at least one deterministic positive, boundary, or isolated known-bug
regression. Catalog authentication and cloud storage permutations stay in their existing connector
suites because they do not add format semantics to this matrix.
