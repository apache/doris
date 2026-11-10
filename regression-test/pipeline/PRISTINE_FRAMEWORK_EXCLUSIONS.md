<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Temporary pristine-framework CI exclusions

`DORIS_CI_PRISTINE_FRAMEWORK=true` opts the P0, Cloud P0 and External profiles into
temporary environment compatibility exclusions. Other environments keep their
existing selection. These exclusions do not establish product defects and do not
change case bodies, assertions, credentials or expected results.

`excludeSuites` still accepts the existing short names and wildcards. To exclude
one source's suite without excluding another suite with the same name, use
`relative/path.groovy#suite_name`. The path is relative to the configured suite
root, uses `/`, and is independent of the checkout location. Wildcards are
supported on the qualified identifier. For example,
`query_p0/outfile/test_outfile.groovy#test_outfile` preserves
`export_p0/test_outfile.groovy#test_outfile`.

The qualified form requires a framework built from the change adding this syntax.
An older precompiled framework does not interpret it as a path-qualified
exclusion. Build the reviewed source normally; do not patch or recompile the
framework inside downstream regression pipelines.

## Environment cases temporarily excluded

The following 14 suites were identified in a branch-4.2 validation. Paths below
are their current master locations. The former `nereids_p0` files moved to
`query_p0`; the two independent-provider credential cases moved to P2. Every
listed file declares the suite named by its basename. New entries use the full
`path#suite` key, including the three outfile names shared with passing export
suites. No directory-wide exclusion is added.

| Current source relative to `regression-test/suites` | Reason | Newly excluded profiles |
| --- | --- | --- |
| `datatype_p0/nested_types/query/test_nested_types_insert_into_with_s3.groovy` | Shared parent database prerequisite in targeted runs | P0, Cloud P0 |
| `datatype_p0/nested_types/query/test_nestedtypes_csv_insert_into_with_s3.groovy` | Shared parent database prerequisite in targeted runs | P0, Cloud P0 |
| `datatype_p0/nested_types/query/test_nestedtypes_json_insert_into_with_s3.groovy` | Shared parent database prerequisite in targeted runs | P0, Cloud P0 |
| `export_p2/outfile/s3/test_outfile_with_different_s3.groovy` | Independent Tencent COS / Huawei OBS credentials | None: already outside P0 selection via explicit P2 group |
| `export_p0/test_export_view.groovy` | Root SSH / remote filesystem prerequisite | P0, Cloud P0 |
| `mysqldump_p0/test_mysqldump.groovy` | Client column-statistics compatibility | P0, Cloud P0 |
| `query_p0/test_install_plugin_command.groovy` | Root SCP / plugin deployment prerequisite | P0, Cloud P0 |
| `external_table_p0/kerberos/test_single_hive_kerberos.groovy` | `sudo docker` fixture prerequisite | P0, External |
| `external_table_p0/kerberos/test_two_hive_kerberos.groovy` | `sudo docker` fixture prerequisite | P0, External |
| `external_table_p0/tvf/test_parquet_meta_tvf.groovy` | Root SCP prerequisite | P0, External |
| `external_table_p2/tvf/test_file_tvf_s3.groovy` | Independent Tencent COS / Huawei OBS credentials | None: already outside P0 selection via explicit P2 group |
| `query_p0/outfile/test_outfile_parquet.groovy` | Root SSH / SCP prerequisite | P0, Cloud P0 |
| `query_p0/outfile/test_outfile_expr.groovy` | Root SSH / SCP prerequisite | P0, Cloud P0 |
| `query_p0/outfile/test_outfile.groovy` | Root SSH / SCP prerequisite | P0, Cloud P0 |

This adds 12 P0, 9 Cloud P0 and 3 External exclusions. Cloud P0 already excludes
`external_table_p0`. None of these suites belongs to the nonConcurrent group or
the Vault profile's `vault_p0` directory, so those profiles are unchanged.
The two P2 cases receive no redundant P0 exclusion.

Re-enable individual entries after their prerequisite or case portability issue
is addressed, reviewed and validated in the intended environment. Root-requiring
cases should be made compatible with the CI execution model through reviewed
source changes; granting broader runtime privilege is not a substitute. Tests
requiring other providers remain disabled until the appropriate independent
credentials are available. The temporary configuration must not be interpreted
as evidence that these cases passed.
