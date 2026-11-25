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

# Build the MaxCompute fixtures

Follow the [shared workflow](../README.md) first. These are MaxCompute SQL seeds;
execute them through MaxCompute, then verify with Doris. They cover the 18
MaxCompute P2 suites, including writes that construct their own runtime tables.

## Allocate four project roles

Project names are globally unique. Create new names and map these roles through a
private regression config:

| Role | Config key | Existing suite default | Preloaded data |
| --- | --- | --- | --- |
| base | `mcDefaultProject` | `mc_datalake` | 21 tables |
| cross | `mcCrossProject` | `other_mc_datalake_test` | 2 tables |
| schema | `mcSchemaProject` | `mc_datalake_schema` | 5 tables in three schemas |
| write | `mcWriteProject` | `mc_doris_test_write` | No seed rows; the write suites create/drop their own objects |

Create the projects in the chosen region with data type system 2.0 and timezone
`Asia/Shanghai`, as used by the current fixtures. Enable project-level Schema
support for **schema** and **write**. Read back project properties; changing a
tenant-wide Schema setting is not part of this recipe.

Project creation requires an available compute quota. Select its actual name
from the target account; the tested pay-as-you-go compute quota was
`os_PayAsYouGoQuota`. This is separate from the Storage API resource
`pay-as-you-go`. Enable Storage API/open storage for the tenant. Besides project
permissions, the test identity needs tenant-role `odps:List` and `odps:Usage` on
`acs:odps:*:regions/<region>/quotas/pay-as-you-go`; creating the role and binding it
to the test user are separate operations. Verify real Storage API read and
write/commit/readback, not just SQL job execution.

Set `enableMaxComputeTest=true`, `ak`, `sk`, the four project keys, and `mcEndpoint`
for an endpoint reachable from Doris. Keep credentials outside source. A writer
running outside the VPC can need a different endpoint from the Doris catalog.

## Extract the current seed SQL

The source of truth is the SQL comment block in each suite. Avoid maintaining a
second 900-line copy that can drift when assertions change. The offline helper
extracts these blocks into three project-role files and a hash manifest:

```bash
python3 -m venv /tmp/p2-mc-seeds-venv
/tmp/p2-mc-seeds-venv/bin/pip install sqlparse==0.6.1
/tmp/p2-mc-seeds-venv/bin/python \
  regression-test/script/external_table_p2/maxcompute/extract_seeds.py \
  --output /tmp/p2-mc-seeds
```

The output directory must not exist. The helper does not connect to a service or
load credentials. It removes reset `DROP TABLE` statements, original project
`USE` statements and example read queries, preserves schema switches and the
explicit `MERGE SMALLFILES`, and emits `base.sql`, `cross.sql`, `schema.sql` and
`manifest.json`. It asserts the 21/2/5 table contract and rejects unsupported seed
statements. Run Python without `-O` so the source-contract assertions execute.

| Source suite | Target role / fixture family |
| --- | --- |
| `test_external_catalog_maxcompute` | base primitives/partitions and cross-project tables |
| `test_max_compute_all_type` | base all types |
| `test_max_compute_complex_type` | base arrays/maps/structs |
| `test_max_compute_partition_prune` | base partition layouts |
| `test_max_compute_timestamp` | base datetime/timestamp boundaries |
| `test_max_compute_schema` | default/analytics/iot schema tables |

`other_db_mc_tb` and `other_db_mc_parts` belong to **cross**; `mc_parts2` belongs to
**base**, even though its source block appears after the cross-project examples.
Keep the manifest's source and generated-file hashes with the migration record.

## Execute in the correct project and schema context

Select the target project from its role before loading each file. Submit seed SQL
directly; converting timestamp literals through Python `datetime` loses their
nanoseconds. Type boundaries include years 0001–9999, NULLs and nested types.

With the tested PyODPS 0.13.3 client, submit statements separately with `run_sql`,
record the returned instance ID immediately, and `wait_for_success()` before
submitting the next statement. Preserve session context across API jobs:

1. Parse `SET` statements into the SQL hints map; start with
   `odps.sql.type.system.odps2=true` and `odps.sql.allow.fullscan=true`.
2. For `USE schema <name>`, update `default_schema`; start with `default` in the
   schema project and no explicit schema in base/cross.
3. Submit CREATE/INSERT/ALTER with those hints and `default_schema`. Store the
   SQL hash, project role, schema, instance ID and completion status in a journal.
4. On interruption, inspect the saved instance and resume it; do not append the
   same INSERT a second time. Existing-table errors require inspection, not DROP.

A CLI with persistent session settings can also execute each generated file in
its mapped project. Preserve the same timezone and schema context. Keep the write
project dedicated and serialize builds that reuse its runtime table names.

## Verify native data and Doris behavior

Expected preloaded row counts:

| Role | Schema.table | Rows |
| --- | --- | ---: |
| `base` | `default.multi_partitions` | 10 |
| `base` | `default.mc_parts` | 5 |
| `base` | `default.int_types` | 16 |
| `base` | `default.web_site` | 5 |
| `base` | `default.mc_test_null` | 6 |
| `base` | `default.mc_parts2` | 4 |
| `cross` | `default.other_db_mc_tb` | 3 |
| `cross` | `default.other_db_mc_parts` | 5 |
| `base` | `default.mc_all_types` | 4 |
| `base` | `default.array_table` | 3 |
| `base` | `default.map_table` | 3 |
| `base` | `default.struct_table` | 3 |
| `base` | `default.nested_complex_table` | 3 |
| `base` | `default.one_partition_tb` | 5 |
| `base` | `default.two_partition_tb` | 10 |
| `base` | `default.three_partition_tb` | 15 |
| `base` | `default.no_partition_tb` | 5 |
| `base` | `default.datetime_tb1` | 1 |
| `base` | `default.timestamp_tb1` | 1 |
| `base` | `default.timestamp_tb2` | 1 |
| `base` | `default.datetime_tb2` | 11 |
| `base` | `default.timestamp_tb3` | 11 |
| `base` | `default.timestamp_tb4` | 11 |
| `schema` | `default.user_info` | 4 |
| `schema` | `default.order_detail` | 7 |
| `schema` | `analytics.product_sales` | 5 |
| `schema` | `analytics.web_log` | 8 |
| `schema` | `iot.employee_salary` | 4 |

Also inspect each partition definition, numeric/NULL boundary and complex type.
`timestamp_tb1` contains both `timestamp` and `timestamp_ntz` with
`.123456789`; check all nine fractional digits and keep both types distinct.
Use a native reader that retains nanoseconds, or verify their formatted SQL
values, before the Doris golden comparisons.

Run the read/schema/DDL/MTMV suites first, then the eight suites under `write`.
The latter generate up to two million rows and require Storage API commit access.
A zero-row/missing-table exception is not a reason to regenerate golden output.
Use the exact current suite list in the source and inspect actual queries when
an enable guard can return early:

```bash
./run-regression-test.sh --run --conf /path/to/private-regression-conf.groovy \
  -g p2 -d external_table_p2/maxcompute -parallel 1
```
